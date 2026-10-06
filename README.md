# 租约锁协调服务（Lease-Lock Coordinator）

一个可在本地运行的分布式锁协调服务，提供：

- 多客户端的锁获取 / 续租 / 释放，互斥语义基于**带 TTL 的租约**；
- 每次成功获取产生**全服务严格递增的隔离令牌（fencing token）**，旧持有者的迟到请求被明确拒绝；
- 等待者 **FIFO** 唤醒，同把锁的事件流序号单调、允许重复投递但不会乱序；
- 所有状态变更先写 **WAL（fsync）再生效**，进程重启后租约、令牌、会话与幂等缓存一致恢复；
- 续租对**时钟回拨**做了防护，租约不会被无限延长。

## 目录结构

```
internal/clock        时钟抽象：Real（Unix 纳秒）与 Fake（测试用，可快进/回拨）
internal/wal          长度前缀 JSON 的追加式预写日志（fsync、撕裂尾部容忍）
internal/coordinator  核心语义：租约、令牌、等待队列、事件流、幂等缓存、WAL 回放
internal/httpapi      HTTP API（长轮询等待、稳定错误码映射）
cmd/llc               本地服务端与命令行客户端
```

## 本地运行

```bash
go build ./...

# 启动服务
go run ./cmd/llc serve --addr 127.0.0.1:8080 --wal ./data/llc.wal --sweep 100ms

# 另一个终端：客户端
llc session open alice
llc session open bob
llc acquire alice order-42 60000 req-1        # -> fencing_token=1
llc acquire alice order-42 60000 req-1        # 重复请求：replayed=true，同一令牌
llc acquire bob   order-42 60000              # 409 lock is held by another session
llc acquire bob   order-42 60000 "" 5000      # 长轮询等待 5s
llc release alice order-42 1                  # 释放后 bob 被唤醒，拿到 token=2
llc renew   bob   order-42 2 60000 hb-1       # 续租
llc peek    order-42                          # 查看状态
```

也可用 `curl`：

```bash
curl -XPOST localhost:8080/v1/sessions/alice
curl -XPOST -H 'X-Session-Id: alice' -H 'X-Request-Id: req-1' \
  -d '{"ttl_ms":60000}' localhost:8080/v1/locks/order-42
```

## HTTP API

| 方法 | 路径 | 含义 |
| --- | --- | --- |
| `POST` | `/v1/sessions/{sid}` | 打开会话（幂等） |
| `DELETE` | `/v1/sessions/{sid}` | 结束会话（释放其全部锁） |
| `POST` | `/v1/locks/{key}` | 获取；body 带 `wait_ms` 时长轮询等待 |
| `PATCH` | `/v1/locks/{key}` | 续租 |
| `DELETE` | `/v1/locks/{key}` | 释放 |
| `GET` | `/v1/locks/{key}` | 查询当前状态 |

请求头：`X-Session-Id`、`X-Fencing-Token`、`X-Request-Id`；请求体：`ttl_ms`、`token`、`request_id`、`wait_ms`。

稳定的错误码：

| 状态码 | 错误 | 场景 |
| --- | --- | --- |
| 409 | `lock is held by another session` / 等待超时 | 锁被活跃持有者占用 |
| 410 | `lease has expired` | 自己的租约已过期且锁尚空闲 |
| 404 | `... not the current lock owner` / `session not found` | 锁已归他人 / 会话不存在 |
| 412 | `stale fencing token` | 携带了旧持有者的令牌 |
| 400 | 参数错误 | 空 key、TTL 非正、令牌为 0 |

## 语义模型

### 租约模型

- 获取成功返回 `expires_at`（绝对 Unix 纳秒时间戳）。判断过期一律比较
  `now >= expires_at`，并在两处触发：访问该锁时惰性过期、后台 sweeper
  周期扫描（生产默认 100ms）。
- 续租把截止时间设为 `max(expires_at, now) + ttl`，且新截止时间必须严格
  大于已持久化的旧截止时间。即使主机时钟回拨（包括重启到更慢时钟的机器），
  租约也不会缩短，更不会通过反复续租被无限延长——到期判断依据持久化的绝对
  时间点。
- 租约到期时持有者**立即**失去锁；锁变空闲后按 FIFO 授予等待队首。

### 隔离令牌（fencing token）

- 令牌是服务级单调递增计数器，随每次成功获取 `+1`，并把计数后的值
  `NextToken` 一并写入 WAL。重启后从历史最大值继续，任何新令牌都严格大于
  重启前所有令牌。
- 下游资源应要求操作携带令牌并拒绝更小令牌的请求；协调服务端同样强制：
  续租 / 释放必须提供与当前持有者一致的令牌，否则返回
  `ErrStaleFencingToken`（412）。旧持有者在锁易主后的迟到请求返回
  `ErrNotOwner`（404），在“已过期但尚未被他人获取”窗口内返回
  `ErrLeaseExpired`（410）——三类失败原因可区分。

### 幂等

- 所有写操作接受 `request_id`（作用域为会话）。同一 `(session, request_id)`
  的重复提交返回首次结果（响应带 `replayed:true`），不产生新令牌、不重复
  释放、不重复唤醒。
- 幂等缓存随 WAL 重放重建：获取结果、续租结果、释放结果都会在重启后保留。
  重放的获取结果是**历史授予**（含历史到期点），调用方依据 `expires_at`
  判断其当前是否仍有效，系统绝不会因为重试而二次授予。

### 等待与事件

- `WaitAcquire` 按入队顺序排队；空缺只授予队首，队首出队发生在授予之前，
  因此同一空缺不可能授予两次。放弃等待（context 取消）的队首会被跳过。
- 每把锁有独立的有序事件流（`GRANTED`/`REVOKED`），序号严格递增。订阅支持
  指定 `afterSeq`：迟到订阅者只收更新的事件；通知通道满时投递是最佳努力，
  订阅者可凭最后处理的序号重新订阅，因此**允许重复投递、但顺序永不错乱**。
- 等待队列与会话在进程内的事件订阅是**临时性**的：重启后权威的归属 / 令牌
  / 租约 / 会话状态从 WAL 恢复，等待者需要重新排队（长轮询客户端按超时
  重试即可）。

### 恢复规则

- 启动时顺序重放 WAL：
  - `SessionOpened/Ended` 恢复会话集合（结束后的会话保持结束，其锁为空闲）；
  - `LockAcquired` 恢复归属与令牌；若重放时租约已过期，则恢复为“空闲 +
    最后持有者”历史，使迟到的续租 / 释放得到 `ErrLeaseExpired` 而不是
    `ErrNotOwner`；
  - `LockRenewed` 是截止时间的权威更新；
  - `LockReleased` 将锁置为空闲；
  - 服务级 `NextToken` 取所有记录中的最大值后继续递增；
  - 幂等缓存按记录重建。
- 每条状态变更都先 `Append + fsync` 成功才更新内存；追加失败不会留下
  “幽灵持有者”。崩溃时末尾撕裂的记录在回放时被安全忽略。

## 验证方法

测试使用假时钟与 `httptest`，**不依赖真实等待与真实网络端口**，所有过期 /
回拨 / 重启场景都可瞬间复现：

```bash
go test ./... -v
go test ./... -race
```

`-v` 日志按业务模块分组打印覆盖了哪些业务、用例名与结果：

- 互斥获取：首取成功、占用冲突
- 租约过期：到期失锁、他人可获取、令牌递增
- 续租：正常延期、过期续租 `ErrLeaseExpired`、易主后续租 `ErrNotOwner`
- 释放：正常释放、过期释放 410、释放他人锁 404
- 隔离令牌：严格递增、旧令牌续租/释放 412/404、新持有者不受迟到请求破坏
- 幂等请求：重复获取/续租/释放重放、并发重复提交仅一次授予
- 等待队列：FIFO 严格顺序、取消不影响后继、事件序号单调与迟到订阅
- 时钟回拨：回拨续租不缩短/不无限延长、慢主机重启仍按绝对时间过期
- 会话故障：结束会话释放全部锁并唤醒等待者
- 并发争抢：50 个并发会话恰好 1 个成功
- 重启恢复：会话/租约/令牌/幂等缓存恢复、重启时已过期租约的处理、会话
  结束状态持久化

WAL 层另有测试覆盖顺序回放与崩溃后撕裂尾部记录的容忍。
