# lease-lock-coordinator-go

可本地运行的分布式锁协调服务，提供语义明确的**租约（lease）**、**隔离令牌（fencing token）**与**会话故障恢复**，全部行为可通过单测在不依赖真实等待、真实网络的前提下复现验证。

## 结构

- `coordinator/` — 协调器核心：租约、隔离令牌、FIFO 等待队列、幂等请求日志、快照持久化与重启恢复
- `clock/` — 时钟抽象：真实时钟（单调）与测试用手动时钟
- `cmd/lockd/` — 本地可运行演示程序

## 租约模型

- 同一把锁任意时刻最多被一个会话持有；获取时必须指定 TTL。
- 持有期间可通过 `Renew` 续租（携带当前令牌），到期未续租即自动失去锁。
- 过期是**惰性判定 + 后台回收**：任何访问锁的操作都会先判定过期（`gcLocked`），真实时钟部署可额外启动 `StartReaper` 缩短唤醒时延。
- 对已过期锁的续租/释放返回 `ErrLeaseExpired`；从未持有返回 `ErrNotHolder`；锁被他人持有且未选择等待返回 `ErrLockHeld` —— 失败原因可区分。

## 隔离令牌语义

- 每次成功授予产生**全局严格递增**的令牌（`fencing` 计数器，随快照持久化，重启不回退）。
- `Renew` / `Release` / `ValidateToken` 携带的令牌与当前租约不一致时返回 `ErrStaleToken`。
- 旧持有者的迟到请求（令牌已失效）被明确拒绝，服务端状态不会因之改变，新持有者的独占性不受破坏。
- 下游受护资源在写操作前应调用 `ValidateToken(lock, token)` 校验。

## 幂等与等待队列

- 所有变更操作以 `requestID` 去重：重复提交返回首次记录的结果，不会重复授予、重复入队或重复唤醒。
- 等待者按 FIFO 唤醒；所有状态变更在单一互斥锁下全序化，同一把锁的事件顺序不会错乱。通知通道缓冲为 1，重复通知安全。
- `FailSession` 模拟会话故障：释放其全部锁并按序唤醒等待者，幂等。

## 恢复规则

- 每次状态变更后将完整快照（锁、令牌计数器、会话、请求日志、等待队列、时钟高水位）原子写入 JSON 文件（tmp + rename）。
- 重启时从快照恢复全部状态；停机期间过期的租约在恢复时立即失效并唤醒等待者。
- **时钟回拨防护**：协调器维护单调递增的时间高水位（`highWater`），`effectiveNow = max(now, highWater)` 且随快照持久化，因此墙钟回拨（无论进程内还是跨重启）都不会让租约被无限延长。真实时钟使用 Go 单调时钟读数，进程内天然免疫墙钟回拨。

## 验证方法

```bash
go test -v ./...          # 全部单测，日志可见每个业务的授予/过期/唤醒/恢复事件
CGO_ENABLED=1 go test -race ./...   # 并发安全
go run ./cmd/lockd        # 本地演示；再运行一次可观察重启恢复
```

测试覆盖（`coordinator/coordinator_test.go`，全部使用手动时钟，无真实等待）：

| 用例 | 覆盖业务 |
| --- | --- |
| `TestBasicAcquireRenewRelease` | 获取/续租/释放生命周期、令牌从 1 递增 |
| `TestConcurrentContention` | 32 个 goroutine 争抢，互斥性与令牌唯一性 |
| `TestLeaseExpiry` | 到期失锁、过期后续租/释放返回 `ErrLeaseExpired` |
| `TestFencingToken` | 旧持有者迟到请求被拒、独占性不被破坏 |
| `TestIdempotentRequests` | 重复获取/续租/释放/等待的幂等重放 |
| `TestWaitQueueFIFO` | 等待者按先来后到唤醒，顺序不错乱 |
| `TestSessionFailover` | 会话故障释放锁并唤醒等待者 |
| `TestRestartRecovery` | 重启后租约/令牌/会话/等待队列一致恢复 |
| `TestClockRollback` | 时钟回拨不延长租约 |
