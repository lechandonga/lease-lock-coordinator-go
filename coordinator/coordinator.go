// Package coordinator 实现可本地运行的分布式锁协调服务。
//
// 核心语义：
//   - 租约：同一把锁任意时刻最多一个持有者，租约到期即失去锁。
//   - 隔离令牌（fencing token）：每次授予产生严格递增令牌，
//     携带失效令牌的续租/释放/受护操作被明确拒绝。
//   - 幂等：以 requestID 去重，重复提交返回首次结果，
//     不会重复授予或重复唤醒。
//   - 等待队列：等待者按 FIFO 唤醒，同一把锁的事件顺序在单互斥锁下
//     全序化，通知允许重复但顺序不错乱。
//   - 恢复：所有状态快照持久化，重启后一致恢复；
//     时钟高水位线保证时钟回拨不会延长租约。
package coordinator

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/clock"
)

// 可区分的失败原因。
var (
	// ErrLockHeld 锁被其他会话持有且调用方未选择等待。
	ErrLockHeld = errors.New("lock held by another session")
	// ErrLeaseExpired 目标租约已过期（原持有者已失去锁）。
	ErrLeaseExpired = errors.New("lease expired")
	// ErrNotHolder 会话从未持有该锁。
	ErrNotHolder = errors.New("session does not hold the lock")
	// ErrStaleToken 携带的隔离令牌已失效（旧持有者的迟到请求）。
	ErrStaleToken = errors.New("stale fencing token")
	// ErrUnknownSession 会话未注册或已被故障移除。
	ErrUnknownSession = errors.New("unknown session")
)

// Grant 是一次成功授予的结果。
type Grant struct {
	Lock      string
	Session   string
	Token     uint64
	ExpiresAt time.Time
}

// AcquireResult 是 Acquire 的返回值。Granted 为 true 时锁已到手；
// 否则 WaitCh 会在等待者被唤醒时收到一次 Grant（缓冲为 1，重复通知安全）。
type AcquireResult struct {
	Granted bool
	Grant   Grant
	WaitCh  <-chan Grant
}

type requestRecord struct {
	Kind    string // "acquire" | "renew" | "release"
	Lock    string
	Session string
	Done    bool
	Grant   *Grant  // acquire 成功/被唤醒后的结果
	Err     string  // 已完成请求的失败原因（对应导出的 sentinel）
	TTL     time.Duration // acquire 请求的租约时长，唤醒授予时使用
	waitCh  chan Grant
}

type waiter struct {
	SessionID string
	RequestID string
}

type lockState struct {
	Name      string
	Holder    string // 持有会话，空表示空闲
	Token     uint64 // 当前租约的隔离令牌
	ExpiresAt time.Time
	LastHolder string // 最近一个持有者，用于区分 ErrLeaseExpired 与 ErrNotHolder
	Waiters   []waiter
}

// Config 配置协调器。
type Config struct {
	Clock  clock.Clock  // 必填，测试传手动时钟
	Store  *Store       // 可选，nil 表示纯内存
	Logger *log.Logger  // 可选
}

// Coordinator 是锁协调服务本体，所有方法可并发调用。
type Coordinator struct {
	mu   sync.Mutex
	clk  clock.Clock
	st   *Store
	logf func(string, ...any)

	locks    map[string]*lockState
	sessions map[string]bool
	requests map[string]*requestRecord
	fencing  uint64    // 全局严格递增的令牌计数器
	highWater time.Time // 时钟高水位：effectiveNow 永不倒退
}

func New(cfg Config) (*Coordinator, error) {
	if cfg.Clock == nil {
		return nil, errors.New("coordinator: Clock is required")
	}
	c := &Coordinator{
		clk:      cfg.Clock,
		st:       cfg.Store,
		locks:    map[string]*lockState{},
		sessions: map[string]bool{},
		requests: map[string]*requestRecord{},
	}
	c.logf = func(string, ...any) {}
	if cfg.Logger != nil {
		c.logf = cfg.Logger.Printf
	}
	if c.st != nil {
		snap, err := c.st.Load()
		if err != nil {
			return nil, fmt.Errorf("coordinator: load snapshot: %w", err)
		}
		if snap != nil {
			c.restore(snap)
		}
	}
	// 重启恢复：以高水位校正时间，惰性过期所有租约并唤醒等待者。
	for _, l := range c.locks {
		c.gcLocked(l)
	}
	c.logf("coordinator ready: locks=%d sessions=%d requests=%d fencing=%d",
		len(c.locks), len(c.sessions), len(c.requests), c.fencing)
	return c, nil
}

// effectiveNow 返回永不倒退的当前时间：时钟回拨不会让租约被延长。
func (c *Coordinator) effectiveNow() time.Time {
	now := c.clk.Now()
	if now.Before(c.highWater) {
		return c.highWater
	}
	c.highWater = now
	return now
}

// RegisterSession 注册会话；重复注册幂等。
func (c *Coordinator) RegisterSession(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions[id] = true
	c.persistLocked()
	c.logf("session registered: %s", id)
}

// FailSession 模拟会话故障：移除会话并释放其持有的所有锁，
// 按 FIFO 唤醒等待者。幂等。
func (c *Coordinator) FailSession(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sessions[id] {
		return
	}
	delete(c.sessions, id)
	for _, l := range c.locks {
		if l.Holder == id {
			c.logf("session failed: %s, releasing lock %s (token=%d)", id, l.Name, l.Token)
			c.releaseLocked(l)
		}
	}
	c.persistLocked()
}

// Acquire 获取锁。wait 为 true 且锁被占用时进入 FIFO 等待队列。
// 相同 requestID 重复提交返回首次结果（幂等），不会重复入队或重复授予。
func (c *Coordinator) Acquire(sessionID, lockName, requestID string, ttl time.Duration, wait bool) (AcquireResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sessions[sessionID] {
		return AcquireResult{}, ErrUnknownSession
	}
	l := c.lockOrCreate(lockName)
	c.gcLocked(l)
	if rec, ok := c.requests[requestID]; ok {
		if rec.Done {
			return AcquireResult{Granted: rec.Grant != nil, Grant: deref(rec.Grant)}, recErr(rec)
		}
		return AcquireResult{WaitCh: rec.waitCh}, nil // 等待中的重复提交：返回同一通道
	}
	rec := &requestRecord{Kind: "acquire", Lock: lockName, Session: sessionID, TTL: ttl}
	c.requests[requestID] = rec

	if l.Holder == "" {
		g := c.grantLocked(l, sessionID, ttl)
		rec.Done, rec.Grant = true, &g
		c.persistLocked()
		return AcquireResult{Granted: true, Grant: g}, nil
	}
	if !wait {
		rec.Done, rec.Err = true, ErrLockHeld.Error()
		c.persistLocked()
		return AcquireResult{}, ErrLockHeld
	}
	rec.waitCh = make(chan Grant, 1)
	l.Waiters = append(l.Waiters, waiter{SessionID: sessionID, RequestID: requestID})
	c.persistLocked()
	c.logf("waiter enqueued: lock=%s session=%s pos=%d", lockName, sessionID, len(l.Waiters))
	return AcquireResult{WaitCh: rec.waitCh}, nil
}

// Renew 续租。令牌不匹配返回 ErrStaleToken；租约已过期返回 ErrLeaseExpired。
// 相同 requestID 重复提交幂等返回首次结果。
func (c *Coordinator) Renew(sessionID, lockName, requestID string, token uint64, ttl time.Duration) (Grant, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sessions[sessionID] {
		return Grant{}, ErrUnknownSession
	}
	if rec, ok := c.requests[requestID]; ok {
		return deref(rec.Grant), recErr(rec)
	}
	rec := &requestRecord{Kind: "renew", Lock: lockName, Session: sessionID, Done: true}
	c.requests[requestID] = rec

	l := c.lockOrCreate(lockName)
	c.gcLocked(l)
	g, err := c.renewLocked(l, sessionID, token, ttl)
	if err != nil {
		rec.Err = err.Error()
		c.persistLocked()
		return Grant{}, err
	}
	rec.Grant = &g
	c.persistLocked()
	return g, nil
}

func (c *Coordinator) renewLocked(l *lockState, sessionID string, token uint64, ttl time.Duration) (Grant, error) {
	if l.Holder != sessionID {
		if l.LastHolder == sessionID {
			return Grant{}, ErrLeaseExpired
		}
		return Grant{}, ErrNotHolder
	}
	if token != l.Token {
		return Grant{}, ErrStaleToken
	}
	l.ExpiresAt = c.effectiveNow().Add(ttl)
	g := Grant{Lock: l.Name, Session: sessionID, Token: l.Token, ExpiresAt: l.ExpiresAt}
	c.logf("renewed: lock=%s session=%s token=%d expires=%s", l.Name, sessionID, l.Token, l.ExpiresAt.Format(time.RFC3339Nano))
	return g, nil
}

// Release 释放锁并唤醒下一个等待者。幂等；过期锁的释放返回 ErrLeaseExpired。
func (c *Coordinator) Release(sessionID, lockName, requestID string, token uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.sessions[sessionID] {
		return ErrUnknownSession
	}
	if rec, ok := c.requests[requestID]; ok {
		return recErr(rec)
	}
	rec := &requestRecord{Kind: "release", Lock: lockName, Session: sessionID, Done: true}
	c.requests[requestID] = rec

	l := c.lockOrCreate(lockName)
	c.gcLocked(l)
	if l.Holder != sessionID {
		if l.LastHolder == sessionID {
			rec.Err = ErrLeaseExpired.Error()
		} else {
			rec.Err = ErrNotHolder.Error()
		}
		c.persistLocked()
		return recErr(rec)
	}
	if token != l.Token {
		rec.Err = ErrStaleToken.Error()
		c.persistLocked()
		return recErr(rec)
	}
	c.releaseLocked(l)
	c.persistLocked()
	return nil
}

// ValidateToken 供受护资源在写操作前校验隔离令牌。
func (c *Coordinator) ValidateToken(lockName string, token uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.lockOrCreate(lockName)
	c.gcLocked(l)
	if l.Holder == "" || token != l.Token {
		return ErrStaleToken
	}
	return nil
}

// Holder 返回当前持有者与其令牌，供测试与观测。
func (c *Coordinator) Holder(lockName string) (string, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.lockOrCreate(lockName)
	c.gcLocked(l)
	return l.Holder, l.Token
}

// --- 内部机制（调用时必须持有 c.mu） ---

func (c *Coordinator) lockOrCreate(name string) *lockState {
	l, ok := c.locks[name]
	if !ok {
		l = &lockState{Name: name}
		c.locks[name] = l
	}
	return l
}

func (c *Coordinator) grantLocked(l *lockState, sessionID string, ttl time.Duration) Grant {
	c.fencing++
	l.Holder = sessionID
	l.Token = c.fencing
	l.ExpiresAt = c.effectiveNow().Add(ttl)
	g := Grant{Lock: l.Name, Session: sessionID, Token: l.Token, ExpiresAt: l.ExpiresAt}
	c.logf("granted: lock=%s session=%s token=%d expires=%s", l.Name, sessionID, l.Token, l.ExpiresAt.Format(time.RFC3339Nano))
	return g
}

// releaseLocked 释放锁并按 FIFO 唤醒下一个等待者（通知幂等、顺序确定）。
func (c *Coordinator) releaseLocked(l *lockState) {
	c.logf("released: lock=%s session=%s token=%d", l.Name, l.Holder, l.Token)
	l.LastHolder = l.Holder
	l.Holder = ""
	for len(l.Waiters) > 0 {
		w := l.Waiters[0]
		l.Waiters = l.Waiters[1:]
		rec := c.requests[w.RequestID]
		if rec == nil || rec.Done || !c.sessions[w.SessionID] {
			continue // 请求已取消或会话已故障，跳过
		}
		g := c.grantLocked(l, w.SessionID, rec.TTL)
		rec.Done, rec.Grant = true, &g
		if rec.waitCh != nil {
			rec.waitCh <- g // 缓冲通道，重复唤醒安全
		}
		c.logf("waiter woken: lock=%s session=%s token=%d", l.Name, w.SessionID, g.Token)
		return
	}
}

// gcLocked 惰性过期：到期租约立即失效并触发等待者唤醒。
func (c *Coordinator) gcLocked(l *lockState) {
	if l.Holder != "" && !c.effectiveNow().Before(l.ExpiresAt) {
		c.logf("lease expired: lock=%s session=%s token=%d", l.Name, l.Holder, l.Token)
		c.releaseLocked(l)
	}
}

func deref(g *Grant) Grant {
	if g == nil {
		return Grant{}
	}
	return *g
}

func recErr(rec *requestRecord) error {
	if rec == nil || rec.Err == "" {
		return nil
	}
	switch rec.Err {
	case ErrLockHeld.Error():
		return ErrLockHeld
	case ErrLeaseExpired.Error():
		return ErrLeaseExpired
	case ErrNotHolder.Error():
		return ErrNotHolder
	case ErrStaleToken.Error():
		return ErrStaleToken
	}
	return errors.New(rec.Err)
}

// Reap 主动扫描并回收所有到期租约。惰性过期已保证正确性，
// Reap 只影响"到期后多久唤醒等待者"的时延。
func (c *Coordinator) Reap() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.locks {
		c.gcLocked(l)
	}
	c.persistLocked()
}

// StartReaper 以后台 goroutine 周期性回收到期租约（真实时钟部署时使用）。
// 返回停止函数。测试使用手动时钟时无需启动。
func (c *Coordinator) StartReaper(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				c.Reap()
			}
		}
	}()
	return func() { close(done) }
}
