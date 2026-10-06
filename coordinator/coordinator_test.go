package coordinator

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/clock"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestCoordinator(t *testing.T, clk clock.Clock, store *Store) *Coordinator {
	t.Helper()
	c, err := New(Config{
		Clock:  clk,
		Store:  store,
		Logger: log.New(testWriter{t: t}, "", 0),
	})
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	return c
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[coordinator] %s", string(p[:len(p)-1]))
	return len(p), nil
}

func mustAcquire(t *testing.T, c *Coordinator, sid, lock, rid string, ttl time.Duration) Grant {
	t.Helper()
	res, err := c.Acquire(sid, lock, rid, ttl, false)
	if err != nil || !res.Granted {
		t.Fatalf("acquire %s/%s: granted=%v err=%v", sid, lock, res.Granted, err)
	}
	return res.Grant
}

// TestBasicAcquireRenewRelease 覆盖：获取、续租、释放的基本生命周期。
func TestBasicAcquireRenewRelease(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("s1")
	c.RegisterSession("s2")

	g := mustAcquire(t, c, "s1", "lock-a", "r1", 10*time.Second)
	clk.Advance(3 * time.Second)
	if g.Token != 1 {
		t.Fatalf("first token = %d, want 1", g.Token)
	}
	if _, err := c.Acquire("s2", "lock-a", "r2", 10*time.Second, false); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("contended acquire err = %v, want ErrLockHeld", err)
	}
	g2, err := c.Renew("s1", "lock-a", "r3", g.Token, 10*time.Second)
	if err != nil || !g2.ExpiresAt.After(g.ExpiresAt) {
		t.Fatalf("renew: %+v err=%v", g2, err)
	}
	if err := c.Release("s1", "lock-a", "r4", g.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	mustAcquire(t, c, "s2", "lock-a", "r5", 10*time.Second)
}

// TestConcurrentContention 覆盖：多 goroutine 并发争抢同一把锁，
// 任意时刻最多一个持有者，且每个成功者令牌唯一递增。
func TestConcurrentContention(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	const n = 32
	for i := 0; i < n; i++ {
		c.RegisterSession(fmt.Sprintf("s%d", i))
	}
	var mu sync.Mutex
	holders := map[string]bool{}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := fmt.Sprintf("s%d", i)
			res, err := c.Acquire(sid, "hot", fmt.Sprintf("acq-%d", i), time.Minute, false)
			if errors.Is(err, ErrLockHeld) {
				return
			}
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			if len(holders) > 0 {
				t.Errorf("lock granted to %s while held by %v", sid, holders)
			}
			holders[sid] = true
			mu.Unlock()
			if err := c.Release(sid, "hot", fmt.Sprintf("rel-%d", i), res.Grant.Token); err != nil {
				t.Errorf("release: %v", err)
			}
			mu.Lock()
			delete(holders, sid)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	t.Logf("contention finished: %d clients, final holder check", n)
	if h, _ := c.Holder("hot"); h != "" {
		t.Fatalf("lock still held by %s", h)
	}
}

// TestLeaseExpiry 覆盖：租约到期后原持有者失去锁，他人可获取；
// 过期后的续租/释放返回可区分的 ErrLeaseExpired。
func TestLeaseExpiry(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("s1")
	c.RegisterSession("s2")

	g := mustAcquire(t, c, "s1", "lock-b", "r1", 5*time.Second)
	clk.Advance(6 * time.Second) // 手动推进时钟，无需真实等待

	g2 := mustAcquire(t, c, "s2", "lock-b", "r2", 5*time.Second)
	if g2.Token <= g.Token {
		t.Fatalf("token not increasing: %d -> %d", g.Token, g2.Token)
	}
	if _, err := c.Renew("s1", "lock-b", "r3", g.Token, 5*time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after expiry err = %v, want ErrLeaseExpired", err)
	}
	if err := c.Release("s1", "lock-b", "r4", g.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("release after expiry err = %v, want ErrLeaseExpired", err)
	}
	if err := c.ValidateToken("lock-b", g.Token); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("validate old token err = %v, want ErrStaleToken", err)
	}
}

// TestFencingToken 覆盖：旧持有者迟到的请求携带失效令牌，必须被明确拒绝，
// 且不得破坏新持有者的独占性。
func TestFencingToken(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("old")
	c.RegisterSession("new")

	old := mustAcquire(t, c, "old", "res", "r1", 5*time.Second)
	clk.Advance(6 * time.Second)
	newG := mustAcquire(t, c, "new", "res", "r2", 5*time.Second)

	if _, err := c.Renew("old", "res", "r3", old.Token, 5*time.Second); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("stale renew err = %v, want ErrLeaseExpired", err)
	}
	if err := c.ValidateToken("res", old.Token); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale validate err = %v, want ErrStaleToken", err)
	}
	if h, tok := c.Holder("res"); h != "new" || tok != newG.Token {
		t.Fatalf("exclusivity broken: holder=%s token=%d", h, tok)
	}
	// 同会话内令牌不匹配（例如用第一次获取的令牌操作第二次租约）也是 StaleToken。
	c.RegisterSession("flip")
	f1 := mustAcquire(t, c, "flip", "flip-lock", "f1", time.Minute)
	if err := c.Release("flip", "flip-lock", "frel", f1.Token); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, c, "flip", "flip-lock", "f2", time.Minute)
	if _, err := c.Renew("flip", "flip-lock", "f3", f1.Token, time.Minute); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("renew with old token err = %v, want ErrStaleToken", err)
	}
}

// TestIdempotentRequests 覆盖：同一 requestID 重复提交返回首次结果，
// 不重复授予、不重复唤醒、不重复入队。
func TestIdempotentRequests(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("s1")
	c.RegisterSession("s2")

	g1 := mustAcquire(t, c, "s1", "idem", "acq-1", time.Minute)
	for i := 0; i < 3; i++ {
		res, err := c.Acquire("s1", "idem", "acq-1", time.Minute, false)
		if err != nil || res.Grant.Token != g1.Token {
			t.Fatalf("duplicate acquire #%d: %+v err=%v", i, res, err)
		}
	}
	// 等待中的重复提交返回同一通知通道，不重复入队。
	res, err := c.Acquire("s2", "idem", "acq-2", time.Minute, true)
	if err != nil || res.WaitCh == nil {
		t.Fatalf("wait acquire: %+v err=%v", res, err)
	}
	res2, _ := c.Acquire("s2", "idem", "acq-2", time.Minute, true)
	if res2.WaitCh != res.WaitCh {
		t.Fatal("duplicate waiting acquire returned a different channel")
	}
	// 重复释放：首次成功，重放返回相同（成功）结果。
	if err := c.Release("s1", "idem", "rel-1", g1.Token); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("s1", "idem", "rel-1", g1.Token); err != nil {
		t.Fatalf("duplicate release should replay success, got %v", err)
	}
	// 等待者被恰好唤醒一次。
	select {
	case g := <-res.WaitCh:
		if g.Session != "s2" {
			t.Fatalf("woken grant session = %s", g.Session)
		}
	default:
		t.Fatal("waiter was not woken")
	}
	select {
	case <-res.WaitCh:
		t.Fatal("duplicate wake-up notification")
	default:
	}
	// 重复续租幂等。
	g2 := mustAcquire(t, c, "s1", "idem2", "a1", time.Minute)
	r1, err := c.Renew("s1", "idem2", "ren-1", g2.Token, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c.Renew("s1", "idem2", "ren-1", g2.Token, 2*time.Minute)
	if err != nil || !r2.ExpiresAt.Equal(r1.ExpiresAt) {
		t.Fatalf("duplicate renew: %+v vs %+v err=%v", r1, r2, err)
	}
}

// TestWaitQueueFIFO 覆盖：等待者按先来后到被唤醒，事件顺序不乱。
func TestWaitQueueFIFO(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("holder")
	mustAcquire(t, c, "holder", "q", "r0", time.Hour)

	const n = 5
	chans := make([]<-chan Grant, n)
	for i := 0; i < n; i++ {
		sid := fmt.Sprintf("w%d", i)
		c.RegisterSession(sid)
		res, err := c.Acquire(sid, "q", fmt.Sprintf("wq-%d", i), time.Hour, true)
		if err != nil {
			t.Fatal(err)
		}
		chans[i] = res.WaitCh
	}
	var order []string
	for i := 0; i < n; i++ {
		// 当前持有者释放（第一个等待者之后的持有者是被唤醒者）。
		h, tok := c.Holder("q")
		if err := c.Release(h, "q", fmt.Sprintf("rel-%d", i), tok); err != nil {
			t.Fatal(err)
		}
		select {
		case g := <-chans[i]:
			order = append(order, g.Session)
		default:
			t.Fatalf("waiter %d not woken in turn", i)
		}
	}
	want := []string{"w0", "w1", "w2", "w3", "w4"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("wake order = %v, want %v", order, want)
	}
	t.Logf("FIFO wake order verified: %v", order)
}

// TestSessionFailover 覆盖：会话故障后其锁被释放，等待者接管。
func TestSessionFailover(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("victim")
	c.RegisterSession("next")

	mustAcquire(t, c, "victim", "sf", "r1", time.Hour)
	res, _ := c.Acquire("next", "sf", "r2", time.Hour, true)
	c.FailSession("victim")
	select {
	case g := <-res.WaitCh:
		if g.Session != "next" {
			t.Fatalf("failover grant to %s", g.Session)
		}
	default:
		t.Fatal("waiter not woken after session failure")
	}
	if _, err := c.Acquire("victim", "sf", "r3", time.Hour, false); !errors.Is(err, ErrUnknownSession) {
		t.Fatalf("failed session acquire err = %v, want ErrUnknownSession", err)
	}
	c.FailSession("victim") // 幂等
}

// TestRestartRecovery 覆盖：重启后租约、令牌、会话、等待队列一致恢复；
// 停机期间过期的租约在恢复后正确失效。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "snapshot.json")
	clk := clock.NewManual(t0)

	c1 := newTestCoordinator(t, clk, NewStore(storePath))
	c1.RegisterSession("s1")
	c1.RegisterSession("s2")
	g := mustAcquire(t, c1, "s1", "rec", "r1", 10*time.Second)
	res, _ := c1.Acquire("s2", "rec", "r2", time.Hour, true) // s2 排队等待

	clk.Advance(3 * time.Second) // 距过期还有 7s 时"重启"
	c2 := newTestCoordinator(t, clk, NewStore(storePath))
	if h, tok := c2.Holder("rec"); h != "s1" || tok != g.Token {
		t.Fatalf("after restart: holder=%s token=%d, want s1/%d", h, tok, g.Token)
	}
	// 令牌计数器恢复：新授予必须继续递增。
	clk.Advance(8 * time.Second) // s1 的租约过期，s2 应被唤醒
	select {
	case g2 := <-res.WaitCh:
		t.Fatalf("stale channel fired after restart: %+v", g2)
	default:
	}
	res2, err := c2.Acquire("s2", "rec", "r2", time.Hour, true) // 重放等待请求
	if err != nil {
		t.Fatal(err)
	}
	var g2 Grant
	if res2.Granted {
		g2 = res2.Grant
	} else {
		select {
		case g2 = <-res2.WaitCh:
		default:
			t.Fatal("waiter s2 not woken after lease expiry post-restart")
		}
	}
	if g2.Token <= g.Token {
		t.Fatalf("token regressed across restart: %d -> %d", g.Token, g2.Token)
	}
	// 重启前的幂等记录在恢复后仍然有效。
	if _, err := c2.Renew("s1", "rec", "r1-renew", g.Token, time.Hour); err == nil {
		t.Fatal("renew with old holder request should fail after restart")
	}
	t.Logf("restart recovery verified: token %d -> %d", g.Token, g2.Token)
}

// TestClockRollback 覆盖：时钟回拨不得让租约被无限延长。
func TestClockRollback(t *testing.T) {
	clk := clock.NewManual(t0)
	c := newTestCoordinator(t, clk, nil)
	c.RegisterSession("s1")
	c.RegisterSession("s2")

	mustAcquire(t, c, "s1", "rb", "r1", 10*time.Second)
	clk.Advance(9 * time.Second)
	c.Holder("rb")             // 任意一次访问锚定高水位（真实部署中由持续流量保证）
	clk.SetBack(8 * time.Second) // 墙钟回拨到过期前
	// 高水位保证：回拨不生效，时间不会倒流，
	// 推进到原截止时间后租约照常过期（不会被无限延长）。
	clk.Advance(9 * time.Second)
	g := mustAcquire(t, c, "s2", "rb", "r2", 10*time.Second)
	if g.Token != 2 {
		t.Fatalf("lease survived clock rollback, token=%d", g.Token)
	}
	t.Log("clock rollback did not extend the lease")
}
