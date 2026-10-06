package coordinator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/internal/clock"
)

// t0 is an arbitrary fixed epoch so all tests are fully deterministic.
var t0 = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

// testCase is one named business behavior. The suite logs which business
// area each case belongs to, its description and pass/fail, so `go test -v`
// reads as a coverage report of the required semantics.
type testCase struct {
	business string
	name     string
	run      func(t *testing.T, env *testEnv)
}

type testEnv struct {
	dir string
}

var suite []testCase

func tc(business, name string, run func(t *testing.T, env *testEnv)) {
	suite = append(suite, testCase{business, name, run})
}

func TestBusinessSuite(t *testing.T) {
	env := &testEnv{dir: t.TempDir()}
	var lastBiz string
	for _, c := range suite {
		if c.business != lastBiz {
			t.Logf("========== 业务模块: %s ==========", c.business)
			lastBiz = c.business
		}
		t.Run(fmt.Sprintf("%s/%s", c.business, c.name), func(t *testing.T) {
			t.Logf("[用例] %s/%s", c.business, c.name)
			c.run(t, env)
			t.Log("[结果] PASS")
		})
	}
}

func newCoord(t *testing.T, env *testEnv, name string) (*Coordinator, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(t0)
	c, err := Open(fc, filepath.Join(env.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// wal.Log closes via coordinator? We reopen per name; ensure no
		// goroutines: sweep disabled.
	})
	return c, fc
}

func reopen(t *testing.T, env *testEnv, name string, fc *clock.Fake) *Coordinator {
	t.Helper()
	c, err := Open(fc, filepath.Join(env.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustOpen(t *testing.T, c *Coordinator, sid string) {
	t.Helper()
	if err := c.OpenSession(sid); err != nil {
		t.Fatalf("open session %s: %v", sid, err)
	}
}

// ---------------- 1. mutual exclusion ----------------

func init() {
	tc("互斥获取", "首个客户端获取成功并拿到令牌", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "acq-ok")
		mustOpen(t, c, "s1")
		g, err := c.Acquire("s1", "order-1", time.Second, "r1")
		if err != nil {
			t.Fatal(err)
		}
		if g.FencingToken != 1 || g.SessionID != "s1" {
			t.Fatalf("unexpected grant %+v", g)
		}
		if g.ExpiresAt != fc.Tick()+int64(time.Second) {
			t.Fatalf("expiry mismatch: %d vs %d", g.ExpiresAt, fc.Tick()+int64(time.Second))
		}
	})

	tc("互斥获取", "锁被活跃持有时第二个客户端被拒绝", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "acq-busy")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		if _, err := c.Acquire("s1", "L", time.Second, ""); err != nil {
			t.Fatal(err)
		}
		_, err := c.Acquire("s2", "L", time.Second, "")
		if !errors.Is(err, ErrLockHeldByOther) {
			t.Fatalf("want ErrLockHeldByOther, got %v", err)
		}
	})

	tc("租约过期", "租约到期后原持有者失去锁且他人可获取", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "expire-lost")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g1, _ := c.Acquire("s1", "L", time.Second, "")
		fc.Advance(time.Second + time.Millisecond)
		if holder, _ := c.HolderAt("L"); holder != "" {
			t.Fatalf("holder should be empty after expiry, got %q", holder)
		}
		g2, err := c.Acquire("s2", "L", time.Second, "")
		if err != nil {
			t.Fatal(err)
		}
		if g2.FencingToken <= g1.FencingToken {
			t.Fatalf("token must strictly increase: %d -> %d", g1.FencingToken, g2.FencingToken)
		}
	})

	tc("续租", "到期前续租延长租约", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "renew-ok")
		mustOpen(t, c, "s1")
		g, _ := c.Acquire("s1", "L", time.Second, "")
		fc.Advance(800 * time.Millisecond)
		r, err := c.Renew("s1", "L", g.FencingToken, time.Second, "hb1")
		if err != nil {
			t.Fatal(err)
		}
		want := fc.Tick() + int64(time.Second)
		if r.ExpiresAt != want {
			t.Fatalf("renew expiry = %d want %d", r.ExpiresAt, want)
		}
		fc.Advance(time.Second - time.Millisecond)
		if holder, _ := c.HolderAt("L"); holder != "s1" {
			t.Fatal("lock should still be held after timely renew")
		}
	})

	tc("续租", "对已过期且尚未被他人获取的锁续租返回 ErrLeaseExpired", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "renew-expired")
		mustOpen(t, c, "s1")
		g, _ := c.Acquire("s1", "L", time.Second, "")
		fc.Advance(time.Second + 1)
		_, err := c.Renew("s1", "L", g.FencingToken, time.Second, "")
		if !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("want ErrLeaseExpired, got %v", err)
		}
	})

	tc("续租", "过期后锁已归他人时旧持有者续租返回 ErrNotOwner", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "renew-other")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g, _ := c.Acquire("s1", "L", 500*time.Millisecond, "")
		fc.Advance(time.Second)
		if _, err := c.Acquire("s2", "L", time.Second, ""); err != nil {
			t.Fatal(err)
		}
		_, err := c.Renew("s1", "L", g.FencingToken, time.Second, "")
		if !errors.Is(err, ErrNotOwner) {
			t.Fatalf("want ErrNotOwner, got %v", err)
		}
	})

	tc("释放", "正常释放后他人可立即获取", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "release-ok")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g, _ := c.Acquire("s1", "L", time.Second, "")
		if _, err := c.Release("s1", "L", g.FencingToken, "rel1"); err != nil {
			t.Fatal(err)
		}
		if holder, _ := c.HolderAt("L"); holder != "" {
			t.Fatal("lock should be free")
		}
		if _, err := c.Acquire("s2", "L", time.Second, ""); err != nil {
			t.Fatalf("s2 should acquire: %v", err)
		}
	})

	tc("释放", "释放已过期(未被他人获取)的锁返回 ErrLeaseExpired", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "release-expired")
		mustOpen(t, c, "s1")
		g, _ := c.Acquire("s1", "L", time.Second, "")
		fc.Advance(time.Second + 1)
		_, err := c.Release("s1", "L", g.FencingToken, "")
		if !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("want ErrLeaseExpired, got %v", err)
		}
	})

	tc("释放", "释放他人持有的锁返回 ErrNotOwner", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "release-foreign")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		_, _ = c.Acquire("s1", "L", time.Second, "")
		_, err := c.Release("s2", "L", 99, "")
		if !errors.Is(err, ErrNotOwner) {
			t.Fatalf("want ErrNotOwner, got %v", err)
		}
	})
}

// ---------------- 2. fencing tokens ----------------

func init() {
	tc("隔离令牌", "每次成功获取产生严格递增的令牌", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "token-incr")
		mustOpen(t, c, "s1")
		var prev uint64
		for i := 0; i < 5; i++ {
			g, err := c.Acquire("s1", "L", 100*time.Millisecond, "")
			if err != nil {
				t.Fatal(err)
			}
			if g.FencingToken <= prev {
				t.Fatalf("token not increasing: %d after %d", g.FencingToken, prev)
			}
			prev = g.FencingToken
			fc.Advance(200 * time.Millisecond)
		}
	})

	tc("隔离令牌", "携带旧令牌的续租被 ErrStaleFencingToken 拒绝", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "stale-renew")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g1, _ := c.Acquire("s1", "L", 100*time.Millisecond, "")
		fc.Advance(200 * time.Millisecond)
		g2, err := c.Acquire("s2", "L", time.Second, "")
		if err != nil {
			t.Fatal(err)
		}
		// s1 的迟到续租（旧令牌、旧持有者）不能影响 s2。
		if _, err := c.Renew("s1", "L", g1.FencingToken, time.Second, "late"); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("want ErrNotOwner for expired old owner, got %v", err)
		}
		// 伪造的过期前旧令牌续租：即使持有者还是自己也必须按令牌判否。
		// 构造: s2 持有期间用 s2 的旧令牌续租 (release/acquire 同会话)。
		if _, err := c.Release("s2", "L", g2.FencingToken, ""); err != nil {
			t.Fatal(err)
		}
		g3, _ := c.Acquire("s2", "L", time.Second, "")
		if _, err := c.Renew("s2", "L", g2.FencingToken, time.Second, ""); !errors.Is(err, ErrStaleFencingToken) {
			t.Fatalf("want ErrStaleFencingToken, got %v", err)
		}
		_ = g3
		if holder, tok := c.HolderAt("L"); holder != "s2" || tok != g3.FencingToken {
			t.Fatalf("new owner must remain intact: %s/%d", holder, tok)
		}
	})

	tc("隔离令牌", "携带旧令牌的释放被拒绝且不破坏新持有者", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "stale-release")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g1, _ := c.Acquire("s1", "L", 100*time.Millisecond, "")
		fc.Advance(200 * time.Millisecond)
		g2, _ := c.Acquire("s2", "L", time.Second, "")
		// s1 迟到的释放请求。
		if _, err := c.Release("s1", "L", g1.FencingToken, ""); !errors.Is(err, ErrNotOwner) {
			t.Fatalf("want ErrNotOwner, got %v", err)
		}
		if _, err := c.Release("s2", "L", g1.FencingToken, ""); !errors.Is(err, ErrStaleFencingToken) {
			t.Fatalf("want ErrStaleFencingToken for s2 with old token, got %v", err)
		}
		if holder, tok := c.HolderAt("L"); holder != "s2" || tok != g2.FencingToken {
			t.Fatal("new holder must be untouched by stale releases")
		}
	})

	// ---------------- 3. idempotency ----------------

	tc("幂等请求", "重复 Acquire 返回同一令牌不重复授予", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "idem-acq")
		mustOpen(t, c, "s1")
		g1, err := c.Acquire("s1", "L", time.Second, "req-1")
		if err != nil {
			t.Fatal(err)
		}
		g2, err := c.Acquire("s1", "L", time.Second, "req-1")
		if err != nil {
			t.Fatal(err)
		}
		if !g2.Replayed || g2.FencingToken != g1.FencingToken || g2.ExpiresAt != g1.ExpiresAt {
			t.Fatalf("duplicate acquire must replay identical grant: %+v vs %+v", g1, g2)
		}
	})

	tc("幂等请求", "重复 Renew 与 Release 幂等且可区分重放", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "idem-mut")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		g, _ := c.Acquire("s1", "L", time.Second, "r")
		r1, _ := c.Renew("s1", "L", g.FencingToken, time.Second, "h")
		r2, _ := c.Renew("s1", "L", g.FencingToken, time.Second, "h")
		if !r2.Replayed || r1.ExpiresAt != r2.ExpiresAt {
			t.Fatal("renew must be idempotent")
		}
		rel1, _ := c.Release("s1", "L", g.FencingToken, "x")
		rel2, _ := c.Release("s1", "L", g.FencingToken, "x")
		if !rel2.Replayed || !rel1.Released || !rel2.Released {
			t.Fatal("release must be idempotent")
		}
		// 幂等重放之后锁仍是空闲，且 s2 获取产生新令牌，而不是第二次释放唤醒异常。
		g2, err := c.Acquire("s2", "L", time.Second, "")
		if err != nil || g2.FencingToken != g.FencingToken+1 {
			t.Fatalf("next grant unexpected: %+v %v", g2, err)
		}
	})

	tc("幂等请求", "并发重复提交只有一次授予", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "idem-concurrent")
		mustOpen(t, c, "s1")
		const n = 32
		var wg sync.WaitGroup
		grants := make([]Grant, n)
		errs := make([]error, n)
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				grants[i], errs[i] = c.Acquire("s1", "L", time.Second, "same-request")
			}(i)
		}
		wg.Wait()
		var token uint64
		for i := 0; i < n; i++ {
			if errs[i] != nil {
				t.Fatalf("acquire %d: %v", i, errs[i])
			}
			if token == 0 {
				token = grants[i].FencingToken
			} else if grants[i].FencingToken != token {
				t.Fatalf("duplicate concurrent request got different token: %d vs %d", token, grants[i].FencingToken)
			}
		}
		if snap := c.Peek("L"); snap.FencingToken != token {
			t.Fatalf("exactly one grant expected, snap=%+v", snap)
		}
	})
}

// ---------------- 4. waiting / events ----------------

func init() {
	tc("等待队列", "等待者严格按先来后到(FIFO)获得唤醒与授予", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "fifo")
		mustOpen(t, c, "s1")
		for i := 2; i <= 5; i++ {
			mustOpen(t, c, fmt.Sprintf("s%d", i))
		}
		g0, _ := c.Acquire("s1", "L", 100*time.Millisecond, "")

		type result struct {
			sid string
			g   Grant
		}
		res := make(chan result, 4)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// 用放行栅栏保证入队顺序确定：每个协程先等在 start 上，主协
		// 程确认前一个已入队后再放行下一个，避免并发 Peek 触发的过期
		// 授予打乱顺序。
		starts := make([]chan struct{}, 4)
		ready := make(chan struct{}, 4)
		for i := range starts {
			starts[i] = make(chan struct{})
		}
		for i := 2; i <= 5; i++ {
			sid := fmt.Sprintf("s%d", i)
			start := starts[i-2]
			go func() {
				<-start
				ready <- struct{}{}
				g, err := c.WaitAcquire(ctx, sid, "L", time.Second, "")
				if err == nil {
					res <- result{sid, g}
				}
			}()
		}
		for i := 2; i <= 5; i++ {
			close(starts[i-2])
			<-ready
			want := i - 1
			waitFor(func() bool { return c.Peek("L").Waiters == want })
		}

		// 释放后授予应依次为 s2,s3,s4,s5，每次令牌递增。每个持有者持有不同的锁周期。
		expected := []string{"s2", "s3", "s4", "s5"}
		token := g0.FencingToken
		if _, err := c.Release("s1", "L", g0.FencingToken, ""); err != nil {
			t.Fatal(err)
		}
		for _, want := range expected {
			r := <-res
			if r.sid != want {
				t.Fatalf("FIFO violated: want %s got %s", want, r.sid)
			}
			if r.g.FencingToken <= token {
				t.Fatalf("granted token must increase for %s", want)
			}
			token = r.g.FencingToken
			if _, err := c.Release(want, "L", r.g.FencingToken, ""); err != nil {
				t.Fatal(err)
			}
		}
	})

	tc("等待队列", "取消排队不影响后续等待者", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "fifo-cancel")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		mustOpen(t, c, "s3")
		_, _ = c.Acquire("s1", "L", 100*time.Millisecond, "")
		ctx2, cancel2 := context.WithCancel(context.Background())
		done2 := make(chan error, 1)
		go func() { _, err := c.WaitAcquire(ctx2, "s2", "L", time.Second, ""); done2 <- err }()
		waitFor(func() bool { return c.Peek("L").Waiters == 1 })
		ctx3, cancel3 := context.WithCancel(context.Background())
		defer cancel3()
		got3 := make(chan Grant, 1)
		go func() {
			g, err := c.WaitAcquire(ctx3, "s3", "L", time.Second, "")
			if err == nil {
				got3 <- g
			}
		}()
		waitFor(func() bool { return c.Peek("L").Waiters == 2 })

		cancel2()
		waitFor(func() bool { return <-done2 == context.Canceled && c.Peek("L").Waiters == 1 })

		snap := c.Peek("L")
		g0 := mustGrant(t, c, "s1", "L") // helper below
		_ = snap
		_ = g0
		// s1 释放后 s3 应直接得到锁。
		c.mu.Lock()
		tok := c.locks["L"].fencingToken
		holder := c.locks["L"].holder
		c.mu.Unlock()
		if _, err := c.Release("s1", "L", tok, "rel"); err != nil {
			t.Fatalf("release: %v (holder=%s)", err, holder)
		}
		g := <-got3
		if g.SessionID != "s3" {
			t.Fatalf("want s3, got %s", g.SessionID)
		}
	})

	tc("事件流", "同把锁事件序号严格单调且可重放最新事件给迟到订阅者", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "events")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		ch, cancelSub := c.Subscribe("L", 0)
		defer cancelSub()
		g1, _ := c.Acquire("s1", "L", 100*time.Millisecond, "")
		ev1 := <-ch
		if ev1.Kind != EventGranted || ev1.Seq != 1 || ev1.FencingToken != g1.FencingToken {
			t.Fatalf("bad event1: %+v", ev1)
		}
		if _, err := c.Release("s1", "L", g1.FencingToken, ""); err != nil {
			t.Fatal(err)
		}
		ev2 := <-ch
		if ev2.Kind != EventRevoked || ev2.Seq != 2 || ev2.SessionID != "s1" {
			t.Fatalf("bad event2: %+v", ev2)
		}
		g2, _ := c.Acquire("s2", "L", time.Second, "")
		ev3 := <-ch
		if ev3.Kind != EventGranted || ev3.Seq != 3 || ev3.FencingToken != g2.FencingToken {
			t.Fatalf("bad event3: %+v", ev3)
		}
		// 迟到订阅者 afterSeq=2 只能看到 seq>=3，不会乱序/重放旧事件。
		ch2, cancel2 := c.Subscribe("L", 2)
		defer cancel2()
		ev := <-ch2
		if ev.Seq != 3 {
			t.Fatalf("late subscriber must see seq 3, got %d", ev.Seq)
		}
	})
}

func waitFor(cond func() bool) {
	for i := 0; i < 10000; i++ {
		if cond() {
			return
		}
		runtimeGosched()
	}
	panic("condition never satisfied")
}

func mustGrant(t *testing.T, c *Coordinator, sid, key string) Grant {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.locks[key]
	if l == nil {
		t.Fatal("no lock")
	}
	return Grant{SessionID: l.holder, FencingToken: l.fencingToken, ExpiresAt: l.expiresAt}
}

// ---------------- 5. clock regression ----------------

func init() {
	tc("时钟回拨", "回拨后续租不会缩短或无限延长租约", func(t *testing.T, e *testEnv) {
		c, fc := newCoord(t, e, "rewind")
		mustOpen(t, c, "s1")
		g, _ := c.Acquire("s1", "L", time.Second, "")
		originalExpiry := g.ExpiresAt
		fc.Rewind(2 * time.Second)
		r, err := c.Renew("s1", "L", g.FencingToken, time.Second, "hb")
		if err != nil {
			t.Fatal(err)
		}
		// 回拨时钟下新的到期点必须 = 原到期点 + TTL，而不是 过去时刻 + TTL。
		if r.ExpiresAt != originalExpiry+int64(time.Second) {
			t.Fatalf("rewind renewal expiry = %d, want %d", r.ExpiresAt, originalExpiry+int64(time.Second))
		}
		// 即便时钟停在过去，原租约的判定依据持久化的绝对到期点，不被无限延长。
		if holder, _ := c.HolderAt("L"); holder != "s1" {
			t.Fatal("holder should remain s1 with bounded lease")
		}
		fc.Advance(5 * time.Second) // 越过回拨+续租后的到期点(t0+2s)
		if holder, _ := c.HolderAt("L"); holder != "" {
			t.Fatalf("lease must eventually expire even after rewind+renew, holder=%s", holder)
		}
	})

	tc("时钟回拨", "重启到更慢时钟的机器上租约仍按绝对时间过期", func(t *testing.T, e *testEnv) {
		name := "rewind-restart"
		fc := clock.NewFake(t0)
		c1, err := Open(fc, filepath.Join(e.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		mustOpen(t, c1, "s1")
		g, _ := c1.Acquire("s1", "L", time.Second, "")
		deadline := g.ExpiresAt

		// 新主机时钟慢 10 秒。
		fc2 := clock.NewFake(t0.Add(-10 * time.Second))
		c2 := reopen(t, e, name, fc2)
		if holder, tok := c2.HolderAt("L"); holder != "s1" || tok != g.FencingToken {
			t.Fatalf("lease should still be valid on slower host: %s/%d", holder, tok)
		}
		fc2.Advance(time.Duration(deadline-fc2.Tick()) + 1)
		if holder, _ := c2.HolderAt("L"); holder != "" {
			t.Fatal("absolute deadline must still expire lease on slow host")
		}
	})

	// ---------------- 6. sessions ----------------

	tc("会话故障", "结束会话释放其全部锁并唤醒等待者", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "session-end")
		mustOpen(t, c, "s1")
		mustOpen(t, c, "s2")
		_, _ = c.Acquire("s1", "A", time.Second, "")
		_, _ = c.Acquire("s1", "B", time.Second, "")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		got := make(chan Grant, 2)
		go func() {
			g, err := c.WaitAcquire(ctx, "s2", "A", time.Second, "")
			if err == nil {
				got <- g
			}
		}()
		waitFor(func() bool { return c.Peek("A").Waiters == 1 })

		if err := c.EndSession("s1"); err != nil {
			t.Fatal(err)
		}
		if c.SessionExists("s1") {
			t.Fatal("s1 should be gone")
		}
		g := <-got
		if g.SessionID != "s2" || g.LockKey != "A" {
			t.Fatalf("waiter for A not woken: %+v", g)
		}
		if holder, _ := c.HolderAt("B"); holder != "" {
			t.Fatal("B must be released when session ended")
		}
	})

	// ---------------- 7. concurrent contention ----------------

	tc("并发争抢", "N 个并发会话争抢同一把锁恰好一个成功", func(t *testing.T, e *testEnv) {
		c, _ := newCoord(t, e, "race")
		const n = 50
		for i := 0; i < n; i++ {
			mustOpen(t, c, fmt.Sprintf("c%d", i))
		}
		var wg sync.WaitGroup
		success := make(chan string, n)
		wg.Add(n)
		for i := 0; i < n; i++ {
			sid := fmt.Sprintf("c%d", i)
			go func() {
				defer wg.Done()
				if _, err := c.Acquire(sid, "hot", time.Second, ""); err == nil {
					success <- sid
				}
			}()
		}
		wg.Wait()
		close(success)
		winners := 0
		winner := ""
		for s := range success {
			winners++
			winner = s
		}
		if winners != 1 {
			t.Fatalf("expected exactly 1 winner, got %d", winners)
		}
		if h, _ := c.HolderAt("hot"); h != winner {
			t.Fatalf("holder mismatch: %s vs %s", h, winner)
		}
	})

	// ---------------- 8. restart recovery ----------------

	tc("重启恢复", "租约、令牌、会话与幂等缓存在重启后一致恢复", func(t *testing.T, e *testEnv) {
		name := "restart"
		fc := clock.NewFake(t0)
		c1, err := Open(fc, filepath.Join(e.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		mustOpen(t, c1, "s1")
		mustOpen(t, c1, "s2")
		g, _ := c1.Acquire("s1", "L", time.Second, "req-acq")
		_, _ = c1.Renew("s1", "L", g.FencingToken, 2*time.Second, "req-renew")

		c2 := reopen(t, e, name, fc)
		if !c2.SessionExists("s1") || !c2.SessionExists("s2") {
			t.Fatal("sessions must survive restart")
		}
		snap := c2.Peek("L")
		if snap.Holder != "s1" || snap.FencingToken != g.FencingToken {
			t.Fatalf("ownership/token not recovered: %+v", snap)
		}
		// 新获取的令牌必须在历史最大值之后严格递增。
		if _, err := c2.Release("s1", "L", g.FencingToken, ""); err != nil {
			t.Fatal(err)
		}
		g2, err := c2.Acquire("s2", "L", time.Second, "")
		if err != nil || g2.FencingToken <= g.FencingToken {
			t.Fatalf("token must continue after restart: %+v %v", g2, err)
		}
		// 幂等缓存也恢复：旧 request id 返回历史结果，不产生新令牌。
		rep, err := c1.Acquire("s1", "L", time.Second, "req-acq")
		if err != nil || rep.FencingToken != g.FencingToken {
			t.Fatalf("idempotency cache should recover historical grant, got %+v %v", rep, err)
		}
	})

	tc("重启恢复", "重启时已过期的租约恢复为可获取且旧持有者得到 ErrLeaseExpired", func(t *testing.T, e *testEnv) {
		name := "restart-expired"
		fc := clock.NewFake(t0)
		c1, err := Open(fc, filepath.Join(e.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		mustOpen(t, c1, "s1")
		mustOpen(t, c1, "s2")
		g, _ := c1.Acquire("s1", "L", time.Second, "")
		fc.Advance(2 * time.Second)

		c2 := reopen(t, e, name, fc)
		if holder, _ := c2.HolderAt("L"); holder != "" {
			t.Fatal("expired lease must not be recovered as held")
		}
		if _, err := c2.Renew("s1", "L", g.FencingToken, time.Second, ""); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("late renew after restart: want ErrLeaseExpired, got %v", err)
		}
		if _, err := c2.Acquire("s2", "L", time.Second, ""); err != nil {
			t.Fatalf("expired-on-restart lock must be acquirable: %v", err)
		}
	})

	tc("重启恢复", "已结束会话在重启后保持结束状态(释放结果持久化)", func(t *testing.T, e *testEnv) {
		name := "restart-session"
		fc := clock.NewFake(t0)
		c1, err := Open(fc, filepath.Join(e.dir, name+".wal"), 0, WithDefaultTTL(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		mustOpen(t, c1, "s1")
		_, _ = c1.Acquire("s1", "L", time.Second, "")
		if err := c1.EndSession("s1"); err != nil {
			t.Fatal(err)
		}
		c2 := reopen(t, e, name, fc)
		if c2.SessionExists("s1") {
			t.Fatal("ended session must remain ended after restart")
		}
		if holder, _ := c2.HolderAt("L"); holder != "" {
			t.Fatal("locks of ended session must remain free after restart")
		}
	})
}
