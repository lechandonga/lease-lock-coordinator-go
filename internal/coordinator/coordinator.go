// Package coordinator implements the lock coordinator: mutual exclusion with
// expiring leases, strictly increasing fencing tokens, idempotent requests,
// FIFO waiting with an ordered event stream, and WAL-based crash recovery.
package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/internal/clock"
	"github.com/lechandonga/lease-lock-coordinator-go/internal/wal"
)

// WALAppender is the persistence boundary.
type WALAppender interface {
	Append(v any) ([]byte, error)
}

// Option configures a Coordinator.
type Option func(*Coordinator)

// WithDefaultTTL sets the lease duration used when Acquire receives ttl <= 0.
func WithDefaultTTL(d time.Duration) Option {
	return func(c *Coordinator) {
		if d > 0 {
			c.defaultTTL = d
		}
	}
}

// Coordinator is the in-memory, WAL-backed lock service.
type Coordinator struct {
	mu  sync.Mutex
	clk clock.Clock
	log WALAppender

	defaultTTL time.Duration

	// nextToken is the service-wide fencing token counter. Persisted
	// explicitly inside every acquire entry; it is the value *after*
	// issuing the grant.
	nextToken uint64

	sessions map[string]struct{}
	locks    map[string]*lockState

	// idempotency cache: sessionID -> requestID -> cachedResponse.
	idem map[string]map[string]any
}

type lockState struct {
	holder       string // session id, "" when free
	fencingToken uint64
	expiresAt    int64 // absolute unix nanos

	// FIFO waiters.
	waiters []*waiter

	// Ordered notification stream subscribers.
	subs []*eventSub

	// Per-lock strictly increasing notification sequence. Persisted? No:
	// in-flight waiters are ephemeral by design (clients re-subscribe
	// after a coordinator restart), while the authoritative ownership /
	// token state is recovered from the WAL.
	nextEventSeq uint64
	// last event kept for late subscribers.
	lastEvent *Event

	// lastHolder is the most recent owner of the lock and lastLossByExpiry
	// records whether the most recent loss was a lease expiry. Together
	// they let Renew/Release distinguish "your lease expired" from "you
	// never owned this / it belongs to someone else".
	lastHolder       string
	lastLossByExpiry bool
}

func (l *lockState) expired(now int64) bool { return l.holder != "" && now >= l.expiresAt }

// New creates a Coordinator backed by an already-replayed WAL.
func New(clk clock.Clock, l WALAppender, opts ...Option) *Coordinator {
	c := &Coordinator{
		clk:        clk,
		log:        l,
		defaultTTL: defaultTTL,
		sessions:   make(map[string]struct{}),
		locks:      make(map[string]*lockState),
		idem:       make(map[string]map[string]any),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Open creates a Coordinator whose state is recovered from the given WAL
// path. A background sweep goroutine is started when sweepInterval > 0
// (production); tests pass 0 and drive time explicitly.
func Open(clk clock.Clock, path string, sweepInterval time.Duration, opts ...Option) (*Coordinator, error) {
	l, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	c := New(clk, l, opts...)
	if err := c.recover(l); err != nil {
		return nil, err
	}
	if sweepInterval > 0 {
		go c.sweepLoop(sweepInterval)
	}
	return c, nil
}

func (c *Coordinator) now() int64 { return c.clk.Tick() }

func (c *Coordinator) lock(key string) *lockState {
	l, ok := c.locks[key]
	if !ok {
		l = &lockState{}
		c.locks[key] = l
	}
	return l
}

// validateExpiry performs the lazy lease check while c.mu is held. If the
// holder's lease has expired the lock is revoked (no WAL entry needed: the
// persisted expiry already encodes the fact) and waiters are notified.
func (c *Coordinator) expireIfNeeded(key string, l *lockState) {
	if !l.expired(c.now()) {
		return
	}
	old := l.holder
	l.holder = ""
	l.fencingToken = 0
	l.expiresAt = 0
	l.lastHolder = old
	l.lastLossByExpiry = true
	c.publishLocked(key, l, EventRevoked, old, 0)
	c.tryGrantHeadLocked(key, l)
}

// sweepLoop periodically expires leases using the injected clock.
func (c *Coordinator) sweepLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		c.SweepExpired()
	}
}

// SweepExpired forces an expiration pass over all locks.
func (c *Coordinator) SweepExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, l := range c.locks {
		c.expireIfNeeded(key, l)
	}
}

// ---------- idempotency helpers ----------

func (c *Coordinator) idemGet(sessionID, requestID string) (any, bool) {
	if requestID == "" {
		return nil, false
	}
	m, ok := c.idem[sessionID]
	if !ok {
		return nil, false
	}
	v, ok := m[requestID]
	return v, ok
}

func (c *Coordinator) idemPut(sessionID, requestID string, v any) {
	if requestID == "" {
		return
	}
	m := c.idem[sessionID]
	if m == nil {
		m = make(map[string]any)
		c.idem[sessionID] = m
	}
	m[requestID] = v
}

// ---------- events ----------

type eventSub struct {
	fromSeq uint64
	ch      chan Event
}

func (c *Coordinator) publishLocked(key string, l *lockState, kind EventKind, sessionID string, token uint64) {
	l.nextEventSeq++
	ev := Event{
		LockKey:      key,
		Seq:          l.nextEventSeq,
		Kind:         kind,
		SessionID:    sessionID,
		FencingToken: token,
	}
	l.lastEvent = &ev
	// Duplicate-tolerant fan-out: a slow subscriber that dropped an event
	// will resubscribe with the last seq it saw, so delivery here is
	// best-effort non-blocking; ordering is guaranteed by holding c.mu and
	// assigning seq monotonically.
	alive := l.subs[:0]
	for _, s := range l.subs {
		if ev.Seq < s.fromSeq {
			alive = append(alive, s)
			continue
		}
		select {
		case s.ch <- ev:
			alive = append(alive, s)
		default:
			// buffer full: subscriber must resubscribe / re-wait.
		}
	}
	l.subs = alive
}

// Subscribe returns a buffered channel of lock events starting immediately
// after afterSeq (0 replays the most recent event if any), plus a cancel
// func. Events are never reordered; if a delivery is missed the caller sees
// channel silence and may resubscribe with the last seq it processed.
func (c *Coordinator) Subscribe(key string, afterSeq uint64) (<-chan Event, func()) {
	ch := make(chan Event, 16)
	c.mu.Lock()
	l := c.lock(key)
	s := &eventSub{fromSeq: afterSeq + 1, ch: ch}
	l.subs = append(l.subs, s)
	if l.lastEvent != nil && l.lastEvent.Seq >= s.fromSeq {
		select {
		case ch <- *l.lastEvent:
		default:
		}
	}
	c.mu.Unlock()

	cancel := func() {
		c.mu.Lock()
		if ll, ok := c.locks[key]; ok {
			for i, sub := range ll.subs {
				if sub == s {
					ll.subs = append(ll.subs[:i], ll.subs[i+1:]...)
					break
				}
			}
		}
		c.mu.Unlock()
	}
	return ch, cancel
}

// ---------- waiting ----------

type waiter struct {
	sessionID string
	ttl       int64
	requestID string // idempotency key for the later grant
	ch        chan Grant
	seq       uint64 // event seq of the REVOKED that this waiter reacted to
	ctx       context.Context
}

// tryGrantHeadLocked hands a free lock to the head of the FIFO queue.
// Exactly one waiter can ever receive a grant for a given vacancy: the head
// is removed from the queue before the grant is delivered.
func (c *Coordinator) tryGrantHeadLocked(key string, l *lockState) {
	for len(l.waiters) > 0 {
		w := l.waiters[0]
		l.waiters = l.waiters[1:]
		// Skip waiters whose context was cancelled (they gave up).
		if w.ctx.Err() != nil {
			continue
		}
		if _, sessionAlive := c.sessions[w.sessionID]; !sessionAlive {
			continue
		}
		if err := c.grantLocked(key, l, w); err != nil {
			// Durability failure: requeue at head; the lock stays free
			// and a later sweep/release retries. No other waiter is
			// skipped behind it.
			l.waiters = append([]*waiter{w}, l.waiters...)
		}
		return
	}
}

func (c *Coordinator) grantLocked(key string, l *lockState, w *waiter) error {
	c.nextToken++
	token := c.nextToken
	expires := c.now() + w.ttl

	entry := walEntry{
		Kind:         entryLockAcquired,
		NextToken:    token,
		LockKey:      key,
		SessionID:    w.sessionID,
		FencingToken: token,
		ExpiresAt:    expires,
		TTL:          w.ttl,
		RequestID:    w.requestID,
	}
	// Persist before exposing the grant. (WAL fsyncs.) A failed append
	// must never leave an unpersisted "phantom" owner; the token counter
	// is bumped only conceptually (the persisted NextToken on success is
	// authoritative and leaves gaps harmlessly).
	if _, err := c.log.Append(entry); err != nil {
		c.nextToken--
		return err
	}

	l.holder = w.sessionID
	l.fencingToken = token
	l.expiresAt = expires

	g := Grant{LockKey: key, SessionID: w.sessionID, FencingToken: token, ExpiresAt: expires}
	if w.requestID != "" {
		c.idemPut(w.sessionID, w.requestID, g)
	}
	c.publishLocked(key, l, EventGranted, w.sessionID, token)

	select {
	case w.ch <- g:
	default:
	}
	return nil
}
