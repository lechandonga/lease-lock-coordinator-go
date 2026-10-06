package coordinator

import (
	"context"
	"encoding/json"
	"time"
)

// OpenSession registers a client session. Sessions are durable: after a
// coordinator restart all opened (and not ended) sessions exist again.
func (c *Coordinator) OpenSession(sessionID string) error {
	if sessionID == "" {
		return ErrEmptySessionID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sessions[sessionID]; ok {
		return nil // idempotent
	}
	if _, err := c.log.Append(walEntry{Kind: entrySessionOpened, SessionID: sessionID}); err != nil {
		return err
	}
	c.sessions[sessionID] = struct{}{}
	return nil
}

// EndSession releases every lock held by the session and removes it.
// Released locks are immediately offered to FIFO waiters.
func (c *Coordinator) EndSession(sessionID string) error {
	if sessionID == "" {
		return ErrEmptySessionID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sessions[sessionID]; !ok {
		return ErrSessionNotFound
	}
	if _, err := c.log.Append(walEntry{Kind: entrySessionEnded, SessionID: sessionID}); err != nil {
		return err
	}
	delete(c.sessions, sessionID)
	delete(c.idem, sessionID)
	for key, l := range c.locks {
		c.expireIfNeeded(key, l)
		if l.holder == sessionID {
			l.holder = ""
			l.fencingToken = 0
			l.expiresAt = 0
			l.lastHolder = sessionID
			l.lastLossByExpiry = false
			c.publishLocked(key, l, EventRevoked, sessionID, 0)
			c.tryGrantHeadLocked(key, l)
		}
		alive := l.waiters[:0]
		for _, w := range l.waiters {
			if w.sessionID != sessionID {
				alive = append(alive, w)
			}
		}
		l.waiters = alive
	}
	return nil
}

// SessionExists reports whether the session is currently open.
func (c *Coordinator) SessionExists(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.sessions[sessionID]
	return ok
}

func (c *Coordinator) ttlNanos(ttl time.Duration) int64 {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	return int64(ttl)
}

// Acquire attempts to take key for sessionID with lease ttl. requestID, when
// non-empty, makes the call idempotent: retries with the same
// (sessionID, requestID) return the original Grant instead of issuing a new
// fencing token. If the lock is held by a live, different session the call
// fails with ErrLockHeldByOther; use WaitAcquire to queue.
func (c *Coordinator) Acquire(sessionID, key string, ttl time.Duration, requestID string) (Grant, error) {
	if key == "" {
		return Grant{}, ErrEmptyLockKey
	}
	if sessionID == "" {
		return Grant{}, ErrEmptySessionID
	}
	d := c.ttlNanos(ttl)
	if d <= 0 {
		return Grant{}, ErrInvalidLease
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.idemGet(sessionID, requestID); ok {
		g := v.(Grant)
		g.Replayed = true
		return g, nil
	}
	if _, ok := c.sessions[sessionID]; !ok {
		return Grant{}, ErrSessionNotFound
	}
	l := c.lock(key)
	c.expireIfNeeded(key, l)
	if l.holder != "" && l.holder != sessionID {
		return Grant{}, ErrLockHeldByOther
	}
	if l.holder == sessionID {
		return Grant{LockKey: key, SessionID: sessionID, FencingToken: l.fencingToken, ExpiresAt: l.expiresAt}, nil
	}

	w := &waiter{
		sessionID: sessionID,
		ttl:       d,
		requestID: requestID,
		ch:        make(chan Grant, 1),
		ctx:       context.Background(),
	}
	if err := c.grantLocked(key, l, w); err != nil {
		return Grant{}, err
	}
	return <-w.ch, nil
}

// rejectStale maps free/foreign-lock state to the distinguishable errors.
func (c *Coordinator) rejectStale(sessionID string, l *lockState) error {
	if l.holder != sessionID {
		if l.holder != "" {
			return ErrNotOwner
		}
		if l.lastHolder == sessionID && l.lastLossByExpiry {
			return ErrLeaseExpired
		}
		return ErrNotOwner
	}
	return ErrStaleFencingToken
}

// Renew extends the lease of key held by sessionID. token must be the
// session's current fencing token.
//
// Failure modes:
//   - ErrSessionNotFound: unknown session.
//   - ErrNotOwner: lock is held by another session or was never owned.
//   - ErrLeaseExpired: caller was the last owner but the lease expired and
//     the lock is currently free.
//   - ErrStaleFencingToken: token is older than the current owner token.
func (c *Coordinator) Renew(sessionID, key string, token uint64, ttl time.Duration, requestID string) (RenewResult, error) {
	if key == "" {
		return RenewResult{}, ErrEmptyLockKey
	}
	if sessionID == "" {
		return RenewResult{}, ErrEmptySessionID
	}
	if token == 0 {
		return RenewResult{}, ErrInvalidToken
	}
	d := c.ttlNanos(ttl)
	if d <= 0 {
		return RenewResult{}, ErrInvalidLease
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.idemGet(sessionID, requestID); ok {
		r := v.(RenewResult)
		r.Replayed = true
		return r, nil
	}
	if _, ok := c.sessions[sessionID]; !ok {
		return RenewResult{}, ErrSessionNotFound
	}
	l := c.lock(key)
	c.expireIfNeeded(key, l)

	if l.holder != sessionID {
		return RenewResult{}, c.rejectStale(sessionID, l)
	}
	if token != l.fencingToken {
		return RenewResult{}, ErrStaleFencingToken
	}

	now := c.now()
	newExpiry := now + d
	// Clock regression guard: a renewal never moves the deadline backwards
	// and always advances it relative to the persisted deadline, so a
	// rewound clock can neither shorten a lease silently nor keep it alive
	// past the intended duration by repeated renewals.
	if newExpiry <= l.expiresAt {
		newExpiry = l.expiresAt + d
	}

	entry := walEntry{
		Kind:         entryLockRenewed,
		LockKey:      key,
		SessionID:    sessionID,
		FencingToken: token,
		ExpiresAt:    newExpiry,
		TTL:          d,
		RequestID:    requestID,
	}
	if _, err := c.log.Append(entry); err != nil {
		return RenewResult{}, err
	}
	l.expiresAt = newExpiry
	r := RenewResult{LockKey: key, FencingToken: token, ExpiresAt: newExpiry}
	c.idemPut(sessionID, requestID, r)
	return r, nil
}

// Release frees key for sessionID. token must match the current fencing
// token. Releasing an expired lock (not yet re-acquired) returns
// ErrLeaseExpired; releasing a lock now owned by another session returns
// ErrNotOwner; an old token returns ErrStaleFencingToken. A repeated
// release with the same requestID is idempotent.
func (c *Coordinator) Release(sessionID, key string, token uint64, requestID string) (ReleaseResult, error) {
	if key == "" {
		return ReleaseResult{}, ErrEmptyLockKey
	}
	if sessionID == "" {
		return ReleaseResult{}, ErrEmptySessionID
	}
	if token == 0 {
		return ReleaseResult{}, ErrInvalidToken
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.idemGet(sessionID, requestID); ok {
		r := v.(ReleaseResult)
		r.Replayed = true
		return r, nil
	}
	if _, ok := c.sessions[sessionID]; !ok {
		return ReleaseResult{}, ErrSessionNotFound
	}
	l := c.lock(key)
	c.expireIfNeeded(key, l)

	if l.holder != sessionID {
		return ReleaseResult{}, c.rejectStale(sessionID, l)
	}
	if token != l.fencingToken {
		return ReleaseResult{}, ErrStaleFencingToken
	}

	if _, err := c.log.Append(walEntry{
		Kind:         entryLockReleased,
		LockKey:      key,
		SessionID:    sessionID,
		FencingToken: token,
		RequestID:    requestID,
	}); err != nil {
		return ReleaseResult{}, err
	}
	l.holder = ""
	l.fencingToken = 0
	l.expiresAt = 0
	l.lastHolder = sessionID
	l.lastLossByExpiry = false
	c.publishLocked(key, l, EventRevoked, sessionID, 0)
	c.tryGrantHeadLocked(key, l)

	r := ReleaseResult{LockKey: key, Released: true}
	c.idemPut(sessionID, requestID, r)
	return r, nil
}

// WaitAcquire queues sessionID for key in FIFO order and blocks until the
// lock is granted, the context is cancelled, or the session ends.
func (c *Coordinator) WaitAcquire(ctx context.Context, sessionID, key string, ttl time.Duration, requestID string) (Grant, error) {
	if err := ctx.Err(); err != nil {
		return Grant{}, err
	}
	if key == "" {
		return Grant{}, ErrEmptyLockKey
	}
	if sessionID == "" {
		return Grant{}, ErrEmptySessionID
	}
	d := c.ttlNanos(ttl)
	if d <= 0 {
		return Grant{}, ErrInvalidLease
	}

	w := &waiter{
		sessionID: sessionID,
		ttl:       d,
		requestID: requestID,
		ch:        make(chan Grant, 1),
		ctx:       ctx,
	}

	c.mu.Lock()
	if _, ok := c.sessions[sessionID]; !ok {
		c.mu.Unlock()
		return Grant{}, ErrSessionNotFound
	}
	l := c.lock(key)
	c.expireIfNeeded(key, l)
	if l.holder == "" && len(l.waiters) == 0 {
		err := c.grantLocked(key, l, w)
		c.mu.Unlock()
		if err != nil {
			return Grant{}, err
		}
		select {
		case g := <-w.ch:
			return g, nil
		case <-ctx.Done():
			return Grant{}, ctx.Err()
		}
	}
	l.waiters = append(l.waiters, w)
	c.mu.Unlock()

	select {
	case g := <-w.ch:
		return g, nil
	case <-ctx.Done():
		c.removeWaiter(key, w)
		return Grant{}, ctx.Err()
	}
}

func (c *Coordinator) removeWaiter(key string, w *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.locks[key]; ok {
		for i, x := range l.waiters {
			if x == w {
				l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
				return
			}
		}
	}
}

// Snapshot is a point-in-time diagnostic view.
type Snapshot struct {
	Holder       string `json:"holder"`
	FencingToken uint64 `json:"fencing_token"`
	ExpiresAt    int64  `json:"expires_at"`
	Expired      bool   `json:"expired"`
	Waiters      int    `json:"waiters"`
}

// Peek returns the current state of key after running lazy expiration.
func (c *Coordinator) Peek(key string) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.lock(key)
	c.expireIfNeeded(key, l)
	return Snapshot{
		Holder:       l.holder,
		FencingToken: l.fencingToken,
		ExpiresAt:    l.expiresAt,
		Expired:      l.holder != "" && c.now() >= l.expiresAt,
		Waiters:      len(l.waiters),
	}
}

// HolderAt returns the current holder (empty if free/expired) and token.
func (c *Coordinator) HolderAt(key string) (string, uint64) {
	s := c.Peek(key)
	return s.Holder, s.FencingToken
}

// ---------- recovery ----------

type replayWAL interface {
	WALAppender
	Replay(func(payload []byte) error) error
}

func (c *Coordinator) recover(l replayWAL) error {
	return l.Replay(func(payload []byte) error {
		var e walEntry
		if err := json.Unmarshal(payload, &e); err != nil {
			return err
		}
		if e.NextToken > c.nextToken {
			c.nextToken = e.NextToken
		}
		switch e.Kind {
		case entrySessionOpened:
			c.sessions[e.SessionID] = struct{}{}
		case entrySessionEnded:
			delete(c.sessions, e.SessionID)
			delete(c.idem, e.SessionID)
			// Any lock still attributed to this session at the time it
			// ended must be free; later acquire entries supersede it.
			for k, l := range c.locks {
				if l.holder == e.SessionID {
					l.holder = ""
					l.fencingToken = 0
					l.expiresAt = 0
					l.lastHolder = e.SessionID
					l.lastLossByExpiry = false
				}
				_ = k
			}
		case entryLockAcquired:
			l := c.lock(e.LockKey)
			// A grant whose lease is already expired at restart is
			// recovered as last-owner history so a late Renew/Release
			// returns ErrLeaseExpired rather than ErrNotOwner.
			if e.ExpiresAt > c.now() {
				l.holder = e.SessionID
				l.fencingToken = e.FencingToken
				l.expiresAt = e.ExpiresAt
			} else {
				l.holder = ""
				l.fencingToken = 0
				l.expiresAt = 0
				l.lastLossByExpiry = true
			}
			l.lastHolder = e.SessionID
			g := Grant{
				LockKey: e.LockKey, SessionID: e.SessionID,
				FencingToken: e.FencingToken, ExpiresAt: e.ExpiresAt,
			}
			if e.RequestID != "" {
				c.idemPut(e.SessionID, e.RequestID, g)
			}
		case entryLockRenewed:
			l := c.lock(e.LockKey)
			// Renew entries are authoritative for the owner while the
			// session is open and the deadline is in the future.
			if _, alive := c.sessions[e.SessionID]; alive && e.ExpiresAt > c.now() {
				l.holder = e.SessionID
				l.fencingToken = e.FencingToken
				l.expiresAt = e.ExpiresAt
				l.lastHolder = e.SessionID
				r := RenewResult{
					LockKey: e.LockKey, FencingToken: e.FencingToken, ExpiresAt: e.ExpiresAt,
				}
				if e.RequestID != "" {
					c.idemPut(e.SessionID, e.RequestID, r)
				}
			}
		case entryLockReleased:
			l := c.lock(e.LockKey)
			l.holder = ""
			l.fencingToken = 0
			l.expiresAt = 0
			l.lastHolder = e.SessionID
			l.lastLossByExpiry = false
			r := ReleaseResult{LockKey: e.LockKey, Released: true}
			if e.RequestID != "" {
				c.idemPut(e.SessionID, e.RequestID, r)
			}
		}
		return nil
	})
}
