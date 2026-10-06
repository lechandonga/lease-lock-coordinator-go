package coordinator

import "time"

// Grant is the result of a successful Acquire (or an idempotent replay of
// one). FencingToken is strictly increasing for every successful acquire of
// a given lock across the whole service lifetime (including restarts).
type Grant struct {
	LockKey      string `json:"lock_key"`
	SessionID    string `json:"session_id"`
	FencingToken uint64 `json:"fencing_token"`
	// ExpiresAt is the absolute Unix-nano deadline of the lease.
	ExpiresAt int64 `json:"expires_at"`
	// Replayed is true when this result was served from the idempotency
	// cache (duplicate request) rather than freshly granted.
	Replayed bool `json:"replayed,omitempty"`
}

// RenewResult describes a successful heartbeat.
type RenewResult struct {
	LockKey      string `json:"lock_key"`
	FencingToken uint64 `json:"fencing_token"`
	ExpiresAt    int64  `json:"expires_at"`
	Replayed     bool   `json:"replayed,omitempty"`
}

// ReleaseResult describes a release outcome. Repeated releases with the same
// (session, request) pair are idempotent.
type ReleaseResult struct {
	LockKey  string `json:"lock_key"`
	Released bool   `json:"released"`
	Replayed bool   `json:"replayed,omitempty"`
}

// Event is an element of a lock's ordered notification stream.
//
// Seq is strictly increasing per lock. A waiter that observes GRANTED for
// its own attempt acquires the lock; any other delivery is an advisory
// "try again" signal. The stream tolerates duplicate deliveries but never
// reorders, and a granted attempt is never granted twice.
type Event struct {
	LockKey      string
	Seq          uint64
	Kind         EventKind
	SessionID    string
	FencingToken uint64 // valid when Kind == EventGranted
}

// EventKind enumerates lock notification kinds.
type EventKind uint8

const (
	// EventRevoked means the current holder lost the lock (lease expiry,
	// explicit release, or session end).
	EventRevoked EventKind = iota + 1
	// EventGranted means the lock changed owner.
	EventGranted
)

// ttl default, exposed for tests.
const defaultTTL = 15 * time.Second
