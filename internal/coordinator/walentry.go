package coordinator

// walEntry is the single persisted record type of the write-ahead log.
// Exactly one of the payloads is set. Every state mutation is written and
// fsynced before being applied to memory or reported as successful.
type walEntry struct {
	Kind entryKind `json:"kind"`

	// Sequence fields (global monotonic counters used on replay).
	NextToken uint64 `json:"next_token,omitempty"`

	// SessionOpened / SessionEnded
	SessionID string `json:"session_id,omitempty"`

	// LockAcquired
	LockKey      string `json:"lock_key,omitempty"`
	FencingToken uint64 `json:"fencing_token,omitempty"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`
	TTL          int64  `json:"ttl,omitempty"`
	RequestID    string `json:"request_id,omitempty"`

	// LockRenewed
	// (LockKey/SessionID/FencingToken/ExpiresAt/RequestID reused)

	// LockReleased
	// (LockKey/SessionID/RequestID reused)
}

type entryKind uint8

const (
	entrySessionOpened entryKind = iota + 1
	entrySessionEnded
	entryLockAcquired
	entryLockRenewed
	entryLockReleased
)
