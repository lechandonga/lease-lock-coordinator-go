package coordinator

import "errors"

// Sentinel errors give callers stable, distinguishable failure reasons.
var (
	// ErrLockHeldByOther is returned by Acquire when the lock is currently
	// held by a live session (lease not expired).
	ErrLockHeldByOther = errors.New("lock is held by another session")

	// ErrNotOwner is returned when the caller never held (or no longer
	// holds) the named lock, e.g. it expired and was granted to someone
	// else. Expired-holder renew/release maps here when the lock has moved
	// on; see ErrLeaseExpired.
	ErrNotOwner = errors.New("session is not the current lock owner")

	// ErrLeaseExpired is returned when the caller was the last owner but
	// its lease has expired (and nobody else has acquired the lock yet).
	ErrLeaseExpired = errors.New("lease has expired")

	// ErrStaleFencingToken is returned when a request carries a fencing
	// token that is older than the current owner's token. Late requests
	// from a superseded owner are rejected with this error, even if their
	// lease would otherwise look valid to them.
	ErrStaleFencingToken = errors.New("stale fencing token")

	// ErrSessionNotFound is returned when the session does not exist or
	// has already been ended.
	ErrSessionNotFound = errors.New("session not found")

	// ErrInvalidLease is returned for non-positive TTLs.
	ErrInvalidLease = errors.New("lease ttl must be positive")

	// ErrEmptyLockKey is returned for an empty lock name.
	ErrEmptyLockKey = errors.New("lock key must not be empty")

	// ErrEmptySessionID is returned for an empty session id.
	ErrEmptySessionID = errors.New("session id must not be empty")

	// ErrInvalidToken is returned when a non-positive token is supplied.
	ErrInvalidToken = errors.New("fencing token must be positive")
)
