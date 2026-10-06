// Package clock defines the time source used by the coordinator.
//
// All lease decisions are based on a monotonic tick (nanoseconds). The fake
// clock used by tests implements the same interface, which makes every
// expiration / renewal scenario deterministic: no real waiting is required.
package clock

import "time"

// Clock is the subset of time functionality the coordinator depends on.
type Clock interface {
	// Now returns the current wall-clock time (used for logging only).
	Now() time.Time
	// Tick returns an absolute nanosecond timestamp (Unix nanos) used for
	// all lease-expiry decisions. Absolute values are persisted so that
	// expiries remain comparable across process restarts; coordinator code
	// additionally guards against non-monotonic movement on renew.
	Tick() int64
}

// Real uses the host monotonic clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) Tick() int64 { return time.Now().UnixNano() }

// Fake is a manually controlled clock for tests.
type Fake struct {
	wall time.Time
	tick int64
}

// NewFake returns a fake clock anchored at the given wall time and tick.
func NewFake(start time.Time) *Fake {
	return &Fake{wall: start, tick: start.UnixNano()}
}

func (f *Fake) Now() time.Time { return f.wall }

func (f *Fake) Tick() int64 { return f.tick }

// Advance moves both the monotonic tick and wall clock forward by d.
// Advancing by a negative duration panics: a fake clock never goes backwards.
func (f *Fake) Advance(d time.Duration) {
	if d < 0 {
		panic("clock: fake clock cannot move backwards")
	}
	f.tick += int64(d)
	f.wall = f.wall.Add(d)
}

// Rewind simulates a wall-clock / monotonic regression after a host restart
// (e.g. migration to another machine whose clock is behind).
func (f *Fake) Rewind(d time.Duration) {
	f.tick -= int64(d)
	f.wall = f.wall.Add(-d)
}
