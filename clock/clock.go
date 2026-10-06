// Package clock 提供可替换的时钟抽象。
// 生产环境使用真实时钟（依赖 Go 单调时钟，进程内不受墙钟回拨影响），
// 测试使用手动时钟，从而在不真实等待的前提下精确推进租约到期。
package clock

import (
	"sync"
	"time"
)

// Clock 是协调器使用的唯一时间来源。
type Clock interface {
	Now() time.Time
}

// Real 返回真实时钟。time.Now 携带单调时钟读数，
// 因此基于差值的租约判定不受系统墙钟回拨影响。
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Manual 是测试用手动时钟，只能显式推进，绝不自动流逝。
type Manual struct {
	mu  sync.Mutex
	now time.Time
}

func NewManual(start time.Time) *Manual { return &Manual{now: start} }

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Advance 将时钟前进 d。
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

// SetBack 模拟墙钟回拨：把时钟往回拨 d。
// 协调器实现必须保证回拨不会让租约被无限延长。
func (m *Manual) SetBack(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(-d)
}
