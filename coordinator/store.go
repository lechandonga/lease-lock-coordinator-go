package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Store 是 JSON 快照持久化。每次状态变更后整体落盘（tmp + rename 原子替换），
// 适合本地运行与可复现验证；重启后从快照一致恢复。
type Store struct {
	path string
}

func NewStore(path string) *Store { return &Store{path: path} }

type snapshot struct {
	Fencing   uint64                      `json:"fencing"`
	HighWater time.Time                   `json:"highWater"`
	Sessions  []string                    `json:"sessions"`
	Locks     map[string]lockSnap         `json:"locks"`
	Requests  map[string]requestRecord    `json:"requests"`
}

type lockSnap struct {
	Holder     string    `json:"holder"`
	Token      uint64    `json:"token"`
	ExpiresAt  time.Time `json:"expiresAt"`
	LastHolder string    `json:"lastHolder"`
	Waiters    []waiter  `json:"waiters"`
}

func (s *Store) Load() (*snapshot, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot %s: %w", s.path, err)
	}
	return &snap, nil
}

func (s *Store) save(snap *snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// persistLocked 在持锁状态下落盘当前状态。
func (c *Coordinator) persistLocked() {
	if c.st == nil {
		return
	}
	snap := &snapshot{
		Fencing:   c.fencing,
		HighWater: c.highWater,
		Locks:     map[string]lockSnap{},
		Requests:  map[string]requestRecord{},
	}
	for id := range c.sessions {
		snap.Sessions = append(snap.Sessions, id)
	}
	for name, l := range c.locks {
		snap.Locks[name] = lockSnap{
			Holder: l.Holder, Token: l.Token, ExpiresAt: l.ExpiresAt,
			LastHolder: l.LastHolder, Waiters: l.Waiters,
		}
	}
	for id, rec := range c.requests {
		r := *rec
		r.waitCh = nil
		snap.Requests[id] = r
	}
	if err := c.st.save(snap); err != nil {
		c.logf("persist failed: %v", err)
	}
}

// restore 从快照恢复状态，并保证时钟高水位不回退。
func (c *Coordinator) restore(snap *snapshot) {
	c.fencing = snap.Fencing
	c.highWater = snap.HighWater
	for _, id := range snap.Sessions {
		c.sessions[id] = true
	}
	for name, ls := range snap.Locks {
		c.locks[name] = &lockState{
			Name: name, Holder: ls.Holder, Token: ls.Token,
			ExpiresAt: ls.ExpiresAt, LastHolder: ls.LastHolder, Waiters: ls.Waiters,
		}
	}
	for id, r := range snap.Requests {
		rec := r
		if !rec.Done {
			rec.waitCh = make(chan Grant, 1) // 重启后为未完成等待者重建通知通道
		}
		c.requests[id] = &rec
	}
	c.logf("snapshot restored: fencing=%d highWater=%s", c.fencing, c.highWater.Format(time.RFC3339Nano))
}
