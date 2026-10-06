// Package wal is a tiny write-ahead log used to make coordinator state
// recoverable across process restarts.
//
// Each record is length-prefixed binary JSON:
//
//	[4 bytes big-endian payload length][JSON payload]
//
// Records are appended with O_APPEND and fsynced before the coordinator
// reports success to the caller, so an acknowledged operation is durable.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// ErrClosed is returned when operating on a closed log.
var ErrClosed = errors.New("wal: log closed")

// Log is an append-only, fsynced JSON record log.
type Log struct {
	mu sync.Mutex
	f  *os.File
}

// Open opens (creating if needed) the WAL at path. Parent directory is
// created automatically.
func Open(path string) (*Log, error) {
	if err := os.MkdirAll(fileDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{f: f}, nil
}

func fileDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}

// Append marshals v as JSON, appends it durably and returns the raw payload.
func (l *Log) Append(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil, ErrClosed
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := l.f.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := l.f.Write(payload); err != nil {
		return nil, err
	}
	if err := l.f.Sync(); err != nil {
		return nil, err
	}
	return payload, nil
}

// Replay invokes fn for every record in log order. A truncated final record
// (typical after a hard crash) terminates replay without error.
func (l *Log) Replay(fn func(payload []byte) error) error {
	l.mu.Lock()
	f := l.f
	l.mu.Unlock()
	if f == nil {
		return ErrClosed
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	defer f.Seek(0, io.SeekEnd)

	r := bufioReader(f)
	defer putBufioReader(r)
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n > 16<<20 {
			return fmt.Errorf("wal: implausible record length %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // torn trailing record
			}
			return err
		}
		if err := fn(payload); err != nil {
			return err
		}
	}
}

// Close releases the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
