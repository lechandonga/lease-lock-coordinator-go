package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplayAndTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "log.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	type rec struct{ V int }
	want := []int{1, 2, 3}
	for _, v := range want {
		if _, err := l.Append(rec{v}); err != nil {
			t.Fatal(err)
		}
	}
	got := []int{}
	if err := l.Replay(func(p []byte) error {
		var r rec
		if err := jsonUnmarshal(p, &r); err != nil {
			return err
		}
		got = append(got, r.V)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("replay mismatch: %v", got)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a torn final record: append a valid header claiming 64
	// bytes followed by only 10 bytes. Replay must stop cleanly.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	hdr := []byte{0, 0, 0, 64}
	if _, err := f.Write(append(hdr, make([]byte, 10)...)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	got = nil
	if err := l2.Replay(func(p []byte) error {
		var r rec
		if err := jsonUnmarshal(p, &r); err != nil {
			return err
		}
		got = append(got, r.V)
		return nil
	}); err != nil {
		t.Fatalf("torn tail should be tolerated: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("torn tail changed committed records: %v", got)
	}
}
