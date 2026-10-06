package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/internal/clock"
	"github.com/lechandonga/lease-lock-coordinator-go/internal/coordinator"
)

// 所有 HTTP 用例都直接调用 handler（httptest.NewRecorder），不监听真实
// 端口；长轮询在 goroutine 内执行，主测试用 Peek + Gosched 观察入队，
// 不做真实 sleep 等待。
type apiResp struct {
	status int
	body   map[string]any
}

func (s *Server) do(t *testing.T, ctx context.Context, method, target, sid string, body any, headers ...[2]string) apiResp {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	var req *http.Request
	if ctx != nil {
		req = httptest.NewRequestWithContext(ctx, method, target, rdr)
	} else {
		req = httptest.NewRequest(method, target, rdr)
	}
	if sid != "" {
		req.Header.Set("X-Session-Id", sid)
	}
	for _, h := range headers {
		req.Header.Set(h[0], h[1])
	}
	rec := httptest.NewRecorder()
	s.Mux.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return apiResp{rec.Code, out}
}

func TestHTTPBusiness(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC))
	c, err := coordinator.Open(fc, t.TempDir()+"/api.wal", 0, coordinator.WithDefaultTTL(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s := New(c)

	t.Run("会话/获取/占用冲突 409", func(t *testing.T) {
		if r := s.do(t, nil, http.MethodPost, "/v1/sessions/s1", "", nil); r.status != 200 {
			t.Fatalf("open session: %d", r.status)
		}
		if r := s.do(t, nil, http.MethodPost, "/v1/sessions/s2", "", nil); r.status != 200 {
			t.Fatalf("open session: %d", r.status)
		}
		g := s.do(t, nil, http.MethodPost, "/v1/locks/L", "s1",
			map[string]any{"ttl_ms": 1000, "request_id": "a1"})
		if g.status != 200 || g.body["fencing_token"].(float64) != 1 {
			t.Fatalf("first acquire: %+v", g)
		}
		busy := s.do(t, nil, http.MethodPost, "/v1/locks/L", "s2",
			map[string]any{"ttl_ms": 1000})
		if busy.status != http.StatusConflict {
			t.Fatalf("want 409, got %d (%v)", busy.status, busy.body)
		}
	})

	t.Run("幂等重放不产生新令牌", func(t *testing.T) {
		g := s.do(t, nil, http.MethodPost, "/v1/locks/L", "s1",
			map[string]any{"ttl_ms": 1000, "request_id": "a1"})
		if g.status != 200 || g.body["fencing_token"].(float64) != 1 || g.body["replayed"] != true {
			t.Fatalf("expected replay token 1: %+v", g)
		}
	})

	t.Run("旧令牌续租 412 与过期后续租 410", func(t *testing.T) {
		stale := s.do(t, nil, http.MethodPatch, "/v1/locks/L", "s1",
			map[string]any{"ttl_ms": 1000, "token": 999})
		if stale.status != http.StatusPreconditionFailed {
			t.Fatalf("want 412, got %d", stale.status)
		}
		fc.Advance(2 * time.Second)
		gone := s.do(t, nil, http.MethodPatch, "/v1/locks/L", "s1",
			map[string]any{"ttl_ms": 1000, "token": 1})
		if gone.status != http.StatusGone {
			t.Fatalf("want 410, got %d", gone.status)
		}
	})

	t.Run("长轮询等待被释放唤醒", func(t *testing.T) {
		g := s.do(t, nil, http.MethodPost, "/v1/locks/L", "s1",
			map[string]any{"ttl_ms": 5000})
		if g.status != 200 {
			t.Fatalf("re-acquire: %+v", g)
		}
		tok := uint64(g.body["fencing_token"].(float64))

		got := make(chan apiResp, 1)
		go func() {
			got <- s.do(t, context.Background(), http.MethodPost, "/v1/locks/L", "s2",
				map[string]any{"ttl_ms": 5000, "wait_ms": 30000})
		}()

		for c.Peek("L").Waiters != 1 {
			runtime.Gosched()
		}
		rel := s.do(t, nil, http.MethodDelete, "/v1/locks/L", "s1",
			map[string]any{"token": tok})
		if rel.status != 200 {
			t.Fatalf("release: %d", rel.status)
		}
		for {
			select {
			case pr := <-got:
				if pr.status != 200 || pr.body["session_id"] != "s2" {
					t.Fatalf("long poll grant wrong: %+v", pr)
				}
				return
			default:
				runtime.Gosched()
			}
		}
	})

	t.Run("等待被取消返回 409", func(t *testing.T) {
		s.do(t, nil, http.MethodPost, "/v1/locks/M", "s1",
			map[string]any{"ttl_ms": 5000})
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消，不经过真实等待
		r := s.do(t, ctx, http.MethodPost, "/v1/locks/M", "s2",
			map[string]any{"ttl_ms": 1000, "wait_ms": 60000})
		if r.status != http.StatusConflict {
			t.Fatalf("want 409 on cancelled wait, got %d (%v)", r.status, r.body)
		}
	})

	t.Run("非持有者释放 404", func(t *testing.T) {
		r := s.do(t, nil, http.MethodDelete, "/v1/locks/M", "s2",
			map[string]any{"token": 5})
		if r.status != http.StatusNotFound {
			t.Fatalf("want 404, got %d (%v)", r.status, r.body)
		}
	})
}
