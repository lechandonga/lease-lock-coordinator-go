// Package httpapi exposes the coordinator over a small local HTTP API.
// Waiting uses long polling, so the whole service is usable with curl and
// the test suite exercises the real handler stack without real networks by
// using httptest.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/internal/coordinator"
)

// Server wires the coordinator to HTTP.
type Server struct {
	C   *coordinator.Coordinator
	Mux *http.ServeMux
}

// New builds the HTTP routes on top of c.
func New(c *coordinator.Coordinator) *Server {
	s := &Server{C: c, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("/v1/sessions/", s.handleSession)
	s.Mux.HandleFunc("/v1/locks/", s.handleLock)
}

type body struct {
	TTLMillis  int64  `json:"ttl_ms,omitempty"`
	Token      uint64 `json:"token,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	WaitMillis int64  `json:"wait_ms,omitempty"`
}

func parseBody(r *http.Request) body {
	var b body
	_ = json.NewDecoder(r.Body).Decode(&b)
	return b
}

func ttl(b body) time.Duration {
	if b.TTLMillis <= 0 {
		return 0
	}
	return time.Duration(b.TTLMillis) * time.Millisecond
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errStatus(err error) int {
	switch {
	case errors.Is(err, coordinator.ErrLockHeldByOther):
		return http.StatusConflict
	case errors.Is(err, coordinator.ErrLeaseExpired):
		return http.StatusGone
	case errors.Is(err, coordinator.ErrStaleFencingToken):
		return http.StatusPreconditionFailed
	case errors.Is(err, coordinator.ErrNotOwner), errors.Is(err, coordinator.ErrSessionNotFound):
		return http.StatusNotFound
	case errors.Is(err, coordinator.ErrInvalidLease), errors.Is(err, coordinator.ErrEmptyLockKey),
		errors.Is(err, coordinator.ErrEmptySessionID), errors.Is(err, coordinator.ErrInvalidToken):
		return http.StatusBadRequest
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, errStatus(err), map[string]string{"error": err.Error()})
}

// /v1/sessions/{id}  POST=open  DELETE=end
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
	if id == "" || strings.Contains(id, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "session id required"})
		return
	}
	switch r.Method {
	case http.MethodPost:
		if err := s.C.OpenSession(id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"session_id": id, "status": "open"})
	case http.MethodDelete:
		if err := s.C.EndSession(id); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"session_id": id, "status": "ended"})
	default:
		w.Header().Set("Allow", "POST, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// /v1/locks/{key}   POST=acquire (wait_ms>0 long-polls),
// PATCH=renew, DELETE=release, GET=peek.
// Sessions/token/request_id come via headers for renew/release and body for
// acquire; both are accepted.
func (s *Server) handleLock(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/locks/")
	if key == "" || strings.Contains(key, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "lock key required"})
		return
	}
	b := parseBody(r)
	session := r.Header.Get("X-Session-Id")
	if session == "" {
		session = r.URL.Query().Get("session")
	}
	token := b.Token
	if t := r.Header.Get("X-Fencing-Token"); t != "" {
		if n, err := strconv.ParseUint(t, 10, 64); err == nil {
			token = n
		}
	}
	requestID := r.Header.Get("X-Request-Id")
	if requestID == "" {
		requestID = b.RequestID
	}

	switch r.Method {
	case http.MethodPost:
		if b.WaitMillis > 0 {
			ctx, cancel := contextWithTimeout(r, b.WaitMillis)
			defer cancel()
			g, err := s.C.WaitAcquire(ctx, session, key, ttl(b), requestID)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, g)
			return
		}
		g, err := s.C.Acquire(session, key, ttl(b), requestID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, g)
	case http.MethodPatch:
		res, err := s.C.Renew(session, key, token, ttl(b), requestID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case http.MethodDelete:
		res, err := s.C.Release(session, key, token, requestID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.C.Peek(key))
	default:
		w.Header().Set("Allow", "POST, PATCH, DELETE, GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
