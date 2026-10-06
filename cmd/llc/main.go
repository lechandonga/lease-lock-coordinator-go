// Command llc runs the lease-lock coordinator locally and provides a small
// CLI client.
//
// Server:
//
//	llc serve --addr :8080 --wal ./data/llc.wal [--sweep 100ms]
//
// Client:
//
//	llc session open <sid>
//	llc session end  <sid>
//	llc acquire <sid> <key> [ttl-ms] [request-id] [wait-ms]
//	llc renew   <sid> <key> <token> [ttl-ms] [request-id]
//	llc release <sid> <key> <token> [request-id]
//	llc peek    <key>
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/internal/clock"
	"github.com/lechandonga/lease-lock-coordinator-go/internal/coordinator"
	"github.com/lechandonga/lease-lock-coordinator-go/internal/httpapi"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	if os.Args[1] == "serve" {
		serve(os.Args[2:])
		return
	}
	if err := client(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: llc serve ... | llc {session|acquire|renew|release|peek} ...")
	os.Exit(2)
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	walPath := fs.String("wal", "./data/llc.wal", "WAL file path")
	sweep := fs.Duration("sweep", 100*time.Millisecond, "lease sweep interval")
	ttl := fs.Duration("default-ttl", 15*time.Second, "default lease TTL")
	_ = fs.Parse(args)

	c, err := coordinator.Open(clock.Real{}, *walPath, *sweep, coordinator.WithDefaultTTL(*ttl))
	if err != nil {
		fatal(err)
	}
	srv := httpapi.New(c)
	fmt.Println("lease-lock coordinator listening on", *addr, "wal:", *walPath)
	fatal(http.ListenAndServe(*addr, srv.Mux))
}

func baseURL() string {
	if v := os.Getenv("LLC_ADDR"); v != "" {
		return "http://" + v
	}
	return "http://127.0.0.1:8080"
}

func client(args []string) error {
	switch args[0] {
	case "session":
		if len(args) < 3 {
			return fmt.Errorf("usage: session open|end <sid>")
		}
		method := http.MethodPost
		if args[1] == "end" {
			method = http.MethodDelete
		}
		return call(method, "/v1/sessions/"+args[2], nil, nil)
	case "acquire":
		if len(args) < 3 {
			return fmt.Errorf("usage: acquire <sid> <key> [ttl-ms] [request-id] [wait-ms]")
		}
		b := map[string]any{}
		if len(args) > 3 {
			n, _ := strconv.ParseInt(args[3], 10, 64)
			b["ttl_ms"] = n
		}
		requestID, wait := "", int64(0)
		if len(args) > 4 {
			requestID = args[4]
		}
		if len(args) > 5 {
			wait, _ = strconv.ParseInt(args[5], 10, 64)
			b["wait_ms"] = wait
		}
		return call(http.MethodPost, "/v1/locks/"+args[2], b, headers(args[1], 0, requestID))
	case "renew":
		if len(args) < 4 {
			return fmt.Errorf("usage: renew <sid> <key> <token> [ttl-ms] [request-id]")
		}
		tok, _ := strconv.ParseUint(args[3], 10, 64)
		b := map[string]any{"token": tok}
		if len(args) > 4 {
			n, _ := strconv.ParseInt(args[4], 10, 64)
			b["ttl_ms"] = n
		}
		requestID := ""
		if len(args) > 5 {
			requestID = args[5]
		}
		return call(http.MethodPatch, "/v1/locks/"+args[2], b, headers(args[1], tok, requestID))
	case "release":
		if len(args) < 4 {
			return fmt.Errorf("usage: release <sid> <key> <token> [request-id]")
		}
		tok, _ := strconv.ParseUint(args[3], 10, 64)
		b := map[string]any{"token": tok}
		requestID := ""
		if len(args) > 4 {
			requestID = args[4]
		}
		return call(http.MethodDelete, "/v1/locks/"+args[2], b, headers(args[1], tok, requestID))
	case "peek":
		if len(args) < 2 {
			return fmt.Errorf("usage: peek <key>")
		}
		return call(http.MethodGet, "/v1/locks/"+args[1], nil, nil)
	default:
		usage()
		return nil
	}
}

func headers(session string, token uint64, requestID string) http.Header {
	h := http.Header{}
	h.Set("X-Session-Id", session)
	if token > 0 {
		h.Set("X-Fencing-Token", strconv.FormatUint(token, 10))
	}
	if requestID != "" {
		h.Set("X-Request-Id", requestID)
	}
	return h
}

func call(method, path string, body any, h http.Header) error {
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL()+path, rdr)
	if err != nil {
		return err
	}
	if h != nil {
		req.Header = h
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var pretty bytes.Buffer
	if json.Indent(&pretty, raw, "", "  ") == nil {
		raw = pretty.Bytes()
	}
	fmt.Println(string(raw))
	if resp.StatusCode >= 400 {
		os.Exit(1)
	}
	return nil
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
