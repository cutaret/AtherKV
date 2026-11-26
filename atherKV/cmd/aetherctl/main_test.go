package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestAetherctlFailoverAndCRUD(t *testing.T) {
	// Node 1 simulates a partitioned node returning 503 Service Unavailable
	unavail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
	}))
	defer unavail.Close()

	// Node 2 simulates an active leader
	var mu sync.Mutex
	store := make(map[string]string)
	var seenClientID, seenSeq string

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":       "n2",
			"is_leader":     true,
			"applied_index": 42,
			"total_fsyncs":  12,
		})
	})
	mux.HandleFunc("PUT /v1/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		store[r.PathValue("key")] = string(b)
		seenClientID = r.Header.Get("X-Client-ID")
		seenSeq = r.Header.Get("X-Seq-Num")
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"applied": true})
	})
	mux.HandleFunc("GET /v1/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		v, ok := store[r.PathValue("key")]
		mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(v))
	})
	mux.HandleFunc("POST /v1/cas/{key...}", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Expected string `json:"expected"`
			Value    string `json:"value"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		defer mu.Unlock()
		k := r.PathValue("key")
		if store[k] != req.Expected {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"applied": false})
			return
		}
		store[k] = req.Value
		_ = json.NewEncoder(w).Encode(map[string]any{"applied": true})
	})
	mux.HandleFunc("GET /v1/scan", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count": 1,
			"items": []map[string]string{{"key": "a/1", "value": "v2"}},
		})
	})
	mux.HandleFunc("DELETE /v1/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		_, existed := store[r.PathValue("key")]
		delete(store, r.PathValue("key"))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"previous_found": existed})
	})

	leader := httptest.NewServer(mux)
	defer leader.Close()

	eps := unavail.URL + "," + leader.URL

	exec := func(stdin string, args ...string) (int, string, string) {
		var out, errBuf bytes.Buffer
		fullArgs := append([]string{"-e", eps}, args...)
		code := run(fullArgs, strings.NewReader(stdin), &out, &errBuf)
		return code, out.String(), errBuf.String()
	}

	// 1. PUT via stdin fails over from 503 node to leader and sends idempotency headers
	if code, out, errOut := exec("hello-blob", "put", "a/1", "-"); code != 0 || !strings.Contains(out, "OK (10 bytes)") {
		t.Fatalf("put failed: code=%d out=%q err=%q", code, out, errOut)
	}
	if seenClientID == "" || seenSeq == "" {
		t.Fatalf("expected X-Client-ID and X-Seq-Num headers, got %q / %q", seenClientID, seenSeq)
	}

	// 2. GET returns exact value
	if code, out, _ := exec("", "get", "a/1"); code != 0 || strings.TrimSpace(out) != "hello-blob" {
		t.Fatalf("get failed: code=%d out=%q", code, out)
	}

	// 3. CAS success -> 0, CAS conflict -> 2
	if code, out, _ := exec("", "cas", "a/1", "hello-blob", "v2"); code != 0 || strings.TrimSpace(out) != "APPLIED" {
		t.Fatalf("cas success failed: code=%d out=%q", code, out)
	}
	if code, _, _ := exec("", "cas", "a/1", "wrong", "v3"); code != 2 {
		t.Fatalf("cas conflict expected exit code 2, got %d", code)
	}

	// 4. SCAN, STATUS, HEALTH, DEL
	if code, out, _ := exec("", "scan", "-start", "a/", "-limit", "5"); code != 0 || !strings.Contains(out, "a/1") {
		t.Fatalf("scan failed: code=%d out=%q", code, out)
	}
	if code, out, _ := exec("", "status"); code != 0 || !strings.Contains(out, "LEADER") || !strings.Contains(out, "OFFLINE") {
		t.Fatalf("status table unexpected: code=%d out=%q", code, out)
	}
	if code, _, _ := exec("", "health"); code != 0 {
		t.Fatalf("health expected 0, got %d", code)
	}
	if code, out, _ := exec("", "del", "a/1"); code != 0 || strings.TrimSpace(out) != "DELETED" {
		t.Fatalf("del failed: code=%d out=%q", code, out)
	}
}
