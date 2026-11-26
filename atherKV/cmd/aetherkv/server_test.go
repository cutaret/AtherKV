package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDaemonThreeNodeClusterForwardingAndPersistence(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "aetherkv-daemon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(baseDir) }()

	ids := []string{"n1", "n2", "n3"}
	nodes := make(map[string]*Node, 3)
	servers := make(map[string]*httptest.Server, 3)
	raftPeers := make(map[string]string, 3)
	httpPeers := make(map[string]string, 3)

	// Boot 3 real persistent nodes on ephemeral TCP & HTTP ports
	for i, id := range ids {
		n, err := NewNode(Config{
			NodeID:           id,
			DataDir:          filepath.Join(baseDir, id),
			RaftAddr:         "127.0.0.1:0",
			Peers:            map[string]string{"n1": "", "n2": "", "n3": ""},
			TickInterval:     10 * time.Millisecond,
			OpTimeout:        3 * time.Second,
			WiscKeyThreshold: 4096,
			Seed:             uint64(100 + i),
		})
		if err != nil {
			t.Fatalf("start node %s: %v", id, err)
		}
		srv := httptest.NewServer(n.Handler())
		nodes[id] = n
		servers[id] = srv
		raftPeers[id] = n.RaftAddr()
		httpPeers[id] = srv.URL
	}
	defer func() {
		for _, id := range ids {
			servers[id].Close()
			_ = nodes[id].Close()
		}
	}()

	for _, id := range ids {
		nodes[id].UpdatePeers(raftPeers, httpPeers)
	}

	// Wait for leader election across real TCP sockets
	var leaderID, followerID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if nodes[id].IsLeader() {
				leaderID = id
				break
			}
		}
		if leaderID != "" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if leaderID == "" {
		t.Fatal("cluster failed to elect a leader over TCP within 5s")
	}
	for _, id := range ids {
		if id != leaderID {
			followerID = id
			break
		}
	}

	client := &http.Client{Timeout: 4 * time.Second}
	followerURL := servers[followerID].URL

	// 1. PUT directly to a FOLLOWER — verifies transparent leader forwarding + WiscKey > 4 KiB
	largeVal := bytes.Repeat([]byte("A"), 8192)
	putReq, _ := http.NewRequest(http.MethodPut, followerURL+"/v1/kv/models/llama3/block0", bytes.NewReader(largeVal))
	putReq.Header.Set("X-Client-ID", "777")
	putReq.Header.Set("X-Seq-Num", "1")
	putResp, err := client.Do(putReq)
	if err != nil {
		t.Fatalf("PUT via follower failed: %v", err)
	}
	_ = putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT via follower status = %d, want 200", putResp.StatusCode)
	}
	if got := putResp.Header.Get("X-Aether-Forwarded-By"); got != followerID {
		t.Fatalf("expected X-Aether-Forwarded-By=%s, got %q", followerID, got)
	}

	// 2. Linearizable GET via follower returns exact 8 KiB WiscKey value
	getResp, err := client.Get(followerURL + "/v1/kv/models/llama3/block0")
	if err != nil {
		t.Fatalf("GET via follower failed: %v", err)
	}
	gotBody, _ := io.ReadAll(getResp.Body)
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK || !bytes.Equal(gotBody, largeVal) {
		t.Fatalf("GET mismatch: status=%d len=%d", getResp.StatusCode, len(gotBody))
	}

	// 3. PUT small keys, test CAS success + CAS conflict, and linearizable SCAN
	for _, kvPair := range [][2]string{{"k/1", "v1"}, {"k/2", "v2"}, {"k/3", "v3"}} {
		req, _ := http.NewRequest(http.MethodPut, servers[leaderID].URL+"/v1/kv/"+kvPair[0], bytes.NewBufferString(kvPair[1]))
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s failed: err=%v resp=%v", kvPair[0], err, resp)
		}
		_ = resp.Body.Close()
	}

	casBody := `{"expected":"v2","value":"v2-updated"}`
	casResp, err := client.Post(followerURL+"/v1/cas/k/2", "application/json", bytes.NewBufferString(casBody))
	if err != nil || casResp.StatusCode != http.StatusOK {
		t.Fatalf("CAS expected=v2 failed: err=%v status=%v", err, casResp)
	}
	_ = casResp.Body.Close()

	// Stale CAS must return 409 Conflict
	casConflict, err := client.Post(followerURL+"/v1/cas/k/2", "application/json", bytes.NewBufferString(casBody))
	if err != nil || casConflict.StatusCode != http.StatusConflict {
		t.Fatalf("stale CAS expected 409, got err=%v status=%v", err, casConflict)
	}
	_ = casConflict.Body.Close()

	// Linearizable range SCAN over [k/1, k/9)
	scanResp, err := client.Get(followerURL + "/v1/scan?start=k/1&end=k/9&limit=10")
	if err != nil || scanResp.StatusCode != http.StatusOK {
		t.Fatalf("SCAN failed: err=%v status=%v", err, scanResp)
	}
	var scanOut struct {
		Count int `json:"count"`
		Items []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"items"`
	}
	_ = json.NewDecoder(scanResp.Body).Decode(&scanOut)
	_ = scanResp.Body.Close()
	if scanOut.Count != 3 || scanOut.Items[1].Value != "v2-updated" {
		t.Fatalf("unexpected scan output: %+v", scanOut)
	}

	// 4. DELETE k/3 and confirm 404
	delReq, _ := http.NewRequest(http.MethodDelete, followerURL+"/v1/kv/k/3", nil)
	delResp, err := client.Do(delReq)
	if err != nil || delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE failed: err=%v status=%v", err, delResp)
	}
	_ = delResp.Body.Close()

	afterDel, err := client.Get(followerURL + "/v1/kv/k/3")
	if err != nil || afterDel.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE expected 404, got err=%v status=%v", err, afterDel)
	}
	_ = afterDel.Body.Close()

	// 5. Verify status endpoint responds cleanly
	stResp, err := client.Get(fmt.Sprintf("%s/v1/status", servers[leaderID].URL))
	if err != nil || stResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/status failed: err=%v status=%v", err, stResp)
	}
	_ = stResp.Body.Close()
}
