package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func encodeRESP(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

func readRESPValue(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return "", io.ErrUnexpectedEOF
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "NULL", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			v, err := readRESPValue(r)
			if err != nil {
				return "", err
			}
			parts = append(parts, v)
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	default:
		return line, nil
	}
}

func TestRESP2ThreeNodeFollowerForwardingAndWiscKey(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "aetherkv-resp-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(baseDir) }()

	ids := []string{"n1", "n2", "n3"}
	nodes := make(map[string]*Node, 3)
	httpSrvs := make(map[string]*httptest.Server, 3)
	respSrvs := make(map[string]*RESPServer, 3)
	raftPeers := make(map[string]string, 3)
	httpPeers := make(map[string]string, 3)

	for i, id := range ids {
		n, err := NewNode(Config{
			NodeID:           id,
			DataDir:          filepath.Join(baseDir, id),
			RaftAddr:         "127.0.0.1:0",
			Peers:            map[string]string{"n1": "", "n2": "", "n3": ""},
			TickInterval:     10 * time.Millisecond,
			OpTimeout:        3 * time.Second,
			WiscKeyThreshold: 4096,
			Seed:             uint64(200 + i),
		})
		if err != nil {
			t.Fatalf("start node %s: %v", id, err)
		}
		hs := httptest.NewServer(n.Handler())
		rs, err := NewRESPServer(n, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("start resp %s: %v", id, err)
		}
		nodes[id] = n
		httpSrvs[id] = hs
		respSrvs[id] = rs
		raftPeers[id] = n.RaftAddr()
		httpPeers[id] = hs.URL
	}
	defer func() {
		for _, id := range ids {
			_ = respSrvs[id].Close()
			httpSrvs[id].Close()
			_ = nodes[id].Close()
		}
	}()

	for _, id := range ids {
		nodes[id].UpdatePeers(raftPeers, httpPeers)
	}

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
		t.Fatal("cluster failed to elect leader within 5s")
	}
	for _, id := range ids {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// Connect over raw TCP directly to a FOLLOWER's RESP2 port
	conn, err := net.DialTimeout("tcp", respSrvs[followerID].Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial follower RESP2: %v", err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	send := func(args ...string) string {
		t.Helper()
		if _, err := conn.Write(encodeRESP(args...)); err != nil {
			t.Fatalf("write %v: %v", args, err)
		}
		got, err := readRESPValue(r)
		if err != nil {
			t.Fatalf("read response for %v: %v", args, err)
		}
		return got
	}

	// 1. Handshake & inline PING
	if got := send("COMMAND", "DOCS"); got != "[]" {
		t.Fatalf("COMMAND DOCS = %q, want []", got)
	}
	if got := send("PING"); got != "+PONG" {
		t.Fatalf("PING = %q, want +PONG", got)
	}
	if _, err := conn.Write([]byte("PING hello-inline\r\n")); err != nil {
		t.Fatal(err)
	}
	if got, err := readRESPValue(r); err != nil || got != "hello-inline" {
		t.Fatalf("inline PING = %q (err=%v), want hello-inline", got, err)
	}

	// 2. SET + GET large 8 KiB WiscKey value via follower
	bigVal := strings.Repeat("Z", 8192)
	if got := send("SET", "kv/llama3/token0", bigVal); got != "+OK" {
		t.Fatalf("SET bigVal = %q, want +OK", got)
	}
	if got := send("GET", "kv/llama3/token0"); got != bigVal {
		t.Fatalf("GET bigVal len=%d, want %d", len(got), len(bigVal))
	}

	// 3. MSET, MGET, EXISTS, KEYS, SCAN, DEL
	if got := send("MSET", "user:1", "alice", "user:2", "bob", "other:1", "x"); got != "+OK" {
		t.Fatalf("MSET = %q, want +OK", got)
	}
	if got := send("MGET", "user:1", "missing", "user:2"); got != "[alice,NULL,bob]" {
		t.Fatalf("MGET = %q, want [alice,NULL,bob]", got)
	}
	if got := send("EXISTS", "user:1", "user:2", "nope"); got != ":2" {
		t.Fatalf("EXISTS = %q, want :2", got)
	}
	if got := send("KEYS", "user:*"); got != "[user:1,user:2]" {
		t.Fatalf("KEYS user:* = %q, want [user:1,user:2]", got)
	}
	if got := send("SCAN", "0", "MATCH", "user:*", "COUNT", "10"); got != "[0,[user:1,user:2]]" {
		t.Fatalf("SCAN = %q, want [0,[user:1,user:2]]", got)
	}
	if got := send("DEL", "user:1", "missing"); got != ":1" {
		t.Fatalf("DEL = %q, want :1", got)
	}
	if got := send("GET", "user:1"); got != "NULL" {
		t.Fatalf("GET after DEL = %q, want NULL", got)
	}

	// 4. INFO reports AetherKV metadata
	info := send("INFO")
	if !strings.Contains(info, "engine:aetherkv") || !strings.Contains(info, "role:follower") {
		t.Fatalf("unexpected INFO payload: %q", info)
	}
}
