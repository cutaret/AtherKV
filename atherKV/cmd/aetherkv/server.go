package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/transport/tcp"
	"github.com/cutaret/aetherkv/vfs"
)

// Config configures a persistent AetherKV daemon node.
type Config struct {
	NodeID           string
	DataDir          string
	HTTPAddr         string
	RaftAddr         string
	Peers            map[string]string // nodeID -> raft host:port
	HTTPPeers        map[string]string // nodeID -> http://host:port (for leader forwarding)
	TickInterval     time.Duration
	OpTimeout        time.Duration
	WiscKeyThreshold int
	SingleFsync      bool // Option B: Raft WAL sync + FlushedIndex truncation fence
	Seed             uint64
}

type wallClock struct {
	start time.Time
}

func newWallClock() *wallClock {
	return &wallClock{start: time.Now()}
}

func (w *wallClock) Now() uint64 {
	// Monotonic offset avoids returning 0 at startup.
	return uint64(time.Since(w.start).Nanoseconds()) + 1_000_000_000
}

type writeOutcome struct {
	res statemachine.Result
	ok  bool
}

type readOutcome struct {
	val   []byte
	found bool
	ok    bool
}

type scanOutcome struct {
	items []statemachine.KV
	err   error
	ok    bool
}

type statusPayload struct {
	NodeID       string            `json:"node_id"`
	IsLeader     bool              `json:"is_leader"`
	AppliedIndex uint64            `json:"applied_index"`
	TotalFsyncs  uint64            `json:"total_fsyncs"`
	RaftAddr     string            `json:"raft_addr"`
	HTTPAddr     string            `json:"http_addr"`
	Peers        map[string]string `json:"peers"`
	Raft         any               `json:"raft"`
}

// Node hosts a single persistent Raft+LSM replica behind a single-threaded actor loop.
type Node struct {
	cfg        Config
	rep        *kv.Replica
	transport  *tcp.Transport
	httpClient *http.Client
	clientID   uint64
	seqCounter atomic.Uint64
	readID     atomic.Uint64

	msgCh  chan sim.Message
	cmdCh  chan func()
	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once

	leaderMu     sync.RWMutex
	cachedLeader string
}

func randomSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}

// NewNode initializes OSFS storage, TCP Raft transport, and the actor loop.
func NewNode(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("node ID is required")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 10 * time.Millisecond
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	if cfg.Seed == 0 {
		cfg.Seed = randomSeed()
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	n := &Node{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: cfg.OpTimeout},
		clientID:   randomSeed(),
		msgCh:      make(chan sim.Message, 4096),
		cmdCh:      make(chan func(), 1024),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}

	trans, err := tcp.NewTransport(cfg.NodeID, cfg.RaftAddr, func(msg sim.Message) {
		select {
		case n.msgCh <- msg:
		case <-n.stopCh:
		default:
			// Drop under extreme backpressure; Raft retransmits safely.
		}
	})
	if err != nil {
		return nil, fmt.Errorf("listen raft tcp %s: %w", cfg.RaftAddr, err)
	}
	n.transport = trans

	peersCopy := make(map[string]string, len(cfg.Peers)+1)
	for id, addr := range cfg.Peers {
		peersCopy[id] = addr
	}
	if _, ok := peersCopy[cfg.NodeID]; !ok {
		peersCopy[cfg.NodeID] = trans.Addr()
	}
	n.cfg.Peers = peersCopy
	trans.SetPeers(peersCopy)

	peerIDs := make([]string, 0, len(peersCopy))
	for id := range peersCopy {
		peerIDs = append(peerIDs, id)
	}
	slices.Sort(peerIDs)

	lsmOpts := lsm.DefaultOptions()
	lsmOpts.ValueLogThreshold = cfg.WiscKeyThreshold
	lsmOpts.DisableWALSync = cfg.SingleFsync

	rep, err := kv.NewReplica(
		cfg.NodeID,
		peerIDs,
		vfs.NewOSFS(),
		cfg.DataDir,
		newWallClock(),
		trans,
		sim.NewStreamRand(cfg.Seed, "node-"+cfg.NodeID),
		kv.KVBugConfig{
			UseLSM:  true,
			LSMOpts: lsmOpts,
		},
	)
	if err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("open replica: %w", err)
	}
	n.rep = rep

	go n.runLoop()
	return n, nil
}

// RaftAddr returns the bound Raft TCP listener address.
func (n *Node) RaftAddr() string {
	return n.transport.Addr()
}

// UpdatePeers merges updated Raft and HTTP peer maps (useful for dynamic test ports).
func (n *Node) UpdatePeers(raftPeers, httpPeers map[string]string) {
	if len(raftPeers) > 0 {
		n.transport.SetPeers(raftPeers)
	}
	n.do(func() {
		for k, v := range raftPeers {
			n.cfg.Peers[k] = v
		}
		if n.cfg.HTTPPeers == nil {
			n.cfg.HTTPPeers = make(map[string]string)
		}
		for k, v := range httpPeers {
			n.cfg.HTTPPeers[k] = strings.TrimRight(v, "/")
		}
	})
}

func (n *Node) runLoop() {
	defer close(n.doneCh)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	var tickCount uint64
	for {
		select {
		case <-n.stopCh:
			_ = n.rep.FlushLSM()
			if closer, ok := n.rep.StateMachine().(io.Closer); ok {
				_ = closer.Close()
			}
			return
		case <-ticker.C:
			n.rep.Tick()
			tickCount++
			if n.cfg.SingleFsync && tickCount%200 == 0 {
				n.rep.TruncateRaftLog()
			}
		case msg := <-n.msgCh:
			n.rep.Step(msg)
		case fn := <-n.cmdCh:
			fn()
		}
	}
}

func (n *Node) do(fn func()) bool {
	done := make(chan struct{})
	wrapped := func() {
		defer close(done)
		fn()
	}
	select {
	case n.cmdCh <- wrapped:
	case <-n.stopCh:
		return false
	}
	select {
	case <-done:
		return true
	case <-n.stopCh:
		return false
	}
}

// Close flushes LSM state and shuts down the actor loop and TCP transport.
func (n *Node) Close() error {
	n.once.Do(func() {
		close(n.stopCh)
	})
	<-n.doneCh
	return n.transport.Close()
}

func (n *Node) IsLeader() bool {
	var leader bool
	n.do(func() { leader = n.rep.IsLeader() })
	return leader
}

func (n *Node) Status() statusPayload {
	var st statusPayload
	n.do(func() {
		peers := make(map[string]string, len(n.cfg.Peers))
		for k, v := range n.cfg.Peers {
			peers[k] = v
		}
		st = statusPayload{
			NodeID:       n.cfg.NodeID,
			IsLeader:     n.rep.IsLeader(),
			AppliedIndex: n.rep.StateMachine().AppliedIndex(),
			TotalFsyncs:  n.rep.TotalFsyncCount(),
			RaftAddr:     n.transport.Addr(),
			HTTPAddr:     n.cfg.HTTPAddr,
			Peers:        peers,
			Raft:         n.rep.StateView(),
		}
	})
	return st
}

func (n *Node) submitWrite(ctx context.Context, op statemachine.Op) (statemachine.Result, bool, error) {
	resCh := make(chan writeOutcome, 1)
	okSubmit := n.do(func() {
		if !n.rep.IsLeader() {
			resCh <- writeOutcome{ok: false}
			return
		}
		n.rep.SubmitWrite(op, func(res statemachine.Result, ok bool) {
			select {
			case resCh <- writeOutcome{res: res, ok: ok}:
			default:
			}
		})
	})
	if !okSubmit {
		return statemachine.Result{}, false, errors.New("node shutting down")
	}

	ctx, cancel := context.WithTimeout(ctx, n.cfg.OpTimeout)
	defer cancel()

	select {
	case out := <-resCh:
		return out.res, out.ok, nil
	case <-ctx.Done():
		return statemachine.Result{}, false, ctx.Err()
	case <-n.stopCh:
		return statemachine.Result{}, false, errors.New("node shutting down")
	}
}

func (n *Node) submitRead(ctx context.Context, key []byte, useLease bool) ([]byte, bool, bool, error) {
	resCh := make(chan readOutcome, 1)
	rid := n.readID.Add(1)

	okSubmit := n.do(func() {
		if !n.rep.IsLeader() {
			resCh <- readOutcome{ok: false}
			return
		}
		n.rep.SubmitRead(kv.ClientReadReq{
			ReadID:   rid,
			Key:      key,
			UseLease: useLease,
			Callback: func(val []byte, found bool, ok bool) {
				var copied []byte
				if found && val != nil {
					copied = append([]byte(nil), val...)
				}
				select {
				case resCh <- readOutcome{val: copied, found: found, ok: ok}:
				default:
				}
			},
		})
	})
	if !okSubmit {
		return nil, false, false, errors.New("node shutting down")
	}

	ctx, cancel := context.WithTimeout(ctx, n.cfg.OpTimeout)
	defer cancel()

	select {
	case out := <-resCh:
		return out.val, out.found, out.ok, nil
	case <-ctx.Done():
		return nil, false, false, ctx.Err()
	case <-n.stopCh:
		return nil, false, false, errors.New("node shutting down")
	}
}

func (n *Node) submitScan(ctx context.Context, startKey, endKey []byte, limit int, useLease bool) ([]statemachine.KV, bool, error) {
	resCh := make(chan scanOutcome, 1)
	rid := n.readID.Add(1)
	barrierKey := startKey
	if len(barrierKey) == 0 {
		barrierKey = []byte("__aether_scan_barrier__")
	}

	okSubmit := n.do(func() {
		if !n.rep.IsLeader() {
			resCh <- scanOutcome{ok: false}
			return
		}
		// SubmitRead enforces the linearizable ReadIndex quorum barrier; the callback runs
		// synchronously on the actor goroutine once AppliedIndex >= ReadIndex.
		n.rep.SubmitRead(kv.ClientReadReq{
			ReadID:   rid,
			Key:      barrierKey,
			UseLease: useLease,
			Callback: func(_ []byte, _ bool, ok bool) {
				if !ok {
					select {
					case resCh <- scanOutcome{ok: false}:
					default:
					}
					return
				}
				items, err := n.rep.StateMachine().Scan(startKey, endKey, limit)
				select {
				case resCh <- scanOutcome{items: items, err: err, ok: true}:
				default:
				}
			},
		})
	})
	if !okSubmit {
		return nil, false, errors.New("node shutting down")
	}

	ctx, cancel := context.WithTimeout(ctx, n.cfg.OpTimeout)
	defer cancel()

	select {
	case out := <-resCh:
		return out.items, out.ok, out.err
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-n.stopCh:
		return nil, false, errors.New("node shutting down")
	}
}

// Handler builds the zero-dependency HTTP API handler using Go 1.23 ServeMux routing.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", n.handleHealth)
	mux.HandleFunc("GET /v1/status", n.handleStatus)
	mux.HandleFunc("PUT /v1/kv/{key...}", n.handlePut)
	mux.HandleFunc("GET /v1/kv/{key...}", n.handleGet)
	mux.HandleFunc("DELETE /v1/kv/{key...}", n.handleDelete)
	mux.HandleFunc("POST /v1/cas/{key...}", n.handleCAS)
	mux.HandleFunc("GET /v1/scan", n.handleScan)
	mux.HandleFunc("POST /v1/admin/flush", n.handleFlush)
	mux.HandleFunc("POST /v1/admin/snapshot", n.handleSnapshot)
	return mux
}

func (n *Node) opIdentity(r *http.Request) (uint64, uint64) {
	cid := n.clientID
	if raw := r.Header.Get("X-Client-ID"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil && parsed > 0 {
			cid = parsed
		}
	}
	seq := n.seqCounter.Add(1)
	if raw := r.Header.Get("X-Seq-Num"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil && parsed > 0 {
			seq = parsed
		}
	}
	return cid, seq
}

func (n *Node) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) bool {
	if r.Header.Get("X-Aether-Forwarded") == "1" {
		return false
	}

	var candidates []string
	n.leaderMu.RLock()
	cached := n.cachedLeader
	n.leaderMu.RUnlock()
	if cached != "" {
		candidates = append(candidates, cached)
	}

	n.do(func() {
		ids := make([]string, 0, len(n.cfg.HTTPPeers))
		for id := range n.cfg.HTTPPeers {
			if id != n.cfg.NodeID {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			base := n.cfg.HTTPPeers[id]
			if base != "" && base != cached {
				candidates = append(candidates, base)
			}
		}
	})

	pathAndQuery := r.URL.RequestURI()
	for _, base := range candidates {
		targetURL := strings.TrimRight(base, "/") + pathAndQuery
		req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(body))
		if err != nil {
			continue
		}
		for _, h := range []string{"Content-Type", "X-Client-ID", "X-Seq-Num"} {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
		req.Header.Set("X-Aether-Forwarded", "1")

		resp, err := n.httpClient.Do(req)
		if err != nil {
			continue
		}
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusServiceUnavailable {
			continue
		}

		n.leaderMu.Lock()
		n.cachedLeader = base
		n.leaderMu.Unlock()

		for k, vals := range resp.Header {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("X-Aether-Forwarded-By", n.cfg.NodeID)
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
		return true
	}

	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"node_id":   n.cfg.NodeID,
		"is_leader": n.IsLeader(),
	})
}

func (n *Node) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, n.Status())
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if !n.IsLeader() {
		if n.forwardToLeader(w, r, body) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader; no leader reachable"})
		return
	}

	cid, seq := n.opIdentity(r)
	res, ok, err := n.submitWrite(r.Context(), statemachine.Op{
		ClientID: cid,
		SeqNum:   seq,
		Type:     statemachine.OpPut,
		Key:      []byte(key),
		Value:    body,
	})
	if err != nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if n.forwardToLeader(w, r, body) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership lost during proposal"})
		return
	}

	w.Header().Set("X-Aether-Node", n.cfg.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{
		"key":          key,
		"applied":      res.Applied,
		"deduplicated": res.Deduplicated,
		"bytes":        len(body),
	})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}

	if !n.IsLeader() {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader; no leader reachable"})
		return
	}

	useLease := r.URL.Query().Get("lease") == "1" || r.URL.Query().Get("lease") == "true"
	val, found, ok, err := n.submitRead(r.Context(), []byte(key), useLease)
	if err != nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership lost during read"})
		return
	}

	w.Header().Set("X-Aether-Node", n.cfg.NodeID)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found", "key": key})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(val)
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}

	if !n.IsLeader() {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader; no leader reachable"})
		return
	}

	cid, seq := n.opIdentity(r)
	res, ok, err := n.submitWrite(r.Context(), statemachine.Op{
		ClientID: cid,
		SeqNum:   seq,
		Type:     statemachine.OpDelete,
		Key:      []byte(key),
	})
	if err != nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership lost during delete"})
		return
	}

	w.Header().Set("X-Aether-Node", n.cfg.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{
		"key":            key,
		"applied":        res.Applied,
		"deduplicated":   res.Deduplicated,
		"previous_found": res.PreviousFound,
	})
}

type casRequest struct {
	Expected *string `json:"expected"`
	Value    string  `json:"value"`
}

func (n *Node) handleCAS(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var req casRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: expected {\"expected\":\"...\",\"value\":\"...\"}"})
		return
	}

	if !n.IsLeader() {
		if n.forwardToLeader(w, r, body) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader; no leader reachable"})
		return
	}

	var expectedBytes []byte
	if req.Expected != nil {
		expectedBytes = []byte(*req.Expected)
	}
	cid, seq := n.opIdentity(r)
	res, ok, err := n.submitWrite(r.Context(), statemachine.Op{
		ClientID: cid,
		SeqNum:   seq,
		Type:     statemachine.OpCAS,
		Key:      []byte(key),
		Expected: expectedBytes,
		Value:    []byte(req.Value),
	})
	if err != nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if n.forwardToLeader(w, r, body) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership lost during CAS"})
		return
	}

	code := http.StatusOK
	if !res.Applied {
		code = http.StatusConflict
	}
	w.Header().Set("X-Aether-Node", n.cfg.NodeID)
	writeJSON(w, code, map[string]any{
		"key":            key,
		"applied":        res.Applied,
		"deduplicated":   res.Deduplicated,
		"previous_found": res.PreviousFound,
		"previous_value": string(res.Value),
	})
}

func (n *Node) handleScan(w http.ResponseWriter, r *http.Request) {
	if !n.IsLeader() {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader; no leader reachable"})
		return
	}

	q := r.URL.Query()
	startKey := []byte(q.Get("start"))
	endKey := []byte(q.Get("end"))
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = min(parsed, 10000)
		}
	}
	useLease := q.Get("lease") == "1" || q.Get("lease") == "true"

	items, ok, err := n.submitScan(r.Context(), startKey, endKey, limit, useLease)
	if err != nil {
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		if n.forwardToLeader(w, r, nil) {
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership lost during scan"})
		return
	}

	type scanItem struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	out := make([]scanItem, 0, len(items))
	for _, it := range items {
		out = append(out, scanItem{Key: string(it.Key), Value: string(it.Value)})
	}

	w.Header().Set("X-Aether-Node", n.cfg.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{
		"count": len(out),
		"items": out,
	})
}

func (n *Node) handleFlush(w http.ResponseWriter, _ *http.Request) {
	var err error
	n.do(func() { err = n.rep.FlushLSM() })
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "flushed", "node_id": n.cfg.NodeID})
}

func (n *Node) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	var err error
	n.do(func() { err = n.rep.CreateSnapshot() })
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "snapshotted", "node_id": n.cfg.NodeID})
}