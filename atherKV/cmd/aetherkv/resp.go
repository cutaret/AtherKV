package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cutaret/aetherkv/statemachine"
)

// RESPServer exposes a zero-dependency Redis RESP2 TCP interface backed by Node.
type RESPServer struct {
	node     *Node
	listener net.Listener
	wg       sync.WaitGroup
	once     sync.Once
}

// NewRESPServer starts a RESP2 TCP listener on addr (e.g. ":6379" or "127.0.0.1:0").
func NewRESPServer(node *Node, addr string) (*RESPServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen resp tcp %s: %w", addr, err)
	}
	s := &RESPServer{
		node:     node,
		listener: ln,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr returns the bound TCP address of the RESP2 listener.
func (s *RESPServer) Addr() string {
	return s.listener.Addr().String()
}

// Close stops accepting new connections and waits for active handlers to finish.
func (s *RESPServer) Close() error {
	var err error
	s.once.Do(func() {
		err = s.listener.Close()
	})
	s.wg.Wait()
	return err
}

func (s *RESPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.serveConn(c)
		}(conn)
	}
}

func (s *RESPServer) serveConn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 64*1024)
	bw := bufio.NewWriterSize(conn, 64*1024)

	for {
		args, err := readRESPCommand(br)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				writeRESPError(bw, "ERR "+err.Error())
				_ = bw.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		quit := s.dispatch(bw, args)
		if quit || br.Buffered() == 0 {
			if err := bw.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

func readRESPCommand(r *bufio.Reader) ([]string, error) {
	for {
		peek, err := r.Peek(1)
		if err != nil {
			return nil, err
		}

		// Standard RESP2 multibulk array: *<count>\r\n$<len>\r\n<bytes>\r\n...
		if peek[0] == '*' {
			line, err := readRESPLine(r)
			if err != nil {
				return nil, err
			}
			count, err := strconv.Atoi(line[1:])
			if err != nil || count < 0 || count > 10000 {
				return nil, errors.New("protocol error: invalid multibulk length")
			}
			if count == 0 {
				return nil, nil
			}
			args := make([]string, 0, count)
			for i := 0; i < count; i++ {
				hdr, err := readRESPLine(r)
				if err != nil {
					return nil, err
				}
				if len(hdr) == 0 || hdr[0] != '$' {
					return nil, errors.New("protocol error: expected bulk string '$'")
				}
				n, err := strconv.Atoi(hdr[1:])
				if err != nil || n < 0 || n > 32<<20 {
					return nil, errors.New("protocol error: invalid bulk length")
				}
				buf := make([]byte, n+2)
				if _, err := io.ReadFull(r, buf); err != nil {
					return nil, err
				}
				args = append(args, string(buf[:n]))
			}
			return args, nil
		}

		// Inline command fallback (e.g. "PING\r\n" from telnet/nc)
		line, err := readRESPLine(r)
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		return fields, nil
	}
}

func readRESPLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func writeRESPSimple(w *bufio.Writer, s string) {
	_, _ = w.WriteString("+" + s + "\r\n")
}

func writeRESPError(w *bufio.Writer, msg string) {
	clean := strings.ReplaceAll(strings.ReplaceAll(msg, "\r", " "), "\n", " ")
	_, _ = w.WriteString("-" + clean + "\r\n")
}

func writeRESPInt(w *bufio.Writer, n int64) {
	_, _ = w.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func writeRESPBulk(w *bufio.Writer, b []byte) {
	_, _ = w.WriteString("$" + strconv.Itoa(len(b)) + "\r\n")
	_, _ = w.Write(b)
	_, _ = w.WriteString("\r\n")
}

func writeRESPNull(w *bufio.Writer) {
	_, _ = w.WriteString("$-1\r\n")
}

func (s *RESPServer) dispatch(w *bufio.Writer, args []string) (quit bool) {
	cmd := strings.ToUpper(args[0])
	ctx := context.Background()

	switch cmd {
	case "PING":
		if len(args) >= 2 {
			writeRESPBulk(w, []byte(args[1]))
		} else {
			writeRESPSimple(w, "PONG")
		}
	case "ECHO":
		if len(args) != 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'echo' command")
			return false
		}
		writeRESPBulk(w, []byte(args[1]))
	case "COMMAND":
		// Returning an empty array lets redis-cli complete its initial handshake cleanly.
		_, _ = w.WriteString("*0\r\n")
	case "CLIENT":
		writeRESPSimple(w, "OK")
	case "HELLO":
		// Signal RESP2 fallback cleanly to modern Redis clients.
		writeRESPError(w, "ERR unknown command 'HELLO'")
	case "QUIT":
		writeRESPSimple(w, "OK")
		return true
	case "SET":
		if len(args) < 3 {
			writeRESPError(w, "ERR wrong number of arguments for 'set' command")
			return false
		}
		if err := s.node.execPut(ctx, args[1], []byte(args[2])); err != nil {
			writeRESPError(w, "ERR "+err.Error())
			return false
		}
		writeRESPSimple(w, "OK")
	case "MSET":
		if len(args) < 3 || (len(args)-1)%2 != 0 {
			writeRESPError(w, "ERR wrong number of arguments for 'mset' command")
			return false
		}
		for i := 1; i < len(args); i += 2 {
			if err := s.node.execPut(ctx, args[i], []byte(args[i+1])); err != nil {
				writeRESPError(w, "ERR "+err.Error())
				return false
			}
		}
		writeRESPSimple(w, "OK")
	case "GET":
		if len(args) != 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'get' command")
			return false
		}
		val, found, err := s.node.execGet(ctx, args[1])
		if err != nil {
			writeRESPError(w, "ERR "+err.Error())
			return false
		}
		if !found {
			writeRESPNull(w)
			return false
		}
		writeRESPBulk(w, val)
	case "MGET":
		if len(args) < 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'mget' command")
			return false
		}
		vals := make([][]byte, len(args)-1)
		founds := make([]bool, len(args)-1)
		for i, k := range args[1:] {
			v, ok, err := s.node.execGet(ctx, k)
			if err != nil {
				writeRESPError(w, "ERR "+err.Error())
				return false
			}
			vals[i] = v
			founds[i] = ok
		}
		_, _ = w.WriteString("*" + strconv.Itoa(len(vals)) + "\r\n")
		for i := range vals {
			if !founds[i] {
				writeRESPNull(w)
			} else {
				writeRESPBulk(w, vals[i])
			}
		}
	case "DEL":
		if len(args) < 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'del' command")
			return false
		}
		var deleted int64
		for _, k := range args[1:] {
			prevFound, err := s.node.execDelete(ctx, k)
			if err != nil {
				writeRESPError(w, "ERR "+err.Error())
				return false
			}
			if prevFound {
				deleted++
			}
		}
		writeRESPInt(w, deleted)
	case "EXISTS":
		if len(args) < 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'exists' command")
			return false
		}
		var count int64
		for _, k := range args[1:] {
			_, found, err := s.node.execGet(ctx, k)
			if err != nil {
				writeRESPError(w, "ERR "+err.Error())
				return false
			}
			if found {
				count++
			}
		}
		writeRESPInt(w, count)
	case "KEYS":
		pattern := "*"
		if len(args) >= 2 {
			pattern = args[1]
		}
		keys, err := s.matchKeys(ctx, pattern, 10000)
		if err != nil {
			writeRESPError(w, "ERR "+err.Error())
			return false
		}
		_, _ = w.WriteString("*" + strconv.Itoa(len(keys)) + "\r\n")
		for _, k := range keys {
			writeRESPBulk(w, []byte(k))
		}
	case "SCAN":
		if len(args) < 2 {
			writeRESPError(w, "ERR wrong number of arguments for 'scan' command")
			return false
		}
		pattern := "*"
		limit := 100
		for i := 2; i+1 < len(args); i += 2 {
			switch strings.ToUpper(args[i]) {
			case "MATCH":
				pattern = args[i+1]
			case "COUNT":
				if n, err := strconv.Atoi(args[i+1]); err == nil && n > 0 {
					limit = min(n, 10000)
				}
			}
		}
		keys, err := s.matchKeys(ctx, pattern, limit)
		if err != nil {
			writeRESPError(w, "ERR "+err.Error())
			return false
		}
		_, _ = w.WriteString("*2\r\n")
		writeRESPBulk(w, []byte("0"))
		_, _ = w.WriteString("*" + strconv.Itoa(len(keys)) + "\r\n")
		for _, k := range keys {
			writeRESPBulk(w, []byte(k))
		}
	case "INFO":
		st := s.node.Status()
		role := "follower"
		if st.IsLeader {
			role = "leader"
		}
		var b strings.Builder
		b.WriteString("# Server\r\n")
		b.WriteString("engine:aetherkv\r\n")
		b.WriteString("node_id:" + st.NodeID + "\r\n")
		b.WriteString("raft_addr:" + st.RaftAddr + "\r\n")
		b.WriteString("# Replication\r\n")
		b.WriteString("role:" + role + "\r\n")
		b.WriteString("applied_index:" + strconv.FormatUint(st.AppliedIndex, 10) + "\r\n")
		b.WriteString("total_fsyncs:" + strconv.FormatUint(st.TotalFsyncs, 10) + "\r\n")
		writeRESPBulk(w, []byte(b.String()))
	default:
		writeRESPError(w, fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	return false
}

func (s *RESPServer) matchKeys(ctx context.Context, pattern string, limit int) ([]string, error) {
	start, end := prefixBounds(pattern)
	items, err := s.node.execScan(ctx, start, end, limit)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		k := string(it.Key)
		if globMatch(pattern, k) {
			out = append(out, k)
		}
	}
	return out, nil
}

// prefixBounds derives tight [start, end) LSM scan bounds from the literal prefix before '*' or '?'.
func prefixBounds(pattern string) ([]byte, []byte) {
	cut := strings.IndexAny(pattern, "*?")
	if cut == 0 {
		return nil, nil
	}
	prefix := pattern
	if cut > 0 {
		prefix = pattern[:cut]
	}
	start := []byte(prefix)
	end := append([]byte(nil), start...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return start, end[:i+1]
		}
	}
	return start, nil
}

func globMatch(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	pi, si := 0, 0
	starIdx, matchIdx := -1, 0
	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// --- Leader-Aware Internal Operations (Local on Leader, HTTP-Forwarded on Follower) ---

func encodeKVPath(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func (n *Node) httpPeerCandidates() []string {
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
	return candidates
}

func (n *Node) forwardLeaderHTTP(ctx context.Context, method, reqURI string, body []byte) (int, []byte, error) {
	for _, base := range n.httpPeerCandidates() {
		target := strings.TrimRight(base, "/") + reqURI
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bodyReader)
		if err != nil {
			continue
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
		return resp.StatusCode, respBody, nil
	}
	return 0, nil, errors.New("not leader; no leader reachable")
}

func (n *Node) execPut(ctx context.Context, key string, val []byte) error {
	if n.IsLeader() {
		_, ok, err := n.submitWrite(ctx, statemachine.Op{
			ClientID: n.clientID,
			SeqNum:   n.seqCounter.Add(1),
			Type:     statemachine.OpPut,
			Key:      []byte(key),
			Value:    val,
		})
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	code, respBody, err := n.forwardLeaderHTTP(ctx, http.MethodPut, "/v1/kv/"+encodeKVPath(key), val)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("leader put failed (%d): %s", code, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (n *Node) execGet(ctx context.Context, key string) ([]byte, bool, error) {
	if n.IsLeader() {
		val, found, ok, err := n.submitRead(ctx, []byte(key), false)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return val, found, nil
		}
	}
	code, respBody, err := n.forwardLeaderHTTP(ctx, http.MethodGet, "/v1/kv/"+encodeKVPath(key), nil)
	if err != nil {
		return nil, false, err
	}
	if code == http.StatusNotFound {
		return nil, false, nil
	}
	if code != http.StatusOK {
		return nil, false, fmt.Errorf("leader get failed (%d): %s", code, strings.TrimSpace(string(respBody)))
	}
	return respBody, true, nil
}

func (n *Node) execDelete(ctx context.Context, key string) (bool, error) {
	if n.IsLeader() {
		res, ok, err := n.submitWrite(ctx, statemachine.Op{
			ClientID: n.clientID,
			SeqNum:   n.seqCounter.Add(1),
			Type:     statemachine.OpDelete,
			Key:      []byte(key),
		})
		if err != nil {
			return false, err
		}
		if ok {
			return res.PreviousFound, nil
		}
	}
	code, respBody, err := n.forwardLeaderHTTP(ctx, http.MethodDelete, "/v1/kv/"+encodeKVPath(key), nil)
	if err != nil {
		return false, err
	}
	if code != http.StatusOK {
		return false, fmt.Errorf("leader delete failed (%d): %s", code, strings.TrimSpace(string(respBody)))
	}
	var out struct {
		PreviousFound bool `json:"previous_found"`
	}
	_ = json.Unmarshal(respBody, &out)
	return out.PreviousFound, nil
}

func (n *Node) execScan(ctx context.Context, startKey, endKey []byte, limit int) ([]statemachine.KV, error) {
	if n.IsLeader() {
		items, ok, err := n.submitScan(ctx, startKey, endKey, limit, false)
		if err != nil {
			return nil, err
		}
		if ok {
			return items, nil
		}
	}
	q := url.Values{}
	if len(startKey) > 0 {
		q.Set("start", string(startKey))
	}
	if len(endKey) > 0 {
		q.Set("end", string(endKey))
	}
	q.Set("limit", strconv.Itoa(limit))
	code, respBody, err := n.forwardLeaderHTTP(ctx, http.MethodGet, "/v1/scan?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("leader scan failed (%d): %s", code, strings.TrimSpace(string(respBody)))
	}
	var out struct {
		Items []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"items"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	items := make([]statemachine.KV, 0, len(out.Items))
	for _, it := range out.Items {
		items = append(items, statemachine.KV{Key: []byte(it.Key), Value: []byte(it.Value)})
	}
	return items, nil
}
