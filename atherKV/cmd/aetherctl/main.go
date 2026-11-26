package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

type client struct {
	endpoints  []string
	httpClient *http.Client
	clientID   uint64
}

func newClient(endpoints []string, timeout time.Duration) *client {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.LittleEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))
	}
	cid := binary.LittleEndian.Uint64(b[:])
	if cid == 0 {
		cid = 1
	}
	cleaned := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
			ep = "http://" + ep
		}
		cleaned = append(cleaned, strings.TrimRight(ep, "/"))
	}
	if len(cleaned) == 0 {
		cleaned = []string{"http://127.0.0.1:8080"}
	}
	return &client{
		endpoints:  cleaned,
		httpClient: &http.Client{Timeout: timeout},
		clientID:   cid,
	}
}

func encodeKVPath(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// doWithFailover tries every configured endpoint in order, retrying on connection
// failures or 503 Service Unavailable while preserving idempotency headers.
func (c *client) doWithFailover(method, reqPath string, body []byte, contentType string, idempotent bool) (int, []byte, http.Header, error) {
	var lastErr error
	var lastCode int
	var lastBody []byte
	var lastHdr http.Header

	seq := uint64(time.Now().UnixNano())
	for _, base := range c.endpoints {
		var r io.Reader
		if body != nil {
			r = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, base+reqPath, r)
		if err != nil {
			lastErr = err
			continue
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if idempotent {
			req.Header.Set("X-Client-ID", strconv.FormatUint(c.clientID, 10))
			req.Header.Set("X-Seq-Num", strconv.FormatUint(seq, 10))
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		lastCode, lastBody, lastHdr = resp.StatusCode, respBody, resp.Header
		if resp.StatusCode == http.StatusServiceUnavailable {
			lastErr = fmt.Errorf("%s returned 503: %s", base, strings.TrimSpace(string(respBody)))
			continue
		}
		return resp.StatusCode, respBody, resp.Header, nil
	}

	if lastCode != 0 {
		return lastCode, lastBody, lastHdr, lastErr
	}
	if lastErr == nil {
		lastErr = errors.New("no endpoints reachable")
	}
	return 0, nil, nil, lastErr
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `aetherctl - CLI client for AetherKV (Raft + LSM + WiscKey)

Usage:
  aetherctl [global flags] <command> [args]

Global Flags:
  -e, -endpoints   Comma-separated HTTP endpoints (env: AETHERKV_ENDPOINTS, default: http://127.0.0.1:8080)
  -timeout         Request timeout (default: 5s)
  -json            Print raw JSON responses

Commands:
  put <key> <value|->            Linearizable PUT (use '-' to read value bytes from stdin)
  get [-lease] <key>             Linearizable ReadIndex GET (or bounded-lease read)
  del <key>                      Linearizable DELETE
  cas <key> <expected> <value>   Atomic Compare-And-Swap (exits 2 on conflict)
  scan [-start K] [-end K] [-limit N] [-lease]
                                 Linearizable range scan over [start, end)
  status                         Query all endpoints and display cluster Raft/LSM status
  health                         Check node health (exits 0 if healthy)
  flush                          Force LSM memtable flush on reachable node
  snapshot                       Create Raft snapshot & compact log on reachable node
`)
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	defaultEP := os.Getenv("AETHERKV_ENDPOINTS")
	if defaultEP == "" {
		defaultEP = "http://127.0.0.1:8080"
	}

	fs := flag.NewFlagSet("aetherctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var endpointsFlag string
	fs.StringVar(&endpointsFlag, "endpoints", defaultEP, "Comma-separated HTTP endpoints")
	fs.StringVar(&endpointsFlag, "e", defaultEP, "Comma-separated HTTP endpoints (shorthand)")
	timeout := fs.Duration("timeout", 5*time.Second, "Per-request timeout")
	jsonOut := fs.Bool("json", false, "Emit raw JSON output")
	fs.Usage = func() { printUsage(stderr) }

	if err := fs.Parse(args); err != nil {
		return 1
	}
	rest := fs.Args()
	if len(rest) == 0 {
		printUsage(stderr)
		return 1
	}

	cli := newClient(strings.Split(endpointsFlag, ","), *timeout)
	cmd := strings.ToLower(rest[0])
	cmdArgs := rest[1:]

	switch cmd {
	case "put":
		if len(cmdArgs) != 2 {
			fmt.Fprintln(stderr, "usage: aetherctl put <key> <value|->")
			return 1
		}
		key := cmdArgs[0]
		var val []byte
		if cmdArgs[1] == "-" {
			var err error
			val, err = io.ReadAll(io.LimitReader(stdin, 32<<20))
			if err != nil {
				fmt.Fprintf(stderr, "read stdin: %v\n", err)
				return 1
			}
		} else {
			val = []byte(cmdArgs[1])
		}
		code, body, _, err := cli.doWithFailover(http.MethodPut, "/v1/kv/"+encodeKVPath(key), val, "application/octet-stream", true)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		if code != http.StatusOK {
			fmt.Fprintf(stderr, "put failed (%d): %s\n", code, strings.TrimSpace(string(body)))
			return 1
		}
		if *jsonOut {
			fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
		} else {
			fmt.Fprintf(stdout, "OK (%d bytes)\n", len(val))
		}
		return 0

	case "get":
		gfs := flag.NewFlagSet("get", flag.ContinueOnError)
		gfs.SetOutput(stderr)
		lease := gfs.Bool("lease", false, "Use clock-bounded leader lease read")
		if err := gfs.Parse(cmdArgs); err != nil || gfs.NArg() != 1 {
			fmt.Fprintln(stderr, "usage: aetherctl get [-lease] <key>")
			return 1
		}
		path := "/v1/kv/" + encodeKVPath(gfs.Arg(0))
		if *lease {
			path += "?lease=1"
		}
		code, body, _, err := cli.doWithFailover(http.MethodGet, path, nil, "", false)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		if code == http.StatusNotFound {
			fmt.Fprintf(stderr, "key not found: %s\n", gfs.Arg(0))
			return 1
		}
		if code != http.StatusOK {
			fmt.Fprintf(stderr, "get failed (%d): %s\n", code, strings.TrimSpace(string(body)))
			return 1
		}
		if *jsonOut {
			_ = json.NewEncoder(stdout).Encode(map[string]string{"key": gfs.Arg(0), "value": string(body)})
		} else {
			_, _ = stdout.Write(body)
			if len(body) > 0 && body[len(body)-1] != '\n' {
				_, _ = stdout.Write([]byte("\n"))
			}
		}
		return 0

	case "del", "delete":
		if len(cmdArgs) != 1 {
			fmt.Fprintln(stderr, "usage: aetherctl del <key>")
			return 1
		}
		code, body, _, err := cli.doWithFailover(http.MethodDelete, "/v1/kv/"+encodeKVPath(cmdArgs[0]), nil, "", true)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		if code != http.StatusOK {
			fmt.Fprintf(stderr, "del failed (%d): %s\n", code, strings.TrimSpace(string(body)))
			return 1
		}
		if *jsonOut {
			fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
		} else {
			var res struct {
				PreviousFound bool `json:"previous_found"`
			}
			_ = json.Unmarshal(body, &res)
			if res.PreviousFound {
				fmt.Fprintln(stdout, "DELETED")
			} else {
				fmt.Fprintln(stdout, "NOT_FOUND")
			}
		}
		return 0

	case "cas":
		if len(cmdArgs) != 3 {
			fmt.Fprintln(stderr, "usage: aetherctl cas <key> <expected> <new-value>")
			return 1
		}
		payload, _ := json.Marshal(map[string]string{
			"expected": cmdArgs[1],
			"value":    cmdArgs[2],
		})
		code, body, _, err := cli.doWithFailover(http.MethodPost, "/v1/cas/"+encodeKVPath(cmdArgs[0]), payload, "application/json", true)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		if code == http.StatusConflict {
			if *jsonOut {
				fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
			} else {
				fmt.Fprintf(stderr, "CAS CONFLICT: %s\n", strings.TrimSpace(string(body)))
			}
			return 2
		}
		if code != http.StatusOK {
			fmt.Fprintf(stderr, "cas failed (%d): %s\n", code, strings.TrimSpace(string(body)))
			return 1
		}
		if *jsonOut {
			fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
		} else {
			fmt.Fprintln(stdout, "APPLIED")
		}
		return 0

	case "scan":
		sfs := flag.NewFlagSet("scan", flag.ContinueOnError)
		sfs.SetOutput(stderr)
		start := sfs.String("start", "", "Inclusive start key")
		end := sfs.String("end", "", "Exclusive end key")
		limit := sfs.Int("limit", 100, "Max items to return")
		lease := sfs.Bool("lease", false, "Use bounded lease read")
		if err := sfs.Parse(cmdArgs); err != nil {
			return 1
		}
		q := url.Values{}
		if *start != "" {
			q.Set("start", *start)
		}
		if *end != "" {
			q.Set("end", *end)
		}
		q.Set("limit", strconv.Itoa(*limit))
		if *lease {
			q.Set("lease", "1")
		}
		code, body, _, err := cli.doWithFailover(http.MethodGet, "/v1/scan?"+q.Encode(), nil, "", false)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return 1
		}
		if code != http.StatusOK {
			fmt.Fprintf(stderr, "scan failed (%d): %s\n", code, strings.TrimSpace(string(body)))
			return 1
		}
		if *jsonOut {
			fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
			return 0
		}
		var out struct {
			Items []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			fmt.Fprintf(stderr, "decode scan response: %v\n", err)
			return 1
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		for _, it := range out.Items {
			fmt.Fprintf(tw, "%s\t%s\n", it.Key, it.Value)
		}
		_ = tw.Flush()
		return 0

	case "status":
		type nodeStatus struct {
			Endpoint     string `json:"endpoint"`
			NodeID       string `json:"node_id"`
			IsLeader     bool   `json:"is_leader"`
			AppliedIndex uint64 `json:"applied_index"`
			TotalFsyncs  uint64 `json:"total_fsyncs"`
			RaftAddr     string `json:"raft_addr"`
			Error        string `json:"error,omitempty"`
		}
		rows := make([]nodeStatus, 0, len(cli.endpoints))
		anyOK := false
		for _, ep := range cli.endpoints {
			row := nodeStatus{Endpoint: ep}
			resp, err := cli.httpClient.Get(ep + "/v1/status")
			if err != nil {
				row.Error = err.Error()
				rows = append(rows, row)
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				row.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
				rows = append(rows, row)
				continue
			}
			_ = json.Unmarshal(body, &row)
			anyOK = true
			rows = append(rows, row)
		}
		if *jsonOut {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(rows)
		} else {
			tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NODE\tROLE\tAPPLIED\tFSYNCS\tENDPOINT\tSTATUS")
			for _, r := range rows {
				if r.Error != "" {
					fmt.Fprintf(tw, "-\tOFFLINE\t-\t-\t%s\t%s\n", r.Endpoint, r.Error)
					continue
				}
				role := "FOLLOWER"
				if r.IsLeader {
					role = "LEADER"
				}
				fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\tOK\n", r.NodeID, role, r.AppliedIndex, r.TotalFsyncs, r.Endpoint)
			}
			_ = tw.Flush()
		}
		if !anyOK {
			return 1
		}
		return 0

	case "health":
		code, body, _, err := cli.doWithFailover(http.MethodGet, "/healthz", nil, "", false)
		if err != nil || code != http.StatusOK {
			fmt.Fprintf(stderr, "unhealthy: code=%d err=%v\n", code, err)
			return 1
		}
		if *jsonOut {
			fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
		} else {
			fmt.Fprintln(stdout, "OK")
		}
		return 0

	case "flush", "snapshot":
		code, body, _, err := cli.doWithFailover(http.MethodPost, "/v1/admin/"+cmd, nil, "", false)
		if err != nil || code != http.StatusOK {
			fmt.Fprintf(stderr, "%s failed (code=%d): %v %s\n", cmd, code, err, strings.TrimSpace(string(body)))
			return 1
		}
		fmt.Fprintln(stdout, strings.TrimSpace(string(body)))
		return 0

	default:
		fmt.Fprintf(stderr, "unknown command: %s\n\n", cmd)
		printUsage(stderr)
		return 1
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
