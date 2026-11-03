package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
	"github.com/cutaret/aetherkv/zoo"
)

type ClusterServer struct {
	mu       sync.Mutex
	kernel   *sim.Kernel
	net      *sim.SimNet
	peers    []string
	disks    map[string]*vfs.SimFS
	replicas map[string]*kv.Replica
	seqNum   uint64
}

func NewClusterServer() *ClusterServer {
	k := sim.NewKernel(2026, nil)
	net := sim.NewSimNet(k)
	peers := []string{"n1", "n2", "n3"}
	disks := make(map[string]*vfs.SimFS)
	reps := make(map[string]*kv.Replica)

	opts := lsm.DefaultOptions()
	opts.MemtableFlushThreshold = 4096
	opts.ValueLogThreshold = 256

	for _, id := range peers {
		disks[id] = vfs.NewSimFS()
		r, err := kv.NewReplica(id, peers, disks[id], "data_"+id, k.NodeClock(id), net, k.Stream(id), kv.KVBugConfig{
			UseLSM:  true,
			LSMOpts: opts,
		})
		if err != nil {
			panic(err)
		}
		reps[id] = r
		nodeID := id
		net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
	}

	cs := &ClusterServer{
		kernel:   k,
		net:      net,
		peers:    peers,
		disks:    disks,
		replicas: reps,
	}
	cs.advanceTicks(15)
	return cs
}

func (cs *ClusterServer) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		for _, id := range cs.peers {
			cs.replicas[id].Tick()
		}
		cs.kernel.RunUntil(cs.kernel.Now()+10_000_000, 1000)
	}
}

func (cs *ClusterServer) findLeader() *kv.Replica {
	for _, id := range cs.peers {
		if cs.replicas[id].IsLeader() {
			return cs.replicas[id]
		}
	}
	cs.advanceTicks(15)
	for _, id := range cs.peers {
		if cs.replicas[id].IsLeader() {
			return cs.replicas[id]
		}
	}
	return nil
}

type NodeStatus struct {
	ID           string `json:"id"`
	Role         string `json:"role"`
	Term         uint64 `json:"term"`
	CommitIdx    uint64 `json:"commit_idx"`
	AppliedIdx   uint64 `json:"applied_idx"`
	Crashed      bool   `json:"crashed"`
	TotalFsyncs  uint64 `json:"total_fsyncs"`
}

func roleName(r uint8) string {
	switch r {
	case 1:
		return "Follower"
	case 3:
		return "Candidate"
	case 4:
		return "Leader"
	default:
		return "Follower"
	}
}

func (cs *ClusterServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var nodes []NodeStatus
	for _, id := range cs.peers {
		rep := cs.replicas[id]
		sv := rep.StateView()
		nodes = append(nodes, NodeStatus{
			ID:          id,
			Role:        roleName(uint8(sv.Role)),
			Term:        sv.Term,
			CommitIdx:   sv.CommitIdx,
			AppliedIdx:  rep.StateMachine().AppliedIndex(),
			Crashed:     sv.Crashed,
			TotalFsyncs: rep.TotalFsyncCount(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"nodes": nodes, "virtual_time_ms": float64(cs.kernel.Now()) / 1e6})
}

func (cs *ClusterServer) handleKV(w http.ResponseWriter, r *http.Request) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var req struct {
		Op       string `json:"op"`
		Key      string `json:"key"`
		Value    string `json:"value"`
		Expected string `json:"expected"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	leader := cs.findLeader()
	if leader == nil {
		http.Error(w, "no active Raft leader (quorum unavailable)", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch req.Op {
	case "GET":
		var outVal string
		var found, ok bool
		cs.seqNum++
		leader.SubmitRead(kv.ClientReadReq{
			ReadID: cs.seqNum,
			Key:    []byte(req.Key),
			Callback: func(val []byte, f bool, s bool) {
				outVal = string(val)
				found = f
				ok = s
			},
		})
		cs.advanceTicks(4)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": ok, "found": found, "value": outVal, "leader": leader.StateView().ID,
		})

	case "SCAN":
		items, err := leader.StateMachine().Scan([]byte(req.Key), nil, 50)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		type kvItem struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		var out []kvItem
		for _, it := range items {
			out = append(out, kvItem{Key: string(it.Key), Value: string(it.Value)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "items": out, "leader": leader.StateView().ID})

	case "PUT", "DELETE", "CAS":
		cs.seqNum++
		opType := statemachine.OpPut
		if req.Op == "DELETE" {
			opType = statemachine.OpDelete
		} else if req.Op == "CAS" {
			opType = statemachine.OpCAS
		}
		var res statemachine.Result
		var ok bool
		leader.SubmitWrite(statemachine.Op{
			ClientID: 1,
			SeqNum:   cs.seqNum,
			Type:     opType,
			Key:      []byte(req.Key),
			Value:    []byte(req.Value),
			Expected: []byte(req.Expected),
		}, func(r statemachine.Result, s bool) {
			res = r
			ok = s
		})
		cs.advanceTicks(4)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      ok,
			"applied": res.Applied,
			"value":   string(res.Value),
			"leader":  leader.StateView().ID,
		})
	}
}

func (cs *ClusterServer) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var req struct {
		NodeID string `json:"node_id"`
		Action string `json:"action"` // "crash", "restart", "flush"
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	rep, ok := cs.replicas[req.NodeID]
	if !ok {
		http.Error(w, "unknown node", http.StatusBadRequest)
		return
	}

	switch req.Action {
	case "crash":
		rep.Crash(cs.disks[req.NodeID], cs.kernel.Stream("crash_"+req.NodeID))
		cs.advanceTicks(12)
	case "restart":
		_ = rep.Restart()
		cs.advanceTicks(8)
	case "flush":
		_ = rep.FlushLSM()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func handleSimBugZoo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Seed  uint64 `json:"seed"`
		Nodes int    `json:"nodes"`
		BugID int    `json:"bug_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Seed == 0 {
		req.Seed = 4
	}
	if req.Nodes < 3 || req.Nodes > 5 {
		req.Nodes = 4
	}

	bugs := kv.KVBugConfig{UseLSM: true, LSMOpts: lsm.DefaultOptions()}
	switch req.BugID {
	case 2:
		bugs.RaftBugs.Bug02SkipVotePersist = true
	case 5:
		bugs.Bug05BlindLeaderRead = true
	case 13:
		bugs.RaftBugs.Bug13EvenQuorum = true
	}

	out := zoo.RunClusterSeed(req.Seed, req.Nodes, bugs, nil)
	var htmlTrace string
	initialFaults, shrunkFaults := 0, 0

	if out.Failed {
		nodeIDs := make([]string, req.Nodes)
		for i := 0; i < req.Nodes; i++ {
			nodeIDs[i] = fmt.Sprintf("n%d", i+1)
		}
		k := sim.NewKernel(req.Seed, nil)
		sched := sim.BuildNemesisSchedule(k.Stream("nemesis"), nodeIDs, 14)
		initialFaults = len(sched)
		shrunk := sim.ShrinkFaultSchedule(sched, func(cand []sim.FaultAction) bool {
			res := zoo.RunClusterSeed(req.Seed, req.Nodes, bugs, cand)
			return res.Failed && res.Detector == out.Detector
		})
		shrunkFaults = len(shrunk)

		var buf bytes.Buffer
		_ = sim.RenderHTMLTraceViewer(&buf, sim.TraceReport{
			Title:             fmt.Sprintf("Bug Zoo #%d Replay", req.BugID),
			Seed:              req.Seed,
			NumNodes:          req.Nodes,
			Detector:          out.Detector,
			ViolationDetails:  out.Details,
			InitialFaultCount: initialFaults,
			ShrunkFaultCount:  shrunkFaults,
			Faults:            shrunk,
		})
		htmlTrace = buf.String()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"failed":         out.Failed,
		"detector":       out.Detector,
		"details":        out.Details,
		"initial_faults": initialFaults,
		"shrunk_faults":  shrunkFaults,
		"html_trace":     htmlTrace,
	})
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>AetherKV — Live Replicated Cluster & Simulation Playground</title>
<style>
  body { background: #0d1117; color: #c9d1d9; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; margin: 0; padding: 24px; }
  h1 { color: #58a6ff; margin-top: 0; }
  h2 { color: #79c0ff; font-size: 18px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
  .panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 18px; }
  .nodes { display: flex; gap: 12px; margin-bottom: 16px; }
  .node-card { flex: 1; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 12px; }
  .node-leader { border-color: #238636; box-shadow: 0 0 8px rgba(35,134,54,0.4); }
  .node-crashed { border-color: #f85149; opacity: 0.6; }
  input, select, button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 8px 12px; border-radius: 6px; font-family: inherit; margin-right: 6px; margin-bottom: 8px; }
  button { cursor: pointer; background: #238636; color: #fff; border: none; font-weight: bold; }
  button:hover { background: #2ea043; }
  .btn-danger { background: #da3633; }
  .btn-secondary { background: #30363d; }
  pre { background: #0d1117; border: 1px solid #30363d; padding: 12px; border-radius: 6px; overflow-x: auto; max-height: 300px; }
</style>
</head>
<body>
<h1>AetherKV — Replicated Raft + LSM + WiscKey Playground</h1>
<div class="grid">
  <div class="panel">
    <h2>1. Live 3-Node Raft + LSM Cluster</h2>
    <div id="nodes" class="nodes"></div>
    <div>
      <input id="kvKey" placeholder="Key (e.g. user:101)" value="llama3:block:01">
      <input id="kvVal" placeholder="Value" value="kv-cache-tensor-payload">
      <input id="kvExp" placeholder="CAS Expected (optional)">
    </div>
    <div>
      <button onclick="sendKV('PUT')">PUT</button>
      <button onclick="sendKV('GET')" class="btn-secondary">Linearizable GET (ReadIndex)</button>
      <button onclick="sendKV('CAS')" class="btn-secondary">CAS</button>
      <button onclick="sendKV('DELETE')" class="btn-danger">DELETE</button>
      <button onclick="sendKV('SCAN')" class="btn-secondary">SCAN All Keys</button>
    </div>
    <pre id="kvOut">Ready. Submit a PUT, GET, CAS, DELETE, or SCAN operation above.</pre>
  </div>

  <div class="panel">
    <h2>2. Deterministic Bug Zoo & Fault Shrinker Studio</h2>
    <div>
      <label>Bug Preset:</label>
      <select id="bugSelect">
        <option value="13">Zoo #13: 4-Node Even Quorum Split-Brain (Shrinks 20 -> 2 Faults)</option>
        <option value="2">Zoo #2: Reply RequestVote Before Persisting HardState</option>
        <option value="0">Clean Cluster (0 Violations Expected)</option>
      </select>
      <input id="simSeed" type="number" value="4" style="width:70px" title="Seed">
      <button onclick="runSim()">Run Simulation & Shrink</button>
    </div>
    <pre id="simOut">Select a Bug Zoo entry and click "Run Simulation & Shrink".</pre>
  </div>
</div>

<script>
async function refreshStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  const container = document.getElementById('nodes');
  container.innerHTML = '';
  for (const n of data.nodes) {
    const cls = n.crashed ? 'node-card node-crashed' : (n.role === 'Leader' ? 'node-card node-leader' : 'node-card');
    container.innerHTML += '<div class="' + cls + '">' +
      '<div><strong>Node ' + n.id + '</strong> (' + (n.crashed ? 'CRASHED' : n.role) + ')</div>' +
      '<div>Term: ' + n.term + ' | Applied: ' + n.applied_idx + '</div>' +
      '<div>Fsyncs: ' + n.total_fsyncs + '</div>' +
      '<div style="margin-top:8px">' +
      (n.crashed
        ? '<button onclick="nodeAct(\''+n.id+'\',\'restart\')">Restart</button>'
        : '<button class="btn-danger" onclick="nodeAct(\''+n.id+'\',\'crash\')">Crash</button>' +
          '<button class="btn-secondary" onclick="nodeAct(\''+n.id+'\',\'flush\')">Flush SST</button>') +
      '</div></div>';
  }
}

async function sendKV(op) {
  const key = document.getElementById('kvKey').value;
  const value = document.getElementById('kvVal').value;
  const expected = document.getElementById('kvExp').value;
  const res = await fetch('/api/kv', {
    method: 'POST',
    body: JSON.stringify({op, key, value, expected})
  });
  const txt = await res.text();
  document.getElementById('kvOut').textContent = op + ' Response:\n' + txt;
  refreshStatus();
}

async function nodeAct(node_id, action) {
  await fetch('/api/node', {method: 'POST', body: JSON.stringify({node_id, action})});
  refreshStatus();
}

async function runSim() {
  const bug_id = parseInt(document.getElementById('bugSelect').value);
  const seed = parseInt(document.getElementById('simSeed').value);
  const nodes = bug_id === 13 ? 4 : 3;
  document.getElementById('simOut').textContent = 'Running deterministic simulation...';
  const res = await fetch('/api/sim', {
    method: 'POST',
    body: JSON.stringify({seed, nodes, bug_id})
  });
  const data = await res.json();
  document.getElementById('simOut').textContent = JSON.stringify({
    failed: data.failed,
    detector: data.detector,
    details: data.details,
    shrunk_schedule: data.initial_faults + ' -> ' + data.shrunk_faults + ' minimal faults'
  }, null, 2);
}

refreshStatus();
</script>
</body>
</html>`

func main() {
	port := flag.String("port", "8080", "HTTP listen port")
	flag.Parse()
	if envPort := os.Getenv("PORT"); envPort != "" {
		*port = envPort
	}

	srv := NewClusterServer()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(indexHTML))
	})
	http.HandleFunc("/api/status", srv.handleStatus)
	http.HandleFunc("/api/kv", srv.handleKV)
	http.HandleFunc("/api/node", srv.handleNodeAction)
	http.HandleFunc("/api/sim", handleSimBugZoo)

	fmt.Printf("AetherKV Interactive Playground listening on http://localhost:%s\n", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		panic(err)
	}
}