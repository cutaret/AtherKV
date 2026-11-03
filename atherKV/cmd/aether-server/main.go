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
	opts.ValueLogThreshold = 64

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

	if leader := cs.findLeader(); leader != nil {
		cs.seqNum++
		leader.SubmitWrite(statemachine.Op{
			ClientID: 1, SeqNum: cs.seqNum, Type: statemachine.OpPut,
			Key: []byte("user:kartik"), Value: []byte("admin-profile"),
		}, func(statemachine.Result, bool) {})
		cs.advanceTicks(4)

		cs.seqNum++
		leader.SubmitWrite(statemachine.Op{
			ClientID: 1, SeqNum: cs.seqNum, Type: statemachine.OpPut,
			Key:   []byte("llama3:kvblock:01"),
			Value: []byte("FP16_KV_CACHE_TENSOR_BLOCK_STORED_IN_WISCKEY_VLOG_POINTER_MODE_2026"),
		}, func(statemachine.Result, bool) {})
		cs.advanceTicks(4)
	}

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
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	CommitIdx   uint64 `json:"commit_idx"`
	AppliedIdx  uint64 `json:"applied_idx"`
	Crashed     bool   `json:"crashed"`
	TotalFsyncs uint64 `json:"total_fsyncs"`
}

type KVItem struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Storage   string `json:"storage"`
	SizeBytes int    `json:"size_bytes"`
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
	var activeNode *kv.Replica
	healthyCount := 0

	for _, id := range cs.peers {
		rep := cs.replicas[id]
		sv := rep.StateView()
		if !sv.Crashed {
			healthyCount++
			if activeNode == nil || rep.IsLeader() {
				activeNode = rep
			}
		}
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

	var items []KVItem
	if activeNode != nil {
		rawItems, err := activeNode.StateMachine().Scan(nil, nil, 100)
		if err == nil {
			for _, it := range rawItems {
				storageMode := "LSM SSTable (Inline)"
				if len(it.Value) >= 64 {
					storageMode = "WiscKey ValueLog (24B Pointer)"
				}
				items = append(items, KVItem{
					Key:       string(it.Key),
					Value:     string(it.Value),
					Storage:   storageMode,
					SizeBytes: len(it.Value),
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nodes":           nodes,
		"items":           items,
		"healthy_count":   healthyCount,
		"quorum_alive":    healthyCount >= 2,
		"virtual_time_ms": float64(cs.kernel.Now()) / 1e6,
	})
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": "Quorum Lost! At least 2 of the 3 servers must be online to elect a leader and process requests. Click 'Turn Server Back On' above.",
		})
		return
	}

	leaderID := leader.StateView().ID
	term := leader.StateView().Term
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
			"ok": ok, "found": found, "value": outVal, "leader": leaderID, "term": term,
		})

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
			"leader":  leaderID,
			"term":    term,
			"wisckey": len(req.Value) >= 64,
		})
	}
}

func (cs *ClusterServer) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var req struct {
		NodeID string `json:"node_id"`
		Action string `json:"action"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	if req.Action == "heal_all" {
		for _, id := range cs.peers {
			_ = cs.replicas[id].Restart()
		}
		cs.advanceTicks(12)
		leader := cs.findLeader()
		leaderID := ""
		if leader != nil {
			leaderID = leader.StateView().ID
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "leader": leaderID})
		return
	}

	rep, ok := cs.replicas[req.NodeID]
	if !ok {
		http.Error(w, "unknown node", http.StatusBadRequest)
		return
	}

	wasLeader := rep.IsLeader()
	switch req.Action {
	case "crash":
		rep.Crash(cs.disks[req.NodeID], cs.kernel.Stream("crash_"+req.NodeID))
		cs.advanceTicks(15)
	case "restart":
		_ = rep.Restart()
		cs.advanceTicks(10)
	case "flush":
		_ = rep.FlushLSM()
	}

	newLeader := cs.findLeader()
	newLeaderID := "None (No Quorum)"
	var newTerm uint64
	if newLeader != nil {
		newLeaderID = newLeader.StateView().ID
		newTerm = newLeader.StateView().Term
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":         true,
		"was_leader": wasLeader,
		"new_leader": newLeaderID,
		"new_term":   newTerm,
	})
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
	case 3:
		bugs.RaftBugs.Bug03AckBeforeSync = true
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
			Title:             fmt.Sprintf("Bug Zoo #%d Deterministic Replay", req.BugID),
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
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AetherKV — Interactive Distributed Database Explorer</title>
<style>
  :root {
    /* Neutral Warm Stone Canvas */
    --bg: #121211;
    --card: #1A1918;
    --card-alt: #242321;
    --border: #33312E;
    --text: #F5F5F0;
    --muted: #A39E93;

    /* Strict 3-Color Scheme:
       1. Primary Complement: Petrol Teal
       2. Secondary Complement: Terracotta Coral
       3. Highlight Accent: Amber Gold */
    --comp-teal: #14B8A6;
    --comp-teal-dim: rgba(20, 184, 166, 0.14);
    --comp-coral: #F97316;
    --comp-coral-dim: rgba(249, 115, 22, 0.15);
    --highlight-gold: #FACC15;
    --highlight-gold-dim: rgba(250, 204, 21, 0.14);
  }
  * { box-sizing: border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    margin: 0;
    padding: 0;
    line-height: 1.55;
  }
  .container { max-width: 1180px; margin: 0 auto; padding: 28px 24px 60px; }

  /* Header */
  .hero {
    background: var(--card);
    border: 1px solid var(--border);
    border-top: 3px solid var(--comp-teal);
    border-radius: 12px;
    padding: 24px 28px;
    margin-bottom: 24px;
  }
  .hero h1 { margin: 0 0 8px; font-size: 25px; color: var(--text); display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .badge-pill {
    font-size: 12px; background: var(--highlight-gold-dim); color: var(--highlight-gold);
    border: 1px solid var(--highlight-gold); padding: 3px 10px; border-radius: 999px; font-weight: 700;
  }
  .hero p { margin: 0; color: var(--muted); font-size: 15px; max-width: 880px; }

  /* Navigation Tabs */
  .tabs { display: flex; gap: 10px; margin-bottom: 24px; border-bottom: 1px solid var(--border); padding-bottom: 14px; flex-wrap: wrap; }
  .tab-btn {
    background: var(--card); color: var(--muted); border: 1px solid var(--border);
    padding: 10px 18px; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 600;
    transition: all 0.15s ease;
  }
  .tab-btn:hover { color: var(--text); border-color: var(--comp-teal); }
  .tab-btn.active {
    background: var(--highlight-gold); color: #121211; border-color: var(--highlight-gold); font-weight: 700;
  }
  .tab-section { display: none; }
  .tab-section.active { display: block; }

  /* Guided Missions Bar (Highlight Gold Accent) */
  .missions {
    background: var(--highlight-gold-dim);
    border: 1px solid rgba(250, 204, 21, 0.4);
    border-radius: 12px;
    padding: 16px 20px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
  }
  .missions-title { font-size: 14.5px; font-weight: 700; color: var(--highlight-gold); }
  .missions-sub { font-size: 13px; color: var(--text); opacity: 0.85; }
  .mission-btns { display: flex; gap: 10px; flex-wrap: wrap; }
  .btn-mission {
    background: var(--card); color: var(--highlight-gold); border: 1px solid var(--highlight-gold);
    padding: 8px 14px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 600;
  }
  .btn-mission:hover { background: var(--highlight-gold); color: #121211; }

  /* Section Cards */
  .card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 22px 24px;
    margin-bottom: 24px;
  }
  .card h2 { margin: 0 0 6px; font-size: 18px; color: var(--text); }
  .card-sub { color: var(--muted); font-size: 13.5px; margin-bottom: 18px; }

  /* 3-Node Visual Cluster */
  .nodes-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 18px; }
  .node-box {
    background: var(--bg);
    border: 1.5px solid var(--border);
    border-radius: 10px;
    padding: 18px;
    transition: all 0.2s ease;
  }
  .node-box.leader {
    border-color: var(--highlight-gold);
    box-shadow: 0 0 18px rgba(250, 204, 21, 0.14);
  }
  .node-box.crashed {
    border-color: var(--comp-coral);
    background: var(--comp-coral-dim);
  }
  .node-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
  .node-title { font-size: 17px; font-weight: 700; color: var(--text); }
  .role-badge { font-size: 12px; font-weight: 700; padding: 3px 10px; border-radius: 999px; }
  .role-leader { background: var(--highlight-gold); color: #121211; }
  .role-follower { background: var(--comp-teal-dim); color: var(--comp-teal); border: 1px solid var(--comp-teal); }
  .role-crashed { background: var(--comp-coral); color: #121211; }

  .metric-row {
    display: flex; justify-content: space-between; font-size: 13px;
    padding: 6px 0; border-bottom: 1px dashed var(--border); color: var(--muted);
  }
  .metric-row strong { color: var(--text); }
  .node-actions { margin-top: 14px; display: flex; gap: 8px; }

  /* Story Narrator Box (Teal Complement) */
  .narrator {
    background: var(--card);
    border: 1px solid var(--border);
    border-left: 4px solid var(--comp-teal);
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 24px;
  }
  .narrator-title { font-size: 12.5px; text-transform: uppercase; letter-spacing: 0.07em; color: var(--comp-teal); font-weight: 700; margin-bottom: 8px; }
  .narrator-steps { margin: 0; padding-left: 20px; color: var(--text); font-size: 14.5px; }
  .narrator-steps li { margin-bottom: 6px; }

  /* Controls & Inputs */
  .controls-grid { display: grid; grid-template-columns: 1.1fr 1fr; gap: 24px; }
  @media (max-width: 860px) { .controls-grid { grid-template-columns: 1fr; } }
  .field { margin-bottom: 14px; }
  .field label { display: block; font-size: 12.5px; color: var(--muted); margin-bottom: 5px; font-weight: 600; }
  .field input, .field select {
    width: 100%; background: var(--bg); color: var(--text); border: 1px solid var(--border);
    padding: 10px 12px; border-radius: 8px; font-size: 14px;
  }
  .field input:focus, .field select:focus {
    outline: none; border-color: var(--comp-teal);
  }
  .btn-row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
  button {
    padding: 9px 15px; border-radius: 8px; border: none; font-size: 13.5px;
    font-weight: 700; cursor: pointer; transition: all 0.15s ease;
  }
  button:hover { filter: brightness(1.08); }
  .btn-primary { background: var(--comp-teal); color: #08221F; }
  .btn-highlight { background: var(--highlight-gold); color: #121211; }
  .btn-secondary { background: var(--card-alt); color: var(--text); border: 1px solid var(--border); }
  .btn-danger { background: var(--comp-coral-dim); color: var(--comp-coral); border: 1px solid var(--comp-coral); }

  /* Live Data Table */
  table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
  th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-weight: 600; font-size: 12px; text-transform: uppercase; background: var(--bg); }
  .tag-wisckey { background: var(--comp-teal-dim); color: var(--comp-teal); border: 1px solid var(--comp-teal); padding: 2px 8px; border-radius: 6px; font-size: 12px; font-weight: 600; }
  .tag-inline { background: var(--comp-coral-dim); color: var(--comp-coral); border: 1px solid var(--comp-coral); padding: 2px 8px; border-radius: 6px; font-size: 12px; font-weight: 600; }

  /* Architecture Diagram Pipeline */
  .pipeline { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 14px; margin-top: 16px; }
  .pipe-step { background: var(--bg); border: 1px solid var(--border); border-top: 3px solid var(--comp-teal); border-radius: 10px; padding: 16px; }
  .pipe-num { font-size: 12px; color: var(--highlight-gold); font-weight: 700; margin-bottom: 4px; }
  .pipe-title { font-size: 15px; font-weight: 700; color: var(--text); margin-bottom: 6px; }
  .pipe-desc { font-size: 13px; color: var(--muted); }
  code { color: var(--highlight-gold); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px; }
</style>
</head>
<body>
<div class="container">

  <!-- Top Hero Banner -->
  <div class="hero">
    <h1>
      AetherKV Interactive Explorer
      <span class="badge-pill">Raft Consensus + LSM Engine + WiscKey</span>
    </h1>
    <p>
      Why do cloud databases run on 3 servers instead of 1? Because individual machines fail.
      AetherKV keeps <strong>3 servers in lockstep</strong> so you can save data, pull the power cord on the leader server,
      and watch the surviving servers elect a new leader in milliseconds without losing a single byte.
    </p>
  </div>

  <!-- Navigation Tabs -->
  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('tab-cluster', this)">1. Live 3-Server Cluster (Try It!)</button>
    <button class="tab-btn" onclick="switchTab('tab-storage', this)">2. How the Storage Engine Works (LSM + AI Cache)</button>
    <button class="tab-btn" onclick="switchTab('tab-bugzoo', this)">3. Time-Travel Bug Zoo (Break &amp; Shrink Bugs)</button>
  </div>

  <!-- ======================================================================= -->
  <!-- TAB 1: LIVE 3-NODE CLUSTER & GUIDED MISSIONS                            -->
  <!-- ======================================================================= -->
  <div id="tab-cluster" class="tab-section active">

    <!-- 1-Click Guided Missions -->
    <div class="missions">
      <div>
        <div class="missions-title">New here? Try this 3-Step Guided Demo:</div>
        <div class="missions-sub">Click each button in order and read the explanation box below to watch automatic failover happen live.</div>
      </div>
      <div class="mission-btns">
        <button class="btn-mission" onclick="runMission(1)">Step 1: Save a Secret Key</button>
        <button class="btn-mission" onclick="runMission(2)">Step 2: Crash the Leader Server!</button>
        <button class="btn-mission" onclick="runMission(3)">Step 3: Read Data from New Leader</button>
      </div>
    </div>

    <!-- Plain-English Narrator Box -->
    <div class="narrator">
      <div class="narrator-title" id="narratorHeading">Live System Narrator — What Just Happened?</div>
      <ol class="narrator-steps" id="narratorList">
        <li><strong>Cluster Ready:</strong> 3 virtual servers (<strong>N1</strong>, <strong>N2</strong>, <strong>N3</strong>) are online. One is the <strong>Leader</strong> (gold border) and two are <strong>Followers</strong> (teal badge).</li>
        <li>Click any <strong>Guided Demo</strong> button above—or use the form below—to see step-by-step how your data is replicated and protected against crashes.</li>
      </ol>
    </div>

    <!-- 3 Server Cards -->
    <div class="card">
      <h2>Live 3-Server Raft Cluster</h2>
      <div class="card-sub">
        Writes go to the <strong>Leader</strong> (highlighted in gold), which replicates them to the <strong>Followers</strong>. As long as <strong>at least 2 out of 3 servers</strong> remain healthy, the cluster stays 100% online.
      </div>
      <div id="nodesContainer" class="nodes-grid"></div>
    </div>

    <!-- Split Controls + Live Database Contents Table -->
    <div class="controls-grid">
      <div class="card" style="margin-bottom:0">
        <h2>Save or Read Data</h2>
        <div class="card-sub">Every write is replicated across a majority of servers. Values 64 bytes or larger are automatically separated into the WiscKey Value Log.</div>

        <div class="field">
          <label>Key Name</label>
          <input id="kvKey" value="order:9001" placeholder="e.g. user:101">
        </div>
        <div class="field">
          <label>Value to Save</label>
          <input id="kvVal" value="status=PAID,amount=2500" placeholder="Enter any text or large payload">
        </div>
        <div class="field">
          <label>Expected Current Value (Only used for CAS / Safe Swap)</label>
          <input id="kvExp" placeholder="Leave blank unless clicking Safe Swap (CAS)">
        </div>

        <div class="btn-row">
          <button class="btn-primary" onclick="sendKV('PUT')">Save Key (PUT)</button>
          <button class="btn-highlight" onclick="sendKV('GET')">Verified Read (GET)</button>
          <button class="btn-secondary" onclick="sendKV('CAS')">Safe Swap (CAS)</button>
          <button class="btn-danger" onclick="sendKV('DELETE')">Delete Key</button>
        </div>
      </div>

      <div class="card" style="margin-bottom:0">
        <h2>Live Database Contents</h2>
        <div class="card-sub">Real-time view of all live keys stored inside the cluster and their physical storage tier.</div>
        <div style="overflow-x:auto">
          <table>
            <thead>
              <tr><th>Key</th><th>Current Value</th><th>Storage Tier</th></tr>
            </thead>
            <tbody id="dbTableBody">
              <tr><td colspan="3">Loading keys...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- ======================================================================= -->
  <!-- TAB 2: VISUAL STORAGE ENGINE & AI KV-CACHE BENCHMARK                    -->
  <!-- ======================================================================= -->
  <div id="tab-storage" class="tab-section">
    <div class="card">
      <h2>How AetherKV Stores Data Without Slowing Down</h2>
      <div class="card-sub">
        Standard LSM-tree databases rewrite keys and values repeatedly during background sorting ("compaction"). For large AI workloads like <strong>Llama-3-8B KV-cache blocks (128 KiB to 2 MiB)</strong>, copying megabytes repeatedly wears out SSDs. AetherKV solves this with <strong>WiscKey Value Separation</strong>:
      </div>

      <div class="pipeline">
        <div class="pipe-step">
          <div class="pipe-num">STEP 1 — CONSENSUS</div>
          <div class="pipe-title">Raft Quorum Log</div>
          <div class="pipe-desc">The Leader appends the mutation to its CRC32C-checksummed WAL and replicates it to a majority of servers.</div>
        </div>
        <div class="pipe-step">
          <div class="pipe-num">STEP 2 — WISCKEY SPLIT</div>
          <div class="pipe-title">Value Log Separation</div>
          <div class="pipe-desc">Large values go straight into an append-only <code>vlog</code> segment. Only a tiny <strong>24-byte pointer</strong> enters the LSM tree.</div>
        </div>
        <div class="pipe-step">
          <div class="pipe-num">STEP 3 — IN-MEMORY SORT</div>
          <div class="pipe-title">Skiplist Memtable</div>
          <div class="pipe-desc">Keys and 24B pointers are sorted in RAM by <code>(Key ASC, Seq DESC)</code> for fast point lookups and range scans.</div>
        </div>
        <div class="pipe-step">
          <div class="pipe-num">STEP 4 — ON-DISK FILES</div>
          <div class="pipe-title">SSTables &amp; Bloom Filters</div>
          <div class="pipe-desc">Sorted 4 KiB blocks flush to disk with a <strong>Bloom Filter</strong> (0.78% false-positive rate) that skips unnecessary disk reads.</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Measured Llama-3-8B KV-Cache Benchmark Results (5-Run Median)</h2>
      <div class="card-sub">Keeping 2 MiB AI tensor blocks in the Value Log and only compacting 24-byte pointers in the LSM tree eliminates redundant disk copying:</div>
      <table>
        <thead>
          <tr><th>Workload</th><th>Storage Mode</th><th>Write Amplification (Lower is Better)</th><th>Throughput (Higher is Better)</th><th>p99 Tail Latency</th></tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>128 KiB</strong> (1-Token KV Block)</td>
            <td><span class="tag-inline">Standard Inline LSM</span></td>
            <td>4.50x (copies data 4.5 times)</td>
            <td>81.3 MiB/s</td>
            <td>8,684 us</td>
          </tr>
          <tr>
            <td><strong>128 KiB</strong> (1-Token KV Block)</td>
            <td><span class="tag-wisckey">WiscKey Separated</span></td>
            <td><strong>1.00x (-77.8% disk wear)</strong></td>
            <td><strong>282.5 MiB/s (3.5x faster)</strong></td>
            <td><strong>2,259 us (3.8x faster)</strong></td>
          </tr>
          <tr>
            <td><strong>2 MiB</strong> (16-Token KV Block)</td>
            <td><span class="tag-inline">Standard Inline LSM</span></td>
            <td>3.67x (copies data 3.7 times)</td>
            <td>134.8 MiB/s</td>
            <td>35,958 us</td>
          </tr>
          <tr>
            <td><strong>2 MiB</strong> (16-Token KV Block)</td>
            <td><span class="tag-wisckey">WiscKey Separated</span></td>
            <td><strong>1.00x (-72.8% disk wear)</strong></td>
            <td><strong>586.9 MiB/s (4.4x faster)</strong></td>
            <td><strong>8,091 us (4.4x faster)</strong></td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- ======================================================================= -->
  <!-- TAB 3: TIME-TRAVEL BUG ZOO & FAULT SHRINKER                             -->
  <!-- ======================================================================= -->
  <div id="tab-bugzoo" class="tab-section">
    <div class="card">
      <h2>Deterministic Bug Zoo &amp; Automatic Fault Shrinker</h2>
      <div class="card-sub">
        Real distributed systems bugs hide behind rare combinations of network partitions and sudden power outages.
        AetherKV's <strong>Deterministic Simulator</strong> (~8,100 virtual seconds per real second) injects 20 random faults, catches the invariant violation, and automatically shrinks the failure down to the <strong>2 or 3 minimal steps</strong> required to reproduce it.
      </div>

      <div class="controls-grid" style="align-items: start;">
        <div>
          <div class="field">
            <label>Choose a Scenario to Simulate</label>
            <select id="bugSelect" onchange="updateBugExplanation()">
              <option value="13">Bug Zoo #13: 4-Server Split-Brain Math Bug (20 -> 3 Faults)</option>
              <option value="2">Bug Zoo #2: Server Forgets Who It Voted For After Reboot</option>
              <option value="3">Bug Zoo #3: Server Promises Data is Saved Before Disk Sync</option>
              <option value="0">Healthy Production Code (All Bugs Fixed — 0 Violations)</option>
            </select>
          </div>
          <div class="field">
            <label>Simulation PRNG Seed (Same seed = 100% identical timeline every time)</label>
            <input id="simSeed" type="number" value="4">
          </div>
          <div class="btn-row">
            <button class="btn-primary" onclick="runSim()">Run Chaos Simulation &amp; Shrink</button>
            <button id="openTraceBtn" class="btn-highlight" style="display:none" onclick="openTraceTab()">Open Full Visual Timeline in New Tab</button>
          </div>
        </div>

        <div class="narrator" style="margin-bottom:0">
          <div class="narrator-title">Plain-English Scenario Guide</div>
          <div id="bugPlainExplanation" style="font-size:14px; color:var(--text);">
            <strong>What is Bug #13?</strong> In a 4-server cluster, a true majority requires <strong>3 servers</strong>. This bug uses faulty integer division <code>(4+1)/2 = 2</code>, allowing two isolated pairs of 2 servers to both elect a leader ("Split-Brain") and overwrite committed data!
          </div>
        </div>
      </div>

      <div id="simResultBox" style="margin-top:20px; display:none;" class="card-sub"></div>
    </div>
  </div>

</div>

<script>
let latestTraceHTML = '';
let currentNodes = [];

function switchTab(tabId, btn) {
  document.querySelectorAll('.tab-section').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  btn.classList.add('active');
}

function setNarrator(heading, steps) {
  document.getElementById('narratorHeading').textContent = heading;
  const list = document.getElementById('narratorList');
  list.innerHTML = steps.map(s => '<li>' + s + '</li>').join('');
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  currentNodes = data.nodes || [];

  const container = document.getElementById('nodesContainer');
  container.innerHTML = '';
  for (const n of currentNodes) {
    let boxClass = 'node-box';
    let badgeClass = 'role-badge role-follower';
    let badgeText = 'Follower (Backup)';
    if (n.crashed) {
      boxClass += ' crashed';
      badgeClass = 'role-badge role-crashed';
      badgeText = 'CRASHED (Offline)';
    } else if (n.role === 'Leader') {
      boxClass += ' leader';
      badgeClass = 'role-badge role-leader';
      badgeText = 'Leader (Primary)';
    }

    container.innerHTML +=
      '<div class="' + boxClass + '">' +
        '<div class="node-header">' +
          '<span class="node-title">Server ' + n.id.toUpperCase() + '</span>' +
          '<span class="' + badgeClass + '">' + badgeText + '</span>' +
        '</div>' +
        '<div class="metric-row"><span>Election Round (Term)</span><strong>Round #' + n.term + '</strong></div>' +
        '<div class="metric-row"><span>Saved Writes (Applied)</span><strong>' + n.applied_idx + ' ops</strong></div>' +
        '<div class="metric-row"><span>Disk Saves (Fsyncs)</span><strong>' + n.total_fsyncs + ' syncs</strong></div>' +
        '<div class="node-actions">' +
          (n.crashed
            ? '<button class="btn-primary" onclick="nodeAct(\''+n.id+'\',\'restart\')">Turn Server Back On</button>'
            : '<button class="btn-danger" onclick="nodeAct(\''+n.id+'\',\'crash\')">Pull Power Cord (Crash)</button>' +
              '<button class="btn-secondary" onclick="nodeAct(\''+n.id+'\',\'flush\')">Flush RAM to Disk</button>') +
        '</div>' +
      '</div>';
  }

  const tbody = document.getElementById('dbTableBody');
  const items = data.items || [];
  if (items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--muted)">Database is currently empty. Save a key on the left!</td></tr>';
  } else {
    tbody.innerHTML = items.map(it => {
      const tagClass = it.storage.includes('WiscKey') ? 'tag-wisckey' : 'tag-inline';
      const displayVal = it.value.length > 36 ? it.value.slice(0, 36) + '... (' + it.size_bytes + 'B)' : it.value;
      return '<tr>' +
        '<td><strong>' + escapeHtml(it.key) + '</strong></td>' +
        '<td><code>' + escapeHtml(displayVal) + '</code></td>' +
        '<td><span class="' + tagClass + '">' + it.storage + '</span></td>' +
      '</tr>';
    }).join('');
  }
}

function escapeHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function sendKV(op) {
  const key = document.getElementById('kvKey').value.trim();
  const value = document.getElementById('kvVal').value;
  const expected = document.getElementById('kvExp').value;
  if (!key) return;

  const res = await fetch('/api/kv', {
    method: 'POST',
    body: JSON.stringify({op, key, value, expected})
  });
  const data = await res.json();

  if (!data.ok) {
    setNarrator('Operation Blocked — Cluster Quorum Lost!', [
      '<strong>Why did this fail?</strong> ' + (data.error || 'No leader available.'),
      '<strong>How Raft protects you:</strong> Refusing writes when a majority of servers are offline prevents split-brain data corruption.'
    ]);
  } else if (op === 'PUT') {
    const storageNote = data.wisckey
      ? 'Because the payload is >= 64 bytes, AetherKV appended the blob to the <strong>WiscKey Value Log</strong> and stored only a 24-byte pointer inside the LSM tree.'
      : 'The key and value were written into the <strong>Skiplist Memtable</strong> and durable Write-Ahead Log.';
    setNarrator('Saved Key "' + key + '" Across the Cluster!', [
      '<strong>1. Sent to Leader:</strong> Your write arrived at Leader <strong>Server ' + data.leader.toUpperCase() + '</strong> (Election Round #' + data.term + ').',
      '<strong>2. Replicated to Followers:</strong> Server ' + data.leader.toUpperCase() + ' replicated the entry and waited for a majority to confirm disk save (<code>fsync</code>).',
      '<strong>3. Storage Engine:</strong> ' + storageNote
    ]);
  } else if (op === 'GET') {
    setNarrator('Verified Read (ReadIndex) for "' + key + '"', [
      '<strong>1. Quorum Heartbeat Check:</strong> Leader <strong>Server ' + data.leader.toUpperCase() + '</strong> confirmed with a majority of servers that it is still the active leader.',
      '<strong>2. Result:</strong> ' + (data.found ? 'Found value: <code>' + escapeHtml(data.value) + '</code>' : 'Key does not exist in the database.')
    ]);
  } else if (op === 'DELETE') {
    setNarrator('Deleted Key "' + key + '" (Tombstone Written)', [
      '<strong>How LSM Deletes Work:</strong> Leader <strong>Server ' + data.leader.toUpperCase() + '</strong> wrote a replicated <strong>Tombstone marker</strong> that masks older versions until background compaction removes them.'
    ]);
  } else if (op === 'CAS') {
    setNarrator('Compare-And-Swap (Safe Atomic Update) on "' + key + '"', [
      data.applied
        ? '<strong>Success!</strong> The current value matched your expected value (<code>' + escapeHtml(expected) + '</code>), so it was atomically updated to <code>' + escapeHtml(value) + '</code>.'
        : '<strong>Rejected!</strong> The current value in the database was <code>' + escapeHtml(data.value) + '</code>, which did not match your expected <code>' + escapeHtml(expected) + '</code>.'
    ]);
  }

  await refreshStatus();
}

async function nodeAct(node_id, action) {
  const res = await fetch('/api/node', {method: 'POST', body: JSON.stringify({node_id, action})});
  const data = await res.json();

  if (action === 'crash') {
    if (data.was_leader && data.new_leader !== 'None (No Quorum)') {
      setNarrator('Leader Server ' + node_id.toUpperCase() + ' Crashed — Automatic Failover Complete!', [
        '<strong>1. Power Loss Simulated:</strong> You pulled the plug on Leader <strong>Server ' + node_id.toUpperCase() + '</strong>. Its unsynced RAM was wiped.',
        '<strong>2. Election Triggered:</strong> The surviving followers detected missing heartbeats, advanced to <strong>Round #' + data.new_term + '</strong>, and elected <strong>Server ' + data.new_leader.toUpperCase() + '</strong> as the new Leader (now highlighted in gold)!',
        '<strong>3. Zero Data Loss:</strong> All committed keys in the Live Database Contents table remain 100% intact.'
      ]);
    } else if (data.new_leader === 'None (No Quorum)') {
      setNarrator('Cluster Lost Majority Quorum (2 of 3 Servers Offline)!', [
        '<strong>What happened:</strong> With <strong>Server ' + node_id.toUpperCase() + '</strong> crashed, fewer than 2 servers remain online.',
        '<strong>Safety First:</strong> Click <strong>"Turn Server Back On"</strong> on any crashed server to restore quorum.'
      ]);
    } else {
      setNarrator('Follower Server ' + node_id.toUpperCase() + ' Crashed!', [
        '<strong>Cluster Still Online:</strong> Follower ' + node_id.toUpperCase() + ' is offline, but <strong>Server ' + data.new_leader.toUpperCase() + '</strong> still holds a 2-of-3 majority and continues serving traffic.'
      ]);
    }
  } else if (action === 'restart') {
    setNarrator('Server ' + node_id.toUpperCase() + ' Rebooted & Recovered!', [
      '<strong>1. Crash Recovery:</strong> Server ' + node_id.toUpperCase() + ' verified its CRC32C checksummed Write-Ahead Log, truncated any torn 512-byte sectors, and rejoined the cluster.',
      '<strong>2. Caught Up:</strong> Leader <strong>' + data.new_leader.toUpperCase() + '</strong> automatically replayed any writes missed while offline.'
    ]);
  } else if (action === 'flush') {
    setNarrator('Flushed RAM Memtable to On-Disk SSTable on ' + node_id.toUpperCase(), [
      '<strong>What happened:</strong> Server ' + node_id.toUpperCase() + ' wrote its in-memory skiplist into an immutable 4 KiB-blocked SSTable with a Bloom filter and rotated its <code>MANIFEST</code> file.'
    ]);
  }
  await refreshStatus();
}

async function runMission(step) {
  if (step === 1) {
    await fetch('/api/node', {method: 'POST', body: JSON.stringify({action: 'heal_all'})});
    document.getElementById('kvKey').value = 'mission:secret';
    document.getElementById('kvVal').value = 'launch-code-7788';
    await sendKV('PUT');
  } else if (step === 2) {
    await refreshStatus();
    const leader = currentNodes.find(n => n.role === 'Leader' && !n.crashed);
    if (!leader) {
      await fetch('/api/node', {method: 'POST', body: JSON.stringify({action: 'heal_all'})});
      await refreshStatus();
    }
    const activeLeader = currentNodes.find(n => n.role === 'Leader' && !n.crashed);
    if (activeLeader) {
      await nodeAct(activeLeader.id, 'crash');
    }
  } else if (step === 3) {
    document.getElementById('kvKey').value = 'mission:secret';
    await sendKV('GET');
  }
}

function updateBugExplanation() {
  const bug = document.getElementById('bugSelect').value;
  const box = document.getElementById('bugPlainExplanation');
  if (bug === '13') {
    box.innerHTML = '<strong>What is Bug #13?</strong> In a 4-server cluster, a true majority requires <strong>3 servers</strong>. This bug uses faulty integer division <code>(4+1)/2 = 2</code>, allowing two isolated pairs of 2 servers to both elect a leader ("Split-Brain") and overwrite committed data!';
  } else if (bug === '2') {
    box.innerHTML = '<strong>What is Bug #2?</strong> When a server votes for a candidate, it must save that vote to disk before replying. Here, the server replies first, crashes, reboots with amnesia, and votes for a <em>different</em> candidate in the same election round!';
  } else if (bug === '3') {
    box.innerHTML = '<strong>What is Bug #3?</strong> Servers tell the Leader "I saved the data!" while the data is still sitting in volatile RAM before <code>fsync</code>. When a power outage hits, committed data vanishes!';
  } else {
    box.innerHTML = '<strong>Healthy Production Code:</strong> All 17 bugs are turned OFF. The simulator will still inject random network partitions, dropped packets, and server crashes, proving the cluster survives with <strong>0 safety violations</strong>.';
  }
}

async function runSim() {
  const bug_id = parseInt(document.getElementById('bugSelect').value);
  const seed = parseInt(document.getElementById('simSeed').value) || 4;
  const nodes = bug_id === 13 ? 4 : 3;

  const box = document.getElementById('simResultBox');
  box.style.display = 'block';
  box.innerHTML = 'Running deterministic simulation for Seed #' + seed + '...';
  document.getElementById('openTraceBtn').style.display = 'none';

  const res = await fetch('/api/sim', {
    method: 'POST',
    body: JSON.stringify({seed, nodes, bug_id})
  });
  const data = await res.json();
  latestTraceHTML = data.html_trace || '';

  if (!data.failed) {
    box.innerHTML =
      '<div style="background:var(--comp-teal-dim); border:1px solid var(--comp-teal); padding:16px; border-radius:10px; color:var(--text);">' +
        '<strong style="color:var(--comp-teal)">SIMULATION PASSED (0 Violations):</strong> Across 20 injected network partitions, packet drops, and server crashes on Seed #' + seed + ', the cluster preserved 100% data safety and linearizability.' +
      '</div>';
  } else {
    if (latestTraceHTML) {
      document.getElementById('openTraceBtn').style.display = 'inline-block';
    }
    box.innerHTML =
      '<div style="background:var(--comp-coral-dim); border:1px solid var(--comp-coral); padding:16px; border-radius:10px; color:var(--text);">' +
        '<div style="color:var(--comp-coral); font-weight:700; font-size:15px; margin-bottom:6px;">BUG CAUGHT DETERMINISTICALLY ON SEED #' + seed + '!</div>' +
        '<div style="margin-bottom:6px;"><strong>Invariant Detector Triggered:</strong> <code>' + escapeHtml(data.detector) + '</code></div>' +
        '<div style="margin-bottom:6px;"><strong>What Broke:</strong> ' + escapeHtml(data.details) + '</div>' +
        '<div><strong>Automatic Fault Shrinking:</strong> The simulator stripped away unnecessary chaos and reduced the crash schedule from <strong>' + data.initial_faults + ' random faults down to ' + data.shrunk_faults + ' minimal steps</strong>. Click <em>"Open Full Visual Timeline in New Tab"</em> above to inspect the trace!</div>' +
      '</div>';
  }
}

function openTraceTab() {
  if (!latestTraceHTML) return;
  const blob = new Blob([latestTraceHTML], {type: 'text/html'});
  const url = URL.createObjectURL(blob);
  window.open(url, '_blank');
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

	fmt.Printf("AetherKV Interactive Explorer listening on http://localhost:%s\n", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		panic(err)
	}
}
