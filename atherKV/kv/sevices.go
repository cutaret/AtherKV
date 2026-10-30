package kv

import (
	"encoding/json"
	"path/filepath"
	"slices"

	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/raft/wal"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

// KVBugConfig controls KV-layer and persistence-seam Bug Zoo switches.
type KVBugConfig struct {
	RaftBugs             raft.BugConfig
	Bug05BlindLeaderRead bool // Zoo #5: Serve Get/Scan locally without ReadIndex quorum check
	Bug06LeaseUnderSkew  bool // Zoo #6: Serve lease read ignoring clock drift bound
	Bug07NoClientDedup   bool // Zoo #7: Disable (ClientID, SeqNum) session deduplication
}

type ClientReadReq struct {
	ReadID   uint64
	Key      []byte
	UseLease bool
	Callback func(val []byte, found bool, ok bool)
}

type waitingRead struct {
	req       ClientReadReq
	readIndex uint64
}

type waitingWrite struct {
	clientID uint64
	seqNum   uint64
	callback func(res statemachine.Result, ok bool)
}

// Replica binds Raft consensus, WAL/HardState persistence on vfs.FS, and the StateMachine.
type Replica struct {
	id        string
	peers     []string
	fs        vfs.FS
	dir       string
	clock     sim.Clock
	transport sim.Transport
	rand      sim.Rand
	bugs      KVBugConfig
	crashed   bool

	node       *raft.Node
	walStore   *wal.SegmentedWAL
	sm         statemachine.StateMachine
	appliedLog []raft.Entry

	leaseUntilNs  uint64
	leaseDuration uint64
	maxDriftBound uint64

	pendingReads  []ClientReadReq
	waitingReads  []waitingRead
	waitingWrites []waitingWrite
}

func NewReplica(
	id string,
	peers []string,
	fs vfs.FS,
	dir string,
	clock sim.Clock,
	transport sim.Transport,
	rng sim.Rand,
	bugs KVBugConfig,
) (*Replica, error) {
	r := &Replica{
		id:            id,
		peers:         slices.Clone(peers),
		fs:            fs,
		dir:           dir,
		clock:         clock,
		transport:     transport,
		rand:          rng,
		bugs:          bugs,
		leaseDuration: 50_000_000, // 50ms nominal lease
		maxDriftBound: 15_000_000, // 15ms safety margin against clock drift
	}
	if err := r.recoverAndStart(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Replica) hardStatePath() string {
	return filepath.Join(r.dir, "hardstate.json")
}

func (r *Replica) loadHardState() raft.HardState {
	f, err := r.fs.Open(r.hardStatePath())
	if err != nil {
		return raft.HardState{}
	}
	defer f.Close()
	sz, err := f.Size()
	if err != nil || sz <= 0 {
		return raft.HardState{}
	}
	buf := make([]byte, sz)
	n, _ := f.ReadAt(buf, 0)
	var hs raft.HardState
	if err := json.Unmarshal(buf[:n], &hs); err != nil {
		return raft.HardState{}
	}
	return hs
}

func (r *Replica) saveHardState(hs raft.HardState, syncDisk bool) {
	data, _ := json.Marshal(hs)
	f, err := r.fs.Create(r.hardStatePath())
	if err != nil {
		return
	}
	_, _ = f.WriteAt(data, 0)
	if syncDisk {
		_ = f.Sync()
		_ = r.fs.SyncDir(r.dir)
	}
	_ = f.Close()
}

func (r *Replica) recoverAndStart() error {
	if err := r.fs.MkdirAll(r.dir); err != nil {
		return err
	}
	_ = r.fs.SyncDir(".")

	hs := r.loadHardState()
	w, err := wal.Open(r.fs, filepath.Join(r.dir, "wal"))
	if err != nil {
		return err
	}
	r.walStore = w

	var entries []raft.Entry
	for idx := uint64(1); idx <= w.LastIndex(); idx++ {
		if e, ok := w.GetEntry(idx); ok {
			entries = append(entries, e)
		}
	}

	r.sm = statemachine.NewMemStore()
	r.appliedLog = nil
	r.pendingReads = nil
	r.waitingReads = nil
	r.waitingWrites = nil
	r.leaseUntilNs = 0

	r.node = raft.NewNode(raft.Config{
		ID:              r.id,
		Peers:           r.peers,
		ElectionTimeout: 5,
		HeartbeatPeriod: 2,
		Rand:            r.rand,
		Bugs:            r.bugs.RaftBugs,
	}, hs, entries)
	r.crashed = false
	return nil
}

func (r *Replica) Tick() {
	if r.crashed {
		return
	}
	rd := r.node.Tick()
	r.processReady(rd)
}

func (r *Replica) Step(msg sim.Message) {
	if r.crashed {
		return
	}
	// Extend lease when leader receives valid heartbeat/append acks from followers
	if r.node.Role() == raft.RoleLeader && (msg.Type == string(raft.MsgAppendResp) || msg.Type == string(raft.MsgHeartbeatResp)) {
		if r.bugs.Bug06LeaseUnderSkew {
			// Zoo #6: Grant full lease without subtracting clock-drift safety bound!
			r.leaseUntilNs = r.clock.Now() + r.leaseDuration
		} else {
			r.leaseUntilNs = r.clock.Now() + (r.leaseDuration - r.maxDriftBound)
		}
	}
	rd := r.node.Step(msg)
	r.processReady(rd)
}

// SubmitWrite proposes a client mutation (Put, Delete, CAS) to the Raft leader.
func (r *Replica) SubmitWrite(op statemachine.Op, cb func(res statemachine.Result, ok bool)) {
	if r.crashed || r.node.Role() != raft.RoleLeader {
		cb(statemachine.Result{}, false)
		return
	}

	appliedOp := op
	if r.bugs.Bug07NoClientDedup {
		// Zoo #7: Strip ClientID so session table never deduplicates retried commands!
		appliedOp.ClientID = 0
	}

	payload, _ := json.Marshal(appliedOp)
	r.waitingWrites = append(r.waitingWrites, waitingWrite{
		clientID: op.ClientID,
		seqNum:   op.SeqNum,
		callback: cb,
	})
	rd := r.node.Propose(payload)
	r.processReady(rd)
}

// SubmitRead executes a linearizable read via ReadIndex (or lease read if requested).
func (r *Replica) SubmitRead(req ClientReadReq) {
	if r.crashed || r.node.Role() != raft.RoleLeader {
		req.Callback(nil, false, false)
		return
	}

	// Zoo #5: Blind leader read skips ReadIndex quorum check and serves stale local state
	if r.bugs.Bug05BlindLeaderRead {
		val, found, _ := r.sm.Get(req.Key)
		req.Callback(val, found, true)
		return
	}

	// Opt-in lease read
	if req.UseLease && r.clock.Now() < r.leaseUntilNs {
		val, found, _ := r.sm.Get(req.Key)
		req.Callback(val, found, true)
		return
	}

	r.pendingReads = append(r.pendingReads, req)
	rd := r.node.ReadIndex(req.ReadID)
	r.processReady(rd)
}

func (r *Replica) processReady(rd raft.Ready) {
	// 1. Persist HardState and WAL entries BEFORE sending messages
	if rd.Persist != nil {
		syncDisk := !r.bugs.RaftBugs.Bug03AckBeforeSync // Zoo #3 skips fsync before sending!
		r.saveHardState(rd.Persist.HardState, syncDisk)

		if rd.Persist.TruncateFrom > 0 {
			r.walStore.TruncateSuffix(rd.Persist.TruncateFrom)
		}
		if len(rd.Persist.Entries) > 0 {
			_ = r.walStore.Append(rd.Persist.Entries)
			if syncDisk {
				_ = r.walStore.Sync()
			}
		}
	}

	// 2. Dispatch outbound Raft messages
	for _, m := range rd.Send {
		r.transport.Send(m)
	}

	// 3. Apply committed entries to StateMachine in order
	for _, ent := range rd.Committed {
		var op statemachine.Op
		if err := json.Unmarshal(ent.Data, &op); err == nil {
			resSlice := r.sm.ApplyBatch(ent.Index, []statemachine.Op{op})
			r.appliedLog = append(r.appliedLog, ent)

			if len(resSlice) > 0 {
				var remaining []waitingWrite
				fired := false
				for _, ww := range r.waitingWrites {
					if !fired && ww.clientID == op.ClientID && ww.seqNum == op.SeqNum {
						fired = true
						ww.callback(resSlice[0], true)
					} else {
						remaining = append(remaining, ww)
					}
				}
				r.waitingWrites = remaining
			}
		}
	}

	// 4. Advance confirmed ReadStates
	for _, rs := range rd.ReadStates {
		var stillPending []ClientReadReq
		for _, req := range r.pendingReads {
			if req.ReadID == rs.ReadID {
				r.waitingReads = append(r.waitingReads, waitingRead{req: req, readIndex: rs.Index})
			} else {
				stillPending = append(stillPending, req)
			}
		}
		r.pendingReads = stillPending
	}

	// 5. Serve any waiting reads whose readIndex <= sm.AppliedIndex()
	var stillWaiting []waitingRead
	for _, wr := range r.waitingReads {
		if r.sm.AppliedIndex() >= wr.readIndex {
			val, found, _ := r.sm.Get(wr.req.Key)
			wr.req.Callback(val, found, true)
		} else {
			stillWaiting = append(stillWaiting, wr)
		}
	}
	r.waitingReads = stillWaiting
}

// Crash simulates power loss on this replica, tearing unsynced writes in SimFS.
func (r *Replica) Crash(simFS *vfs.SimFS, diskRng sim.Rand) {
	if r.crashed {
		return
	}
	r.crashed = true
	// Fail any in-flight waiting callbacks as indeterminate timeouts
	for _, ww := range r.waitingWrites {
		ww.callback(statemachine.Result{}, false)
	}
	for _, pr := range r.pendingReads {
		pr.Callback(nil, false, false)
	}
	for _, wr := range r.waitingReads {
		wr.req.Callback(nil, false, false)
	}
	r.waitingWrites = nil
	r.pendingReads = nil
	r.waitingReads = nil
	simFS.Crash(diskRng)
}

// Restart recovers the replica from durable SimFS state.
func (r *Replica) Restart() error {
	if !r.crashed {
		return nil
	}
	return r.recoverAndStart()
}

func (r *Replica) StateView() raft.NodeStateView {
	return raft.NodeStateView{
		ID:         r.id,
		Crashed:    r.crashed,
		Role:       r.node.Role(),
		Term:       r.node.Term(),
		CommitIdx:  r.node.CommitIdx(),
		Log:        r.node.LogSnapshot(),
		AppliedLog: slices.Clone(r.appliedLog),
	}
}

func (r *Replica) IsLeader() bool {
	return !r.crashed && r.node.Role() == raft.RoleLeader
}
