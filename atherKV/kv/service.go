package kv

import (
	"encoding/json"
	"path/filepath"
	"slices"

	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/raft/wal"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

type KVBugConfig struct {
	RaftBugs                  raft.BugConfig
	UseLSM                    bool
	LSMOpts                   lsm.Options
	Bug05BlindLeaderRead      bool // Zoo #5: Serve Get/Scan locally without ReadIndex quorum check
	Bug06LeaseUnderSkew       bool // Zoo #6: Serve lease read ignoring clock drift bound
	Bug07NoClientDedup        bool // Zoo #7: Disable (ClientID, SeqNum) session deduplication
	Bug11PrematureLogTruncate bool // Zoo #11: Under Option B, truncate Raft log past LSM FlushedIndex
	Bug12SnapshotOmitDedup    bool // Zoo #12: InstallSnapshot omits client session deduplication table
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
	lsmEngine  *lsm.LSMStore
	appliedLog []raft.Entry

	leaseUntilNs  uint64
	leaseDuration uint64
	maxDriftBound uint64

	pendingReads  []ClientReadReq
	waitingReads  []waitingRead
	waitingWrites []waitingWrite
	raftFsyncs    uint64
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
		leaseDuration: 50_000_000,
		maxDriftBound: 15_000_000,
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
		_ = r.fs.SyncDir(".")
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
	for idx := w.FirstIndex(); idx <= w.LastIndex(); idx++ {
		if e, ok := w.GetEntry(idx); ok {
			entries = append(entries, e)
		}
	}

	if r.bugs.UseLSM {
		opts := r.bugs.LSMOpts
		opts.Bugs.Bug12SnapshotOmitDedup = r.bugs.Bug12SnapshotOmitDedup
		lsmDB, lErr := lsm.OpenLSMStore(r.fs, filepath.Join(r.dir, "lsm"), r.rand, opts)
		if lErr != nil {
			return lErr
		}
		r.lsmEngine = lsmDB
		r.sm = lsmDB
	} else {
		r.lsmEngine = nil
		r.sm = statemachine.NewMemStore()
	}

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

func (r *Replica) failInFlightOnStepDown() {
	if r.node.Role() != raft.RoleLeader {
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
	}
}

func (r *Replica) Tick() {
	if r.crashed {
		return
	}
	rd := r.node.Tick()
	r.failInFlightOnStepDown()
	r.processReady(rd)
}

func (r *Replica) Step(msg sim.Message) {
	if r.crashed {
		return
	}
	if r.node.Role() == raft.RoleLeader && (msg.Type == string(raft.MsgAppendResp) || msg.Type == string(raft.MsgHeartbeatResp)) {
		if r.bugs.Bug06LeaseUnderSkew {
			r.leaseUntilNs = r.clock.Now() + r.leaseDuration
		} else {
			r.leaseUntilNs = r.clock.Now() + (r.leaseDuration - r.maxDriftBound)
		}
	}
	rd := r.node.Step(msg)
	r.failInFlightOnStepDown()
	r.processReady(rd)
}

func (r *Replica) SubmitWrite(op statemachine.Op, cb func(res statemachine.Result, ok bool)) {
	if r.crashed || r.node.Role() != raft.RoleLeader {
		cb(statemachine.Result{}, false)
		return
	}

	appliedOp := op
	if r.bugs.Bug07NoClientDedup {
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

func (r *Replica) SubmitRead(req ClientReadReq) {
	if r.crashed || r.node.Role() != raft.RoleLeader {
		req.Callback(nil, false, false)
		return
	}

	if r.bugs.Bug05BlindLeaderRead {
		val, found, _ := r.sm.Get(req.Key)
		req.Callback(val, found, true)
		return
	}

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
	if rd.Persist != nil {
		syncHardState := !r.bugs.RaftBugs.Bug02SkipVotePersist
		r.saveHardState(rd.Persist.HardState, syncHardState)

		if rd.Persist.TruncateFrom > 0 {
			r.walStore.TruncateSuffix(rd.Persist.TruncateFrom)
		}
		if len(rd.Persist.Entries) > 0 {
			_ = r.walStore.Append(rd.Persist.Entries)
			if !r.bugs.RaftBugs.Bug03AckBeforeSync {
				_ = r.walStore.Sync()
				r.raftFsyncs++
			}
		}
	}

	if rd.Snapshot != nil {
		_ = r.sm.Restore(rd.Snapshot.Data)
		r.walStore.CompactPrefix(rd.Snapshot.Index)
	}

	for _, m := range rd.Send {
		r.transport.Send(m)
	}

	for _, ent := range rd.Committed {
		// Skip entries already durable in the StateMachine (e.g. recovered from LSM SSTables/WAL)
		if ent.Index <= r.sm.AppliedIndex() {
			r.appliedLog = append(r.appliedLog, ent)
			continue
		}
		if len(ent.Data) == 0 {
			r.sm.ApplyBatch(ent.Index, nil)
			r.appliedLog = append(r.appliedLog, ent)
			continue
		}
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

// FlushLSM forces the underlying LSM engine (if active) to flush its Memtable to an L0 SSTable.
func (r *Replica) FlushLSM() error {
	if r.lsmEngine != nil {
		return r.lsmEngine.Flush()
	}
	return nil
}

// TruncateRaftLog compacts the Raft WAL prefix.
// Under Option B, safe truncation only truncates up to LSMStore.FlushedIndex();
// Bug11PrematureLogTruncate truncates up to AppliedIndex() (losing unflushed memtable data on crash!).
func (r *Replica) TruncateRaftLog() {
	if r.lsmEngine == nil {
		r.walStore.CompactPrefix(r.sm.AppliedIndex())
		return
	}
	if r.bugs.Bug11PrematureLogTruncate {
		// Zoo #11: Truncate Raft WAL past FlushedIndex up to in-memory AppliedIndex!
		r.walStore.CompactPrefix(r.lsmEngine.AppliedIndex())
		return
	}
	if r.bugs.LSMOpts.DisableWALSync {
		// Safe Option B: Only truncate up to durable FlushedIndex
		r.walStore.CompactPrefix(r.lsmEngine.FlushedIndex())
	} else {
		// Safe Option A: LSM WAL is synced on every batch, so AppliedIndex is durable
		r.walStore.CompactPrefix(r.lsmEngine.AppliedIndex())
	}
}

// CreateSnapshot captures a state machine snapshot at AppliedIndex and compacts the Raft log.
func (r *Replica) CreateSnapshot() error {
	idx := r.sm.AppliedIndex()
	if idx == 0 {
		return nil
	}
	snapBytes, err := r.sm.Snapshot()
	if err != nil {
		return err
	}
	r.node.CompactLog(idx, snapBytes)
	r.walStore.CompactPrefix(idx)
	return nil
}

func (r *Replica) Crash(simFS *vfs.SimFS, diskRng sim.Rand) {
	if r.crashed {
		return
	}
	r.crashed = true
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

func (r *Replica) Restart() error {
	if !r.crashed {
		return nil
	}
	if err := r.recoverAndStart(); err != nil {
		return err
	}
	// Replay any durable Raft WAL entries that were not yet flushed to LSMStore before the crash
	for idx := r.sm.AppliedIndex() + 1; idx <= r.walStore.LastIndex(); idx++ {
		if ent, ok := r.walStore.GetEntry(idx); ok {
			if len(ent.Data) == 0 {
				r.sm.ApplyBatch(ent.Index, nil)
				continue
			}
			var op statemachine.Op
			if err := json.Unmarshal(ent.Data, &op); err == nil {
				r.sm.ApplyBatch(ent.Index, []statemachine.Op{op})
			}
		}
	}
	return nil
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

func (r *Replica) IsLeader() bool                          { return !r.crashed && r.node.Role() == raft.RoleLeader }
func (r *Replica) StateMachine() statemachine.StateMachine { return r.sm }
func (r *Replica) TotalFsyncCount() uint64 {
	total := r.raftFsyncs
	if r.lsmEngine != nil {
		total += r.lsmEngine.FsyncCount()
	}
	return total
}
