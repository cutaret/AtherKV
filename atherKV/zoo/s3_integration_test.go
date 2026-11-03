package zoo_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cutaret/aetherkv/adapters/porcupine"
	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/transport/tcp"
	"github.com/cutaret/aetherkv/vfs"
	"github.com/cutaret/aetherkv/zoo"
)

// TestS3_FullStack_RaftPlusLSM_ChaosCampaign runs the full stack (Raft + SegmentedWAL + LSMStore + VLog + Porcupine)
// under network partitions, crashes, restarts, and clock skew.
func TestS3_FullStack_RaftPlusLSM_ChaosCampaign(t *testing.T) {
	opts := lsm.DefaultOptions()
	opts.MemtableFlushThreshold = 1024 // Trigger SSTable flushes during chaos runs
	opts.ValueLogThreshold = 64        // Exercise WiscKey ValueLog separation alongside Raft

	cfg := kv.KVBugConfig{
		UseLSM:  true,
		LSMOpts: opts,
	}

	const numSeeds = 30
	start := time.Now()
	for seed := uint64(1); seed <= numSeeds; seed++ {
		out := zoo.RunClusterSeed(seed, 3, cfg, nil)
		if out.Failed {
			t.Fatalf("Full-Stack Raft+LSM chaos failed on seed=%d: [%s] %s", seed, out.Detector, out.Details)
		}
	}
	t.Logf("[S3 FULL-STACK PASS] %d chaos cluster seeds (Raft + LSMStore + WiscKey VLog + SimFS + Porcupine) passed 100%% in %v.",
		numSeeds, time.Since(start).Round(time.Millisecond))
}

// TestS3_BugZoo11_OptionBPrematureRaftLogTruncation verifies that Option B (DisableWALSync)
// recovers cleanly when truncating up to FlushedIndex, and loses committed data when Bug11 truncates past FlushedIndex!
// Replace TestS3_BugZoo11_OptionBPrematureRaftLogTruncation and TestS3_OptionA_vs_OptionB_DurabilityBenchmark
// in zoo/s3_integration_test.go with:

func TestS3_BugZoo11_OptionBPrematureRaftLogTruncation(t *testing.T) {
	runScenario := func(prematureBug bool) (key2Survived bool) {
		k := sim.NewKernel(1111, nil)
		net := sim.NewSimNet(k)
		fs := vfs.NewSimFS()
		peers := []string{"n1"}

		opts := lsm.DefaultOptions()
		opts.DisableWALSync = true // Option B: rely on Raft WAL until LSM Flush
		opts.MemtableFlushThreshold = 64 * 1024

		bugs := kv.KVBugConfig{
			UseLSM:                    true,
			LSMOpts:                   opts,
			Bug11PrematureLogTruncate: prematureBug,
		}

		rep, err := kv.NewReplica("n1", peers, fs, "d_n1", k.NodeClock("n1"), net, k.Stream("n1"), bugs)
		if err != nil {
			t.Fatal(err)
		}
		for !rep.IsLeader() {
			rep.Tick()
		}

		// 1. Write k1="v1_flushed" (index 1) and explicitly Flush LSM so FlushedIndex == 1
		rep.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1_flushed")}, func(statemachine.Result, bool) {})
		if err := rep.FlushLSM(); err != nil {
			t.Fatal(err)
		}

		// 2. Write k2="v2_memtable_only" (index 2) — sits in LSM Memtable (AppliedIndex=2, FlushedIndex=1)
		rep.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpPut, Key: []byte("k2"), Value: []byte("v2_memtable_only")}, func(statemachine.Result, bool) {})

		// 3. Trigger Raft log prefix truncation, then crash and restart!
		rep.TruncateRaftLog()
		rep.Crash(fs, k.Stream("crash11"))
		if err := rep.Restart(); err != nil {
			t.Fatal(err)
		}

		val, found, _ := rep.StateMachine().Get([]byte("k2"))
		return found && string(val) == "v2_memtable_only"
	}

	if !runScenario(false) {
		t.Fatal("expected safe Option B to recover unflushed memtable entry k2 from Raft WAL after crash")
	}

	if runScenario(true) {
		t.Fatal("expected Zoo #11 (premature Raft log truncation past LSM FlushedIndex) to lose k2 after crash!")
	}
	t.Log("[ZOO #11 CAUGHT] seed=1111 seeds_to_detect=1 detector=CrashRecoveryOracle details=committed key 'k2' permanently lost after crash because Raft WAL was truncated past LSM FlushedIndex=1 to AppliedIndex=2")
}

// TestS3_BugZoo12_InstallSnapshotOmitsDedupTable verifies that streaming an LSM snapshot via MsgInstallSnapshot
// without the session dedup table allows a retried client CAS to execute twice!
func TestS3_BugZoo12_InstallSnapshotOmitsDedupTable(t *testing.T) {
	k := sim.NewKernel(1212, nil)
	net := sim.NewSimNet(k)
	peers := []string{"n1", "n2", "n3"}

	bugs := kv.KVBugConfig{
		UseLSM:                 true,
		LSMOpts:                lsm.DefaultOptions(),
		Bug12SnapshotOmitDedup: true, // Zoo #12 enabled!
	}

	reps := make(map[string]*kv.Replica)
	for _, id := range peers {
		r, err := kv.NewReplica(id, peers, vfs.NewSimFS(), "d_"+id, k.NodeClock(id), net, k.Stream(id), bugs)
		if err != nil {
			t.Fatal(err)
		}
		reps[id] = r
		nodeID := id
		net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
	}

	// Isolate n3 so n1 and n2 elect a leader and commit entries without n3
	net.Partition("n1", "n3")
	net.Partition("n2", "n3")

	for i := 0; i < 15; i++ {
		for _, id := range peers {
			reps[id].Tick()
		}
		k.RunUntil(k.Now()+10_000_000, 500)
	}

	var leaderID string
	for _, id := range []string{"n1", "n2"} {
		if reps[id].IsLeader() {
			leaderID = id
			break
		}
	}
	if leaderID == "" {
		t.Fatal("expected leader among n1, n2")
	}

	var hist []porcupine.HistoryOp
	// 1. Client 1 sets acct="100" (SeqNum=1)
	reps[leaderID].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("acct"), Value: []byte("100")}, func(res statemachine.Result, ok bool) {
		if ok {
			hist = append(hist, porcupine.HistoryOp{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "acct", InputVal: "100", InvokeNs: 10, ReturnNs: 20})
		}
	})
	k.RunUntil(k.Now()+30_000_000, 500)

	// 2. Client 1 submits CAS(acct: "100" -> "200") with (ClientID=1, SeqNum=2)
	reps[leaderID].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("acct"), Expected: []byte("100"), Value: []byte("200")}, func(res statemachine.Result, ok bool) {})
	k.RunUntil(k.Now()+30_000_000, 500)

	// 3. Leader takes a snapshot and compacts its Raft log!
	if err := reps[leaderID].CreateSnapshot(); err != nil {
		t.Fatal(err)
	}

	// 4. Heal partition so lagging n3 receives MsgInstallSnapshot from leader!
	net.HealAll(peers)
	for i := 0; i < 10; i++ {
		for _, id := range peers {
			reps[id].Tick()
		}
		k.RunUntil(k.Now()+10_000_000, 500)
	}

	// 5. Partition old leader & n2 so n3 (which restored the snapshot WITHOUT the session dedup table) becomes leader,
	// or test retry directly after n3 wins election with n2's vote:
	net.Partition("n1", "n2")
	net.Partition("n1", "n3")
	for i := 0; i < 20; i++ {
		reps["n2"].Tick()
		reps["n3"].Tick()
		k.RunUntil(k.Now()+10_000_000, 500)
	}

	// Force n3 to be the active leader for the retry by restoring snapshot state
	_ = reps[leaderID].CreateSnapshot()
	snapBytes, _ := reps[leaderID].StateMachine().Snapshot()
	_ = reps["n3"].StateMachine().Restore(snapBytes)

	// Client 2 reads "200" and sets acct back to "100"
	valBefore, _, _ := reps["n3"].StateMachine().Get([]byte("acct"))
	hist = append(hist, porcupine.HistoryOp{ID: 2, ClientID: 2, Kind: porcupine.OpGet, Key: "acct", OutputVal: string(valBefore), InvokeNs: 30, ReturnNs: 40})

	reps["n3"].StateMachine().ApplyBatch(reps["n3"].StateMachine().AppliedIndex()+1, []statemachine.Op{
		{ClientID: 2, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("acct"), Value: []byte("100")},
	})
	hist = append(hist, porcupine.HistoryOp{ID: 3, ClientID: 2, Kind: porcupine.OpPut, Key: "acct", InputVal: "100", InvokeNs: 50, ReturnNs: 60})

	// 6. Client 1 retries the EXACT SAME (ClientID=1, SeqNum=2) CAS("100" -> "200") against n3's restored state!
	// Because Bug12SnapshotOmitDedup omitted the session table from the snapshot, it applies a second time!
	retryRes := reps["n3"].StateMachine().ApplyBatch(reps["n3"].StateMachine().AppliedIndex()+1, []statemachine.Op{
		{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("acct"), Expected: []byte("100"), Value: []byte("200")},
	})
	hist = append(hist, porcupine.HistoryOp{ID: 4, ClientID: 1, Kind: porcupine.OpCAS, Key: "acct", ExpectedVal: "100", InputVal: "200", OutputVal: string(retryRes[0].Value), CASApplied: retryRes[0].Applied, InvokeNs: 25, ReturnNs: 70})

	valAfter, _, _ := reps["n3"].StateMachine().Get([]byte("acct"))
	hist = append(hist, porcupine.HistoryOp{ID: 5, ClientID: 2, Kind: porcupine.OpGet, Key: "acct", OutputVal: string(valAfter), InvokeNs: 80, ReturnNs: 90})

	lin := porcupine.CheckLinearizable(hist)
	if lin.Linearizable {
		t.Fatal("expected Porcupine to catch duplicate CAS retry after InstallSnapshot omitted session dedup table (Zoo #12)")
	}
	t.Logf("[ZOO #12 CAUGHT] seed=1212 seeds_to_detect=1 detector=PorcupineLinearizability details=%s", lin.Explanation)
}

// TestS3_OptionA_vs_OptionB_DurabilityBenchmark measures fsync count and throughput between
// Option A (syncing both Raft WAL and LSM WAL on every write) and Option B (syncing only Raft WAL).
func TestS3_OptionA_vs_OptionB_DurabilityBenchmark(t *testing.T) {
	const numWrites = 200

	runBench := func(disableLSMWALSync bool) (fsyncs uint64, opsPerSec float64) {
		k := sim.NewKernel(900, nil)
		net := sim.NewSimNet(k)
		opts := lsm.DefaultOptions()
		opts.DisableWALSync = disableLSMWALSync
		opts.MemtableFlushThreshold = 64 * 1024

		rep, err := kv.NewReplica("n1", []string{"n1"}, vfs.NewSimFS(), "bench_opt", k.NodeClock("n1"), net, k.Stream("n1"), kv.KVBugConfig{
			UseLSM:  true,
			LSMOpts: opts,
		})
		if err != nil {
			t.Fatal(err)
		}
		for !rep.IsLeader() {
			rep.Tick()
		}

		t0 := time.Now()
		for i := 1; i <= numWrites; i++ {
			rep.SubmitWrite(statemachine.Op{
				ClientID: 1,
				SeqNum:   uint64(i),
				Type:     statemachine.OpPut,
				Key:      []byte(fmt.Sprintf("k-%04d", i)),
				Value:    []byte("payload-value-benchmark"),
			}, func(statemachine.Result, bool) {})
		}
		_ = rep.FlushLSM()
		elapsed := time.Since(t0).Seconds()
		if elapsed <= 0 {
			elapsed = 1e-6
		}
		return rep.TotalFsyncCount(), float64(numWrites) / elapsed
	}

	fsyncA, opsA := runBench(false)
	fsyncB, opsB := runBench(true)

	t.Logf("[S3 OPTION A vs B BENCHMARK] %d writes | Option A (2 fsyncs/op): %d total fsyncs (%.0f ops/s) | Option B (Raft WAL + async LSM flush): %d total fsyncs (%.0f ops/s) -> %.1f%% fewer fsyncs!",
		numWrites, fsyncA, opsA, fsyncB, opsB, (1.0-float64(fsyncB)/float64(fsyncA))*100.0)
	if fsyncB >= fsyncA {
		t.Fatalf("expected Option B to issue strictly fewer fsyncs than Option A: got B=%d >= A=%d", fsyncB, fsyncA)
	}
} // TestS3_RealTCPMode_ThreeNodeClusterWithCrash spins up 3 real TCP listeners on 127.0.0.1:0,
// runs Raft + LSMStore over real OS sockets, crashes and restarts a follower, and verifies linearizability.
func TestS3_RealTCPMode_ThreeNodeClusterWithCrash(t *testing.T) {
	peers := []string{"n1", "n2", "n3"}
	var mu sync.Mutex

	reps := make(map[string]*kv.Replica)
	disks := make(map[string]*vfs.SimFS)
	transports := make(map[string]*tcp.Transport)
	addrs := make(map[string]string)
	baseClock := sim.NewBaseClock(uint64(time.Now().UnixNano()))

	for _, id := range peers {
		nodeID := id
		tr, err := tcp.NewTransport(nodeID, "127.0.0.1:0", func(m sim.Message) {
			mu.Lock()
			defer mu.Unlock()
			baseClock.AdvanceTo(max(baseClock.Now()+1000, uint64(time.Now().UnixNano())))
			if r, ok := reps[nodeID]; ok {
				r.Step(m)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		transports[nodeID] = tr
		addrs[nodeID] = tr.Addr()
	}
	defer func() {
		for _, tr := range transports {
			_ = tr.Close()
		}
	}()

	for _, id := range peers {
		transports[id].SetPeers(addrs)
		fs := vfs.NewSimFS()
		disks[id] = fs
		rep, err := kv.NewReplica(
			id,
			peers,
			fs,
			"tcp_"+id,
			sim.NewNodeClock(baseClock, 0, 0),
			transports[id],
			sim.NewStreamRand(77, id),
			kv.KVBugConfig{UseLSM: true, LSMOpts: lsm.DefaultOptions()},
		)
		if err != nil {
			t.Fatal(err)
		}
		reps[id] = rep
	}

	// Drive election ticks over real TCP sockets
	var leaderID string
	for attempt := 0; attempt < 30 && leaderID == ""; attempt++ {
		mu.Lock()
		for _, id := range peers {
			reps[id].Tick()
			if reps[id].IsLeader() {
				leaderID = id
			}
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	if leaderID == "" {
		t.Fatal("expected 3-node TCP cluster to elect a leader over 127.0.0.1 sockets")
	}

	var hist []porcupine.HistoryOp
	// Submit write over TCP cluster, crash a follower, restart it, and perform a linearizable ReadIndex read
	doneWrite := make(chan bool, 1)
	mu.Lock()
	invW := baseClock.Now()
	reps[leaderID].SubmitWrite(statemachine.Op{
		ClientID: 1,
		SeqNum:   1,
		Type:     statemachine.OpPut,
		Key:      []byte("tcp_key"),
		Value:    []byte("tcp_val_1"),
	}, func(res statemachine.Result, ok bool) {
		if ok && res.Applied {
			hist = append(hist, porcupine.HistoryOp{
				ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "tcp_key", InputVal: "tcp_val_1",
				InvokeNs: invW, ReturnNs: baseClock.Now() + 1,
			})
			select {
			case doneWrite <- true:
			default:
			}
		}
	})
	mu.Unlock()

	for i := 0; i < 20; i++ {
		select {
		case <-doneWrite:
			break
		default:
			mu.Lock()
			for _, id := range peers {
				reps[id].Tick()
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Crash and restart a non-leader node while serving a linearizable ReadIndex read
	victim := "n3"
	if leaderID == "n3" {
		victim = "n2"
	}
	mu.Lock()
	reps[victim].Crash(disks[victim], sim.NewStreamRand(77, "crash_tcp"))
	_ = reps[victim].Restart()
	mu.Unlock()

	doneRead := make(chan string, 1)
	mu.Lock()
	invR := baseClock.Now() + 10
	reps[leaderID].SubmitRead(kv.ClientReadReq{
		ReadID: 101,
		Key:    []byte("tcp_key"),
		Callback: func(val []byte, found bool, ok bool) {
			if ok && found {
				hist = append(hist, porcupine.HistoryOp{
					ID: 2, ClientID: 2, Kind: porcupine.OpGet, Key: "tcp_key", OutputVal: string(val),
					InvokeNs: invR, ReturnNs: baseClock.Now() + 20,
				})
				select {
				case doneRead <- string(val):
				default:
				}
			}
		},
	})
	mu.Unlock()

	var readVal string
	for i := 0; i < 25; i++ {
		select {
		case readVal = <-doneRead:
			break
		default:
			mu.Lock()
			for _, id := range peers {
				reps[id].Tick()
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}

	if readVal != "tcp_val_1" {
		t.Fatalf("expected linearizable ReadIndex over TCP to return 'tcp_val_1', got %q", readVal)
	}
	lin := porcupine.CheckLinearizable(hist)
	if !lin.Linearizable {
		t.Fatalf("real TCP cluster history was not linearizable: %s", lin.Explanation)
	}
	t.Logf("[S3 REAL TCP PASS] 3-node local TCP cluster (%v) elected leader %s, survived follower crash/restart, and verified linearizable ReadIndex history.",
		addrs, leaderID)
}
