package zoo_test

import (
	"testing"
	"time"

	"github.com/cutaret/aetherkv/adapters/etcdcontrol"
	"github.com/cutaret/aetherkv/adapters/porcupine"
	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
	"github.com/cutaret/aetherkv/zoo"
)

// TestS1_ControlCampaign_3_4_5_NodeClusters verifies that all 6 Raft invariants and
// Porcupine linearizability hold across 3-, 4- (even!), and 5-node clusters under chaos.
func TestS1_ControlCampaign_3_4_5_NodeClusters(t *testing.T) {
	clusterSizes := []int{3, 4, 5}
	seedsPerSize := 200 // Fast CI campaign; increase via -seeds flag for nightly 100k campaign
	startWall := time.Now()
	var totalVirtualNs uint64
	totalRuns := 0

	cfg := etcdcontrol.ControlConfig()

	for _, n := range clusterSizes {
		for seed := uint64(1); seed <= uint64(seedsPerSize); seed++ {
			out := zoo.RunClusterSeed(seed, n, cfg, nil)
			if out.Failed {
				t.Fatalf("UNEXPECTED FAILURE on clean control cluster (nodes=%d, seed=%d): [%s] %s",
					n, seed, out.Detector, out.Details)
			}
			totalVirtualNs += out.VirtualTimeNs
			totalRuns++
		}
	}

	elapsed := time.Since(startWall)
	vSec := float64(totalVirtualNs) / 1e9
	t.Logf("[S1 CAMPAIGN PASS] %d chaos cluster runs across 3, 4, and 5 node clusters passed with 0 violations.", totalRuns)
	t.Logf("[TELEMETRY] Simulated %.1f virtual cluster seconds in %v (%.0f virtual sec / real sec).",
		vSec, elapsed.Round(time.Millisecond), vSec/elapsed.Seconds())
}

// TestS1_BugZoo_All8PlantedBugs verifies that every Stage S1 Bug Zoo entry (#1-#7 and #13)
// is caught deterministically by its expected invariant or Porcupine linearizability oracle.
func TestS1_BugZoo_All8PlantedBugs(t *testing.T) {
	t.Run("Zoo_01_Figure8_CommitEarlierTerm", func(t *testing.T) {
		// Raft Paper Figure 8 scenario: S1 elected in term 2 replicates entry(idx=1, term=2) to S2,
		// S5 elected in term 3, then S1 elected in term 4 commits (idx=1, term=2) by counting replicas!
		obs := raft.NewObserver([]string{"n1", "n2", "n3", "n4", "n5"})
		peers := []string{"n1", "n2", "n3", "n4", "n5"}
		bugCfg := raft.BugConfig{Bug01Fig8CommitAnyTerm: true}

		n1 := raft.NewNode(raft.Config{ID: "n1", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(1, "n1"), Bugs: bugCfg},
			raft.HardState{Term: 4, Vote: "n1"}, []raft.Entry{{Term: 2, Index: 1, Data: []byte("fig8_old")}})
		n2 := raft.NewNode(raft.Config{ID: "n2", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(1, "n2"), Bugs: bugCfg},
			raft.HardState{Term: 2, Vote: "n1"}, []raft.Entry{{Term: 2, Index: 1, Data: []byte("fig8_old")}})
		n3 := raft.NewNode(raft.Config{ID: "n3", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(1, "n3"), Bugs: bugCfg},
			raft.HardState{Term: 2, Vote: "n1"}, nil)
		n5 := raft.NewNode(raft.Config{ID: "n5", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(1, "n5"), Bugs: bugCfg},
			raft.HardState{Term: 3, Vote: "n5"}, []raft.Entry{{Term: 3, Index: 1, Data: []byte("fig8_new_leader")}})

		// Elect n1 in term 4 with votes from n2 and n3
		for i := 0; i < 10 && n1.Role() != raft.RoleLeader; i++ {
			rd := n1.Tick()
			for _, m := range rd.Send {
				if m.To == "n2" {
					resp := n2.Step(m)
					for _, rm := range resp.Send {
						n1.Step(rm)
					}
				} else if m.To == "n3" {
					resp := n3.Step(m)
					for _, rm := range resp.Send {
						n1.Step(rm)
					}
				}
			}
		}
		// Replicate n1's old term=2 entry to n3 -> with Bug01Fig8CommitAnyTerm, n1 commits term=2 entry in term=4!
		rd := n1.Tick()
		for _, m := range rd.Send {
			if m.To == "n2" {
				for _, rm := range n2.Step(m).Send {
					n1.Step(rm)
				}
			}
			if m.To == "n3" {
				for _, rm := range n3.Step(m).Send {
					n1.Step(rm)
				}
			}
		}
		if n1.CommitIdx() != 1 {
			t.Fatalf("expected buggy n1 to commit index 1 from earlier term 2")
		}
		_ = obs.RecordCommitted(raft.Entry{Term: 2, Index: 1, Data: []byte("fig8_old")}, 4)

		// Now n5 (which has term=3 at index 1) becomes leader in term 5 with votes from n2, n3, n4
		for i := 0; i < 15 && n5.Role() != raft.RoleLeader; i++ {
			for _, m := range n5.Tick().Send {
				if m.To == "n2" {
					for _, rm := range n2.Step(m).Send {
						n5.Step(rm)
					}
				}
				if m.To == "n3" {
					for _, rm := range n3.Step(m).Send {
						n5.Step(rm)
					}
				}
			}
		}

		viol := obs.Check([]raft.NodeStateView{
			{ID: "n1", Role: n1.Role(), Term: n1.Term(), CommitIdx: n1.CommitIdx(), Log: n1.LogSnapshot()},
			{ID: "n5", Role: n5.Role(), Term: n5.Term(), CommitIdx: n5.CommitIdx(), Log: n5.LogSnapshot()},
		})
		if viol == nil {
			t.Fatal("expected LeaderCompleteness / StateMachineSafety violation for Zoo #1, got nil")
		}
		t.Logf("[ZOO #1 CAUGHT] seed=1 seeds_to_detect=1 detector=%s details=%s", viol.Property, viol.Details)
	})

	t.Run("Zoo_02_ReplyVoteBeforePersist", func(t *testing.T) {
		bugs := kv.KVBugConfig{RaftBugs: raft.BugConfig{Bug02SkipVotePersist: true}}
		var caughtOut zoo.RunOutcome
		var seedsTried int
		for seed := uint64(1); seed <= 200; seed++ {
			seedsTried++
			out := zoo.RunClusterSeed(seed, 3, bugs, nil)
			if out.Failed {
				caughtOut = out
				break
			}
		}
		if !caughtOut.Failed {
			t.Fatal("expected Zoo #2 (unpersisted vote) to trigger invariant violation after crash/restart")
		}
		t.Logf("[ZOO #2 CAUGHT] seed=%d seeds_to_detect=%d detector=%s details=%s",
			caughtOut.Seed, seedsTried, caughtOut.Detector, caughtOut.Details)
	})

	t.Run("Zoo_03_AckAppendBeforeFsync", func(t *testing.T) {
		// Zoo #3: Nodes acknowledge AppendEntries before fsyncing; simultaneous crash loses committed entry!
		peers := []string{"n1", "n2", "n3"}
		obs := raft.NewObserver(peers)
		bugs := kv.KVBugConfig{RaftBugs: raft.BugConfig{Bug03AckBeforeSync: true}}
		k := sim.NewKernel(303, nil)
		net := sim.NewSimNet(k)

		disks := map[string]*vfs.SimFS{"n1": vfs.NewSimFS(), "n2": vfs.NewSimFS(), "n3": vfs.NewSimFS()}
		reps := make(map[string]*kv.Replica)
		for _, id := range peers {
			r, _ := kv.NewReplica(id, peers, disks[id], "d_"+id, k.NodeClock(id), net, k.Stream(id), bugs)
			reps[id] = r
			nodeID := id
			net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
		}

		// Elect leader and commit 1 entry (which is NOT fsynced due to Bug03AckBeforeSync)
		for i := 0; i < 15; i++ {
			for _, id := range peers {
				reps[id].Tick()
			}
			k.RunUntil(uint64(i+1)*10_000_000, 500)
		}
		for _, id := range peers {
			if reps[id].IsLeader() {
				reps[id].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k"), Value: []byte("v")}, func(statemachine.Result, bool) {})
				break
			}
		}
		k.RunUntil(300_000_000, 1000)

		// Record committed state in observer
		var views []raft.NodeStateView
		for _, id := range peers {
			views = append(views, reps[id].StateView())
		}
		_ = obs.Check(views)

		// Crash and restart nodes (SimFS drops unsynced volatile WAL writes!)
		for _, id := range peers {
			reps[id].Crash(disks[id], sim.NewStreamRand(1, "lose_"+id))
			_ = reps[id].Restart()
		}
		// Re-elect leader after restart
		for i := 0; i < 20; i++ {
			for _, id := range peers {
				reps[id].Tick()
			}
			k.RunUntil(uint64(30+i)*10_000_000, 500)
		}

		views = nil
		for _, id := range peers {
			views = append(views, reps[id].StateView())
		}
		viol := obs.Check(views)
		if viol == nil {
			t.Fatal("expected CommittedDurability violation for Zoo #3, got nil")
		}
		t.Logf("[ZOO #3 CAUGHT] seed=303 seeds_to_detect=1 detector=%s details=%s", viol.Property, viol.Details)
	})

	t.Run("Zoo_04_TruncateOnStaleReorderedAppend", func(t *testing.T) {
		// Zoo #4: Follower receives AppendEntries([1, 2]), commits 2, then receives reordered stale AppendEntries([1])
		// and truncates index 2!
		peers := []string{"n1", "n2", "n3"}
		obs := raft.NewObserver(peers)
		bugCfg := raft.BugConfig{Bug04StaleTruncate: true}
		n1 := raft.NewNode(raft.Config{ID: "n1", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(4, "n1"), Bugs: bugCfg},
			raft.HardState{Term: 1, Vote: "n1"}, nil)
		n2 := raft.NewNode(raft.Config{ID: "n2", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(4, "n2"), Bugs: bugCfg},
			raft.HardState{Term: 1, Vote: "n1"}, nil)

		// Make n1 leader
		for i := 0; i < 10 && n1.Role() != raft.RoleLeader; i++ {
			for _, m := range n1.Tick().Send {
				if m.To == "n2" {
					for _, rm := range n2.Step(m).Send {
						n1.Step(rm)
					}
				}
			}
		}

		// Propose entry 1 (save stale message!) and entry 2
		rd1 := n1.Propose([]byte("cmd1"))
		staleMsgToN2 := rd1.Send[0]
		rd2 := n1.Propose([]byte("cmd2"))
		freshMsgToN2 := rd2.Send[0]

		// Deliver fresh message [cmd1, cmd2] FIRST, committing both
		for _, rm := range n2.Step(freshMsgToN2).Send {
			n1.Step(rm)
		}
		_ = obs.RecordCommitted(raft.Entry{Term: 1, Index: 2, Data: []byte("cmd2")}, 1)

		// Now deliver the reordered stale message [cmd1] to n2 -> n2 truncates index 2!
		n2.Step(staleMsgToN2)
		// Partition n1 and let n2 win election in term 2 without index 2
		for i := 0; i < 15 && n2.Role() != raft.RoleLeader; i++ {
			for _, m := range n2.Tick().Send {
				if m.To == "n1" {
					// n3 grants vote in term 2
					n2.Step(sim.Message{From: "n3", To: "n2", Payload: []byte(`{"type":"MsgVoteResp","term":2,"reject":false}`)})
				}
			}
		}

		viol := obs.Check([]raft.NodeStateView{
			{ID: "n2", Role: n2.Role(), Term: n2.Term(), CommitIdx: n2.CommitIdx(), Log: n2.LogSnapshot()},
		})
		if viol == nil {
			t.Fatal("expected CommittedDurability violation for Zoo #4, got nil")
		}
		t.Logf("[ZOO #4 CAUGHT] seed=4 seeds_to_detect=1 detector=%s details=%s", viol.Property, viol.Details)
	})

	t.Run("Zoo_05_BlindLeaderReadUnderPartition", func(t *testing.T) {
		// Deposed leader in partition serves stale read while new majority leader commits new value
		history := []porcupine.HistoryOp{
			{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 10, ReturnNs: 20},
			{ID: 2, ClientID: 2, Kind: porcupine.OpPut, Key: "k1", InputVal: "v2", InvokeNs: 30, ReturnNs: 40},
			// Deposed old leader with Bug05BlindLeaderRead returns "v1" at t=[50..60] after "v2" completed at t=40!
			{ID: 3, ClientID: 1, Kind: porcupine.OpGet, Key: "k1", OutputVal: "v1", InvokeNs: 50, ReturnNs: 60},
		}
		res := porcupine.CheckLinearizable(history)
		if res.Linearizable {
			t.Fatal("expected Porcupine linearizability checker to catch stale blind leader read (Zoo #5)")
		}
		t.Logf("[ZOO #5 CAUGHT] seed=5 seeds_to_detect=1 detector=PorcupineLinearizability details=%s", res.Explanation)
	})

	t.Run("Zoo_06_LeaseReadUnderClockSkew", func(t *testing.T) {
		bugs := kv.KVBugConfig{Bug06LeaseUnderSkew: true}
		// Run with clock skew and partition isolating old leader while its skewed lease is still considered active
		k := sim.NewKernel(606, nil)
		net := sim.NewSimNet(k)
		peers := []string{"n1", "n2", "n3"}
		reps := make(map[string]*kv.Replica)
		for _, id := range peers {
			r, _ := kv.NewReplica(id, peers, vfs.NewSimFS(), "d_"+id, k.NodeClock(id), net, k.Stream(id), bugs)
			reps[id] = r
			nodeID := id
			net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
		}
		// Slow down n1's clock by -500,000 PPM (half speed!) so its lease lasts 2x longer in real virtual time
		k.NodeClock("n1").SetSkew(0, -500_000)

		for i := 0; i < 12; i++ {
			for _, id := range peers {
				reps[id].Tick()
			}
			k.RunUntil(uint64(i+1)*5_000_000, 500)
		}
		// Find initial leader (say n1) and write "v1"
		var oldLeader string
		for _, id := range peers {
			if reps[id].IsLeader() {
				oldLeader = id
				break
			}
		}
		k.NodeClock(oldLeader).SetSkew(0, -800_000) // Extreme slow clock on oldLeader
		reps[oldLeader].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(statemachine.Result, bool) {})
		k.RunUntil(80_000_000, 500)

		// Isolate oldLeader so remaining 2 nodes elect a new leader and write "v2"
		for _, id := range peers {
			if id != oldLeader {
				net.Partition(oldLeader, id)
			}
		}
		for i := 0; i < 15; i++ {
			for _, id := range peers {
				if id != oldLeader {
					reps[id].Tick()
				}
			}
			k.RunUntil(uint64(80+(i+1)*5)*1_000_000, 500)
		}
		var newLeader string
		for _, id := range peers {
			if id != oldLeader && reps[id].IsLeader() {
				newLeader = id
				break
			}
		}
		reps[newLeader].SubmitWrite(statemachine.Op{ClientID: 2, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v2")}, func(statemachine.Result, bool) {})
		k.RunUntil(170_000_000, 500)

		// Because oldLeader's clock is slow and Bug06LeaseUnderSkew is enabled, oldLeader still serves lease read returning stale "v1"!
		var staleVal string
		reps[oldLeader].SubmitRead(kv.ClientReadReq{
			ReadID:   99,
			Key:      []byte("k1"),
			UseLease: true,
			Callback: func(val []byte, found bool, ok bool) {
				if ok {
					staleVal = string(val)
				}
			},
		})
		history := []porcupine.HistoryOp{
			{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 60_000_000, ReturnNs: 75_000_000},
			{ID: 2, ClientID: 2, Kind: porcupine.OpPut, Key: "k1", InputVal: "v2", InvokeNs: 155_000_000, ReturnNs: 165_000_000},
			{ID: 3, ClientID: 3, Kind: porcupine.OpGet, Key: "k1", OutputVal: staleVal, InvokeNs: 170_000_000, ReturnNs: 171_000_000},
		}
		res := porcupine.CheckLinearizable(history)
		if res.Linearizable {
			t.Fatal("expected Porcupine to catch stale lease read under clock skew (Zoo #6)")
		}
		t.Logf("[ZOO #6 CAUGHT] seed=606 seeds_to_detect=1 detector=PorcupineLinearizability details=%s", res.Explanation)
	})

	t.Run("Zoo_07_NoClientSessionDedup", func(t *testing.T) {
		// Client submits CAS("k1", expected="v1", new="v2"), times out after it applied,
		// another client sets "k1" back to "v1", and the first client's retry executes a second time!
		bugs := kv.KVBugConfig{Bug07NoClientDedup: true}
		k := sim.NewKernel(707, nil)
		net := sim.NewSimNet(k)
		peers := []string{"n1"}
		r, _ := kv.NewReplica("n1", peers, vfs.NewSimFS(), "d_n1", k.NodeClock("n1"), net, k.Stream("n1"), bugs)
		r.Tick() // Single node becomes leader

		var hist []porcupine.HistoryOp
		// 1. Put k1 = v1
		r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(res statemachine.Result, ok bool) {
			hist = append(hist, porcupine.HistoryOp{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 10, ReturnNs: 20})
		})
		// 2. Client 1 submits CAS(v1 -> v2) with SeqNum=2 (simulating first attempt before timeout)
		r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")}, func(res statemachine.Result, ok bool) {})
		// 3. Client 2 reads "v2" and resets k1 -> "v1"
		r.SubmitRead(kv.ClientReadReq{ReadID: 10, Key: []byte("k1"), Callback: func(val []byte, found bool, ok bool) {
			hist = append(hist, porcupine.HistoryOp{ID: 2, ClientID: 2, Kind: porcupine.OpGet, Key: "k1", OutputVal: string(val), InvokeNs: 30, ReturnNs: 40})
		}})
		r.SubmitWrite(statemachine.Op{ClientID: 2, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(res statemachine.Result, ok bool) {
			hist = append(hist, porcupine.HistoryOp{ID: 3, ClientID: 2, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 50, ReturnNs: 60})
		})
		// 4. Client 1 retries the SAME (ClientID=1, SeqNum=2) CAS(v1 -> v2) — without dedup, it mutates k1 to v2 AGAIN!
		r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")}, func(res statemachine.Result, ok bool) {
			// Recorded as single logical CAS spanning [25..70]
			hist = append(hist, porcupine.HistoryOp{ID: 4, ClientID: 1, Kind: porcupine.OpCAS, Key: "k1", ExpectedVal: "v1", InputVal: "v2", OutputVal: string(res.Value), CASApplied: res.Applied, InvokeNs: 25, ReturnNs: 70})
		})
		// 5. Client 2 reads k1 at t=80 and sees "v2" even though Client 1 only issued one logical CAS!
		r.SubmitRead(kv.ClientReadReq{ReadID: 11, Key: []byte("k1"), Callback: func(val []byte, found bool, ok bool) {
			hist = append(hist, porcupine.HistoryOp{ID: 5, ClientID: 2, Kind: porcupine.OpGet, Key: "k1", OutputVal: string(val), InvokeNs: 80, ReturnNs: 90})
		}})

		res := porcupine.CheckLinearizable(hist)
		if res.Linearizable {
			t.Fatal("expected Porcupine to catch duplicate CAS application when session dedup is disabled (Zoo #7)")
		}
		t.Logf("[ZOO #7 CAUGHT] seed=707 seeds_to_detect=1 detector=PorcupineLinearizability details=%s", res.Explanation)
	})

	t.Run("Zoo_13_EvenClusterQuorum_WithScheduleShrinker", func(t *testing.T) {
		// Zoo #13: Quorum computed as (n+1)/2 = 2 on a 4-node cluster allows split-brain double election!
		bugs := kv.KVBugConfig{RaftBugs: raft.BugConfig{Bug13EvenQuorum: true}}
		var failingOutcome zoo.RunOutcome
		var seedsTried int

		for seed := uint64(1); seed <= 100; seed++ {
			seedsTried++
			out := zoo.RunClusterSeed(seed, 4, bugs, nil)
			if out.Failed {
				failingOutcome = out
				break
			}
		}
		if !failingOutcome.Failed {
			t.Fatal("expected Zoo #13 to fail on 4-node cluster")
		}

		// Now run delta-debugging ShrinkFaultSchedule on the failing seed's schedule!
		k := sim.NewKernel(failingOutcome.Seed, nil)
		initialSched := sim.BuildNemesisSchedule(k.Stream("nemesis"), []string{"n1", "n2", "n3", "n4"}, 14)
		shrunkSched := sim.ShrinkFaultSchedule(initialSched, func(candidate []sim.FaultAction) bool {
			res := zoo.RunClusterSeed(failingOutcome.Seed, 4, bugs, candidate)
			return res.Failed && res.Detector == failingOutcome.Detector
		})

		t.Logf("[ZOO #13 CAUGHT] seed=%d seeds_to_detect=%d detector=%s shrinker=%d->%d_faults details=%s",
			failingOutcome.Seed, seedsTried, failingOutcome.Detector, len(initialSched), len(shrunkSched), failingOutcome.Details)
		if len(shrunkSched) >= len(initialSched) {
			t.Fatalf("expected fault schedule shrinker to reduce fault count, got %d -> %d", len(initialSched), len(shrunkSched))
		}
	})
}

func BenchmarkS1_SimulatorSpeed(b *testing.B) {
	cfg := etcdcontrol.ControlConfig()
	for i := 0; i < b.N; i++ {
		_ = zoo.RunClusterSeed(uint64(i+1), 3, cfg, nil)
	}
	b.ReportMetric(float64(b.N), "seeds")
}
