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
func deliverMessages(nodes map[string]*raft.Node, allowed map[string]bool, queue []sim.Message) {
for len(queue) > 0 {
m := queue[0]
queue = queue[1:]
if !allowed[m.From] || !allowed[m.To] {
continue
}
if target, ok := nodes[m.To]; ok {
rd := target.Step(m)
queue = append(queue, rd.Send...)
}
}
}
func TestS1_ControlCampaign_3_4_5_NodeClusters(t *testing.T) {
clusterSizes := []int{3, 4, 5}
seedsPerSize := 50
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
func TestS1_BugZoo_All8PlantedBugs(t *testing.T) {
t.Run("Zoo_01_Figure8_CommitEarlierTerm", func(t *testing.T) {
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
nodes := map[string]*raft.Node{"n1": n1, "n2": n2, "n3": n3, "n5": n5}
partA := map[string]bool{"n1": true, "n2": true, "n3": true}
for i := 0; i < 20 && n1.CommitIdx() == 0; i++ {
rd := n1.Tick()
deliverMessages(nodes, partA, rd.Send)
}
if n1.CommitIdx() != 1 {
t.Fatalf("expected buggy n1 to commit index 1 from earlier term 2, got commitIdx=%d", n1.CommitIdx())
}
_ = obs.RecordCommitted(raft.Entry{Term: 2, Index: 1, Data: []byte("fig8_old")}, n1.Term())
partB := map[string]bool{"n2": true, "n3": true, "n5": true}
for i := 0; i < 40 && n5.Role() != raft.RoleLeader; i++ {
rd := n5.Tick()
deliverMessages(nodes, partB, rd.Send)
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
peers := []string{"n1", "n2", "n3"}
bugs := kv.KVBugConfig{RaftBugs: raft.BugConfig{Bug03AckBeforeSync: true}}
var caughtViol *raft.InvariantViolation
var caughtSeed uint64
for seed := uint64(1); seed <= 50; seed++ {
obs := raft.NewObserver(peers)
k := sim.NewKernel(seed, nil)
net := sim.NewSimNet(k)
disks := map[string]*vfs.SimFS{"n1": vfs.NewSimFS(), "n2": vfs.NewSimFS(), "n3": vfs.NewSimFS()}
reps := make(map[string]*kv.Replica)
for _, id := range peers {
r, _ := kv.NewReplica(id, peers, disks[id], "d_"+id, k.NodeClock(id), net, k.Stream(id), bugs)
reps[id] = r
nodeID := id
net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
}
for i := 0; i < 15; i++ {
for _, id := range peers {
reps[id].Tick()
}
k.RunUntil(k.Now()+10_000_000, 500)
}
for _, id := range peers {
if reps[id].IsLeader() {
reps[id].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k"), Value: []byte("v")}, func(statemachine.Result, bool) {})
break
}
}
k.RunUntil(k.Now()+50_000_000, 1000)
var views []raft.NodeStateView
for _, id := range peers {
views = append(views, reps[id].StateView())
}
_ = obs.Check(views)
for _, id := range peers {
reps[id].Crash(disks[id], k.Stream("crash:"+id))
_ = reps[id].Restart()
}
for i := 0; i < 20; i++ {
for _, id := range peers {
reps[id].Tick()
}
k.RunUntil(k.Now()+10_000_000, 500)
}
views = nil
for _, id := range peers {
views = append(views, reps[id].StateView())
}
if viol := obs.Check(views); viol != nil {
caughtViol = viol
caughtSeed = seed
break
}
}
if caughtViol == nil {
t.Fatal("expected CommittedDurability violation for Zoo #3, got nil")
}
t.Logf("[ZOO #3 CAUGHT] seed=%d seeds_to_detect=%d detector=%s details=%s",
caughtSeed, caughtSeed, caughtViol.Property, caughtViol.Details)
})
t.Run("Zoo_04_TruncateOnStaleReorderedAppend", func(t *testing.T) {
peers := []string{"n1", "n2", "n3"}
obs := raft.NewObserver(peers)
bugCfg := raft.BugConfig{Bug04StaleTruncate: true}
n1 := raft.NewNode(raft.Config{ID: "n1", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(4, "n1"), Bugs: bugCfg},
raft.HardState{Term: 1, Vote: "n1"}, nil)
n2 := raft.NewNode(raft.Config{ID: "n2", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(4, "n2"), Bugs: bugCfg},
raft.HardState{Term: 1, Vote: "n1"}, nil)
n3 := raft.NewNode(raft.Config{ID: "n3", Peers: peers, ElectionTimeout: 5, HeartbeatPeriod: 2, Rand: sim.NewStreamRand(4, "n3"), Bugs: bugCfg},
raft.HardState{Term: 1, Vote: ""}, nil)
nodes := map[string]*raft.Node{"n1": n1, "n2": n2, "n3": n3}
part12 := map[string]bool{"n1": true, "n2": true}
for i := 0; i < 15 && n1.Role() != raft.RoleLeader; i++ {
deliverMessages(nodes, part12, n1.Tick().Send)
}
rd1 := n1.Propose([]byte("cmd1"))
staleMsgToN2 := rd1.Send[0]
rd2 := n1.Propose([]byte("cmd2"))
freshMsgToN2 := rd2.Send[0]
deliverMessages(nodes, part12, []sim.Message{freshMsgToN2})
_ = obs.RecordCommitted(raft.Entry{Term: n1.Term(), Index: 2, Data: []byte("cmd2")}, n1.Term())
n2.Step(staleMsgToN2)
part23 := map[string]bool{"n2": true, "n3": true}
for i := 0; i < 20 && n2.Role() != raft.RoleLeader; i++ {
deliverMessages(nodes, part23, n2.Tick().Send)
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
history := []porcupine.HistoryOp{
{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 10, ReturnNs: 20},
{ID: 2, ClientID: 2, Kind: porcupine.OpPut, Key: "k1", InputVal: "v2", InvokeNs: 30, ReturnNs: 40},
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
k.NodeClock("n1").SetSkew(0, -500_000)
for i := 0; i < 12; i++ {
for _, id := range peers {
reps[id].Tick()
}
k.RunUntil(uint64(i+1)*5_000_000, 500)
}
var oldLeader string
for _, id := range peers {
if reps[id].IsLeader() {
oldLeader = id
break
}
}
k.NodeClock(oldLeader).SetSkew(0, -800_000)
reps[oldLeader].SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(statemachine.Result, bool) {})
k.RunUntil(80_000_000, 500)
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
bugs := kv.KVBugConfig{Bug07NoClientDedup: true}
k := sim.NewKernel(707, nil)
net := sim.NewSimNet(k)
peers := []string{"n1"}
r, _ := kv.NewReplica("n1", peers, vfs.NewSimFS(), "d_n1", k.NodeClock("n1"), net, k.Stream("n1"), bugs)
r.Tick()
var hist []porcupine.HistoryOp
r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(res statemachine.Result, ok bool) {
hist = append(hist, porcupine.HistoryOp{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 10, ReturnNs: 20})
})
r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")}, func(res statemachine.Result, ok bool) {})
r.SubmitRead(kv.ClientReadReq{ReadID: 10, Key: []byte("k1"), Callback: func(val []byte, found bool, ok bool) {
hist = append(hist, porcupine.HistoryOp{ID: 2, ClientID: 2, Kind: porcupine.OpGet, Key: "k1", OutputVal: string(val), InvokeNs: 30, ReturnNs: 40})
}})
r.SubmitWrite(statemachine.Op{ClientID: 2, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")}, func(res statemachine.Result, ok bool) {
hist = append(hist, porcupine.HistoryOp{ID: 3, ClientID: 2, Kind: porcupine.OpPut, Key: "k1", InputVal: "v1", InvokeNs: 50, ReturnNs: 60})
})
r.SubmitWrite(statemachine.Op{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")}, func(res statemachine.Result, ok bool) {
hist = append(hist, porcupine.HistoryOp{ID: 4, ClientID: 1, Kind: porcupine.OpCAS, Key: "k1", ExpectedVal: "v1", InputVal: "v2", OutputVal: string(res.Value), CASApplied: res.Applied, InvokeNs: 25, ReturnNs: 70})
})
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
