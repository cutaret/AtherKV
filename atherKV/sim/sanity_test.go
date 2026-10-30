package sim_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/cutaret/aetherkv/internal/detlint"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
)

// runSimulatedWorkload executes a deterministic multi-node schedule on the Kernel and MemStore
// and returns the final rolling SHA-256 trace hash and MemStore snapshot bytes.
func runSimulatedWorkload(seed uint64) (traceHash string, snapBytes []byte, virtualNs uint64) {
	k := sim.NewKernel(seed, nil)
	store := statemachine.NewMemStore()

	// Configure 3 node clocks with seed-derived offsets and drift
	nemesisRand := k.Stream("nemesis")
	nodes := []string{"n1", "n2", "n3"}
	for _, n := range nodes {
		offsetNs := nemesisRand.Int63n(2_000_000) - 1_000_000 // +/- 1ms
		driftPPM := nemesisRand.Int63n(400) - 200             // +/- 200 PPM
		k.NodeClock(n).SetSkew(offsetNs, driftPPM)
	}

	workloadRand := k.Stream("workload:client1")
	var appliedIdx uint64

	// Schedule 15 recursive client operations across virtual time
	var scheduleNextOp func(step int)
	scheduleNextOp = func(step int) {
		if step >= 15 {
			return
		}
		node := nodes[step%len(nodes)]
		sched := k.NodeScheduler(node)
		delayNs := uint64(workloadRand.Int63n(50_000_000) + 1_000_000) // 1ms to 51ms

		sched.ScheduleAfter(delayNs, fmt.Sprintf("client_op_%d", step), func() {
			appliedIdx++
			key := []byte(fmt.Sprintf("k-%d", workloadRand.Intn(8)))
			val := []byte(fmt.Sprintf("v-%d-%d", seed, step))
			clientID := uint64(1 + (step % 2))
			seqNum := uint64(step/2 + 1)

			res := store.ApplyBatch(appliedIdx, []statemachine.Op{
				{
					ClientID: clientID,
					SeqNum:   seqNum,
					Type:     statemachine.OpPut,
					Key:      key,
					Value:    val,
				},
			})
			nodeTime := k.NodeClock(node).Now()
			k.RecordTrace(node, "apply_put", fmt.Sprintf("idx=%d key=%s applied=%v node_ts=%d", appliedIdx, key, res[0].Applied, nodeTime))
			scheduleNextOp(step + 1)
		})
	}

	scheduleNextOp(0)
	k.RunUntil(10_000_000_000, 1000) // Up to 10 virtual seconds

	snap, err := store.Snapshot()
	if err != nil {
		panic(err)
	}
	return k.TraceHash(), snap, k.Now()
}

func TestS0_Sanity1_TraceHashDeterminism_10000Seeds(t *testing.T) {
	const numSeeds = 10_000
	startWall := time.Now()
	var totalVirtualNs uint64

	for seed := uint64(1); seed <= numSeeds; seed++ {
		hash1, snap1, vNs1 := runSimulatedWorkload(seed)
		hash2, snap2, vNs2 := runSimulatedWorkload(seed)

		if hash1 != hash2 {
			t.Fatalf("DETERMINISM FAILURE on seed=%d: run1 hash=%s != run2 hash=%s", seed, hash1, hash2)
		}
		if vNs1 != vNs2 || !bytes.Equal(snap1, snap2) {
			t.Fatalf("STATE DIVERGENCE on seed=%d: snapshot mismatch between identical seed runs", seed)
		}
		totalVirtualNs += vNs1 * 2 // Count both runs
	}

	elapsedWall := time.Since(startWall)
	virtualSeconds := float64(totalVirtualNs) / 1e9
	speedRatio := virtualSeconds / elapsedWall.Seconds()

	t.Logf("[SANITY 1 PASS] 10,000 seeds x 2 runs (20,000 simulations) matched 100%% of SHA-256 trace hashes.")
	t.Logf("[TELEMETRY] Simulated %.1f virtual seconds in %v (%.0f virtual sec / real sec).",
		virtualSeconds, elapsedWall.Round(time.Millisecond), speedRatio)
}

func TestS0_Sanity2_PRNGStreamIsolation(t *testing.T) {
	const masterSeed = 424242

	// Run A: Normal workload & network draws
	kA := sim.NewKernel(masterSeed, nil)
	netA := kA.Stream("net:n1->n2")
	workA := kA.Stream("workload:c1")

	// Run B: Consume 500 draws from the "nemesis" stream before touching net or workload
	kB := sim.NewKernel(masterSeed, nil)
	nemesisB := kB.Stream("nemesis")
	for i := 0; i < 500; i++ {
		_ = nemesisB.Uint64()
	}
	netB := kB.Stream("net:n1->n2")
	workB := kB.Stream("workload:c1")

	for i := 0; i < 1000; i++ {
		if netA.Uint64() != netB.Uint64() {
			t.Fatalf("PRNG stream 'net:n1->n2' was polluted by draws on 'nemesis' at step %d", i)
		}
		if workA.Uint64() != workB.Uint64() {
			t.Fatalf("PRNG stream 'workload:c1' was polluted by draws on 'nemesis' at step %d", i)
		}
	}
	t.Log("[SANITY 2 PASS] PRNG streams are 100% isolated; mutating 'nemesis' leaves 'net' and 'workload' untouched.")
}

func TestS0_Sanity3_ClockSkewAndDrift(t *testing.T) {
	base := sim.NewBaseClock(1_000_000_000) // Start at 1s
	// +5ms static offset, +500 PPM fast drift (+500us per elapsed second)
	nc := sim.NewNodeClock(base, 5_000_000, 500)

	if got, want := nc.Now(), uint64(1_005_000_000); got != want {
		t.Fatalf("initial skewed clock: got %d, want %d", got, want)
	}

	// Advance global clock by 10 seconds (10_000_000_000 ns)
	base.AdvanceTo(11_000_000_000)
	// Expected: 11s + 5ms offset + (10s * 500 PPM = 5ms drift) = 11.010s = 11_010_000_000 ns
	if got, want := nc.Now(), uint64(11_010_000_000); got != want {
		t.Fatalf("drifted clock after 10s: got %d, want %d", got, want)
	}
	t.Log("[SANITY 3 PASS] Virtual clock static offset and PPM linear drift verified.")
}

func TestS0_Sanity4_MemStoreAndSessionDedup(t *testing.T) {
	sm := statemachine.NewMemStore()

	// 1. Apply Put + CAS + duplicate Put (same ClientID=7, SeqNum=1)
	res := sm.ApplyBatch(1, []statemachine.Op{
		{ClientID: 7, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("alpha"), Value: []byte("v1")},
		{ClientID: 7, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("alpha"), Value: []byte("OVERWRITE_BUG")},
		{ClientID: 8, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("beta"), Value: []byte("v2")},
		{ClientID: 8, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("beta"), Expected: []byte("v2"), Value: []byte("v2_cas")},
	})

	if !res[0].Applied || res[0].Deduplicated {
		t.Fatalf("expected op 0 to apply cleanly, got %+v", res[0])
	}
	if !res[1].Deduplicated || string(res[1].Value) != "v1" {
		t.Fatalf("expected duplicate (ClientID=7, SeqNum=1) to be deduplicated and return original value 'v1', got %+v", res[1])
	}
	if !res[3].Applied || string(res[3].Value) != "v2_cas" {
		t.Fatalf("expected CAS to succeed, got %+v", res[3])
	}

	// Verify underlying value of "alpha" was NOT overwritten by the duplicate retry
	val, ok, _ := sm.Get([]byte("alpha"))
	if !ok || string(val) != "v1" {
		t.Fatalf("session dedup failed: expected alpha='v1', got %q", val)
	}

	// 2. Snapshot & Restore into a brand-new MemStore and verify session dedup survives snapshot
	snapBytes, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	restored := statemachine.NewMemStore()
	if err := restored.Restore(snapBytes); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Retry (ClientID=7, SeqNum=1) against restored store — must still be deduplicated!
	retryRes := restored.ApplyBatch(2, []statemachine.Op{
		{ClientID: 7, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("alpha"), Value: []byte("POST_SNAP_OVERWRITE")},
	})
	if !retryRes[0].Deduplicated {
		t.Fatalf("session dedup table was lost across Snapshot/Restore!")
	}
	valAfter, _, _ := restored.Get([]byte("alpha"))
	if string(valAfter) != "v1" {
		t.Fatalf("expected alpha='v1' after post-restore retry, got %q", valAfter)
	}

	// 3. Verify bounded range scan order
	scanRes, err := restored.Scan([]byte("a"), []byte("z"), 10)
	if err != nil || len(scanRes) != 2 || string(scanRes[0].Key) != "alpha" || string(scanRes[1].Key) != "beta" {
		t.Fatalf("unexpected Scan output: %+v", scanRes)
	}
	t.Log("[SANITY 4 PASS] MemStore Put/Get/CAS/Scan, (ClientID, SeqNum) deduplication, and Snapshot/Restore verified.")
}

func TestS0_Sanity5_DetlintCatchesAllViolations(t *testing.T) {
	badCode := `package badcore

import (
	"math/rand"
	"time"
)

func BrokenConsensusStep() {
	_ = rand.Intn(10)
	_ = time.Now()
	go func() {}()

	peers := map[string]int{"n1": 1, "n2": 2}
	for k, v := range peers {
		_, _ = k, v
	}
}
`
	violations, err := detlint.CheckSourceString("bad_core.go", badCode)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	expectedRules := map[string]bool{
		"forbidden-rand-import": false,
		"no-wall-clock":         false,
		"no-goroutines":         false,
		"no-unsorted-map-range": false,
	}
	for _, v := range violations {
		expectedRules[v.Rule] = true
	}
	for rule, caught := range expectedRules {
		if !caught {
			t.Fatalf("detlint FAILED to catch required rule %q; violations=%+v", rule, violations)
		}
	}
	t.Logf("[SANITY 5 PASS] detlint caught all 4 determinism violations (%d total findings).", len(violations))
}
