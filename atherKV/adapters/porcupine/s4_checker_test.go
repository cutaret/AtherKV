package porcupine_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cutaret/aetherkv/adapters/porcupine"
	"github.com/cutaret/aetherkv/sim"
)

func TestS4_Checker_RandomHistoriesAndCounterexampleShrinking(t *testing.T) {
	rng := sim.NewStreamRand(404, "s4_checker")
	const numHistories = 1000

	for h := 0; h < numHistories; h++ {
		// Generate a valid sequential/concurrent history of 15 operations on key "k1"
		var ops []porcupine.HistoryOp
		curVal := ""
		var tNs uint64 = 10

		for i := 1; i <= 15; i++ {
			inv := tNs
			ret := tNs + 10
			tNs += 15
			if rng.Intn(2) == 0 {
				curVal = fmt.Sprintf("v-%d", i)
				ops = append(ops, porcupine.HistoryOp{
					ID: i, ClientID: uint64(1 + i%2), Kind: porcupine.OpPut,
					Key: "k1", InputVal: curVal, InvokeNs: inv, ReturnNs: ret,
				})
			} else {
				ops = append(ops, porcupine.HistoryOp{
					ID: i, ClientID: uint64(1 + i%2), Kind: porcupine.OpGet,
					Key: "k1", OutputVal: curVal, InvokeNs: inv, ReturnNs: ret,
				})
			}
		}

		res := porcupine.CheckLinearizable(ops)
		if !res.Linearizable {
			t.Fatalf("history %d should be linearizable, got %s", h, res.Explanation)
		}
	}

	// Inject a non-linearizable stale read embedded inside a noisy 12-operation history
	// and verify that delta-debugging shrinks it down to <= 3 minimal operations with a plain-English causal reason!
	noisyBad := []porcupine.HistoryOp{
		{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "x", InputVal: "v1", InvokeNs: 10, ReturnNs: 20},
		{ID: 2, ClientID: 2, Kind: porcupine.OpGet, Key: "x", OutputVal: "v1", InvokeNs: 25, ReturnNs: 30},
		{ID: 3, ClientID: 3, Kind: porcupine.OpPut, Key: "x", InputVal: "v1", InvokeNs: 35, ReturnNs: 40},
		{ID: 4, ClientID: 1, Kind: porcupine.OpGet, Key: "x", OutputVal: "v1", InvokeNs: 45, ReturnNs: 50},
		{ID: 5, ClientID: 2, Kind: porcupine.OpPut, Key: "x", InputVal: "v2", InvokeNs: 60, ReturnNs: 70},
		{ID: 6, ClientID: 3, Kind: porcupine.OpGet, Key: "x", OutputVal: "v2", InvokeNs: 75, ReturnNs: 80},
		{ID: 7, ClientID: 1, Kind: porcupine.OpGet, Key: "x", OutputVal: "v1", InvokeNs: 90, ReturnNs: 100}, // Stale read!
	}
	badRes := porcupine.CheckLinearizable(noisyBad)
	if badRes.Linearizable {
		t.Fatal("expected noisyBad history to fail linearizability check")
	}
	if len(badRes.MinimalHistory) > 3 {
		t.Fatalf("expected counterexample shrinker to reduce 7 ops to <= 3 ops, got %d", len(badRes.MinimalHistory))
	}
	if !strings.Contains(badRes.Explanation, "stale read:") {
		t.Fatalf("expected plain-language stale read explanation, got: %s", badRes.Explanation)
	}
	t.Logf("[S4 CHECKER PASS] 1,000 random linearizable histories verified; 7-op failing history shrunk to %d minimal ops: %s",
		len(badRes.MinimalHistory), badRes.Explanation)
}

func TestS4_Checker_MultiKeyRangeScanLinearizability(t *testing.T) {
	// Valid multi-key range scan history
	validScanHist := []porcupine.HistoryOp{
		{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "a", InputVal: "1", InvokeNs: 10, ReturnNs: 20},
		{ID: 2, ClientID: 1, Kind: porcupine.OpPut, Key: "b", InputVal: "2", InvokeNs: 30, ReturnNs: 40},
		{ID: 3, ClientID: 2, Kind: porcupine.OpScan, Key: "a", EndKey: "z", ScanOut: "a=1;b=2", InvokeNs: 50, ReturnNs: 60},
	}
	if res := porcupine.CheckLinearizable(validScanHist); !res.Linearizable {
		t.Fatalf("expected validScanHist to be linearizable, got %s", res.Explanation)
	}

	// Non-linearizable fractured range scan: PUT(a=1) and PUT(b=2) both complete before SCAN(a..z),
	// but SCAN only returns "a=1" (missing b=2)!
	invalidScanHist := []porcupine.HistoryOp{
		{ID: 1, ClientID: 1, Kind: porcupine.OpPut, Key: "a", InputVal: "1", InvokeNs: 10, ReturnNs: 20},
		{ID: 2, ClientID: 1, Kind: porcupine.OpPut, Key: "b", InputVal: "2", InvokeNs: 30, ReturnNs: 40},
		{ID: 3, ClientID: 2, Kind: porcupine.OpScan, Key: "a", EndKey: "z", ScanOut: "a=1", InvokeNs: 50, ReturnNs: 60},
	}
	badScanRes := porcupine.CheckLinearizable(invalidScanHist)
	if badScanRes.Linearizable {
		t.Fatal("expected fractured range scan missing committed key 'b=2' to fail linearizability check")
	}
	t.Logf("[S4 RANGE-SCAN CHECKER PASS] Multi-key SCAN model caught fractured range scan: %s", badScanRes.Explanation)
}

func TestS4_HTMLTraceViewerGeneration(t *testing.T) {
	var buf bytes.Buffer
	err := sim.RenderHTMLTraceViewer(&buf, sim.TraceReport{
		Title:             "Bug Zoo #13 (Even-Cluster Quorum Split-Brain)",
		Seed:              4,
		NumNodes:          4,
		Detector:          "CommittedDurability",
		ViolationDetails:  "leader n3 (term 5) is missing committed entry at index 1",
		InitialFaultCount: 20,
		ShrunkFaultCount:  2,
		Faults: []sim.FaultAction{
			{ID: 1, TimeNs: 65_000_000, Type: sim.FaultPartitionSym, TargetA: "n1", TargetB: "n3"},
			{ID: 2, TimeNs: 90_000_000, Type: sim.FaultNodeCrash, TargetA: "n2"},
		},
		Events: []sim.TraceEvent{
			{Seq: 1, TimeNs: 10_000_000, Node: "n1", Kind: "task_exec", Details: "raft_tick"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "AetherKV Deterministic Simulation Trace Viewer") {
		t.Fatal("HTML output missing expected header")
	}
	t.Logf("[S4 HTML VIEWER PASS] Generated self-contained %d-byte interactive HTML trace viewer.", buf.Len())
}
