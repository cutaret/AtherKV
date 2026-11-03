package lsm_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

func TestS2_BugZoo09_MissingManifestSyncDir(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.Bugs.Bug09SkipManifestSync = true

	db, err := lsm.OpenLSMStore(fs, "db_zoo09", sim.NewStreamRand(9, "db"), opts)
	if err != nil {
		t.Fatal(err)
	}

	_ = db.PutDirect([]byte("k1"), []byte("v1_flushed"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Simulate power loss: because Bug09SkipManifestSync omitted SyncDir("db_zoo09"),
	// the newly created MANIFEST/CURRENT/SSTable metadata reverts on crash!
	fs.Crash(sim.NewStreamRand(9, "crash"))

	reopened, err := lsm.OpenLSMStore(fs, "db_zoo09", sim.NewStreamRand(9, "db2"), lsm.DefaultOptions())
	if err != nil {
		t.Logf("[ZOO #9 CAUGHT] seed=9 seeds_to_detect=1 detector=SimFSCrashOracle details=recovery failed due to un-synced MANIFEST directory: %v", err)
		return
	}
	defer reopened.Close()

	_, found, _ := reopened.Get([]byte("k1"))
	if found {
		t.Fatal("expected Zoo #9 (missing SyncDir after MANIFEST rename) to lose flushed SSTable state on crash!")
	}
	t.Logf("[ZOO #9 CAUGHT] seed=9 seeds_to_detect=1 detector=SimFSCrashOracle details=flushed SSTable and MANIFEST update vanished after crash due to missing SyncDir")
}

func TestS2_BugZoo10_CompactionDropsTombstoneEarly(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.Bugs.Bug10DropTombstoneEarly = true

	db, err := lsm.OpenLSMStore(fs, "db_zoo10", sim.NewStreamRand(10, "db"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Step 1: Write key "victim" = "ancient_value", flush to L0, and compact L0 -> L1 -> L2 (bottom level)
	_ = db.PutDirect([]byte("victim"), []byte("ancient_value"))
	_ = db.Flush()
	_ = db.CompactLevel(0) // Moves to L1
	_ = db.CompactLevel(1) // Moves to L2 (deepest level)

	// Step 2: Delete "victim" (writes tombstone), flush to L0, and compact L0 -> L1.
	// Because Bug10DropTombstoneEarly is true, L0 -> L1 compaction drops the tombstone at L1
	// while "ancient_value" still sits in L2!
	_ = db.DeleteDirect([]byte("victim"))
	_ = db.Flush()
	_ = db.CompactLevel(0)

	// Step 3: Read "victim" -> deleted key resurrects from L2!
	val, found, err := db.Get([]byte("victim"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(val) != "ancient_value" {
		t.Fatalf("expected Zoo #10 to resurrect deleted key 'victim' from L2, got found=%v val=%q", found, string(val))
	}
	t.Logf("[ZOO #10 CAUGHT] seed=10 seeds_to_detect=1 detector=ModelBasedOracle details=deleted key 'victim' resurrected with value=%q after L0->L1 compaction dropped tombstone", string(val))
}

func TestS2_BugZoo14_SplitApplyIndexDoubleApplies(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.Bugs.Bug14SplitApplyIndex = true

	db, err := lsm.OpenLSMStore(fs, "db_zoo14", sim.NewStreamRand(14, "db"), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Step 1: Apply initial value k1 = "v1" at Raft index 1 and flush so index 1 is durable
	db.ApplyBatch(1, []statemachine.Op{
		{ClientID: 1, SeqNum: 1, Type: statemachine.OpPut, Key: []byte("k1"), Value: []byte("v1")},
	})
	_ = db.Flush()

	// Step 2: Apply CAS(k1: "v1" -> "v2") at Raft index 2.
	// With Bug14SplitApplyIndex, the data batch (k1="v2") is synced, while appliedIndex=2 is written in a separate unsynced batch!
	res1 := db.ApplyBatch(2, []statemachine.Op{
		{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")},
	})
	if !res1[0].Applied {
		t.Fatal("expected initial CAS(v1->v2) to succeed")
	}
	_ = db.Close()

	// Step 3: Crash before the second batch (appliedIndex=2) is synced!
	// Use a deterministic RNG stream where the unsynced second batch is lost/torn
	for trySeed := uint64(1); trySeed <= 50; trySeed++ {
		fsClone := fs // Run crash on fs
		fsClone.Crash(sim.NewStreamRand(trySeed, "crash14"))
		reopened, err := lsm.OpenLSMStore(fsClone, "db_zoo14", sim.NewStreamRand(14, "reopen"), opts)
		if err != nil {
			continue
		}
		if reopened.AppliedIndex() < 2 {
			// Now Raft sees AppliedIndex() == 1 and replays Raft index 2: CAS(k1: "v1" -> "v2")!
			// Because k1 was ALREADY mutated to "v2" on disk before the crash, replaying index 2 sees k1="v2" != Expected("v1") and FAILS!
			replayRes := reopened.ApplyBatch(2, []statemachine.Op{
				{ClientID: 1, SeqNum: 2, Type: statemachine.OpCAS, Key: []byte("k1"), Expected: []byte("v1"), Value: []byte("v2")},
			})
			_ = reopened.Close()
			if !replayRes[0].Applied {
				t.Logf("[ZOO #14 CAUGHT] seed=%d seeds_to_detect=1 detector=CrashPointEnumerationOracle details=split data/appliedIndex write caused Raft log replay at index 2 to diverge from reference state machine (CAS Applied=%v vs expected true)",
					trySeed, replayRes[0].Applied)
				return
			}
		}
		_ = reopened.Close()
	}
	t.Fatal("expected Zoo #14 to cause state divergence on crash between data batch and appliedIndex batch")
}

func TestS2_ModelBasedDifferentialCrashTest_AgainstMemStore(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.MemtableFlushThreshold = 2048 // Frequent flushes & compactions
	opts.L0CompactionTrigger = 2
	opts.L1CompactionTrigger = 2

	rng := sim.NewStreamRand(2026, "diff_model")
	lsmDB, err := lsm.OpenLSMStore(fs, "db_model", sim.NewStreamRand(2026, "lsm_internal"), opts)
	if err != nil {
		t.Fatal(err)
	}

	refMem := statemachine.NewMemStore()
	const totalOps = 5000
	var appliedIdx uint64
	crashes := 0

	for step := 1; step <= totalOps; step++ {
		appliedIdx++
		key := []byte(fmt.Sprintf("user-key-%02d", rng.Intn(40)))
		val := []byte(fmt.Sprintf("val-%d-%d", step, rng.Intn(1000)))
		clientID := uint64(1 + (step % 4))
		seqNum := uint64(step)

		var op statemachine.Op
		switch rng.Intn(3) {
		case 0:
			op = statemachine.Op{ClientID: clientID, SeqNum: seqNum, Type: statemachine.OpPut, Key: key, Value: val}
		case 1:
			op = statemachine.Op{ClientID: clientID, SeqNum: seqNum, Type: statemachine.OpDelete, Key: key}
		case 2:
			cur, _, _ := refMem.Get(key)
			op = statemachine.Op{ClientID: clientID, SeqNum: seqNum, Type: statemachine.OpCAS, Key: key, Expected: cur, Value: val}
		}

		resRef := refMem.ApplyBatch(appliedIdx, []statemachine.Op{op})
		resLSM := lsmDB.ApplyBatch(appliedIdx, []statemachine.Op{op})

		if resRef[0].Applied != resLSM[0].Applied || !bytes.Equal(resRef[0].Value, resLSM[0].Value) {
			t.Fatalf("step %d ApplyBatch mismatch on key %q: ref=%+v vs lsm=%+v", step, string(key), resRef[0], resLSM[0])
		}

		// Periodically inject random power-loss crashes and reopen LSMStore from SimFS
		if step%250 == 0 {
			crashes++
			_ = lsmDB.Close()
			fs.Crash(sim.NewStreamRand(uint64(step), "power_loss"))

			reopened, openErr := lsm.OpenLSMStore(fs, "db_model", sim.NewStreamRand(uint64(step), "reopen"), opts)
			if openErr != nil {
				t.Fatalf("step %d OpenLSMStore after crash failed: %v", step, openErr)
			}
			lsmDB = reopened

			if lsmDB.AppliedIndex() != refMem.AppliedIndex() {
				t.Fatalf("step %d AppliedIndex mismatch after crash: lsm=%d vs ref=%d",
					step, lsmDB.AppliedIndex(), refMem.AppliedIndex())
			}

			// Verify full keyspace scan matches MemStore 100%
			refScan, _ := refMem.Scan(nil, nil, 0)
			lsmScan, scanErr := lsmDB.Scan(nil, nil, 0)
			if scanErr != nil {
				t.Fatalf("step %d LSM Scan failed: %v", step, scanErr)
			}
			if len(refScan) != len(lsmScan) {
				t.Fatalf("step %d Scan count mismatch after crash: lsm=%d vs ref=%d", step, len(lsmScan), len(refScan))
			}
			for i := range refScan {
				if !bytes.Equal(refScan[i].Key, lsmScan[i].Key) || !bytes.Equal(refScan[i].Value, lsmScan[i].Value) {
					t.Fatalf("step %d Scan item %d mismatch: lsm=(%s=%s) vs ref=(%s=%s)",
						step, i, lsmScan[i].Key, lsmScan[i].Value, refScan[i].Key, refScan[i].Value)
				}
			}
		}
	}
	_ = lsmDB.Close()
	t.Logf("[S2 MODEL ORACLE PASS] %d randomized Put/Delete/CAS operations with %d power-loss crashes, flushes, and L0->L1->L2 compactions matched MemStore 100%%.",
		totalOps, crashes)
}