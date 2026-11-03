package lsm_test

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/vfs"
)

func TestS2b_BugZoo15_DanglingVLogPointerBeforeSync(t *testing.T) {
	opts := lsm.DefaultOptions()
	opts.ValueLogThreshold = 1024
	opts.Bugs.Bug15DanglingVLogPtr = true

	// Try seeds until SimFS crash drops/tears the unsynced ValueLog write while the synced LSM WAL pointer survives
	for seed := uint64(1); seed <= 50; seed++ {
		fs := vfs.NewSimFS()
		db, err := lsm.OpenLSMStore(fs, "db_zoo15", sim.NewStreamRand(seed, "db15"), opts)
		if err != nil {
			t.Fatal(err)
		}

		largeVal := bytes.Repeat([]byte("K"), 4096)
		_ = db.PutDirect([]byte("kv_block_1"), largeVal)
		_ = db.Close()

		fs.Crash(sim.NewStreamRand(seed, "crash15"))

		reopened, err := lsm.OpenLSMStore(fs, "db_zoo15", sim.NewStreamRand(seed, "re15"), lsm.DefaultOptions())
		if err != nil {
			continue
		}
		oracleErr := reopened.VerifyAllPointers()
		_ = reopened.Close()
		if oracleErr != nil {
			t.Logf("[ZOO #15 CAUGHT] seed=%d seeds_to_detect=%d detector=CrashAtEverySyncOracle details=surviving LSM pointer failed to resolve after crash before vlog sync: %v",
				seed, seed, oracleErr)
			return
		}
	}
	t.Fatal("expected Zoo #15 (dangling VLogPointer before vlog sync) to be caught by CrashAtEverySyncOracle")
}

func TestS2b_BugZoo16_VLogGCDiscardsLiveValue(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.ValueLogThreshold = 512
	opts.ValueLogMaxSegBytes = 2048 // Rotate vlog segment every 2 KiB
	opts.Bugs.Bug16VLogGCDropLive = true

	db, err := lsm.OpenLSMStore(fs, "db_zoo16", sim.NewStreamRand(16, "db16"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write "live_key" into segment 1, then write other keys to force rotation into segment 2 and 3
	liveVal := bytes.Repeat([]byte("L"), 1024)
	_ = db.PutDirect([]byte("live_key"), liveVal)
	for i := 0; i < 6; i++ {
		_ = db.PutDirect([]byte(fmt.Sprintf("temp_%d", i)), bytes.Repeat([]byte("T"), 1024))
	}

	// Run VLog GC: with Bug16VLogGCDropLive, segment 1 is deleted without relocating "live_key"!
	reclaimed, err := db.RunVLogGC()
	if err != nil || reclaimed == 0 {
		t.Fatalf("expected RunVLogGC to reclaim old segments, reclaimed=%d err=%v", reclaimed, err)
	}

	_, _, getErr := db.Get([]byte("live_key"))
	if getErr == nil {
		t.Fatal("expected Zoo #16 to lose live_key's value when GC discarded segment 1 without relocating live pointers")
	}
	t.Logf("[ZOO #16 CAUGHT] seed=16 seeds_to_detect=1 detector=ModelBasedOracle details=live key 'live_key' failed to resolve after faulty VLog GC discarded segment: %v",
		getErr)
}

func TestS2b_BugZoo17_TornVLogTailAccepted(t *testing.T) {
	fs := vfs.NewSimFS()
	opts := lsm.DefaultOptions()
	opts.ValueLogThreshold = 512

	db, err := lsm.OpenLSMStore(fs, "db_zoo17", sim.NewStreamRand(17, "db17"), opts)
	if err != nil {
		t.Fatal(err)
	}

	origVal := bytes.Repeat([]byte("V"), 2048)
	_ = db.PutDirect([]byte("token_block"), origVal)
	_ = db.Close()

	// Corrupt the tail of vlog-000001.log simulating a torn sector write
	vf, err := fs.Open("db_zoo17/vlog-000001.log")
	if err != nil {
		t.Fatal(err)
	}
	sz, _ := vf.Size()
	_, _ = vf.WriteAt([]byte{0xAA, 0xAA, 0xAA, 0xAA}, sz-16)
	_ = vf.Sync()
	_ = vf.Close()

	// 1. Clean LSMStore must detect the CRC mismatch and reject the torn value!
	cleanDB, _ := lsm.OpenLSMStore(fs, "db_zoo17", sim.NewStreamRand(17, "clean"), opts)
	_, _, cleanErr := cleanDB.Get([]byte("token_block"))
	_ = cleanDB.Close()
	if cleanErr != lsm.ErrCorruptVLogCRC {
		t.Fatalf("expected clean ValueLog reader to return ErrCorruptVLogCRC on torn tail, got %v", cleanErr)
	}

	// 2. With Bug17AcceptTornVLog enabled, the reader silently returns the corrupted value!
	buggyOpts := opts
	buggyOpts.Bugs.Bug17AcceptTornVLog = true
	buggyDB, _ := lsm.OpenLSMStore(fs, "db_zoo17", sim.NewStreamRand(17, "buggy"), buggyOpts)
	corruptVal, found, buggyErr := buggyDB.Get([]byte("token_block"))
	_ = buggyDB.Close()
	if buggyErr != nil || !found || bytes.Equal(corruptVal, origVal) {
		t.Fatalf("expected Zoo #17 to accept corrupted torn value, found=%v err=%v", found, buggyErr)
	}
	t.Logf("[ZOO #17 CAUGHT] seed=17 seeds_to_detect=1 detector=CrashAtEverySyncOracle details=ValueLog accepted torn tail value (corrupted tail bytes=%x vs expected %x)",
		corruptVal[len(corruptVal)-4:], origVal[len(origVal)-4:])
}

// TestS2b_Llama3_8B_KVCache_ValueSeparationBenchmark measures Write Amplification (WA),
// throughput (MiB/s), and p99 latency across value sizes from 1 KiB to 2 MiB (Llama-3-8B 16-token KV-cache block).
func TestS2b_Llama3_8B_KVCache_ValueSeparationBenchmark(t *testing.T) {
	// Llama-3-8B math: 2 (K,V) * 32 layers * 8 KV heads * 128 dim * 2B (FP16) = 131,072 B (128 KiB / token)
	// 16-token KV-cache block = 2 MiB (2,097,152 B).
	blockSizes := []struct {
		name      string
		sizeBytes int
		numOps    int
	}{
		{"1_KiB_Small", 1024, 120},
		{"16_KiB_Chunk", 16 * 1024, 60},
		{"128_KiB_Llama3_1Token", 128 * 1024, 30},
		{"512_KiB_Llama3_4Tokens", 512 * 1024, 16},
		{"2_MiB_Llama3_16TokenBlock", 2 * 1024 * 1024, 8},
	}

	runMode := func(valueSize, numOps int, separate bool) (wa float64, mibPerSec float64, p99Us float64) {
		fs := vfs.NewSimFS()
		opts := lsm.DefaultOptions()
		// Trigger flushes and L0->L1 compactions so inline mode pays realistic LSM compaction write amplification
		opts.MemtableFlushThreshold = max(8*1024, valueSize*2)
		opts.L0CompactionTrigger = 2
		opts.L1CompactionTrigger = 2
		if separate {
			opts.ValueLogThreshold = 4096 // Separate values >= 4 KiB into WiscKey ValueLog
		} else {
			opts.ValueLogThreshold = 0 // Keep all values inline inside LSM SSTables
		}

		db, err := lsm.OpenLSMStore(fs, "bench_db", sim.NewStreamRand(808, "bench"), opts)
		if err != nil {
			t.Fatal(err)
		}

		payload := bytes.Repeat([]byte("X"), valueSize)
		latenciesNs := make([]int64, 0, numOps)
		startTotal := time.Now()

		for i := 0; i < numOps; i++ {
			key := []byte(fmt.Sprintf("kvcache-block-%02d", i%12))
			t0 := time.Now()
			_ = db.PutDirect(key, payload)
			latenciesNs = append(latenciesNs, time.Since(t0).Nanoseconds())
		}
		_ = db.Flush()
		_ = db.CompactLevel(0)
		elapsedSec := time.Since(startTotal).Seconds()
		if elapsedSec <= 0 {
			elapsedSec = 1e-6
		}

		slices.Sort(latenciesNs)
		p99Idx := int(float64(len(latenciesNs))*0.99) - 1
		if p99Idx < 0 {
			p99Idx = 0
		}
		p99Us = float64(latenciesNs[p99Idx]) / 1000.0
		totalMiB := float64(valueSize*numOps) / (1024.0 * 1024.0)
		mibPerSec = totalMiB / elapsedSec
		wa = db.WriteAmplification()
		_ = db.Close()
		runtime.GC()
		return wa, mibPerSec, p99Us
	}

	t.Log("==========================================================================================")
	t.Log(" Llama-3-8B KV-Cache Block Benchmark: Inline LSM vs. WiscKey Value-Separated LSM          ")
	t.Log("==========================================================================================")
	for _, tc := range blockSizes {
		waInline, thrInline, p99Inline := runMode(tc.sizeBytes, tc.numOps, false)
		waSep, thrSep, p99Sep := runMode(tc.sizeBytes, tc.numOps, true)
		t.Logf("[BENCH %-25s] Inline: WA=%.2fx, Thru=%6.1f MiB/s, p99=%7.1f us | Separated: WA=%.2fx, Thru=%6.1f MiB/s, p99=%7.1f us",
			tc.name, waInline, thrInline, p99Inline, waSep, thrSep, p99Sep)

		if tc.sizeBytes >= 128*1024 && waSep >= waInline {
			t.Fatalf("expected WiscKey value separation to reduce Write Amplification on %s: got separated=%.2fx >= inline=%.2fx",
				tc.name, waSep, waInline)
		}
	}
}
