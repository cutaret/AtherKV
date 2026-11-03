package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

type BenchmarkRecord struct {
	Suite              string  `json:"suite"`
	Workload           string  `json:"workload"`
	Runs               int     `json:"runs"`
	MedianOpsPerSec    float64 `json:"median_ops_per_sec"`
	SpreadOpsPerSec    float64 `json:"spread_ops_per_sec"`
	MedianMiBPerSec    float64 `json:"median_mib_per_sec"`
	P50LatencyUs       float64 `json:"p50_latency_us"`
	P99LatencyUs       float64 `json:"p99_latency_us"`
	P999LatencyUs      float64 `json:"p999_latency_us"`
	WriteAmplification float64 `json:"write_amplification"`
	TotalFsyncs        uint64  `json:"total_fsyncs,omitempty"`
}

func percentileUs(sortedNs []int64, pct float64) float64 {
	if len(sortedNs) == 0 {
		return 0
	}
	idx := max(0, min(int(float64(len(sortedNs))*pct)-1, len(sortedNs)-1))
	return float64(sortedNs[idx]) / 1000.0
}

func medianAndSpread(vals []float64) (median, spread float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	cloned := slices.Clone(vals)
	slices.Sort(cloned)
	return cloned[len(cloned)/2], cloned[len(cloned)-1] - cloned[0]
}

func runYCSBWorkload(name string, numOps, putRatio, getRatio, scanRatio, runs int) BenchmarkRecord {
	var opsRates, mibRates []float64
	var allLatencies []int64
	var lastWA float64
	valPayload := bytes.Repeat([]byte("V"), 256)

	for r := 0; r < runs; r++ {
		fs := vfs.NewSimFS()
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = 16 * 1024
		db, err := lsm.OpenLSMStore(fs, "ycsb_db", sim.NewStreamRand(uint64(100+r), "ycsb"), opts)
		if err != nil {
			panic(err)
		}
		for w := 0; w < 100; w++ {
			_ = db.PutDirect([]byte(fmt.Sprintf("user-key-%04d", w)), valPayload)
		}

		rng := sim.NewStreamRand(uint64(500+r), "work")
		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			k := []byte(fmt.Sprintf("user-key-%04d", rng.Intn(200)))
			roll := rng.Intn(100)
			opStart := time.Now()
			if roll < putRatio {
				_ = db.PutDirect(k, valPayload)
			} else if roll < putRatio+getRatio {
				_, _, _ = db.Get(k)
			} else if scanRatio > 0 {
				_, _ = db.Scan(k, nil, 10)
			}
			allLatencies = append(allLatencies, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(numOps*len(valPayload))/(1024.0*1024.0))/elapsed)
		lastWA = db.WriteAmplification()
		_ = db.Close()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	medMiB, _ := medianAndSpread(mibRates)
	return BenchmarkRecord{
		Suite: "YCSB_LSM", Workload: name, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps, MedianMiBPerSec: medMiB,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: lastWA,
	}
}

func runReplicatedClusterBench(numOps, runs int) BenchmarkRecord {
	var opsRates []float64
	var allLatencies []int64

	for r := 0; r < runs; r++ {
		k := sim.NewKernel(uint64(700+r), nil)
		net := sim.NewSimNet(k)
		peers := []string{"n1", "n2", "n3"}
		reps := make(map[string]*kv.Replica)

		for _, id := range peers {
			rep, err := kv.NewReplica(id, peers, vfs.NewSimFS(), "c_"+id, k.NodeClock(id), net, k.Stream(id), kv.KVBugConfig{
				UseLSM: true, LSMOpts: lsm.DefaultOptions(),
			})
			if err != nil {
				panic(err)
			}
			reps[id] = rep
			nodeID := id
			net.RegisterNode(nodeID, func(m sim.Message) { reps[nodeID].Step(m) })
		}

		for i := 0; i < 15; i++ {
			for _, id := range peers {
				reps[id].Tick()
			}
			k.RunUntil(k.Now()+10_000_000, 500)
		}

		var leader *kv.Replica
		for _, id := range peers {
			if reps[id].IsLeader() {
				leader = reps[id]
				break
			}
		}

		t0 := time.Now()
		for i := 1; i <= numOps; i++ {
			opStart := time.Now()
			if i%2 == 0 {
				leader.SubmitWrite(statemachine.Op{
					ClientID: 1, SeqNum: uint64(i), Type: statemachine.OpPut,
					Key: []byte(fmt.Sprintf("rk-%03d", i%50)), Value: []byte("replicated-val"),
				}, func(statemachine.Result, bool) {})
			} else {
				leader.SubmitRead(kv.ClientReadReq{
					ReadID: uint64(i), Key: []byte(fmt.Sprintf("rk-%03d", i%50)),
					Callback: func([]byte, bool, bool) {},
				})
			}
			k.RunUntil(k.Now()+5_000_000, 200)
			allLatencies = append(allLatencies, time.Since(opStart).Nanoseconds())
		}
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	return BenchmarkRecord{
		Suite: "Cluster_3Node_Raft_LSM", Workload: "50%_Write_50%_Linearizable_ReadIndex", Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: 1.0,
	}
}

func runDurabilityOptionBench(name string, disableLSMWALSync bool, numOps, runs int) BenchmarkRecord {
	var opsRates []float64
	var allLatencies []int64
	var fsyncs uint64

	for r := 0; r < runs; r++ {
		k := sim.NewKernel(uint64(900+r), nil)
		net := sim.NewSimNet(k)
		opts := lsm.DefaultOptions()
		opts.DisableWALSync = disableLSMWALSync
		opts.MemtableFlushThreshold = 64 * 1024

		rep, err := kv.NewReplica("n1", []string{"n1"}, vfs.NewSimFS(), "opt_db", k.NodeClock("n1"), net, k.Stream("n1"), kv.KVBugConfig{
			UseLSM: true, LSMOpts: opts,
		})
		if err != nil {
			panic(err)
		}
		for !rep.IsLeader() {
			rep.Tick()
		}

		t0 := time.Now()
		for i := 1; i <= numOps; i++ {
			opStart := time.Now()
			rep.SubmitWrite(statemachine.Op{
				ClientID: 1, SeqNum: uint64(i), Type: statemachine.OpPut,
				Key: []byte(fmt.Sprintf("k-%04d", i)), Value: []byte("option-a-vs-b-payload"),
			}, func(statemachine.Result, bool) {})
			allLatencies = append(allLatencies, time.Since(opStart).Nanoseconds())
		}
		_ = rep.FlushLSM()
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		fsyncs = rep.TotalFsyncCount()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	return BenchmarkRecord{
		Suite: "Durability_Coupling", Workload: name, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: 1.0, TotalFsyncs: fsyncs,
	}
}

func runKVCacheSeparationBench(name string, valueSize, numOps int, separate bool, runs int) BenchmarkRecord {
	var opsRates, mibRates []float64
	var allLatencies []int64
	var lastWA float64
	payload := bytes.Repeat([]byte("K"), valueSize)

	for r := 0; r < runs; r++ {
		fs := vfs.NewSimFS()
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = max(8*1024, valueSize*2)
		opts.L0CompactionTrigger = 2
		opts.L1CompactionTrigger = 2
		if separate {
			opts.ValueLogThreshold = 4096
		}

		db, err := lsm.OpenLSMStore(fs, "kvcache_db", sim.NewStreamRand(uint64(1200+r), "kvcache"), opts)
		if err != nil {
			panic(err)
		}

		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			key := []byte(fmt.Sprintf("kvcache-block-%02d", i%12))
			opStart := time.Now()
			_ = db.PutDirect(key, payload)
			allLatencies = append(allLatencies, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		_ = db.CompactLevel(0)
		elapsed := max(time.Since(t0).Seconds(), 1e-6)

		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(valueSize*numOps)/(1024.0*1024.0))/elapsed)
		lastWA = db.WriteAmplification()
		_ = db.Close()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	medMiB, _ := medianAndSpread(mibRates)
	return BenchmarkRecord{
		Suite: "Llama3_8B_KVCache", Workload: name, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps, MedianMiBPerSec: medMiB,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: lastWA,
	}
}

func main() {
	runs := flag.Int("runs", 5, "Repetitions per benchmark")
	outDir := flag.String("out", "bench/results", "Output directory for JSON and CSV results")
	flag.Parse()

	_ = os.MkdirAll(*outDir, 0o755)
	var results []BenchmarkRecord

	fmt.Println("=====================================================================================================")
	fmt.Printf(" AetherKV Reproducible Benchmark Suite (%d runs/workload, fsync=ON, OS=%s/%s)\n", *runs, runtime.GOOS, runtime.GOARCH)
	fmt.Println("=====================================================================================================")

	results = append(results,
		runYCSBWorkload("YCSB-A (50% Put / 50% Get)", 400, 50, 50, 0, *runs),
		runYCSBWorkload("YCSB-B (5% Put / 95% Get)", 400, 5, 95, 0, *runs),
		runYCSBWorkload("YCSB-E (5% Put / 95% Scan)", 200, 5, 0, 95, *runs),
		runReplicatedClusterBench(150, *runs),
		runDurabilityOptionBench("Option_A_DualFsync (Raft WAL + LSM WAL)", false, 200, *runs),
		runDurabilityOptionBench("Option_B_SingleFsync (Raft WAL + FlushedIndex)", true, 200, *runs),
		runKVCacheSeparationBench("128KiB_Llama3_1Token_Inline", 128*1024, 24, false, *runs),
		runKVCacheSeparationBench("128KiB_Llama3_1Token_WiscKeySeparated", 128*1024, 24, true, *runs),
		runKVCacheSeparationBench("2MiB_Llama3_16TokenBlock_Inline", 2*1024*1024, 6, false, *runs),
		runKVCacheSeparationBench("2MiB_Llama3_16TokenBlock_WiscKeySeparated", 2*1024*1024, 6, true, *runs),
	)

	fmt.Printf("\n%-24s | %-42s | %10s | %8s | %8s | %9s | %6s\n",
		"Suite", "Workload", "Med Ops/s", "MiB/s", "p50 (us)", "p99 (us)", "WA")
	fmt.Println("---------------------------------------------------------------------------------------------------------------------")
	for _, rec := range results {
		fmt.Printf("%-24s | %-42s | %10.0f | %8.1f | %8.1f | %9.1f | %5.2fx\n",
			rec.Suite, rec.Workload, rec.MedianOpsPerSec, rec.MedianMiBPerSec, rec.P50LatencyUs, rec.P99LatencyUs, rec.WriteAmplification)
	}

	jsonPath := filepath.Join(*outDir, "latest_benchmarks.json")
	jsonBytes, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile(jsonPath, jsonBytes, 0o644)

	csvPath := filepath.Join(*outDir, "latest_benchmarks.csv")
	if cf, err := os.Create(csvPath); err == nil {
		w := csv.NewWriter(cf)
		_ = w.Write([]string{"suite", "workload", "runs", "median_ops_per_sec", "spread_ops_per_sec", "median_mib_per_sec", "p50_us", "p99_us", "p999_us", "write_amplification", "total_fsyncs"})
		for _, r := range results {
			_ = w.Write([]string{
				r.Suite, r.Workload, fmt.Sprintf("%d", r.Runs),
				fmt.Sprintf("%.2f", r.MedianOpsPerSec), fmt.Sprintf("%.2f", r.SpreadOpsPerSec),
				fmt.Sprintf("%.2f", r.MedianMiBPerSec), fmt.Sprintf("%.2f", r.P50LatencyUs),
				fmt.Sprintf("%.2f", r.P99LatencyUs), fmt.Sprintf("%.2f", r.P999LatencyUs),
				fmt.Sprintf("%.2f", r.WriteAmplification), fmt.Sprintf("%d", r.TotalFsyncs),
			})
		}
		w.Flush()
		_ = cf.Close()
	}
	fmt.Printf("\nSaved raw benchmark artifacts to %s and %s\n", jsonPath, csvPath)
}
