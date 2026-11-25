package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cutaret/aetherkv/kv"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

type BenchEnvironment struct {
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	GoVersion    string `json:"go_version"`
	NumCPU       int    `json:"num_cpu"`
	FSMode       string `json:"fs_mode"`
	Runs         int    `json:"runs"`
	TimestampUTC string `json:"timestamp_utc"`
}

type BenchmarkRecord struct {
	Suite              string  `json:"suite"`
	Workload           string  `json:"workload"`
	FSMode             string  `json:"fs_mode"`
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

type BenchmarkReport struct {
	Environment BenchEnvironment  `json:"environment"`
	Results     []BenchmarkRecord `json:"results"`
}

func makeBenchFS(fsMode, prefix string) (vfs.FS, string, func()) {
	if fsMode == "os" {
		dir, err := os.MkdirTemp("", "aetherkv-bench-"+prefix+"-*")
		if err != nil {
			panic(err)
		}
		return vfs.NewOSFS(), dir, func() { _ = os.RemoveAll(dir) }
	}
	return vfs.NewSimFS(), prefix, func() {}
}

// makeIncompressiblePayload generates deterministic high-entropy bytes
// modeling FP16 KV-cache tensors so disk/compression baselines stay realistic.
func makeIncompressiblePayload(seed uint64, size int) []byte {
	rng := sim.NewStreamRand(seed, "payload")
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
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

func runYCSBWorkload(fsMode, name string, numOps, putRatio, getRatio, scanRatio, runs int) BenchmarkRecord {
	var opsRates, mibRates []float64
	var allLatencies []int64
	var lastWA float64
	valPayload := makeIncompressiblePayload(42, 256)

	for r := 0; r < runs; r++ {
		fs, baseDir, cleanup := makeBenchFS(fsMode, "ycsb")
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = 16 * 1024
		db, err := lsm.OpenLSMStore(fs, filepath.Join(baseDir, "db"), sim.NewStreamRand(uint64(100+r), "ycsb"), opts)
		if err != nil {
			cleanup()
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
		cleanup()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	medMiB, _ := medianAndSpread(mibRates)
	return BenchmarkRecord{
		Suite: "YCSB_LSM", Workload: name, FSMode: fsMode, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps, MedianMiBPerSec: medMiB,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: lastWA,
	}
}

func runReplicatedClusterBench(fsMode string, numOps, runs int) BenchmarkRecord {
	var opsRates []float64
	var allLatencies []int64

	for r := 0; r < runs; r++ {
		k := sim.NewKernel(uint64(700+r), nil)
		net := sim.NewSimNet(k)
		peers := []string{"n1", "n2", "n3"}
		reps := make(map[string]*kv.Replica)
		var cleanups []func()

		for _, id := range peers {
			fs, baseDir, cleanup := makeBenchFS(fsMode, "cluster_"+id)
			cleanups = append(cleanups, cleanup)
			rep, err := kv.NewReplica(id, peers, fs, filepath.Join(baseDir, "c_"+id), k.NodeClock(id), net, k.Stream(id), kv.KVBugConfig{
				UseLSM: true, LSMOpts: lsm.DefaultOptions(),
			})
			if err != nil {
				for _, fn := range cleanups {
					fn()
				}
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
		for _, fn := range cleanups {
			fn()
		}
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	return BenchmarkRecord{
		Suite: "Cluster_3Node_Raft_LSM", Workload: "50%_Write_50%_Linearizable_ReadIndex", FSMode: fsMode, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: 1.0,
	}
}

func runDurabilityOptionBench(fsMode, name string, disableLSMWALSync bool, numOps, runs int) BenchmarkRecord {
	var opsRates []float64
	var allLatencies []int64
	var fsyncs uint64

	for r := 0; r < runs; r++ {
		k := sim.NewKernel(uint64(900+r), nil)
		net := sim.NewSimNet(k)
		fs, baseDir, cleanup := makeBenchFS(fsMode, "durability")
		opts := lsm.DefaultOptions()
		opts.DisableWALSync = disableLSMWALSync
		opts.MemtableFlushThreshold = 64 * 1024

		rep, err := kv.NewReplica("n1", []string{"n1"}, fs, filepath.Join(baseDir, "opt_db"), k.NodeClock("n1"), net, k.Stream("n1"), kv.KVBugConfig{
			UseLSM: true, LSMOpts: opts,
		})
		if err != nil {
			cleanup()
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
		cleanup()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	return BenchmarkRecord{
		Suite: "Durability_Coupling", Workload: name, FSMode: fsMode, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: 1.0, TotalFsyncs: fsyncs,
	}
}

func runKVCacheSeparationBench(fsMode, name string, valueSize, numOps int, separate bool, runs int) BenchmarkRecord {
	var opsRates, mibRates []float64
	var allLatencies []int64
	var lastWA float64
	payload := makeIncompressiblePayload(uint64(valueSize), valueSize)

	for r := 0; r < runs; r++ {
		fs, baseDir, cleanup := makeBenchFS(fsMode, "kvcache")
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = max(8*1024, valueSize*2)
		opts.L0CompactionTrigger = 2
		opts.L1CompactionTrigger = 2
		if separate {
			opts.ValueLogThreshold = 4096
		}

		db, err := lsm.OpenLSMStore(fs, filepath.Join(baseDir, "db"), sim.NewStreamRand(uint64(1200+r), "kvcache"), opts)
		if err != nil {
			cleanup()
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
		cleanup()
		runtime.GC()
	}

	slices.Sort(allLatencies)
	medOps, spreadOps := medianAndSpread(opsRates)
	medMiB, _ := medianAndSpread(mibRates)
	return BenchmarkRecord{
		Suite: "Llama3_8B_KVCache", Workload: name, FSMode: fsMode, Runs: runs,
		MedianOpsPerSec: medOps, SpreadOpsPerSec: spreadOps, MedianMiBPerSec: medMiB,
		P50LatencyUs: percentileUs(allLatencies, 0.50), P99LatencyUs: percentileUs(allLatencies, 0.99),
		P999LatencyUs: percentileUs(allLatencies, 0.999), WriteAmplification: lastWA,
	}
}

func main() {
	runs := flag.Int("runs", 5, "Repetitions per benchmark")
	fsMode := flag.String("fs", "os", "Filesystem backend: 'os' (real POSIX fsync) or 'sim' (SimFS)")
	outDir := flag.String("out", "bench/results", "Output directory for JSON, CSV, and Markdown results")
	prefix := flag.String("prefix", "latest_benchmarks", "Filename prefix for exported results")
	flag.Parse()

	mode := strings.ToLower(*fsMode)
	if mode != "os" && mode != "sim" {
		panic("invalid -fs flag: must be 'os' or 'sim'")
	}

	_ = os.MkdirAll(*outDir, 0o755)
	env := BenchEnvironment{
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		GoVersion:    runtime.Version(),
		NumCPU:       runtime.NumCPU(),
		FSMode:       mode,
		Runs:         *runs,
		TimestampUTC: time.Now().UTC().Format(time.RFC3339),
	}

	fmt.Println("=====================================================================================================")
	fmt.Printf(" AetherKV Reproducible Benchmark Suite (%d runs/workload, fs=%s, OS=%s/%s, CPUs=%d)\n",
		*runs, mode, env.GOOS, env.GOARCH, env.NumCPU)
	fmt.Println("=====================================================================================================")

	results := []BenchmarkRecord{
		runYCSBWorkload(mode, "YCSB-A (50% Put / 50% Get)", 400, 50, 50, 0, *runs),
		runYCSBWorkload(mode, "YCSB-B (5% Put / 95% Get)", 400, 5, 95, 0, *runs),
		runYCSBWorkload(mode, "YCSB-E (5% Put / 95% Scan)", 200, 5, 0, 95, *runs),
		runReplicatedClusterBench(mode, 150, *runs),
		runDurabilityOptionBench(mode, "Option_A_DualFsync (Raft WAL + LSM WAL)", false, 200, *runs),
		runDurabilityOptionBench(mode, "Option_B_SingleFsync (Raft WAL + FlushedIndex)", true, 200, *runs),
		// Unified 5-run Llama-3-8B KV-cache sweep across all 5 block geometries
		runKVCacheSeparationBench(mode, "1KiB_Inline", 1024, 48, false, *runs),
		runKVCacheSeparationBench(mode, "1KiB_WiscKey", 1024, 48, true, *runs),
		runKVCacheSeparationBench(mode, "16KiB_Inline", 16*1024, 32, false, *runs),
		runKVCacheSeparationBench(mode, "16KiB_WiscKey", 16*1024, 32, true, *runs),
		runKVCacheSeparationBench(mode, "128KiB_Llama3_1Token_Inline", 128*1024, 24, false, *runs),
		runKVCacheSeparationBench(mode, "128KiB_Llama3_1Token_WiscKey", 128*1024, 24, true, *runs),
		runKVCacheSeparationBench(mode, "512KiB_Llama3_4Token_Inline", 512*1024, 12, false, *runs),
		runKVCacheSeparationBench(mode, "512KiB_Llama3_4Token_WiscKey", 512*1024, 12, true, *runs),
		runKVCacheSeparationBench(mode, "2MiB_Llama3_16Token_Inline", 2*1024*1024, 6, false, *runs),
		runKVCacheSeparationBench(mode, "2MiB_Llama3_16Token_WiscKey", 2*1024*1024, 6, true, *runs),
	}

	fmt.Printf("\n%-24s | %-44s | %10s | %8s | %8s | %9s | %6s\n",
		"Suite", "Workload", "Med Ops/s", "MiB/s", "p50 (us)", "p99 (us)", "WA")
	fmt.Println("-----------------------------------------------------------------------------------------------------------------------")
	for _, rec := range results {
		fmt.Printf("%-24s | %-44s | %10.0f | %8.1f | %8.1f | %9.1f | %5.2fx\n",
			rec.Suite, rec.Workload, rec.MedianOpsPerSec, rec.MedianMiBPerSec, rec.P50LatencyUs, rec.P99LatencyUs, rec.WriteAmplification)
	}

	report := BenchmarkReport{Environment: env, Results: results}
	jsonPath := filepath.Join(*outDir, *prefix+".json")
	jsonBytes, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(jsonPath, jsonBytes, 0o644)

	csvPath := filepath.Join(*outDir, *prefix+".csv")
	if cf, err := os.Create(csvPath); err == nil {
		w := csv.NewWriter(cf)
		_ = w.Write([]string{"suite", "workload", "fs_mode", "runs", "median_ops_per_sec", "spread_ops_per_sec", "median_mib_per_sec", "p50_us", "p99_us", "p999_us", "write_amplification", "total_fsyncs"})
		for _, r := range results {
			_ = w.Write([]string{
				r.Suite, r.Workload, r.FSMode, fmt.Sprintf("%d", r.Runs),
				fmt.Sprintf("%.2f", r.MedianOpsPerSec), fmt.Sprintf("%.2f", r.SpreadOpsPerSec),
				fmt.Sprintf("%.2f", r.MedianMiBPerSec), fmt.Sprintf("%.2f", r.P50LatencyUs),
				fmt.Sprintf("%.2f", r.P99LatencyUs), fmt.Sprintf("%.2f", r.P999LatencyUs),
				fmt.Sprintf("%.2f", r.WriteAmplification), fmt.Sprintf("%d", r.TotalFsyncs),
			})
		}
		w.Flush()
		_ = cf.Close()
	}

	mdPath := filepath.Join(*outDir, *prefix+".md")
	var md strings.Builder
	md.WriteString(fmt.Sprintf("### AetherKV Benchmark Results (`fs=%s`, `%s/%s`, `%d CPUs`, `%s`, `%d runs`)\n\n",
		env.FSMode, env.GOOS, env.GOARCH, env.NumCPU, env.GoVersion, env.Runs))
	md.WriteString("| Suite | Workload | Med Ops/s | Med MiB/s | p50 (us) | p99 (us) | Write Amp | Fsyncs |\n")
	md.WriteString("| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range results {
		fsyncCol := "-"
		if r.TotalFsyncs > 0 {
			fsyncCol = fmt.Sprintf("%d", r.TotalFsyncs)
		}
		md.WriteString(fmt.Sprintf("| %s | %s | %.0f | %.1f | %.1f | %.1f | %.2fx | %s |\n",
			r.Suite, r.Workload, r.MedianOpsPerSec, r.MedianMiBPerSec, r.P50LatencyUs, r.P99LatencyUs, r.WriteAmplification, fsyncCol))
	}
	_ = os.WriteFile(mdPath, []byte(md.String()), 0o644)

	fmt.Printf("\nSaved benchmark artifacts to %s, %s, and %s\n", jsonPath, csvPath, mdPath)
}
