package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cutaret/aetherkv/lsm"
	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/vfs"
)

type CompareRow struct {
	Workload        string  `json:"workload"`
	Engine          string  `json:"engine"`
	Runs            int     `json:"runs"`
	MedianOpsPerSec float64 `json:"median_ops_per_sec"`
	MedianMiBPerSec float64 `json:"median_mib_per_sec"`
	P50LatencyUs    float64 `json:"p50_latency_us"`
	P99LatencyUs    float64 `json:"p99_latency_us"`
}

func makePayload(seed uint64, size int) []byte {
	rng := sim.NewStreamRand(seed, "pebble_cmp")
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(rng.Intn(256))
	}
	return buf
}

func median(vals []float64) float64 {
	c := slices.Clone(vals)
	slices.Sort(c)
	return c[len(c)/2]
}

func pctUs(sortedNs []int64, p float64) float64 {
	if len(sortedNs) == 0 {
		return 0
	}
	idx := max(0, min(int(float64(len(sortedNs))*p)-1, len(sortedNs)-1))
	return float64(sortedNs[idx]) / 1000.0
}

func benchAetherYCSB(name string, numOps, putRatio, runs int) CompareRow {
	var opsRates, mibRates []float64
	var lats []int64
	val := makePayload(42, 256)

	for r := 0; r < runs; r++ {
		dir, err := os.MkdirTemp("", "aether-ycsb-*")
		if err != nil {
			panic(err)
		}
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = 64 * 1024
		db, err := lsm.OpenLSMStore(vfs.NewOSFS(), filepath.Join(dir, "db"), sim.NewStreamRand(uint64(100+r), "y"), opts)
		if err != nil {
			panic(err)
		}
		for w := 0; w < 100; w++ {
			_ = db.PutDirect([]byte(fmt.Sprintf("user-key-%04d", w)), val)
		}

		rng := sim.NewStreamRand(uint64(500+r), "work")
		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			k := []byte(fmt.Sprintf("user-key-%04d", rng.Intn(200)))
			opStart := time.Now()
			if rng.Intn(100) < putRatio {
				_ = db.PutDirect(k, val)
			} else {
				_, _, _ = db.Get(k)
			}
			lats = append(lats, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(numOps*len(val))/(1024.0*1024.0))/elapsed)
		_ = db.Close()
		_ = os.RemoveAll(dir)
		runtime.GC()
	}

	slices.Sort(lats)
	return CompareRow{
		Workload: name, Engine: "AetherKV (OSFS, fsync=ON)", Runs: runs,
		MedianOpsPerSec: median(opsRates), MedianMiBPerSec: median(mibRates),
		P50LatencyUs: pctUs(lats, 0.50), P99LatencyUs: pctUs(lats, 0.99),
	}
}

func benchPebbleYCSB(name string, numOps, putRatio, runs int) CompareRow {
	var opsRates, mibRates []float64
	var lats []int64
	val := makePayload(42, 256)

	for r := 0; r < runs; r++ {
		dir, err := os.MkdirTemp("", "pebble-ycsb-*")
		if err != nil {
			panic(err)
		}
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			panic(err)
		}
		for w := 0; w < 100; w++ {
			_ = db.Set([]byte(fmt.Sprintf("user-key-%04d", w)), val, pebble.Sync)
		}

		rng := sim.NewStreamRand(uint64(500+r), "work")
		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			k := []byte(fmt.Sprintf("user-key-%04d", rng.Intn(200)))
			opStart := time.Now()
			if rng.Intn(100) < putRatio {
				_ = db.Set(k, val, pebble.Sync)
			} else {
				_, closer, gerr := db.Get(k)
				if gerr == nil {
					_ = closer.Close()
				} else if !errors.Is(gerr, pebble.ErrNotFound) {
					panic(gerr)
				}
			}
			lats = append(lats, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(numOps*len(val))/(1024.0*1024.0))/elapsed)
		_ = db.Close()
		_ = os.RemoveAll(dir)
		runtime.GC()
	}

	slices.Sort(lats)
	return CompareRow{
		Workload: name, Engine: "CockroachDB Pebble v1.1.5 (Sync)", Runs: runs,
		MedianOpsPerSec: median(opsRates), MedianMiBPerSec: median(mibRates),
		P50LatencyUs: pctUs(lats, 0.50), P99LatencyUs: pctUs(lats, 0.99),
	}
}

func benchAetherLarge(name string, valueSize, numOps int, separate bool, runs int) CompareRow {
	var opsRates, mibRates []float64
	var lats []int64
	val := makePayload(uint64(valueSize), valueSize)

	for r := 0; r < runs; r++ {
		dir, err := os.MkdirTemp("", "aether-large-*")
		if err != nil {
			panic(err)
		}
		opts := lsm.DefaultOptions()
		opts.MemtableFlushThreshold = max(64*1024, valueSize*2)
		opts.L0CompactionTrigger = 2
		opts.L1CompactionTrigger = 2
		if separate {
			opts.ValueLogThreshold = 4096
		}
		db, err := lsm.OpenLSMStore(vfs.NewOSFS(), filepath.Join(dir, "db"), sim.NewStreamRand(uint64(1200+r), "lg"), opts)
		if err != nil {
			panic(err)
		}

		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			k := []byte(fmt.Sprintf("kvcache-block-%02d", i%12))
			opStart := time.Now()
			_ = db.PutDirect(k, val)
			lats = append(lats, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		_ = db.CompactLevel(0)
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(valueSize*numOps)/(1024.0*1024.0))/elapsed)
		_ = db.Close()
		_ = os.RemoveAll(dir)
		runtime.GC()
	}

	label := "AetherKV Inline LSM (OSFS)"
	if separate {
		label = "AetherKV WiscKey VLog (OSFS)"
	}
	slices.Sort(lats)
	return CompareRow{
		Workload: name, Engine: label, Runs: runs,
		MedianOpsPerSec: median(opsRates), MedianMiBPerSec: median(mibRates),
		P50LatencyUs: pctUs(lats, 0.50), P99LatencyUs: pctUs(lats, 0.99),
	}
}

func benchPebbleLarge(name string, valueSize, numOps, runs int) CompareRow {
	var opsRates, mibRates []float64
	var lats []int64
	val := makePayload(uint64(valueSize), valueSize)

	for r := 0; r < runs; r++ {
		dir, err := os.MkdirTemp("", "pebble-large-*")
		if err != nil {
			panic(err)
		}
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			panic(err)
		}

		t0 := time.Now()
		for i := 0; i < numOps; i++ {
			k := []byte(fmt.Sprintf("kvcache-block-%02d", i%12))
			opStart := time.Now()
			_ = db.Set(k, val, pebble.Sync)
			lats = append(lats, time.Since(opStart).Nanoseconds())
		}
		_ = db.Flush()
		_ = db.Compact([]byte("kvcache-block-00"), []byte("kvcache-block-99"), true)
		elapsed := max(time.Since(t0).Seconds(), 1e-6)
		opsRates = append(opsRates, float64(numOps)/elapsed)
		mibRates = append(mibRates, (float64(valueSize*numOps)/(1024.0*1024.0))/elapsed)
		_ = db.Close()
		_ = os.RemoveAll(dir)
		runtime.GC()
	}

	slices.Sort(lats)
	return CompareRow{
		Workload: name, Engine: "CockroachDB Pebble v1.1.5 (Sync)", Runs: runs,
		MedianOpsPerSec: median(opsRates), MedianMiBPerSec: median(mibRates),
		P50LatencyUs: pctUs(lats, 0.50), P99LatencyUs: pctUs(lats, 0.99),
	}
}

func main() {
	runs := flag.Int("runs", 5, "Repetitions per workload")
	outDir := flag.String("out", "../results", "Output directory")
	flag.Parse()

	_ = os.MkdirAll(*outDir, 0o755)

	rows := []CompareRow{
		benchAetherYCSB("YCSB-A (50% Put / 50% Get, 256B)", 400, 50, *runs),
		benchPebbleYCSB("YCSB-A (50% Put / 50% Get, 256B)", 400, 50, *runs),
		benchAetherYCSB("YCSB-B (5% Put / 95% Get, 256B)", 400, 5, *runs),
		benchPebbleYCSB("YCSB-B (5% Put / 95% Get, 256B)", 400, 5, *runs),
		benchAetherLarge("128 KiB Llama-3 1-Token KV Block", 128*1024, 24, false, *runs),
		benchAetherLarge("128 KiB Llama-3 1-Token KV Block", 128*1024, 24, true, *runs),
		benchPebbleLarge("128 KiB Llama-3 1-Token KV Block", 128*1024, 24, *runs),
		benchAetherLarge("2 MiB Llama-3 16-Token KV Block", 2*1024*1024, 6, false, *runs),
		benchAetherLarge("2 MiB Llama-3 16-Token KV Block", 2*1024*1024, 6, true, *runs),
		benchPebbleLarge("2 MiB Llama-3 16-Token KV Block", 2*1024*1024, 6, *runs),
	}

	var md strings.Builder
	md.WriteString(fmt.Sprintf("### AetherKV vs. CockroachDB Pebble v1.1.5 (`%s/%s`, `%d CPUs`, `fsync=ON`, `%d-run median`)\n\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), *runs))
	md.WriteString("| Workload | Engine | Med Ops/s | Med MiB/s | p50 (us) | p99 (us) |\n")
	md.WriteString("| :--- | :--- | ---: | ---: | ---: | ---: |\n")
	for _, r := range rows {
		md.WriteString(fmt.Sprintf("| %s | %s | %.0f | %.1f | %.1f | %.1f |\n",
			r.Workload, r.Engine, r.MedianOpsPerSec, r.MedianMiBPerSec, r.P50LatencyUs, r.P99LatencyUs))
	}

	fmt.Println(md.String())
	rawJSON, _ := json.MarshalIndent(rows, "", "  ")
	_ = os.WriteFile(filepath.Join(*outDir, "linux_pebble_compare.json"), rawJSON, 0o644)
	_ = os.WriteFile(filepath.Join(*outDir, "linux_pebble_compare.md"), []byte(md.String()), 0o644)
}
