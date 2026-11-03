# AetherKV — Complete Empirical Telemetry & Verification Ledger

This ledger records all measured simulator performance numbers, reproducible 5-run benchmark artifacts (`bench/results/latest_benchmarks.csv`), Bug Zoo mutation-testing detections, and the 7 real implementation defects discovered by the deterministic simulator and crash oracles during development.

## 1. Simulator Determinism, Velocity & Verification Campaigns

| Stage  | Verification Campaign                         | Volume / Scale                 | Result          | Measured Speed           |
| :----- | :-------------------------------------------- | :----------------------------- | :-------------- | :----------------------- |
| **S0** | Run-Twice SHA-256 Trace Hash Check            | 10,000 seeds x 2 (20,000 runs) | 100.0% match    | 8,174 virtual s / real s |
| **S1** | Multi-Cluster Chaos (3, 4, 5 nodes)           | 150 cluster chaos runs         | 0 violations    | 196 virtual s / real s   |
| **S2** | Differential Crash (`LSMStore` vs `MemStore`) | 5,000 ops + 20 crashes         | 100.0% match    | 0.18 s total             |
| **S2** | SSTable Double-Hashing Bloom Filter           | 1,000 keys + 10,000 probes     | 0% FN, 0.78% FP | < 1 ms                   |
| **S3** | Full-Stack Chaos (`Raft + LSM + VLog`)        | 30 full-stack chaos seeds      | 0 violations    | 78 ms total              |
| **S3** | Real TCP 3-Node Cluster (`127.0.0.1`)         | 3 OS sockets + follower crash  | Linearizable    | 0.52 s total             |
| **S4** | `aether-check` + Multi-Key `SCAN` Oracle      | 1,000 histories + scan tests   | 100.0% match    | 7 ops shrunk to 2 ops    |

## 2. Reproducible 5-Run Benchmark Suite (`cmd/benchrun`)

Executed via `go run ./cmd/benchrun -runs 5 -out bench/results` on `windows/amd64` with `fsync=ON`:

| Workload                                           |  Med Ops/s | Med MiB/s |    p99 (us) | Write Amp |
| :------------------------------------------------- | ---------: | --------: | ----------: | --------: |
| `YCSB-A` (50% Put / 50% Get, 256B)                 |     59,803 |      14.6 |       577.2 |     3.47x |
| `YCSB-B` (5% Put / 95% Get, 256B)                  |    154,565 |      37.7 |       < 1.0 |     3.45x |
| `YCSB-E` (5% Put / 95% Scan, 256B)                 |     11,042 |       2.7 |     2,827.7 |     3.49x |
| `Cluster_3Node` (50% Write / 50% `ReadIndex`)      |      6,963 |         - |     4,165.5 |     1.00x |
| `Option_A_DualFsync` (Raft WAL + LSM WAL)          |     21,658 |         - |     1,784.2 |     1.00x |
| `Option_B_SingleFsync` (Raft WAL + `FlushedIndex`) | **28,481** |         - | **1,497.9** | **1.00x** |
| `Llama3_1Token_128KiB` (Inline LSM)                |        651 |      81.3 |     8,684.0 |     4.50x |
| `Llama3_1Token_128KiB` (WiscKey Separated)         |  **2,260** | **282.5** | **2,259.2** | **1.00x** |
| `Llama3_16Token_2MiB` (Inline LSM)                 |         67 |     134.8 |    35,957.6 |     3.67x |
| `Llama3_16Token_2MiB` (WiscKey Separated)          |    **293** | **586.9** | **8,090.9** | **1.00x** |

- **Option B Durability Coupling Gain:** Skipping redundant LSM WAL `fsync`s while gating Raft log prefix truncation on `LSMStore.FlushedIndex()` reduced total `fsync` calls across 200 writes from 401 to 201 (**-49.9% `fsync`s**), increased median write throughput by **+31.5%**, and cut `p99` write latency by **-16.0%**.
- **Llama-3-8B KV-Cache Separation Gain (5-Run Median):** Separating values >= 4 KiB into the append-only WiscKey `ValueLog` cut Write Amplification on 128 KiB blocks by **-77.8%** (3.47x higher throughput, 3.84x lower `p99` latency) and on 2 MiB blocks by **-72.8%** (4.35x higher throughput, 4.44x lower `p99` latency).

## 3. Granular Llama-3-8B Block-Size Sweep (`TestS2b_Llama3_8B_KVCache_ValueSeparationBenchmark`)

Derived from Llama-3-8B tensor dimensions ($2 \times 32\text{ layers} \times 8\text{ KV heads} \times 128\text{ dim} \times 2\text{B FP16} = 128\text{ KiB/token}$):

| Block Size                      | Inline WA | Separated WA |         Inline Thru / p99 |           Separated Thru / p99 |
| :------------------------------ | --------: | -----------: | ------------------------: | -----------------------------: |
| `1 KiB` (Below 4 KiB threshold) |     2.96x |        2.96x |     74.7 MiB/s (522.4 us) |          74.6 MiB/s (520.4 us) |
| `16 KiB` Chunk                  |     4.82x |    **1.01x** |  23.8 MiB/s (10,447.0 us) |    **57.1 MiB/s (4,958.3 us)** |
| `128 KiB` (1-Token KV Block)    |     4.80x |    **1.00x** |   66.2 MiB/s (4,967.1 us) |  **101.7 MiB/s (12,923.8 us)** |
| `512 KiB` (4-Token KV Block)    |     4.25x |    **1.00x** | 109.2 MiB/s (14,429.7 us) | **1,264.5 MiB/s (1,743.1 us)** |
| `2 MiB` (16-Token KV Block)     |     3.50x |    **1.00x** | 171.2 MiB/s (26,565.2 us) | **1,046.1 MiB/s (3,218.0 us)** |

## 4. 17-Entry Bug Zoo Mutation-Testing Record (17 / 17 Caught)

| Zoo #   | Subsystem    | Planted Bug Description                               | Firing Detector            | Detection & Shrink            |
| :------ | :----------- | :---------------------------------------------------- | :------------------------- | :---------------------------- |
| **#1**  | Raft Core    | Commit earlier-term entry by replica count (Fig. 8)   | `StateMachineSafety`       | `seed=1` (1 seed)             |
| **#2**  | Raft + VFS   | Reply to `RequestVote` before persisting `votedFor`   | `TermMonotonicity`         | `seed=1` (1 seed)             |
| **#3**  | Raft + WAL   | Acknowledge `AppendEntries` before WAL `fsync`        | `CommittedDurability`      | `seed=1` (1 seed)             |
| **#4**  | Raft Core    | Unconditional log truncate on stale `AppendEntries`   | `CommittedDurability`      | `seed=4` (1 seed)             |
| **#5**  | KV + Raft    | Deposed leader serves local `Get` without `ReadIndex` | `PorcupineLinearizability` | `seed=5` (3 -> 2 ops)         |
| **#6**  | KV + Clock   | Leader serves lease read under unbounded clock skew   | `PorcupineLinearizability` | `seed=606` (3 -> 2 ops)       |
| **#7**  | KV Sessions  | Disable `(ClientID, SeqNum)` session deduplication    | `PorcupineLinearizability` | `seed=707` (5 -> 2 ops)       |
| **#8**  | LSM WAL      | Accept sector-torn 32 KiB WAL tail without CRC check  | `CrashAtEverySyncOracle`   | `seed=8` (1 seed)             |
| **#9**  | LSM + VFS    | Rename `CURRENT` to `MANIFEST` without `SyncDir()`    | `SimFSCrashOracle`         | `seed=9` (1 seed)             |
| **#10** | LSM Compact  | Drop tombstone in L1 while older value lives in L2    | `ModelBasedOracle`         | `seed=10` (1 seed)            |
| **#11** | Raft + LSM   | Truncate Raft log past `FlushedIndex` under Option B  | `CrashRecoveryOracle`      | `seed=1111` (1 seed)          |
| **#12** | KV Snapshot  | `InstallSnapshot` omits client session dedup table    | `PorcupineLinearizability` | `seed=1212` (5 -> 1 op)       |
| **#13** | Raft Core    | Quorum computed as `(n+1)/2` on 4-node cluster        | `CommittedDurability`      | `seed=4` (**20 -> 2 faults**) |
| **#14** | KV + LSM     | Split KV data batch and `appliedIndex` into 2 writes  | `CrashPointEnumeration`    | `seed=1` (1 seed)             |
| **#15** | WiscKey VLog | Sync LSM pointer batch before `ValueLog.Sync()`       | `CrashAtEverySyncOracle`   | `seed=1` (1 seed)             |
| **#16** | WiscKey VLog | Value-log GC deletes segment with still-live pointers | `ModelBasedOracle`         | `seed=16` (1 seed)            |
| **#17** | WiscKey VLog | Accept torn value-log tail without verifying CRC32C   | `CrashAtEverySyncOracle`   | `seed=17` (1 seed)            |

## 5. Seven Real, Unplanted Bugs Discovered & Fixed Using the Simulator

1. **Raft Sec. 6.4 `ReadIndex` Uncommitted Prefix Bug (`seed=10`):** A newly elected leader with uncommitted earlier-term entries served `ReadIndex` at `commitIdx=0` before committing a current-term no-op entry, returning stale empty reads after leader failover. Caught by `PorcupineLinearizability`.
2. **Unverified `commitIdx` Advance on `MsgHeartbeat` (`seed=12`):** `handleHeartbeat` advanced a follower's `commitIdx` without `prevLogIndex`/`prevLogTerm` checks, causing a deposed Term 1 leader to commit an overwritten Term 1 entry upon receiving a Term 2 heartbeat. Caught by `StateMachineSafety` in Stage S3.
3. **Torn WAL Header Unbounded Allocation OOM (`0xAAAAAAAA` = 2.86 GB):** When `SimFS` tore a 512B sector in `SegmentedWAL`, the corrupted length field (`0xAAAAAAAA`) was passed to `make([]byte, length)` before checking `offset + length <= fileSize`. Caught during `Zoo_03` crash testing.
4. **Zombie LSM WAL Replay After Unsynced Truncation (Step 500 Crash):** After `Flush()` reset `active.wal` without calling `Sync()`, a power-loss crash partially restored pre-flush WAL sectors (`seq=499`) which replayed into `Memtable` and shadowed newer `L0` SSTable entries (`seq=500`). Caught by `TestS2_ModelBasedDifferentialCrashTest_AgainstMemStore`.
5. **`SimFS` Sub-Sector Tear Bleed:** Sector-granular crash tearing initially overwrote the start of a 512B sector (`secOff`) rather than clamping to the unsynced write sub-range `[max(secOff, w.off), min(secEnd, w.off+w.len))`, corrupting previously `fsync`ed records sharing the sector.
6. **`SegmentedWAL.TruncateSuffix` Resurrection Bug:** Suffix truncation removed entries from the in-memory index map without truncating the underlying `SimFS` segment file, causing overwritten uncommitted entries to resurrect on restart.
7. **`SimFS.WriteAt` Quadratic Slice Reallocation (`O(N^2)` Memory Blowup):** `WriteAt` allocated exact-sized slices (`len == cap`) on every 32 KiB WAL append, allocating 4.2 GB across 512 appends on the 2 MiB benchmark until fixed with geometric capacity doubling.

## 6. One-Command Verification & Reproduction

```powershell
# 1. Static determinism linter (forbids raw goroutines, wall clock, global rand, and map iteration)
go run ./cmd/detlint statemachine raft lsm kv

# 2. Full verification suite (S0 + S1 + S2 + S2b + S3 + S4 + all 17 Bug Zoo entries)
go test -p 1 -v ./... -count=1

# 3. Replay Bug Zoo #13, shrink 20-fault schedule -> 2 minimal faults, and export HTML trace viewer
go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 -html docs/trace_zoo13.html

# 4. Run 5-repetition benchmark suite and export bench/results/latest_benchmarks.{json,csv}
go run ./cmd/benchrun -runs 5 -out bench/results
```
