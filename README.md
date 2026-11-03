# AetherKV: Simulation-First Replicated Key-Value Store (Raft + LSM + WiscKey)

AetherKV is a linearizable, crash-safe replicated key-value storage engine written in Go with zero external runtime dependencies. Every consensus transition, storage compaction, disk crash, and network fault can be replayed deterministically from a single 64-bit seed.

Rather than competing with production engines like etcd or Pebble on feature count, AetherKV treats the database as a subject for rigorous systems verification. Its primary engineering artifacts are a deterministic simulation testing (DST) kernel, a sector-tearing virtual filesystem, a Porcupine linearizability checker with multi-key range-scan verification, a 17-entry reproducible Bug Zoo, and a WiscKey value-separation engine benchmarked on Llama-3-8B KV-cache tensor blocks.

## System Architecture

```text
+-----------------------------------------------------------------------+
| Clients: Deterministic Workload Generators / Length-Prefixed TCP      |
+-----------------------------------+-----------------------------------+
                                    | Put / Get / Delete / CAS / Scan
                                    v
+-----------------------------------------------------------------------+
| kv: Client Sessions (Dedup), ReadIndex, Bounded Leases, Apply Loop    |
+-----------------+-----------------------------------+-----------------+
                  | Propose / Ready                   | StateMachine Seam
                  v                                   v
+-----------------------------------+   +-------------------------------+
| raft: Pure Zero-I/O State Machine |   | statemachine: MemStore Oracle |
| + Segmented CRC32C Raft WAL       |   | or LSMStore (32KiB WAL, SSTs, |
+-----------------+-----------------+   | Leveled Compaction, WiscKey)  |
                  |                     +-------------+-----------------+
                  +-----------------+-----------------+
                                    |
        +---------------------------v---------------------------+
        | Seam: Clock | Transport | FS | Rand | Scheduler       |
        +-------------+---------------------------+-------------+
                      |                           |
                      v                           v
        +---------------------------+ +-------------------------+
        | Production: OSFS & TCP    | | Simulation: Seeded      |
        | Sockets (127.0.0.1)       | | Chaos, SimFS & Checkers |
        +---------------------------+ +-------------------------+
```

Every package above the host-interface seam (`raft`, `lsm`, `kv`, `statemachine`) is a pure state machine with zero direct access to the operating system. All external side effects pass through five explicit interfaces defined in **sim/interfaces.go** and **vfs/vfs.go**:

- **Clock (`sim.Clock`):** Provides monotonic virtual time (`Now()`) with configurable per-node static offset and linear clock drift in parts-per-million (PPM).
- **Randomness (`sim.Rand`):** Provides isolated `xoshiro256**` pseudo-random streams seeded via SHA-256 domain separation (`SHA-256(masterSeed, streamName)`).
- **Scheduler (`sim.Scheduler`):** Replaces background goroutines with deterministic task queues ordered by virtual timestamp and sequence number.
- **Network Transport (`sim.Transport`):** Routes RPC messages through either the fault-injecting `SimNet` matrix or real length-prefixed TCP sockets.
- **Filesystem (`vfs.FS` and `vfs.File`):** Abstracts file and directory operations across real disk I/O (`OSFS`) and our crashable in-memory filesystem (`SimFS`).

## Mechanical Determinism Enforcement

Determinism in Go is easily broken by accidental runtime non-determinism. AetherKV enforces deterministic execution at compile/lint time via a custom static AST and `go/types` analyzer located in **internal/detlint/analyzer.go** and **cmd/detlint/main.go**.

- **No Raw Goroutines:** Forbids `go func()` statements inside core packages so the Go runtime scheduler cannot interleave state transitions non-deterministically.
- **No Wall-Clock Reads:** Forbids calls to `time.Now`, `time.Since`, `time.After`, and `time.Sleep`, requiring all timeouts to use `sim.Clock`.
- **No Global PRNGs:** Forbids importing `math/rand`, `math/rand/v2`, or `crypto/rand`, ensuring all randomness flows through isolated `sim.Rand` streams.
- **No Unordered Map Iteration:** Forbids `for k, v := range mapVar` loops unless keys are first extracted and sorted via `slices.Sort`.

## Core Subsystems Deep Dive

### 1. Deterministic Simulation Kernel (`sim`)

- **Single-Threaded Event Loop:** Executes all timers, network deliveries, storage callbacks, and fault injections on a single OS thread using a min-heap priority queue ordered by `(timeNs, seq)`.
- **Stream-Isolated PRNG (`StreamRand`):** Assigns independent PRNG streams to the network, nemesis, workload generator, and individual nodes so mutating fault rates never alters client workloads.
- **Rolling Trace Hash Verification:** Logs every state transition into a structured JSON-lines trace and updates a rolling SHA-256 chain hash to prove bit-for-bit reproducibility across runs.
- **Network Nemesis & Fault Shrinker:** Injects symmetric/asymmetric partitions, packet drops, reordering, and node pauses, then uses delta-debugging (`ShrinkFaultSchedule`) to find minimal failure schedules.

### 2. Crashable Sector-Tearing Filesystem (`vfs`)

- **Dual-Buffer Persistence Model:** Separates every open file into a `volatileData` page-cache buffer and a `durableData` disk buffer that only advances on explicit `Sync()` calls.
- **Directory Metadata Durability:** Tracks `durableMeta` and `overwritten` pointers per directory entry so newly created or renamed files vanish upon crash unless `SyncDir()` was called on the parent directory.
- **Sector-Granular Torn Writes:** Splits unsynced byte ranges across 512-byte hardware sectors during `SimFS.Crash()`, independently choosing whether each sector is lost, persisted, or torn mid-sector with `0xAA` garbage bytes.

### 3. Zero-I/O Raft Consensus & Segmented WAL (`raft`)

- **Pure State Machine (`raft.Node`):** Processes inputs via `Step(msg)`, `Tick()`, `Propose(data)`, and `ReadIndex(id)` without performing any disk or network I/O, returning a `raft.Ready` batch of effects.
- **Linearizable Reads & Bounded Leases:** Implements quorum-verified `ReadIndex` heartbeats (including the Raft Dissertation Section 6.4 current-term no-op rule) and clock-skew-aware leader leases (`leaseDuration - maxDriftBound`).
- **Segmented CRC32C WAL (`raft/wal`):** Persists Raft log entries and `HardState` into 64 MiB segments with 25-byte headers, Castagnoli CRC32C checksums, torn-tail truncation, and prefix/suffix compaction.
- **Global Six-Invariant Observer:** Inspects every node after every simulation step to verify Election Safety, Log Matching, Leader Completeness, State Machine Safety, Term Monotonicity, and Committed Durability.

### 4. Crash-Safe Leveled LSM Engine (`lsm`)

- **32 KiB Block-Fragmented WAL:** Encodes atomic `WriteBatch` mutations into fixed 32 KiB blocks using `FragFull`, `FragFirst`, `FragMiddle`, and `FragLast` headers with CRC32C verification and group commit.
- **Skiplist Memtable & MVCC Ordering:** Orders keys in memory by `(UserKey ASC, SeqNum DESC)` using deterministic skiplist tower heights so point lookups and snapshot reads resolve the newest visible version first.
- **Checksummed SSTables & Bloom Filters:** Flushes immutable Memtables into SSTables containing 4 KiB data blocks, a double-hashing Bloom filter block (10 bits/key), a binary-searchable index block, and a 40-byte magic footer (`0xAE74E255`).
- **Atomic `MANIFEST` Rotation:** Logs `VersionEdit` transitions (added/deleted files, `LastSeq`, `FlushedIndex`) to a `MANIFEST` file and switches `CURRENT` via write-temp, `Sync()`, atomic `Rename()`, and `SyncDir()`.
- **Snapshot-Pinned Leveled Compaction:** Compacts overlapping $L_0$ files into sorted, key-disjoint $L_1$ and $L_2$ levels, dropping deletion tombstones only at the bottommost level when no older snapshot sequence number is pinned.

### 5. WiscKey Value Log Separation (`lsm/vlog.go`)

- **Pointer-Based Value Separation:** Stores values at or above `ValueLogThreshold` (4 KiB) in append-only `vlog` segments while writing only a 24-byte `VLogPointer` (`SegmentID`, `Offset`, `Length`, `Checksum`) into the LSM tree.
- **Write-Ordering Crash Contract:** Enforces `ValueLog.Sync()` before committing the referencing LSM `WriteBatch` so surviving LSM pointers never reference un-synced or out-of-bounds value-log offsets after power loss.
- **Online Segment Garbage Collection:** Scans older `vlog` segments during `RunVLogGC()`, relocates values whose pointers are still live in the LSM tree into the active segment, commits updated pointers, and unlinks dead segments.

### 6. Linearizability & Range-Scan Checker (`adapters/porcupine`)

- **Point-Operation Linearizability:** Implements Wing & Gong graph search with Lowe bitmask memoization (`checkSingleKey`) and P-compositionality to verify `PUT`, `GET`, and `CAS` histories, treating client timeouts as `[invokeNs, +Inf)`.
- **Multi-Key Range-Scan Verification:** Switches to full keyspace state-transition search (`checkMultiKeyScan`) whenever `SCAN` operations are present, catching fractured cross-key range reads that per-key checkers miss.
- **Counterexample Shrinking & Causal Explanations:** Reduces failing client histories down to 1 to 3 minimal conflicting operations and generates plain-English stale-read explanations.

## Empirical Verification & Benchmark Results

### Simulator Determinism & Verification Campaigns

| Stage | Campaign                | Scale           | Result        | Speed             |
| :---- | :---------------------- | :-------------- | :------------ | :---------------- |
| S0    | Trace Hash Determinism  | 20,000 runs     | 100% match    | 8,174 virt s/s    |
| S1    | 3/4/5-Node Chaos        | 150 runs        | 0 violations  | 196 virt s/s      |
| S2    | Differential Crash Test | 5,000 ops       | 100% match    | 0.18 s total      |
| S2    | SSTable Bloom Filter    | 10,000 probes   | 0.78% FP rate | < 1 ms            |
| S3    | Full-Stack Raft+LSM     | 30 seeds        | 0 violations  | 78 ms total       |
| S3    | Real TCP 3-Node Crash   | 3 sockets       | Linearizable  | 0.52 s total      |
| S4    | Linearizability Oracle  | 1,000 histories | 100% match    | Shrunk 7 to 2 ops |

The table above summarizes the automated verification gates across Stages S0 through S4. In Stage S0, 10,000 distinct seeds are executed twice (20,000 runs total), achieving 100% agreement on rolling SHA-256 trace hashes at 8,174 virtual seconds per real second. In Stage S2, `LSMStore` is tested against `statemachine.MemStore` across 5,000 randomized operations and 20 power-loss crashes with 0% false negatives on Bloom filter probes.

### Reproducible 5-Run Benchmark Suite

| Workload                         | Med Ops/s | Med MiB/s | p99 (us) | Write Amp |
| :------------------------------- | --------: | --------: | -------: | --------: |
| YCSB-A (50% Put / 50% Get)       |    59,803 |      14.6 |    577.2 |     3.47x |
| YCSB-B (5% Put / 95% Get)        |   154,565 |      37.7 |    < 1.0 |     3.45x |
| YCSB-E (5% Put / 95% Scan)       |    11,042 |       2.7 |  2,827.7 |     3.49x |
| 3-Node Cluster (Write/ReadIndex) |     6,963 |         - |  4,165.5 |     1.00x |
| Option A (Dual Fsync)            |    21,658 |         - |  1,784.2 |     1.00x |
| Option B (Single Fsync)          |    28,481 |         - |  1,497.9 |     1.00x |
| 128 KiB Inline LSM               |       651 |      81.3 |  8,684.0 |     4.50x |
| 128 KiB WiscKey Separated        |     2,260 |     282.5 |  2,259.2 |     1.00x |
| 2 MiB Inline LSM                 |        67 |     134.8 | 35,957.6 |     3.67x |
| 2 MiB WiscKey Separated          |       293 |     586.9 |  8,090.9 |     1.00x |

All benchmarks above were executed via `cmd/benchrun` across 5 repetitions per workload on `windows/amd64` with `fsync` enabled. Option B durability coupling (skipping LSM WAL `fsync`s and gating Raft log prefix truncation on `FlushedIndex`) reduces total `fsync` calls across 200 writes from 401 to 201 (49.9% fewer `fsync`s), boosting write throughput by 31.5% and cutting p99 latency by 16.0%.

### Granular Llama-3-8B KV-Cache Block Size Sweep

| Block Size              | Inline WA | WiscKey WA | Inline MiB/s | WiscKey MiB/s |
| :---------------------- | --------: | ---------: | -----------: | ------------: |
| 1 KiB (Below Threshold) |     2.96x |      2.96x |         74.7 |          74.6 |
| 16 KiB Chunk            |     4.82x |      1.01x |         23.8 |          57.1 |
| 128 KiB (1-Token Block) |     4.80x |      1.00x |         66.2 |         101.7 |
| 512 KiB (4-Token Block) |     4.25x |      1.00x |        109.2 |       1,264.5 |
| 2 MiB (16-Token Block)  |     3.50x |      1.00x |        171.2 |       1,046.1 |

Block sizes are derived directly from Llama-3-8B KV-cache tensor geometry: 2 (K and V) x 32 layers x 8 KV heads x 128 head dimension x 2 bytes (FP16) equals 131,072 bytes (128 KiB) per token, or 2 MiB for a 16-token PagedAttention block. Separating values >= 4 KiB into the WiscKey `ValueLog` eliminates SSTable compaction copying, dropping Write Amplification to 1.00x and increasing single-run throughput by up to 11.6x on 512 KiB blocks and 6.1x on 2 MiB blocks.

## The 17-Entry Reproducible Bug Zoo

| Bug ID | Subsystem    | Planted Bug                | Detector            | Seed / Shrink    |
| :----- | :----------- | :------------------------- | :------------------ | :--------------- |
| #1     | Raft Core    | Fig. 8 earlier-term commit | StateMachineSafety  | Seed 1           |
| #2     | Raft + VFS   | Vote reply before persist  | TermMonotonicity    | Seed 1           |
| #3     | Raft + WAL   | Ack Append before fsync    | CommittedDurability | Seed 1           |
| #4     | Raft Core    | Stale Append log truncate  | CommittedDurability | Seed 4           |
| #5     | KV + Raft    | Blind leader read          | Porcupine           | Seed 5 (2 ops)   |
| #6     | KV + Clock   | Lease read under skew      | Porcupine           | Seed 606 (2 ops) |
| #7     | KV Session   | No client session dedup    | Porcupine           | Seed 707 (2 ops) |
| #8     | LSM WAL      | Accept torn WAL tail       | CrashSyncOracle     | Seed 8           |
| #9     | LSM + VFS    | Missing MANIFEST SyncDir   | SimFSCrashOracle    | Seed 9           |
| #10    | LSM Compact  | Early tombstone drop       | ModelBasedOracle    | Seed 10          |
| #11    | Raft + LSM   | Truncate past FlushedIndex | CrashRecoveryOracle | Seed 1111        |
| #12    | KV Snapshot  | Snapshot omits dedup table | Porcupine           | Seed 1212 (1 op) |
| #13    | Raft Core    | Even-cluster quorum bug    | CommittedDurability | Seed 4 (20 to 2) |
| #14    | KV + LSM     | Split data & appliedIndex  | CrashEnumeration    | Seed 1           |
| #15    | WiscKey VLog | Pointer synced before VLog | CrashSyncOracle     | Seed 1           |
| #16    | WiscKey VLog | VLog GC drops live value   | ModelBasedOracle    | Seed 16          |
| #17    | WiscKey VLog | Accept torn VLog tail      | CrashSyncOracle     | Seed 17          |

Every planted bug in the table above can be toggled via `raft.BugConfig`, `lsm.LSMBugConfig`, or `kv.KVBugConfig` to validate that our invariant observers and linearizability checkers catch consensus and storage regressions within 1 to 4 seeds. On Bug Zoo #13 (even-cluster split-brain quorum), the fault-schedule shrinker automatically reduces a 20-fault nemesis schedule down to the 2 minimal faults required to trigger the violation.

## Seven Real, Unplanted Bugs Caught by the Simulator

While the 17 Bug Zoo entries measure detector sensitivity, the simulator and crash models also uncovered 7 genuine, unplanted implementation bugs during development:

1. **Raft Section 6.4 `ReadIndex` Uncommitted Prefix Bug (Seed 10):** A newly elected leader with uncommitted earlier-term entries served `ReadIndex` at `commitIdx = 0` before committing a current-term no-op entry, caught by Porcupine.
2. **Unverified `commitIdx` Advance on `MsgHeartbeat` (Seed 12):** `handleHeartbeat` advanced a follower's `commitIdx` without `prevLogIndex`/`prevLogTerm` checks, causing a deposed Term 1 leader to commit an overwritten entry, caught by `StateMachineSafety`.
3. **Torn WAL Header Unbounded Allocation OOM:** When `SimFS` tore a 512B sector in `SegmentedWAL` with `0xAA` bytes, the corrupted length field (`0xAAAAAAAA` = 2.86 GB) was passed to `make([]byte, length)` before checking file bounds.
4. **Zombie LSM WAL Replay After Unsynced Truncation (Step 500 Crash):** Resetting `active.wal` after `Flush()` without calling `Sync()` allowed pre-flush WAL sectors (`seq = 499`) to survive a crash and shadow newer $L_0$ SSTable entries (`seq = 500`).
5. **`SimFS` Sub-Sector Tear Bleed:** Sector-granular crash tearing initially overwrote `secOff` rather than clamping to the unsynced write sub-range `[max(secOff, w.off), min(secEnd, w.off+w.len))`, corrupting previously synced records in the same sector.
6. **`SegmentedWAL.TruncateSuffix` Resurrection Bug:** Suffix truncation removed entries from the in-memory index map without truncating the underlying `SimFS` segment file, causing overwritten uncommitted entries to resurrect on restart.
7. **`SimFS.WriteAt` Quadratic Slice Reallocation:** Allocating exact-sized slices (`len == cap`) on every 32 KiB WAL append caused 4.2 GB of allocations across 512 appends on the 2 MiB benchmark until fixed with geometric capacity doubling.

## Repository Layout

```text
aetherkv/
├── go.mod                                  # Module [github.com/cutaret/aetherkv](https://github.com/cutaret/aetherkv) (zero external deps)
├── README.md                               # Project architecture, verification record & quickstart
├── cmd/
│   ├── detlint/main.go                     # Static AST/type determinism linter CLI
│   ├── aether-check/main.go                # Standalone JSONL linearizability & range-scan checker CLI
│   ├── simrun/main.go                      # Deterministic seed replay, shrinker & HTML trace CLI
│   └── benchrun/main.go                    # 5-run reproducible benchmark runner (JSON & CSV export)
├── internal/
│   └── detlint/analyzer.go                 # AST + go/types analyzer forbidding goroutines/time/rand/maps
├── sim/
│   ├── interfaces.go                       # Clock, Rand, Scheduler, Transport & Message contracts
│   ├── prng.go                             # StreamRand (xoshiro256** seeded via SHA-256)
│   ├── clock.go                            # BaseClock & NodeClock (static offset + PPM linear drift)
│   ├── trace.go                            # Structured JSONL trace logger & rolling SHA-256 chain hash
│   ├── kernel.go                           # Single-threaded min-heap discrete-event simulation kernel
│   ├── net.go                              # SimNet fault matrix, Nemesis generator & fault shrinker
│   ├── htmlviewer.go                       # Zero-dependency dark-mode HTML trace timeline generator
│   └── sanity_test.go                      # Stage S0 10,000-seed x 2 trace-hash determinism suite
├── vfs/
│   ├── vfs.go                              # FS & File interfaces + production OSFS implementation
│   ├── simfs.go                            # Crashable SimFS (volatile/durable buffers & 512B tearing)
│   └── simfs_test.go                       # Unsynced metadata rollback & sector-tearing unit tests
├── statemachine/
│   ├── statemachine.go                     # StateMachine interface (ApplyBatch, Get, Scan, Snapshot)
│   └── memstore.go                         # Reference in-memory sorted store + session deduplication
├── raft/
│   ├── types.go                            # Role, MessageType, Entry, HardState, Ready & BugConfig
│   ├── node.go                             # Pure zero-I/O Raft state machine + ReadIndex + snapshots
│   ├── observer.go                         # Step-by-step global checker for all 6 Raft invariants
│   └── wal/
│       ├── wal.go                          # Segmented CRC32C Raft WAL with torn-tail recovery
│       └── wal_test.go                     # Append, recovery & torn-sector truncation unit tests
├── lsm/
│   ├── format.go                           # InternalKey, WriteBatch, VLogPointer & MergingIterator
│   ├── wal.go                              # 32 KiB block-fragmented LSM WAL + group commit
│   ├── memtable.go                         # Deterministic skiplist ordered by (UserKey ASC, Seq DESC)
│   ├── bloom.go                            # Double-hashing Bloom filter builder and probe
│   ├── sstable.go                          # 4 KiB block SSTable writer/reader + per-block CRC32C
│   ├── manifest.go                         # VersionEdit log + atomic CURRENT rename + SyncDir
│   ├── vlog.go                             # WiscKey append-only ValueLog + online segment GC
│   ├── db.go                               # Crash-safe LSMStore, leveled compaction & Option A/B
│   ├── s2_part1_test.go                    # WAL, Skiplist, Bloom, SSTable & Bug Zoo #8 tests
│   ├── s2_part2_test.go                    # Bug Zoo #9, #10, #14 + 5,000-op differential crash test
│   └── s2b_vlog_test.go                    # Bug Zoo #15-#17 + Llama-3-8B KV-cache benchmark sweep
├── kv/
│   └── service.go                          # Replicated KV service binding Raft, LSMStore & leases
├── transport/
│   └── tcp/tcp.go                          # Real length-prefixed TCP socket transport (127.0.0.1)
├── adapters/
│   ├── porcupine/
│   │   ├── checker.go                      # Wing & Gong + Lowe bitmask checker & SCAN verifier
│   │   └── s4_checker_test.go              # 1,000-history differential test & shrinker suite
│   └── etcdcontrol/adapter.go              # Clean control cluster configuration
├── zoo/
│   ├── harness.go                          # Multi-node cluster simulation runner & workload driver
│   ├── zoo_s1_test.go                      # 150-run control campaign + Bug Zoo #1-#7 & #13
│   └── s3_integration_test.go              # Full-stack chaos, Bug Zoo #11-#12, Option A/B & TCP tests
├── bench/
│   └── results/                            # Raw JSON and CSV benchmark artifacts from benchrun
└── docs/
    ├── METRICS.md                          # Complete empirical telemetry & verification ledger
    ├── trace_zoo13.html                    # Generated interactive HTML trace viewer for Bug Zoo #13
    ├── bug-zoo/CATALOG.md                  # Detailed postmortem catalog for all 17 Bug Zoo entries
    └── adr/                                # Architecture Decision Records (ADR 001 - ADR 007)
```

## CLI Toolchain & One-Command Reproduction

Run all commands below from your repository root in PowerShell or any POSIX shell:

```powershell
# 1. Run the static determinism linter across all core state-machine packages
go run ./cmd/detlint statemachine raft lsm kv

# 2. Run the complete verification test suite sequentially (Stages S0 through S4 + all 17 Bug Zoo tests)
go test -p 1 -v ./... -count=1

# 3. Replay Bug Zoo #13 (4-node split-brain), shrink the 20-fault schedule to 2 faults, and export HTML trace
go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 -html docs/trace_zoo13.html

# 4. Run the standalone linearizability checker CLI on a recorded JSONL client history
go run ./cmd/aether-check -in path/to/history.jsonl

# 5. Reproduce the 5-run benchmark suite and export bench/results/latest_benchmarks.{json,csv}
go run ./cmd/benchrun -runs 5 -out bench/results
```

## Scope Boundaries & Honest Engineering Limits

- **Explicit Non-Goals:** AetherKV intentionally omits multi-Raft range sharding, distributed cross-shard transactions (2PC/Percolator), SQL query parsing, TLS/RBAC authentication, block compression, and non-leveled compaction strategies.
- **Simulation vs. Physical Hardware:** `SimFS` models 512-byte sector tearing and unsynced directory entry rollback based on POSIX/NVMe persistence contracts, while real-mode TCP tests verify OS socket framing; neither replaces physical power-cut testing on raw NVMe controller firmware.
