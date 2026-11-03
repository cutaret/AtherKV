# AetherKV: End-to-End Implementation Plan & Architecture Specification

> **One-Line Pitch:** A simulation-first replicated key-value store ($\text{Raft} + \text{LSM} + \text{WiscKey}$) in Go in which every failure it has ever experienced can be replayed deterministically from a single `(seed, config)` pair.

---

## 1. Thesis, Verifiable Goals & Locked Architectural Decisions

### 1.1 Core Thesis
Nobody replaces `etcd` or `RocksDB` with a solo from-scratch storage engine, so the store itself is the **test subject**, not the commercial product. The primary engineering artifacts are the verification and benchmarking systems surrounding it:
1. A single-threaded deterministic simulation kernel (`sim`) and crashable virtual filesystem (`SimFS`) that uncover real consensus and storage bugs.
2. A linearizability and invariant verification pipeline (Porcupine + `aether-check` delta-debugging shrinker) that reduces multi-thousand-event chaos runs into minimal, human-readable counterexamples.
3. A 17-entry **Bug Zoo** (mutation-testing suite) proving detector sensitivity across consensus, storage, and client semantics.
4. Reproducible benchmarks comparing single-node LSM and WiscKey value separation against `Pebble` and `Badger`, and 3-node cluster performance against `etcd`.

### 1.2 Verifiable Goals (G1–G6)
Every claim in the repository must be verifiable by anyone who clones it:
* **G1 (Deterministic Replay):** Any failing simulated run replays identically from `(seed, config)` with an exact match on its rolling SHA-256 event-trace hash.
* **G2 (Consensus Safety Under Chaos):** All Raft safety invariants hold across millions of simulated virtual seconds under network partitions, packet loss, duplication, reordering, node pauses, crashes, and clock skew.
* **G3 (Linearizability & Shrinking):** Client operation histories are verified for linearizability, and violations shrink automatically to minimal counterexamples.
* **G4 (Crash & Torn-Write Recovery):** Both the segmented Raft WAL and the LSM engine (including the WiscKey value log) recover cleanly from crashes at every `fsync` boundary and under 512-byte sector-granular torn writes.
* **G5 (Mutation-Tested Bug Zoo):** Every planted bug in the 17-entry Bug Zoo is caught by a named invariant or oracle detector, with its exact `seeds-to-detection` recorded.
* **G6 (One-Command Benchmarks):** One command reproduces every throughput, tail-latency ($p_{50}, p_{99}, p_{99.9}$), and write-amplification benchmark.

### 1.3 Explicit Non-Goals
Production deployment, multi-group sharding, distributed cross-key transactions (unless Stretch Stage is reached), SQL query parsing, TLS/authentication, block compression, and compaction strategies beyond leveled compaction.

### 1.4 Locked Architectural Decisions
1. **Language:** **Go (1.25+)** with strict static analysis (`cmd/detlint`) and CI run-twice trace-hash gates to eliminate Go nondeterminism leaks (`go` statements, `time.Now`, global `math/rand`, and randomized map iteration).
2. **Dedicated Raft Log Store:** The Raft log uses its own **segmented, checksummed append-only WAL**, separate from the LSM engine. Raft log workloads (append, suffix truncation on leader change, prefix deletion on snapshot) fit LSM trees poorly, and building the Raft WAL in Stage 1 tests `SimFS` crash semantics before the LSM exists.
3. **StateMachine Seam First:** The `kv` layer interacts with a `StateMachine` interface backed first by `MemStore` (an in-memory deterministic B-tree/sorted map built in Stage 0) and later by `LSMStore` (Stage 2/3). `MemStore` enables full testing of Raft, KV sessions, `ReadIndex`, Porcupine, and 8 Bug Zoo entries by Week 8, and subsequently serves as the reference model oracle for differential testing of the LSM engine.
4. **Raft-to-LSM Durability Coupling (Option A $\rightarrow$ Option B):**
   * **Option A (Default in S3):** The LSM syncs its own WAL on every applied batch, writing `appliedIndex` atomically inside the same write batch as the KV mutations. Cost: $2 \times \text{fsync}$ per write (Raft WAL + LSM WAL), yielding trivial idempotent recovery.
   * **Option B (Optimization & Bug Zoo #11):** Skip the LSM WAL `fsync` and rely on the Raft WAL for durability, truncating the Raft log only up to the LSM's durable `flushedIndex`. Truncating past `flushedIndex` is planted as **Bug Zoo #11**, and the performance delta between Option A and Option B is benchmarked.
5. **Linearizability Oracle Strategy:** Wire **Porcupine** + a fault-schedule shrinker in Stage 1 so consensus and KV bugs are caught immediately. Build the standalone `aether-check` counterexample explainer and HTML Trace Viewer in Stage 4 *after* the LSM and WiscKey engines are complete.
6. **Range Scan Verification:** Verify `Scan` operations against Porcupine on small, bounded keyspaces ($5\text{--}10$ keys) because multi-key scans cross key partitions, defeating per-key P-compositionality and making general linearizability checking NP-complete.

---

## 2. Monorepo Layout & Determinism Enforcement

### 2.1 Repository Structure
```text
aetherkv/
├── sim/             # Virtual clock, single-threaded event loop, PRNG streams, SimNet, Nemesis, shrinker
├── vfs/             # FS & File interfaces, OS production adapter, crashable SimFS
├── raft/            # Pure zero-I/O Raft state machine, segmented Raft WAL, global invariant observer
├── statemachine/    # StateMachine interface + in-memory sorted-map reference oracle (MemStore)
├── lsm/             # WAL, skiplist memtable, SSTable, Bloom filter, MANIFEST, leveled compaction, WiscKey vlog
├── kv/              # Client sessions, retry dedup, ReadIndex, opt-in lease reads, apply loop, snapshots, TCP
├── adapters/        # etcd/raft control harness adapter, Porcupine history exporter & KV models
├── zoo/             # 17 build-tag bug switches (-tags=zoo_01_fig8, etc.) and automated detection tests
├── cmd/
│   ├── aetherkv/    # Production TCP server binary
│   ├── aetherctl/   # Client CLI & workload runner
│   ├── simrun/      # Deterministic seed runner, schedule shrinker, and trace verifier
│   ├── benchrun/    # Automated benchmark suite (AetherKV vs. etcd, Pebble, Badger)
│   └── detlint/     # Custom go/analysis AST linter enforcing the determinism boundary
├── bench/           # YCSB & Llama-3-8B KV-cache benchmark scripts, configs, raw CSV/JSON results
├── docs/
│   ├── DESIGN.md    # Architecture overview
│   ├── METRICS.md   # Live ledger of unplanted bugs caught, seeds-to-detection, and benchmark numbers
│   ├── adr/         # Architecture Decision Records (ADR-001 through ADR-008)
│   └── bug-zoo/     # 1-page postmortems for each of the 17 Bug Zoo entries
└── aether-check/    # Separate Go module: counterexample shrinker, linearizability explainer & HTML trace viewer
```

### 2.2 Mechanical Determinism Rules
Everything above the host seam (`Clock`, `Transport`, `FS`, `Rand`, `Scheduler`) must be a pure function of its inputs:
1. **No Raw Goroutines in Core Packages:** Background work (memtable flush, leveled compaction, value-log GC) is submitted as discrete tasks to `Scheduler`. Production mode runs tasks on goroutines; simulation mode pulls tasks from the single-threaded priority event queue.
2. **Static AST Linter (`cmd/detlint`):** Runs on every `git push` and fails if packages `raft`, `lsm`, `kv`, or `statemachine` contain:
   * Any `go` statement (`*ast.GoStmt`).
   * Any reference to `time.Now`, `time.Since`, `time.After`, or `time.Sleep`.
   * Any import of `math/rand`, `math/rand/v2`, or `crypto/rand` (all randomness must use injected `sim.Rand`).
   * Any `for k, v := range m` over a Go `map` without going through a deterministic sorted-key helper.
3. **CI Run-Twice Meta-Test:** Every CI run executes 10,000 seeds twice and verifies that `traceHash(run1) == traceHash(run2)`.

---

## 3. Component Specifications & Go Contracts

### 3.1 Simulation Kernel (`sim`)
```go
package sim

type Clock interface {
    Now() uint64 // Virtual nanoseconds since epoch
}

type Rand interface {
    Intn(n int) int
    Uint64() uint64
    Float64() float64
}

type Scheduler interface {
    ScheduleAfter(delayNs uint64, taskName string, fn func())
}
```
* **Event Queue:** Min-heap priority queue ordered by `(timestampNs uint64, seqID uint64)`. Handlers execute single-threaded to completion.
* **Per-Node Clock Skew & Drift:** Each node receives a virtual `Clock` parameterized by `(offsetNs int64, driftPpm int64)` so node time is computed as:
  $$T_{\text{node}}(t) = t + \text{offsetNs} + \left\lfloor \frac{t \times \text{driftPpm}}{10^6} \right\rfloor$$
  This deterministically triggers clock-skew bugs in lease reads (Bug Zoo #6).
* **Isolated PRNG Streams (Crucial for Shrinking):** Derived via `SHA256(masterSeed, streamName)`:
  * `net:<src>-><dst>` (one stream per directed network link)
  * `disk:<nodeID>` (one stream per node's `SimFS`)
  * `nemesis` (fault schedule generation)
  * `workload:<clientID>` (client operation generation)
  * *Why this matters:* When the delta-debugging shrinker deletes a fault from the `nemesis` schedule, all network, disk, and workload random draws remain unchanged, preserving the failure path.
* **Phased `Nemesis`:** Generates fault schedules in 4 phases:
  1. **Warm-up ($0\text{--}10\text{ s}$):** Normal cluster boot and initial leader election.
  2. **Chaos ($10\text{--}120\text{ s}$):** Injects asymmetric/symmetric network partitions, packet drop/duplication/reordering, node pauses (`SIGSTOP` emulation), process crashes with volatile memory loss, and disk sector tears.
  3. **Heal ($120\text{--}150\text{ s}$):** Restores network links, restarts crashed nodes, zeroes clock drift, and verifies cluster convergence and liveness within a bounded virtual time window.
  4. **Verify:** Asserts all safety invariants, compares storage state against `MemStore`, and runs Porcupine over recorded client histories.

### 3.2 Virtual File System (`vfs` & `SimFS`)
```go
package vfs

type FS interface {
    Open(name string) (File, error)
    Create(name string) (File, error)
    Rename(oldpath, newpath string) error
    Remove(name string) error
    List(dir string) ([]string, error)
    SyncDir(dir string) error
}

type File interface {
    ReadAt(p []byte, off int64) (n int, err error)
    WriteAt(p []byte, off int64) (n int, err error)
    Truncate(size int64) error
    Sync() error
    Close() error
}
```
* **Volatile vs. Durable State:** Each simulated file maintains `durable []byte` and a list of uncommitted `pendingWrites` since the last `Sync()`.
* **Crash & 512-Byte Sector Torn-Write Model:** On simulated crash:
  * Every `Sync()`ed write is guaranteed intact.
  * Every unsynced `WriteAt` is sliced into $512\text{-byte}$ sectors. Using `disk:<nodeID>` PRNG, each unsynced sector independently survives, drops, or tears mid-record.
  * Unsynced `Create`, `Rename`, and `Remove` calls revert on crash unless `SyncDir(parentDir)` was called after the metadata operation.
* **Bit-Flip & Sync-Error Injection:** Configurable probability of flipping a bit on `ReadAt` (exercising CRC32C verification) and returning `ErrIO` on `Sync()` (requiring the node to halt immediately rather than corrupting state via blind retries).

### 3.3 Pure Raft Core (`raft`) & Segmented Raft WAL
```go
package raft

type Node interface {
    Step(ev Event) Ready
}

type Ready struct {
    Persist    *PersistReq // HardState (Term, Vote) + Entries to append/truncate
    Send       []Message   // Outbound RPCs; host driver MUST Sync(Persist) before sending
    Committed  []Entry     // Ordered entries ready for StateMachine application
    ReadStates []ReadState // Confirmed ReadIndex barriers safe to serve
}
```
* **Protocol Scope:** Leader election, log replication with `ConflictTerm` / `ConflictIndex` fast backtracking, current-term-only replica counting for commitment (Raft Fig. 8 rule), `ReadIndex` quorum heartbeats, `PreVote`, `CheckQuorum`, and `InstallSnapshot`.
* **Segmented Raft WAL:** Append-only segments (`000001.wal`, `000002.wal`) with $[ \text{crc32c (4B)} \mid \text{len (4B)} \mid \text{type (1B)} \mid \text{payload} ]$ framing, supporting suffix truncation on term conflict and prefix segment unlinking after snapshot creation.
* **Global Step-by-Step Invariant Observer:** Runs after every `Step()` in simulation and checks:
  1. `ElectionSafety`: $\le 1$ leader per term across the cluster.
  2. `LeaderAppendOnly`: A leader never overwrites or truncates existing entries in its own log.
  3. `LogMatching`: If two logs contain an entry with the same `(index, term)`, they are byte-identical up to `index`.
  4. `LeaderCompleteness`: Once an entry is committed in term $T$, every leader of term $T' > T$ contains that entry.
  5. `StateMachineSafety`: No two nodes ever apply different commands at the same `logIndex`.
  6. `CommittedDurability`: No entry acknowledged as committed to a client or applied to a state machine is ever lost across any sequence of crashes and restarts.

### 3.4 `StateMachine` Seam (`statemachine`) & KV Service (`kv`)
```go
package statemachine

type StateMachine interface {
    ApplyBatch(appliedIndex uint64, ops []Op) []Result
    Get(key []byte) (val []byte, ok bool, err error)
    Scan(startKey, endKey []byte, limit int) ([]KV, error)
    AppliedIndex() uint64
    Snapshot() ([]byte, error)
    Restore(snapshot []byte) error
}
```
* **Operations:** `Get(key)`, `Put(key, val)`, `Delete(key)`, `CAS(key, expected, desired)`, `Scan(startKey, endKey, limit)`.
* **Session Deduplication:** Every client mutation includes `(ClientID uint64, SeqNum uint64)`. The `StateMachine` stores a map of `ClientID -> (LastSeqNum, LastResult)` inside the state machine itself (persisted atomically in LSM write batches and included in every Raft snapshot) so client retries never apply twice.
* **Read Paths:**
  * `ReadIndex` (Default): Leader records `commitIndex`, confirms leadership with a quorum heartbeat round, waits until `appliedIndex >= readIndex`, and reads from the local `StateMachine`.
  * `LeaseRead` (Opt-in): Serves local reads while `clock.Now() < leaseStart + electionTimeout - maxClockDrift`.

### 3.5 Crash-Safe LSM Engine (`lsm`) & WiscKey Value Separation (`vlog`)
* **LSM WAL:** 32 KiB block-aligned records (`FULL`, `FIRST`, `MIDDLE`, `LAST` fragments) with CRC32C checksums and group commit. Recovery stops and truncates at the first torn or corrupt tail record.
* **Memtable:** Deterministic skiplist ordered by `(userKey ASC, seqNum DESC)`. Write batches insert all mutations plus the internal `_meta/applied_index` key atomically.
* **SSTables & `MANIFEST`:**
  * SSTable layout: `[Data Blocks (4 KiB)] -> [Bloom Filter Block (10 bits/key)] -> [Index Block] -> [Footer (48B with magic + offsets + CRC32C)]`.
  * `MANIFEST` records atomic `VersionEdit` entries (added/deleted SSTable numbers per level, `logNumber`, `lastSequence`). `CURRENT` points to the active `MANIFEST` and is updated via write-temp $\rightarrow$ `Sync()` $\rightarrow$ `Rename()` $\rightarrow$ `SyncDir()`.
* **Leveled Compaction:** $L_0$ allows overlapping key ranges (triggers stall/compaction at 4 files); $L_1\text{--}L_3$ maintain sorted, disjoint key ranges ($10\times$ capacity multiplier per level). Tombstones are dropped *only* at the bottommost level and *only* when `seqNum < oldestPinnedSnapshotSeq`.
* **WiscKey Value Log (`vlog`) & KV-Cache Block Workload:**
  * Values $> 4\text{ KiB}$ are written to append-only `vlog` segments (`vlog_00001.dat`), while the LSM stores a 24-byte pointer: `[segmentID (4B) | offset (8B) | length (8B) | valueCRC32C (4B)]`.
  * **Strict Durability Ordering:** `vlog.Sync()` *must* complete before the LSM WAL batch containing the pointer is synced (violating this triggers **Bug Zoo #15**).
  * **LLM KV-Cache Block Sizing:** Modeled on a Llama-3-8B architecture ($32\text{ layers}$, $8\text{ KV heads}$, $128\text{ head dim}$, FP16/BF16 $2\text{ bytes}$):
    $$\text{Bytes per Token} = 2 \times 32 \times 8 \times 128 \times 2 = 131,072\text{ bytes } (128\text{ KiB})$$
    A 16-token PagedAttention block equals $16 \times 128\text{ KiB} = 2\text{ MiB}$. The single-node LSM benchmark evaluates inline vs. value-separated storage across payload sizes from $1\text{ KiB}$ to $2\text{ MiB}$.

---

## 4. The 17-Entry Bug Zoo Matrix (Mutation Testing Suite)

Every entry is toggled via a Go build tag (`-tags=zoo_XX_...`). Running `make zoo` executes all 17 campaigns and verifies that each bug is caught within its target seed budget, shrunk, and logged.

| # | Build Tag | Layer | Planted Bug Mechanic | Firing Detector / Oracle | Target Cluster / Workload Config |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **01** | `zoo_01_fig8` | Raft | Commit earlier-term entries by counting replicas instead of requiring a current-term entry (Raft Fig. 8) | `StateMachineSafety` & `LeaderCompleteness` | 5-node cluster, asymmetric partitions + leader churn |
| **02** | `zoo_02_vote_sync` | Raft + VFS | Send `RequestVoteResp` before persisting `votedFor` to disk | `ElectionSafety` (two leaders elected in same term after crash/restart) | 3-node cluster with crash + immediate restart nemesis |
| **03** | `zoo_03_append_sync` | Raft + VFS | Reply `AppendEntriesResp(success=true)` before `fsync`ing the Raft WAL | `CommittedDurability` (committed write lost after majority crash) | 3-node cluster with correlated node crashes |
| **04** | `zoo_04_stale_trunc` | Raft | Unconditionally truncate follower log on reordered `AppendEntries` even if existing entries match | `LogMatching` & `CommittedDurability` | 3-node cluster with high packet reordering + duplication |
| **05** | `zoo_05_blind_read` | KV + Raft | Leader serves `Get`/`Scan` locally without executing `ReadIndex` quorum check | Porcupine / `aether-check` linearizability oracle (stale read) | 3-node cluster with split-brain partition isolating deposed leader |
| **06** | `zoo_06_lease_skew` | KV + Sim | Serve lease reads assuming zero clock drift while `SimClock` injects drift $>$ lease margin | Porcupine / `aether-check` linearizability oracle | 3-node cluster with `driftPpm = 5000` on old leader |
| **07** | `zoo_07_no_dedup` | KV | Bypass `(ClientID, SeqNum)` session table so retried `CAS`/`Put` executes twice | Porcupine / `aether-check` linearizability oracle | Drop outbound RPC responses to force client retries |
| **08** | `zoo_08_torn_wal` | LSM + VFS | LSM WAL recovery accepts a torn 512B sector tail record without verifying CRC32C | Crash-at-every-sync storage recovery oracle | Single-node LSM with 512B sector torn writes enabled |
| **09** | `zoo_09_manifest_dir` | LSM + VFS | `Rename` `CURRENT` to new `MANIFEST` without calling `SyncDir()` on the database directory | `SimFS` crash recovery oracle (reverts to stale `MANIFEST` on crash) | Crash injected immediately after compaction `MANIFEST` rotation |
| **10** | `zoo_10_tombstone_gc` | LSM | Drop tombstones during $L_1 \rightarrow L_2$ compaction while older versions exist in $L_3$ | Differential model test vs. `MemStore` (deleted key resurrects) | Multi-level write $\rightarrow$ flush $\rightarrow$ delete $\rightarrow$ compact workload |
| **11** | `zoo_11_premature_trunc` | Raft + LSM | Under Option B (no LSM WAL sync), truncate Raft WAL up to `appliedIndex` instead of `flushedIndex` | Cluster crash recovery oracle (unflushed memtable writes lost) | Option B durability mode + node crash prior to memtable flush |
| **12** | `zoo_12_snap_dedup` | KV + Raft | `InstallSnapshot` transfers KV pairs but omits the client session deduplication table | Porcupine / `aether-check` linearizability oracle (retry double-applies) | Aggressive log compaction (`snapshotInterval = 50`) + dropped replies |
| **13** | `zoo_13_even_quorum` | Raft | Compute quorum as `(n + 1) / 2` integer division ($2/4$ instead of $3/4$ on even clusters) | `ElectionSafety` & `StateMachineSafety` (split-brain) | **4-node and 6-node cluster configurations** |
| **14** | `zoo_14_split_apply` | KV + LSM | Write KV mutations and `appliedIndex` in two separate LSM batches instead of one atomic batch | Crash-point enumerator + `MemStore` differential oracle | Crash injected between data batch write and index batch write |
| **15** | `zoo_15_dangling_ptr` | VLog + LSM | Sync LSM WAL containing WiscKey pointer before calling `Sync()` on the Value Log segment | Crash-at-every-sync oracle (dangling pointer to un-synced `vlog`) | Large-value ($64\text{ KiB}$) writes with crash before `vlog.Sync()` |
| **16** | `zoo_16_vlog_gc_live` | VLog | Value-log GC reclaims a segment without rewriting values still referenced by live keys | Differential model test vs. `MemStore` | Interleaved large-value overwrites + aggressive `vlog` GC threshold |
| **17** | `zoo_17_torn_vlog` | VLog + VFS | Value-log reader returns a sector-torn tail value without checking the pointer's CRC32C | Crash-at-every-sync oracle | 512B torn write during $128\text{ KiB}\text{--}2\text{ MiB}$ `vlog` append |

---

## 5. End-to-End Execution Roadmap (Weeks 1–30)

> **Time Allocation Rule:** Dedicate $10\text{--}15\text{ hours/week}$ to AetherKV while keeping daily DSA/competitive programming hours fixed. Stages S0 through S2b complete the core consensus, LSM, WiscKey, and 15/17 Bug Zoo entries before peak spring interview loops.

### Stage S0: Foundations, Determinism Kernel & Reference Seam (Weeks 1–2)
* [ ] Initialize Go workspace (`go.work`) with root module `github.com/cutaret/aetherkv` and sub-module `aether-check`.
* [ ] Implement `sim.Clock` (with offset/drift), isolated SHA-256 `sim.Rand` streams, `sim.Scheduler`, and the single-threaded min-heap event loop.
* [ ] Implement structured JSON-lines trace logger with rolling SHA-256 hash calculation.
* [ ] Build `cmd/detlint` using `golang.org/x/tools/go/analysis` to ban `go` statements, `time.Now`, global `math/rand`, and unsorted map iteration in core packages.
* [ ] Implement `statemachine.StateMachine` interface and the deterministic in-memory `MemStore` (B-tree / sorted slice).
* [ ] Write `docs/adr/001-determinism-seam.md`, `002-statemachine-seam.md`, and `003-isolated-prng-streams.md`.
* **Exit Gate:** `make test-det` executes 10,000 seeds twice and asserts identical trace hashes across 100% of runs. `detlint` passes in CI.

### Stage S1: Pure Raft, `SimFS`, KV Service & Porcupine Harness (Weeks 3–8)
* [ ] **Week 3:** Build `vfs.FS`, OS adapter, and `SimFS` (volatile/durable buffers, 512B sector torn writes, `SyncDir` requirement, bit flips). Build segmented checksummed Raft WAL on `vfs.FS`.
* [ ] **Week 4:** Build pure `raft.Node` state machine (`Step(Event) Ready`): leader election, log replication, `ConflictTerm` fast backtracking, current-term commit rule, `PreVote`, and `CheckQuorum`.
* [ ] **Week 5:** Build `SimNet` (latency, drop, duplicate, reorder, asymmetric partitions, node pauses), 4-phase `Nemesis`, and the global 6-property Raft Invariant Observer.
* [ ] **Week 6:** Build `kv` service over `MemStore` (client sessions, sequence deduplication, `ReadIndex` barrier, opt-in lease reads) and wire the Porcupine history exporter + KV register/scan model.
* [ ] **Week 7:** Implement delta-debugging fault schedule shrinker in `cmd/simrun` (binary search over injected `Nemesis` events using isolated PRNG streams). Build the `etcd/raft` control adapter to validate zero false positives.
* [ ] **Week 8:** Implement and verify **Bug Zoo #1, #2, #3, #4, #5, #6, #7, and #13** (including 4- and 6-node cluster configs).
* **Exit Gate:** All 6 Raft invariants and Porcupine linearizability hold across 100,000 seeds on 3-, 4-, and 5-node clusters. `etcd/raft` control adapter passes 100,000 seeds with 0 failures. Zoo #1–7 and #13 caught, shrunk, and documented. *(Project is v1 resume-ready at this checkpoint!)*

### Stage S2: Crash-Safe LSM Storage Engine (Weeks 9–15)
* [ ] **Week 9:** Build LSM 32 KiB block WAL with CRC32C framing, group commit, and torn-tail truncation recovery on `SimFS`.
* [ ] **Week 10:** Build skiplist Memtable (`(userKey ASC, seqNum DESC)`), immutable Memtable queue, and atomic write batching (bundling `_meta/applied_index`).
* [ ] **Week 11:** Build SSTable writer/reader (4 KiB data blocks, 10 bits/key Bloom filter block, index block, 48B magic footer, per-block CRC32C).
* [ ] **Week 12:** Build two-level merging iterator (`Memtable` $\rightarrow$ $L_0$ $\rightarrow$ $L_1\text{--}L_3$) supporting point `Get`, bounded `Scan`, and snapshot sequence pinning.
* [ ] **Week 13:** Build `MANIFEST` version edit log, atomic `CURRENT` swap (`Rename` + `SyncDir`), and $L_0 \rightarrow L_1 \rightarrow L_2$ leveled compaction task on `Scheduler` with snapshot-safe tombstone collection.
* [ ] **Week 14:** Build crash-point enumerator (crashing at every `WriteAt` and `Sync` boundary) and differential model tester comparing `LSMStore` against `MemStore` across 1,000,000 random ops.
* [ ] **Week 15:** Run Go native fuzzers (`go test -fuzz`) for 1+ hour each on WAL, SSTable, and `MANIFEST` decoders. Implement and verify **Bug Zoo #8, #9, #10, and #14**.
* **Exit Gate:** 1,000,000-op model test with random crashes passes 100%. Exhaustive crash-point enumeration passes. Fuzzers run 1 hour clean. Zoo #8, #9, #10, #14 caught and documented.

### Stage S2b: WiscKey Value Separation & LLM KV-Cache Benchmark (Weeks 16–18)
* [ ] **Week 16:** Build append-only segmented Value Log (`vlog`), 24-byte LSM value pointers for payloads $> 4\text{ KiB}$, and strict `vlog.Sync()`-before-LSM-WAL-sync ordering.
* [ ] **Week 17:** Build `vlog` garbage collector (scanning live LSM pointers, rewriting live values to tail segment, unlinking dead segments) scheduled via `Scheduler`. Implement **Bug Zoo #15, #16, and #17**.
* [ ] **Week 18:** Build `cmd/benchrun` Llama-3-8B KV-cache workload ($1\text{ KiB}$ to $2\text{ MiB}$ blocks, inline LSM vs. WiscKey value separation) measuring write amplification ($\text{Bytes Written to Disk} / \text{Logical Bytes Written}$), throughput ($\text{MB/s}$), and $p_{99}$ latency.
* **Exit Gate:** Crash oracle verifies zero dangling pointers across 50,000 crash points. Zoo #15–17 caught. KV-cache value separation benchmark tables generated.

### Stage S3: Full-Stack Integration, Snapshots & Real TCP Mode (Weeks 19–22)
* [ ] **Week 19:** Swap `LSMStore` into the `kv` service. Implement both Option A ($2 \times \text{fsync}$) and Option B ($1 \times \text{fsync}$ + `flushedIndex` Raft WAL truncation).
* [ ] **Week 20:** Implement Raft log compaction and `InstallSnapshot` (streaming consistent LSM snapshot + client session deduplication table).
* [ ] **Week 21:** Implement **Bug Zoo #11 and #12** (completing all 17/17 Bug Zoo entries). Run full-stack (`Raft + LSM + VLog + Porcupine`) 100,000-seed campaign.
* [ ] **Week 22:** Build length-prefixed TCP production transport (`cmd/aetherkv`) and real-process chaos smoke test (3 local processes, `kill -9` crash loops, TCP proxy packet drops, Porcupine verification).
* **Exit Gate:** All 17/17 Bug Zoo entries pass automated detection tests. Full-stack 100,000-seed campaign passes. Real-mode `kill -9` test passes linearizability check.

### Stage S4: `aether-check` Counterexample Explainer & HTML Trace Viewer (Weeks 23–26)
* [ ] **Weeks 23–24:** Build `aether-check` CLI module: delta-debugging operation shrinker over non-linearizable histories and plain-English causal violation explainer (e.g., *"Client 2 read key 'k'=v1 at t=142ms after Client 1's write 'k'=v2 completed at t=118ms"*).
* [ ] **Weeks 25–26:** Build static single-file HTML Trace Viewer (`aether-check render trace.jsonl > trace.html`) rendering an interactive swimlane diagram of node terms, network partitions, dropped/reordered RPCs, disk syncs/crashes, and the exact invariant failure point.
* **Exit Gate:** `aether-check` agrees 100% with Porcupine across 100,000 histories and produces minimal counterexamples for Zoo #5, #6, #7, and #12. Interactive HTML traces generated for all 17 Bug Zoo entries.

### Stage S5: Production Comparators & Evidence Polish (Weeks 27–30)
* [ ] **Weeks 27–28:** Benchmark single-node `LSMStore` against `cockroachdb/pebble` and `dgraph-io/badger` (`fsync` enabled, 5 runs, YCSB workloads A/B/C/F, reporting median, spread, $p_{50}, p_{99}, p_{99.9}$, and write amplification). Benchmark Option A vs. Option B durability coupling.
* [ ] **Week 29:** Benchmark 3-node AetherKV TCP cluster against 3-node `etcd` for `Put` and linearizable `ReadIndex` `Get` workloads.
* [ ] **Week 30:** Finalize all 17 `docs/bug-zoo/` 1-page postmortems, `DESIGN.md`, and one-command `make bench` reproduction scripts.
* **Exit Gate:** Stranger can clone the repo and run `make test-det`, `make zoo`, `make replay SEED=...`, and `make bench` out of the box.

---

## 6. Standard Makefile Targets (Developer & Reviewer Interface)

Include these exact targets in the root `Makefile` so interviewers and reviewers can verify any claim in one command:

```makefile
.PHONY: lint test-det sim-campaign zoo replay fuzz bench

# 1. Enforce determinism AST rules + standard vet
lint:
	go run ./cmd/detlint ./raft/... ./lsm/... ./kv/... ./statemachine/...
	go vet ./...

# 2. Verify 100% deterministic replay across 10,000 seeds (run-twice SHA-256 hash check)
test-det: lint
	go run ./cmd/simrun --verify-determinism --seeds=10000

# 3. Run multi-node chaos simulation campaign with Porcupine + Raft/LSM invariants
sim-campaign: lint
	go run ./cmd/simrun --nodes=3,4,5 --seeds=100000 --checker=porcupine

# 4. Run etcd/raft control experiment under identical fault schedules
test-control-etcd:
	go run ./cmd/simrun --engine=etcd-raft --nodes=3,5 --seeds=100000

# 5. Execute all 17 Bug Zoo mutation tests and verify detection within seed budget
zoo:
	go test -v ./zoo -run TestBugZooAll

# 6. Replay and shrink a specific seed, outputting minimal trace + HTML visualization
replay:
	go run ./cmd/simrun --seed=$(SEED) --config=$(CONFIG) --shrink --out=trace.jsonl
	go run ./aether-check render trace.jsonl --out=trace_$(SEED).html

# 7. Reproduce all Pebble, Badger, etcd, and Llama-3-8B KV-cache benchmarks
bench:
	go run ./cmd/benchrun --suite=all --fsync=true --runs=5 --out=bench/results/
```

---

## 7. Resume Telemetry Ledger (`docs/METRICS.md` Template)

Maintain this ledger starting in Week 1. **Never put unmeasured placeholder numbers on your resume**—fill these in as each stage completes:

```markdown
# AetherKV Empirical Telemetry Ledger

## 1. Real (Unplanted) Implementation Bugs Caught by Simulator & Fuzzers
*Total unplanted bugs caught during development: [N]*
1. [Date] [Stage] [Detector]: Brief 1-line description of real bug found in own code (e.g., off-by-one ConflictIndex backtracking on follower log truncation).
2. [Date] [Stage] [Detector]: ...

## 2. Simulator & Shrinker Performance
* Simulation speed (3-node Raft + MemStore): `[X,XXX]` virtual cluster seconds / real second
* Simulation speed (3-node Raft + LSMStore): `[X,XXX]` virtual cluster seconds / real second
* Total campaign seeds executed: `[XXX,XXX]` seeds (`[X.X]B` simulated virtual seconds)
* `etcd/raft` control adapter clean seeds: `100,000 / 100,000` (0 false positives)
* Mean fault-schedule shrink ratio: `[X,XXX]` raw events $\rightarrow$ `[XX]` minimal events (`[XX]%` reduction)

## 3. Bug Zoo Detection Sensitivity (17/17 Planted Bugs)
| Zoo ID | Bug Name | Firing Detector | First Failing Seed | Seeds-to-Detection (Median) | Shrunk Trace Length |
| :--- | :--- | :--- | :--- | :--- | :--- |
| #01 | Raft Fig. 8 Commitment | `StateMachineSafety` | `seed=412` | `184` | `23 events` |
| ... | ... | ... | ... | ... | ... |

## 4. Storage & Cluster Benchmarks (Hardware: [CPU / NVMe Model], fsync=ON)
* **WiscKey KV-Cache Block Benchmark ($2\text{ MiB}$ Llama-3-8B blocks):**
  * Inline LSM Write Amplification: `[X.XX]x` | Throughput: `[XXX] MB/s` | $p_{99}$: `[XX.X] ms`
  * WiscKey Separated Write Amplification: `[1.XX]x` (`[XX]%` reduction) | Throughput: `[XXX] MB/s` | $p_{99}$: `[X.X] ms`
* **Durability Coupling Experiment (3-node cluster):**
  * Option A ($2 \times \text{fsync}$): `[X,XXX] ops/s`, $p_{99} = \text{[XX.X] ms}$
  * Option B ($1 \times \text{fsync}$ + `flushedIndex`): `[X,XXX] ops/s` (`+[XX]%` throughput), $p_{99} = \text{[XX.X] ms}$
* **Comparator Baselines (Pebble / Badger / etcd):**
  * Single-node LSM vs. Pebble (YCSB-A, `fsync` on): `[XX]%` of Pebble write throughput; profile bottleneck explanation: `[...]`
```