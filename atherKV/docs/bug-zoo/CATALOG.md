# AetherKV — 17-Entry Reproducible Bug Zoo Catalog

Every entry in the Bug Zoo reintroduces a classic consensus, storage-engine, or value-separation bug behind an explicit configuration switch. Each failure is 100% deterministic and reproducible from its recorded seed.

## Summary Detection Matrix

| Zoo #   | Switch / Flag               | Subsystem      | Injected Bug & Symptom                                                                                                                                            | Firing Detector / Oracle                    | Minimal Seed | Seeds to Detect               |
| :------ | :-------------------------- | :------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------ | :----------- | :---------------------------- |
| **#1**  | `Bug01Fig8CommitAnyTerm`    | Raft Core      | Leader commits earlier-term log entries by counting replicas (Raft paper Figure 8), allowing a subsequent leader to overwrite a committed entry.                  | `StateMachineSafety` / `LeaderCompleteness` | `seed=1`     | `1`                           |
| **#2**  | `Bug02SkipVotePersist`      | Raft + VFS     | Node replies to `RequestVote` before syncing `votedFor` and `Term` to `hardstate.json`, double-voting in the same term after crash/restart.                       | `TermMonotonicity` / `ElectionSafety`       | `seed=1`     | `1`                           |
| **#3**  | `Bug03AckBeforeSync`        | Raft + WAL     | Follower acknowledges `AppendEntries` before `fsync`ing the segmented Raft WAL; simultaneous majority crash permanently loses committed writes.                   | `CommittedDurability`                       | `seed=1`     | `1`                           |
| **#4**  | `Bug04StaleTruncate`        | Raft Core      | Follower unconditionally truncates its log on a stale/reordered `AppendEntries` RPC even when existing entries already match.                                     | `CommittedDurability` / `LogMatching`       | `seed=4`     | `1`                           |
| **#5**  | `Bug05BlindLeaderRead`      | KV + Raft      | Deposed leader isolated across a network partition serves local `Get`/`Scan` requests without a `ReadIndex` quorum heartbeat check.                               | `PorcupineLinearizability` (`aether-check`) | `seed=5`     | `1`                           |
| **#6**  | `Bug06LeaseUnderSkew`       | KV + Clock     | Leader serves lease reads without subtracting `maxDriftBound` while `NodeClock` injects clock drift exceeding the lease safety margin.                            | `PorcupineLinearizability` (`aether-check`) | `seed=606`   | `1`                           |
| **#7**  | `Bug07NoClientDedup`        | KV Sessions    | State machine omits `(ClientID, SeqNum)` deduplication so a client retry after a network timeout applies a `CAS` mutation twice.                                  | `PorcupineLinearizability` (`aether-check`) | `seed=707`   | `1`                           |
| **#8**  | `Bug08AcceptTornWAL`        | LSM WAL        | 32 KiB block WAL recovery accepts a 512B sector-torn tail record without verifying its CRC32C checksum.                                                           | `CrashAtEverySyncOracle`                    | `seed=8`     | `1`                           |
| **#9**  | `Bug09SkipManifestSync`     | LSM + VFS      | `LogAndApplyManifest` renames `CURRENT.tmp` to `CURRENT` without calling `SyncDir()` on the parent directory; crash reverts the `MANIFEST`.                       | `SimFSCrashOracle`                          | `seed=9`     | `1`                           |
| **#10** | `Bug10DropTombstoneEarly`   | LSM Compaction | $L_0 \rightarrow L_1$ compaction drops a deletion tombstone in $L_1$ while an older version of the deleted key survives in $L_2$, resurrecting deleted data.      | `ModelBasedOracle` (`MemStore` diff)        | `seed=10`    | `1`                           |
| **#11** | `Bug11PrematureLogTruncate` | Raft + LSM     | Under Option B (`DisableWALSync`), the Raft WAL is truncated up to in-memory `AppliedIndex` instead of durable `FlushedIndex`, losing memtable writes on crash.   | `CrashRecoveryOracle`                       | `seed=1111`  | `1`                           |
| **#12** | `Bug12SnapshotOmitDedup`    | KV + Snapshot  | `InstallSnapshot` streams KV items to a lagging follower but omits the client session deduplication table, allowing post-snapshot retries to double-apply.        | `PorcupineLinearizability` (`aether-check`) | `seed=1212`  | `1`                           |
| **#13** | `Bug13EvenQuorum`           | Raft Core      | Quorum size computed as `(n + 1) / 2` integer division (yields 2 out of 4 on a 4-node cluster), enabling split-brain elections on even-sized clusters.            | `CommittedDurability` / `ElectionSafety`    | `seed=4`     | `4` (shrunk `20 -> 2` faults) |
| **#14** | `Bug14SplitApplyIndex`      | KV + LSM       | KV mutations and `\x00sys:applied_index` are written in two separate LSM batches instead of one atomic `WriteBatch`; crash between them double-applies on replay. | `CrashPointEnumerationOracle`               | `seed=1`     | `1`                           |
| **#15** | `Bug15DanglingVLogPtr`      | WiscKey VLog   | LSM `WriteBatch` containing a 24-byte `VLogPointer` is synced before `ValueLog.Sync()`, leaving a dangling pointer when power fails.                              | `CrashAtEverySyncOracle`                    | `seed=1`     | `1`                           |
| **#16** | `Bug16VLogGCDropLive`       | WiscKey VLog   | Value-log garbage collector deletes an old `vlog` segment without relocating still-live key pointers into the active segment.                                     | `ModelBasedOracle`                          | `seed=16`    | `1`                           |
| **#17** | `Bug17AcceptTornVLog`       | WiscKey VLog   | `ValueLog.Read` returns a sector-torn tail blob without verifying the pointer and header CRC32C checksums.                                                        | `CrashAtEverySyncOracle`                    | `seed=17`    | `1`                           |

## Reproducing Any Bug Zoo Entry

```powershell
# Run all S1 Consensus & Linearizability Bug Zoo entries (#1-#7 & #13)
go test -v ./zoo -run "TestS1_BugZoo_All8PlantedBugs" -count=1

# Run all S2 & S2b Storage & WiscKey Value-Log Bug Zoo entries (#8-#10, #14-#17)
go test -v ./lsm -run "Zoo" -count=1

# Run S3 Full-Stack Integration Bug Zoo entries (#11 & #12)
go test -v ./zoo -run "TestS3_BugZoo" -count=1

# Replay Bug Zoo #13 via CLI, shrink fault schedule (20 -> 2 faults), and render interactive HTML Trace Viewer
go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 -html docs/trace_zoo13.html
```
