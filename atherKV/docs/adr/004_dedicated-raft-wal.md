# ADR 004: Dedicated Segmented Raft WAL Separate from the LSM Engine

* **Status:** Accepted (Stage S1)
* **Context:** A replicated key-value store must persist both the Raft consensus log (`HardState` + `[]Entry`) and the state machine's key-value dataset. Storing the Raft log inside the same LSM tree as user data is tempting but introduces write amplification and inefficient range-delete compactions.
* **Decision:** Store the Raft log in its own segmented, CRC32C-checksummed append-only WAL (`raft/wal`) directly on `vfs.FS`.
* **Consequences:** Suffix truncation (on follower conflict) and prefix deletion (on snapshot compaction) execute in O(1) segment operations without polluting LSM compaction levels, matching production designs like TiKV's `raft-engine`.
