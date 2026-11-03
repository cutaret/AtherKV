# ADR 007: Option A vs. Option B Raft-to-LSM Durability Coupling and Snapshot State Transfer

- **Status:** Accepted (Stage S3)
- **Context:** Coupling a replicated Raft log with an LSM storage engine introduces a double-journaling trade-off: persisting every mutation to both the Raft WAL and the LSM WAL requires two `fsync` calls per write.
- **Decision:**
  1. **Option A (Default Safe Mode):** Sync both the Raft WAL and the LSM WAL (with atomic `appliedIndex`) on every batch. The Raft log can be truncated up to `AppliedIndex()`.
  2. **Option B (Single-`fsync` Optimization):** Disable LSM WAL syncing (`DisableWALSync: true`) and rely on the Raft WAL for crash recovery until the LSM Memtable flushes to an SSTable (`FlushedIndex()`). Under Option B, the Raft WAL may only be truncated up to `FlushedIndex()`; truncating past `FlushedIndex()` to `AppliedIndex()` is isolated as **Bug Zoo #11**.
  3. **Snapshot Deduplication Integrity:** `InstallSnapshot` streams both user KV items and the client session deduplication table (`\x00sys:sess:*`). Omitting the session table during snapshot transfer is isolated as **Bug Zoo #12**.
