# ADR 005: Linearizable Reads via ReadIndex and Clock-Bounded Leases

* **Status:** Accepted (Stage S1)
* **Context:** Serving local reads directly from a Raft leader risks returning stale data if the leader has been silently deposed across a network partition (Bug Zoo #5).
* **Decision:** Default all reads to `ReadIndex` (recording the leader's `commitIndex` and confirming leadership via a quorum heartbeat round before waiting for `appliedIndex >= readIndex`). Provide lease reads only as an opt-in path guarded by an explicit `maxDriftBound` subtracted from the nominal lease duration.
* **Consequences:** `ReadIndex` maintains strict linearizability under arbitrary clock skew while avoiding disk writes on the read path. Removing `maxDriftBound` under simulated `NodeClock` drift deterministically triggers Bug Zoo #6.
