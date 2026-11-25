### AetherKV Benchmark Results (`fs=os`, `linux/amd64`, `4 CPUs`, `go1.23.12`, `5 runs`)

| Suite | Workload | Med Ops/s | Med MiB/s | p50 (us) | p99 (us) | Write Amp | Fsyncs |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB_LSM | YCSB-A (50% Put / 50% Get) | 2453 | 0.6 | 159.6 | 2552.1 | 3.47x | - |
| YCSB_LSM | YCSB-B (5% Put / 95% Get) | 17291 | 4.2 | 0.6 | 238.3 | 3.45x | - |
| YCSB_LSM | YCSB-E (5% Put / 95% Scan) | 8869 | 2.2 | 64.5 | 284.8 | 3.49x | - |
| Cluster_3Node_Raft_LSM | 50%_Write_50%_Linearizable_ReadIndex | 239 | 0.0 | 2513.9 | 67338.4 | 1.00x | - |
| Durability_Coupling | Option_A_DualFsync (Raft WAL + LSM WAL) | 662 | 0.0 | 772.4 | 21390.8 | 1.00x | 401 |
| Durability_Coupling | Option_B_SingleFsync (Raft WAL + FlushedIndex) | 1300 | 0.0 | 594.7 | 9118.2 | 1.00x | 201 |
| Llama3_8B_KVCache | 1KiB_Inline | 1798 | 1.8 | 211.7 | 9299.7 | 2.91x | - |
| Llama3_8B_KVCache | 1KiB_WiscKey | 1661 | 1.6 | 208.8 | 4787.4 | 2.91x | - |
| Llama3_8B_KVCache | 16KiB_Inline | 457 | 7.1 | 1510.3 | 53137.3 | 4.65x | - |
| Llama3_8B_KVCache | 16KiB_WiscKey | 1658 | 25.9 | 441.3 | 1122.0 | 1.02x | - |
| Llama3_8B_KVCache | 128KiB_Llama3_1Token_Inline | 311 | 38.8 | 1328.1 | 18302.4 | 4.50x | - |
| Llama3_8B_KVCache | 128KiB_Llama3_1Token_WiscKey | 1260 | 157.5 | 574.7 | 1748.8 | 1.00x | - |
| Llama3_8B_KVCache | 512KiB_Llama3_4Token_Inline | 93 | 46.6 | 4128.5 | 49172.4 | 4.00x | - |
| Llama3_8B_KVCache | 512KiB_Llama3_4Token_WiscKey | 468 | 233.9 | 940.5 | 6341.4 | 1.00x | - |
| Llama3_8B_KVCache | 2MiB_Llama3_16Token_Inline | 25 | 49.1 | 17002.6 | 112999.4 | 3.67x | - |
| Llama3_8B_KVCache | 2MiB_Llama3_16Token_WiscKey | 128 | 256.0 | 8289.3 | 9970.0 | 1.00x | - |
