### AetherKV Benchmark Results (`fs=sim`, `linux/amd64`, `4 CPUs`, `go1.23.12`, `5 runs`)

| Suite | Workload | Med Ops/s | Med MiB/s | p50 (us) | p99 (us) | Write Amp | Fsyncs |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB_LSM | YCSB-A (50% Put / 50% Get) | 162587 | 39.7 | 2.5 | 68.3 | 3.47x | - |
| YCSB_LSM | YCSB-B (5% Put / 95% Get) | 358497 | 87.5 | 0.3 | 14.2 | 3.45x | - |
| YCSB_LSM | YCSB-E (5% Put / 95% Scan) | 33899 | 8.3 | 20.5 | 172.7 | 3.49x | - |
| Cluster_3Node_Raft_LSM | 50%_Write_50%_Linearizable_ReadIndex | 15020 | 0.0 | 50.4 | 264.1 | 1.00x | - |
| Durability_Coupling | Option_A_DualFsync (Raft WAL + LSM WAL) | 80720 | 0.0 | 8.2 | 103.6 | 1.00x | 401 |
| Durability_Coupling | Option_B_SingleFsync (Raft WAL + FlushedIndex) | 119046 | 0.0 | 6.3 | 68.2 | 1.00x | 201 |
| Llama3_8B_KVCache | 1KiB_Inline | 178096 | 173.9 | 1.9 | 64.0 | 2.91x | - |
| Llama3_8B_KVCache | 1KiB_WiscKey | 182762 | 178.5 | 1.8 | 43.7 | 2.91x | - |
| Llama3_8B_KVCache | 16KiB_Inline | 8714 | 136.2 | 35.3 | 638.3 | 4.65x | - |
| Llama3_8B_KVCache | 16KiB_WiscKey | 8721 | 136.3 | 67.6 | 560.1 | 1.02x | - |
| Llama3_8B_KVCache | 128KiB_Llama3_1Token_Inline | 996 | 124.5 | 276.2 | 4159.9 | 4.50x | - |
| Llama3_8B_KVCache | 128KiB_Llama3_1Token_WiscKey | 1289 | 161.1 | 402.1 | 3131.3 | 1.00x | - |
| Llama3_8B_KVCache | 512KiB_Llama3_4Token_Inline | 300 | 150.1 | 1338.4 | 13748.2 | 4.00x | - |
| Llama3_8B_KVCache | 512KiB_Llama3_4Token_WiscKey | 600 | 299.9 | 846.8 | 9258.5 | 1.00x | - |
| Llama3_8B_KVCache | 2MiB_Llama3_16Token_Inline | 72 | 144.2 | 4308.5 | 34839.1 | 3.67x | - |
| Llama3_8B_KVCache | 2MiB_Llama3_16Token_WiscKey | 218 | 436.2 | 2080.4 | 16132.8 | 1.00x | - |
