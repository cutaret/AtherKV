### AetherKV vs. CockroachDB Pebble v1.1.5 (`linux/amd64`, `4 CPUs`, `fsync=ON`, `5-run median`)

| Workload | Engine | Med Ops/s | Med MiB/s | p50 (us) | p99 (us) |
| :--- | :--- | ---: | ---: | ---: | ---: |
| YCSB-A (50% Put / 50% Get, 256B) | AetherKV (OSFS, fsync=ON) | 5594 | 1.4 | 145.8 | 386.2 |
| YCSB-A (50% Put / 50% Get, 256B) | CockroachDB Pebble v1.1.5 (Sync) | 9066 | 2.2 | 147.3 | 403.4 |
| YCSB-B (5% Put / 95% Get, 256B) | AetherKV (OSFS, fsync=ON) | 73114 | 17.9 | 0.2 | 182.5 |
| YCSB-B (5% Put / 95% Get, 256B) | CockroachDB Pebble v1.1.5 (Sync) | 76158 | 18.6 | 0.3 | 192.8 |
| 128 KiB Llama-3 1-Token KV Block | AetherKV Inline LSM (OSFS) | 254 | 31.8 | 2029.0 | 41468.4 |
| 128 KiB Llama-3 1-Token KV Block | AetherKV WiscKey VLog (OSFS) | 1234 | 154.3 | 497.5 | 4812.2 |
| 128 KiB Llama-3 1-Token KV Block | CockroachDB Pebble v1.1.5 (Sync) | 1316 | 164.5 | 372.3 | 2827.8 |
| 2 MiB Llama-3 16-Token KV Block | AetherKV Inline LSM (OSFS) | 20 | 40.5 | 22195.7 | 90003.6 |
| 2 MiB Llama-3 16-Token KV Block | AetherKV WiscKey VLog (OSFS) | 112 | 224.2 | 8782.8 | 29952.9 |
| 2 MiB Llama-3 16-Token KV Block | CockroachDB Pebble v1.1.5 (Sync) | 54 | 108.6 | 16475.4 | 42467.6 |
