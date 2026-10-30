#!/usr/bin/env bash
set -euo pipefail

echo "============================================================"
echo " AetherKV Stage S1 (Weeks 3-8) Sanity & Exit Criteria Check "
echo "============================================================"

echo -e "\n[1/5] Running static determinism linter (cmd/detlint) across all core packages..."
go run ./cmd/detlint statemachine raft lsm kv

echo -e "\n[2/5] Running SimFS & Segmented Raft WAL crash/torn-write recovery suites..."
go test -v ./vfs ./raft/wal -count=1

echo -e "\n[3/5] Running Stage S1 Chaos Control Campaign across 3-, 4-, and 5-node clusters..."
go test -v ./zoo -run "TestS1_ControlCampaign_3_4_5_NodeClusters" -count=1

echo -e "\n[4/5] Running Stage S1 Bug Zoo Detection & Fault Shrinker Suite (#1-#7 & #13)..."
go test -v ./zoo -run "TestS1_BugZoo_All8PlantedBugs" -count=1

echo -e "\n[5/5] Verifying Stage S0 + S1 ADR documentation..."
for adr in \
  "docs/adr/001-determinism-seam.md" \
  "docs/adr/002-statemachine-seam.md" \
  "docs/adr/003-isolated-prng-streams.md" \
  "docs/adr/004-dedicated-raft-wal.md" \
  "docs/adr/005-readindex-and-lease-bounds.md"; do
  if [[ ! -f "$adr" ]]; then
    echo "MISSING ADR: $adr"
    exit 1
  fi
  echo "  ✓ Found $adr"
done

echo -e "\n============================================================"
echo " STAGE S1 COMPLETE: Raft, SimFS, KV, Porcupine & Bug Zoo OK!"
echo "============================================================"