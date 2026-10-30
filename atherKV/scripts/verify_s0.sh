#!/usr/bin/env bash
set -euo pipefail

echo "============================================================"
echo " AetherKV Stage S0 (Weeks 1-2) Sanity & Exit Criteria Check "
echo "============================================================"

echo -e "\n[1/3] Running static determinism linter (cmd/detlint) on core packages..."
go run ./cmd/detlint statemachine raft lsm kv

echo -e "\n[2/3] Running Stage S0 Sanity Suite & 10,000-Seed Trace Hash Meta-Test..."
go test -v ./sim -run "TestS0_Sanity" -count=1

echo -e "\n[3/3] Verifying ADR documentation presence..."
for adr in \
  "docs/adr/001-determinism-seam.md" \
  "docs/adr/002-statemachine-seam.md" \
  "docs/adr/003-isolated-prng-streams.md"; do
  if [[ ! -f "$adr" ]]; then
    echo "MISSING ADR: $adr"
    exit 1
  fi
  echo "  ✓ Found $adr"
done

echo -e "\n============================================================"
echo " STAGE S0 COMPLETE: All exit criteria verified and green!   "
echo "============================================================"