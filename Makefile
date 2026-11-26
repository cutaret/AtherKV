.PHONY: build run-node up down lint test race fuzz sim-zoo13 bench-os bench-sim bench-pebble playground
GO_DIR := atherKV
build:
	mkdir -p bin
	cd $(GO_DIR) && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/aetherkv ./cmd/aetherkv
	cd $(GO_DIR) && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ../bin/aetherctl ./cmd/aetherctl
run-node:
	cd $(GO_DIR) && go run ./cmd/aetherkv -id n1 -data ./aetherkv-data/n1 -http :8080 -resp :6379 -single-fsync
up:
	docker compose up -d --build
down:
	docker compose down -v
lint:
	cd $(GO_DIR) && go run ./cmd/detlint statemachine raft lsm kv
test:
	cd $(GO_DIR) && go test -p 1 -v ./... -count=1
race:
	cd $(GO_DIR) && go test -race -p 1 ./sim ./vfs ./raft/... ./lsm/... ./adapters/... -count=1
fuzz:
	cd $(GO_DIR) && go test -fuzz=FuzzRaftWALRecover -fuzztime=5s ./raft/wal
	cd $(GO_DIR) && go test -fuzz=FuzzLSMWALAndBatchDecode -fuzztime=5s ./lsm
	cd $(GO_DIR) && go test -fuzz=FuzzSSTableOpen -fuzztime=5s ./lsm
	cd $(GO_DIR) && go test -fuzz=FuzzPorcupineChecker -fuzztime=5s ./adapters/porcupine
sim-zoo13:
	cd $(GO_DIR) && go run ./cmd/simrun -seed 4 -nodes 4 -bug 13 -html docs/trace_zoo13.html
bench-os:
	cd $(GO_DIR) && go run ./cmd/benchrun -fs os -runs 5 -out bench/results -prefix local_osfs
bench-sim:
	cd $(GO_DIR) && go run ./cmd/benchrun -fs sim -runs 5 -out bench/results -prefix local_simfs
bench-pebble:
	cd $(GO_DIR)/bench/pebble_compare && go run . -runs 5 -out ../results
playground:
	cd $(GO_DIR) && go run ./cmd/aether-server -port 8080
