package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func parsePeerMap(raw string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if id != "" && addr != "" {
			out[id] = addr
		}
	}
	return out
}

func main() {
	id := flag.String("id", "n1", "Unique cluster node ID (e.g. n1, n2, n3)")
	dataDir := flag.String("data", "./aetherkv-data/n1", "Directory for persistent Raft WAL, LSM SSTables, and WiscKey VLog")
	httpAddr := flag.String("http", ":8080", "HTTP client API listen address")
	respAddr := flag.String("resp", ":6379", "Redis RESP2 TCP listen address (set empty \"\" to disable)")
	raftAddr := flag.String("raft", "127.0.0.1:9001", "Raft peer-to-peer TCP listen address")
	peersFlag := flag.String("peers", "", "Comma-separated Raft peer map: n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003")
	httpPeersFlag := flag.String("http-peers", "", "Comma-separated HTTP peer map for leader forwarding: n1=http://127.0.0.1:8081,n2=http://127.0.0.1:8082")
	tickMs := flag.Int("tick-ms", 10, "Raft logical tick interval in milliseconds")
	opTimeoutMs := flag.Int("op-timeout-ms", 5000, "Client operation timeout in milliseconds")
	wiscKeyBytes := flag.Int("wisckey-threshold", 4096, "Separate values >= N bytes into WiscKey VLog (0 disables separation)")
	singleFsync := flag.Bool("single-fsync", false, "Enable Option B durability (Raft WAL sync + FlushedIndex truncation fence)")
	seed := flag.Uint64("seed", 0, "Optional PRNG seed (0 uses crypto/rand)")
	flag.Parse()

	peers := parsePeerMap(*peersFlag)
	if len(peers) == 0 {
		peers[*id] = *raftAddr
	}
	httpPeers := parsePeerMap(*httpPeersFlag)

	node, err := NewNode(Config{
		NodeID:           *id,
		DataDir:          *dataDir,
		HTTPAddr:         *httpAddr,
		RaftAddr:         *raftAddr,
		Peers:            peers,
		HTTPPeers:        httpPeers,
		TickInterval:     time.Duration(*tickMs) * time.Millisecond,
		OpTimeout:        time.Duration(*opTimeoutMs) * time.Millisecond,
		WiscKeyThreshold: *wiscKeyBytes,
		SingleFsync:      *singleFsync,
		Seed:             *seed,
	})
	if err != nil {
		log.Fatalf("aetherkv startup failed: %v", err)
	}

	var respSrv *RESPServer
	respStatus := "disabled"
	if *respAddr != "" {
		respSrv, err = NewRESPServer(node, *respAddr)
		if err != nil {
			_ = node.Close()
			log.Fatalf("aetherkv resp startup failed: %v", err)
		}
		respStatus = respSrv.Addr()
	}

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           node.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		fmt.Printf("AetherKV node=%s listening http=%s resp=%s raft=%s data=%s peers=%d wisckey>=%dB single-fsync=%v\n",
			*id, *httpAddr, respStatus, node.RaftAddr(), *dataDir, len(peers), *wiscKeyBytes, *singleFsync)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %s, flushing LSM and shutting down node %s...\n", sig, *id)
	case err := <-errCh:
		log.Printf("HTTP server error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if respSrv != nil {
		_ = respSrv.Close()
	}
	_ = node.Close()
}
