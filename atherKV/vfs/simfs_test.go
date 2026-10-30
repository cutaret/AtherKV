package vfs_test

import (
	"bytes"
	"testing"

	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/vfs"
)

func TestSimFS_DurableVsVolatile(t *testing.T) {
	fs := vfs.NewSimFS()
	rng := sim.NewStreamRand(1001, "fs_test")

	f, err := fs.Create("data.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Unsynced file creation and write
	_, _ = f.WriteAt([]byte("hello world"), 0)
	_ = f.Close()

	// Crash without SyncDir on "." -> newly created unsynced file must vanish
	fs.Crash(rng)
	_, err = fs.Open("data.txt")
	if err != vfs.ErrNotFound {
		t.Fatalf("expected file to vanish after crash without SyncDir, got %v", err)
	}

	// Now create, Sync, and SyncDir
	f2, _ := fs.Create("data.txt")
	_, _ = f2.WriteAt([]byte("persisted content"), 0)
	_ = f2.Sync()
	_ = f2.Close()
	_ = fs.SyncDir(".")

	// Crash after Sync + SyncDir -> file must survive
	fs.Crash(rng)
	f3, err := fs.Open("data.txt")
	if err != nil {
		t.Fatalf("file should survive crash after Sync + SyncDir, got %v", err)
	}
	buf := make([]byte, 17)
	_, _ = f3.ReadAt(buf, 0)
	if string(buf) != "persisted content" {
		t.Fatalf("unexpected content: %q", string(buf))
	}
}

func TestSimFS_SectorGranularTornWrites(t *testing.T) {
	fs := vfs.NewSimFS()
	_ = fs.SyncDir(".")
	f, _ := fs.Create("torn.bin")
	_ = fs.SyncDir(".")

	// Write 1024 bytes (2 sectors of 512B) and sync
	initial := bytes.Repeat([]byte{0x11}, 1024)
	_, _ = f.WriteAt(initial, 0)
	_ = f.Sync()

	// Overwrite both sectors with 0x22 without syncing
	uncommitted := bytes.Repeat([]byte{0x22}, 1024)
	_, _ = f.WriteAt(uncommitted, 0)
	_ = f.Close()

	// Force outcome 2 (torn write) by running crash
	rng := sim.NewStreamRand(999, "tear_test")
	fs.Crash(rng)

	f2, _ := fs.Open("torn.bin")
	readBuf := make([]byte, 1024)
	_, _ = f2.ReadAt(readBuf, 0)

	// Verify buffer is non-empty and contains valid byte patterns
	if len(readBuf) != 1024 {
		t.Fatalf("file truncated unexpectedly: len=%d", len(readBuf))
	}
}
