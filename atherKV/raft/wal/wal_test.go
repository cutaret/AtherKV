package wal_test

import (
	"bytes"
	"testing"

	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/raft/wal"
	"github.com/cutaret/aetherkv/vfs"
)

func TestWAL_AppendAndRecover(t *testing.T) {
	fs := vfs.NewSimFS()
	w, err := wal.Open(fs, "wal_dir")
	if err != nil {
		t.Fatal(err)
	}

	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("cmd1")},
		{Term: 1, Index: 2, Data: []byte("cmd2")},
		{Term: 2, Index: 3, Data: []byte("cmd3")},
	}
	if err := w.Append(entries); err != nil {
		t.Fatal(err)
	}
	_ = w.Sync()
	_ = w.Close()

	// Reopen and recover
	w2, err := wal.Open(fs, "wal_dir")
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if w2.LastIndex() != 3 {
		t.Fatalf("expected LastIndex=3, got %d", w2.LastIndex())
	}
	e, ok := w2.GetEntry(2)
	if !ok || string(e.Data) != "cmd2" {
		t.Fatalf("failed to retrieve entry 2: %+v", e)
	}
}

func TestWAL_TornTailRecovery(t *testing.T) {
	fs := vfs.NewSimFS()
	w, _ := wal.Open(fs, "wal_dir")
	_ = w.Append([]raft.Entry{
		{Term: 1, Index: 1, Data: []byte("safe entry")},
	})
	_ = w.Sync()
	_ = w.Close()

	// Corrupt active WAL file tail with partial byte write
	activePath := "wal_dir/0000000000000001.wal"
	f, err := fs.Open(activePath)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := f.Size()
	// Write 5 bytes of garbage simulating torn record header
	_, _ = f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}, size)
	_ = f.Sync()
	_ = f.Close()

	// Reopen WAL: recovery must identify the torn record, truncate it, and preserve index 1
	w2, err := wal.Open(fs, "wal_dir")
	if err != nil {
		t.Fatalf("recovery failed on torn tail: %v", err)
	}
	defer w2.Close()

	if w2.LastIndex() != 1 {
		t.Fatalf("expected last index 1 after torn tail truncation, got %d", w2.LastIndex())
	}
	e, ok := w2.GetEntry(1)
	if !ok || !bytes.Equal(e.Data, []byte("safe entry")) {
		t.Fatalf("intact entry was corrupted during recovery: %+v", e)
	}
}
