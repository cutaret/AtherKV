package wal_test

import (
	"testing"

	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/raft/wal"
	"github.com/cutaret/aetherkv/vfs"
)

func FuzzRaftWALRecover(f *testing.F) {
	// Seed 1: empty file
	f.Add([]byte{})
	// Seed 2: torn 0xAA sector header (regression seed for the 2.86 GB OOM bug)
	f.Add([]byte{
		0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	})
	// Seed 3: valid encoded WAL segment
	seedFS := vfs.NewSimFS()
	if w, err := wal.Open(seedFS, "seed"); err == nil {
		_ = w.Append([]raft.Entry{
			{Term: 1, Index: 1, Data: []byte("hello-raft")},
			{Term: 2, Index: 2, Data: []byte("second-entry")},
		})
		_ = w.Close()
		if sf, err := seedFS.Open("seed/0000000000000001.wal"); err == nil {
			if sz, err := sf.Size(); err == nil && sz > 0 {
				buf := make([]byte, sz)
				_, _ = sf.ReadAt(buf, 0)
				f.Add(buf)
			}
			_ = sf.Close()
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		fs := vfs.NewSimFS()
		_ = fs.MkdirAll("wal_fuzz")
		file, err := fs.Create("wal_fuzz/0000000000000001.wal")
		if err != nil {
			return
		}
		if len(data) > 0 {
			_, _ = file.WriteAt(data, 0)
		}
		_ = file.Sync()
		_ = file.Close()
		_ = fs.SyncDir("wal_fuzz")

		w, err := wal.Open(fs, "wal_fuzz")
		if err != nil {
			return
		}
		if last := w.LastIndex(); last > 0 {
			_, _ = w.GetEntry(last)
		}
		_ = w.Close()
	})
}
