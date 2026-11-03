package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/cutaret/aetherkv/raft"
	"github.com/cutaret/aetherkv/vfs"
)

const (
	RecordHeaderSize = 25
	SegmentMaxBytes  = 64 * 1024 * 1024
	TypeEntry        = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type SegmentedWAL struct {
	fs          vfs.FS
	dir         string
	activeFile  vfs.File
	activeSegID uint64
	activeBytes int64
	firstIndex  uint64
	lastIndex   uint64
	entries     map[uint64]raft.Entry
}

func Open(fs vfs.FS, dir string) (*SegmentedWAL, error) {
	if err := fs.MkdirAll(dir); err != nil {
		return nil, err
	}

	w := &SegmentedWAL{
		fs:         fs,
		dir:        dir,
		firstIndex: 1,
		entries:    make(map[uint64]raft.Entry),
	}

	if err := w.recover(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *SegmentedWAL) segmentPath(segID uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%016d.wal", segID))
}

func (w *SegmentedWAL) recover() error {
	files, err := w.fs.List(w.dir)
	if err != nil {
		return err
	}

	var segIDs []uint64
	for _, f := range files {
		if strings.HasSuffix(f, ".wal") {
			base := strings.TrimSuffix(f, ".wal")
			id, parseErr := strconv.ParseUint(base, 10, 64)
			if parseErr == nil {
				segIDs = append(segIDs, id)
			}
		}
	}
	slices.Sort(segIDs)

	if len(segIDs) == 0 {
		return w.createNewSegment(1)
	}

	var minIdx uint64
	for _, id := range segIDs {
		path := w.segmentPath(id)
		f, err := w.fs.Open(path)
		if err != nil {
			return err
		}

		fileSize, szErr := f.Size()
		if szErr != nil {
			_ = f.Close()
			return szErr
		}

		var offset int64
		for {
			var hdr [RecordHeaderSize]byte
			n, rErr := f.ReadAt(hdr[:], offset)
			if rErr == io.EOF || n < RecordHeaderSize {
				_ = f.Truncate(offset)
				break
			}

			expectedCRC := binary.LittleEndian.Uint32(hdr[0:4])
			length := binary.LittleEndian.Uint32(hdr[4:8])
			recType := hdr[8]
			term := binary.LittleEndian.Uint64(hdr[9:17])
			index := binary.LittleEndian.Uint64(hdr[17:25])

			if int64(length) > SegmentMaxBytes || offset+RecordHeaderSize+int64(length) > fileSize {
				_ = f.Truncate(offset)
				break
			}

			payload := make([]byte, length)
			pn, pErr := f.ReadAt(payload, offset+RecordHeaderSize)
			if pErr != nil || uint32(pn) < length {
				_ = f.Truncate(offset)
				break
			}

			h := crc32.New(crcTable)
			h.Write(hdr[8:RecordHeaderSize])
			h.Write(payload)
			if h.Sum32() != expectedCRC {
				_ = f.Truncate(offset)
				break
			}

			if recType == TypeEntry {
				w.entries[index] = raft.Entry{
					Term:  term,
					Index: index,
					Data:  payload,
				}
				if minIdx == 0 || index < minIdx {
					minIdx = index
				}
				w.lastIndex = max(w.lastIndex, index)
			}
			offset += int64(RecordHeaderSize + length)
		}

		if id == segIDs[len(segIDs)-1] {
			w.activeFile = f
			w.activeSegID = id
			w.activeBytes = offset
		} else {
			_ = f.Close()
		}
	}

	if minIdx > 0 {
		w.firstIndex = minIdx
	}
	return nil
}

func (w *SegmentedWAL) createNewSegment(segID uint64) error {
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		_ = w.activeFile.Close()
	}
	path := w.segmentPath(segID)
	f, err := w.fs.Create(path)
	if err != nil {
		return err
	}
	if err := w.fs.SyncDir(w.dir); err != nil {
		return err
	}
	w.activeFile = f
	w.activeSegID = segID
	w.activeBytes = 0
	return nil
}

func (w *SegmentedWAL) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, e := range entries {
		if w.activeBytes >= SegmentMaxBytes {
			if err := w.createNewSegment(w.activeSegID + 1); err != nil {
				return err
			}
		}

		length := uint32(len(e.Data))
		var hdr [RecordHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[4:8], length)
		hdr[8] = TypeEntry
		binary.LittleEndian.PutUint64(hdr[9:17], e.Term)
		binary.LittleEndian.PutUint64(hdr[17:25], e.Index)

		h := crc32.New(crcTable)
		h.Write(hdr[8:RecordHeaderSize])
		h.Write(e.Data)
		binary.LittleEndian.PutUint32(hdr[0:4], h.Sum32())

		if _, err := w.activeFile.WriteAt(hdr[:], w.activeBytes); err != nil {
			return err
		}
		if length > 0 {
			if _, err := w.activeFile.WriteAt(e.Data, w.activeBytes+RecordHeaderSize); err != nil {
				return err
			}
		}

		w.activeBytes += int64(RecordHeaderSize + length)
		w.entries[e.Index] = e
		w.lastIndex = max(w.lastIndex, e.Index)
	}
	return nil
}

func (w *SegmentedWAL) TruncateSuffix(fromIndex uint64) {
	for idx := fromIndex; idx <= w.lastIndex; idx++ {
		delete(w.entries, idx)
	}
	if fromIndex > 0 {
		w.lastIndex = fromIndex - 1
	} else {
		w.lastIndex = 0
	}
	w.rewriteActive()
}

// CompactPrefix deletes all Raft log entries <= upToIndex (used after snapshot creation or LSM flush).
func (w *SegmentedWAL) CompactPrefix(upToIndex uint64) {
	for idx := w.firstIndex; idx <= upToIndex; idx++ {
		delete(w.entries, idx)
	}
	if upToIndex+1 > w.firstIndex {
		w.firstIndex = upToIndex + 1
	}
	w.rewriteActive()
	_ = w.Sync()
}

func (w *SegmentedWAL) rewriteActive() {
	var surviving []raft.Entry
	for idx := w.firstIndex; idx <= w.lastIndex; idx++ {
		if e, ok := w.entries[idx]; ok {
			surviving = append(surviving, e)
		}
	}
	if w.activeFile != nil {
		_ = w.activeFile.Truncate(0)
		w.activeBytes = 0
		_ = w.Append(surviving)
	}
}

func (w *SegmentedWAL) Sync() error {
	if w.activeFile != nil {
		return w.activeFile.Sync()
	}
	return nil
}

func (w *SegmentedWAL) FirstIndex() uint64 { return w.firstIndex }
func (w *SegmentedWAL) LastIndex() uint64  { return w.lastIndex }

func (w *SegmentedWAL) GetEntry(index uint64) (raft.Entry, bool) {
	e, ok := w.entries[index]
	return e, ok
}

func (w *SegmentedWAL) Close() error {
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		return w.activeFile.Close()
	}
	return nil
}
