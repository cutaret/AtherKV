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
	RecordHeaderSize = 25               // 4 (CRC) + 4 (Len) + 1 (Type) + 8 (Term) + 8 (Index)
	SegmentMaxBytes  = 64 * 1024 * 1024 // 64 MB per WAL segment
	TypeEntry        = 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// SegmentedWAL manages segmented append-only log files backed by vfs.FS.
type SegmentedWAL struct {
	fs          vfs.FS
	dir         string
	activeFile  vfs.File
	activeSegID uint64
	activeBytes int64
	lastIndex   uint64
	entries     map[uint64]raft.Entry // In-memory index of log entries
}

func Open(fs vfs.FS, dir string) (*SegmentedWAL, error) {
	if err := fs.MkdirAll(dir); err != nil {
		return nil, err
	}

	w := &SegmentedWAL{
		fs:      fs,
		dir:     dir,
		entries: make(map[uint64]raft.Entry),
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

	// Scan through all segments in order
	for _, id := range segIDs {
		path := w.segmentPath(id)
		f, err := w.fs.Open(path)
		if err != nil {
			return err
		}

		var offset int64
		for {
			var hdr [RecordHeaderSize]byte
			n, rErr := f.ReadAt(hdr[:], offset)
			if rErr == io.EOF || n < RecordHeaderSize {
				// Clean EOF or torn partial header at tail -> truncate and stop
				_ = f.Truncate(offset)
				break
			}

			expectedCRC := binary.LittleEndian.Uint32(hdr[0:4])
			length := binary.LittleEndian.Uint32(hdr[4:8])
			recType := hdr[8]
			term := binary.LittleEndian.Uint64(hdr[9:17])
			index := binary.LittleEndian.Uint64(hdr[17:25])

			payload := make([]byte, length)
			pn, pErr := f.ReadAt(payload, offset+RecordHeaderSize)
			if pErr != nil || uint32(pn) < length {
				// Torn write on payload -> truncate to offset and stop
				_ = f.Truncate(offset)
				break
			}

			// Validate CRC over [Type | Term | Index | Payload]
			h := crc32.New(crcTable)
			h.Write(hdr[8:RecordHeaderSize])
			h.Write(payload)
			if h.Sum32() != expectedCRC {
				// Corrupted record -> truncate to last valid offset
				_ = f.Truncate(offset)
				break
			}

			if recType == TypeEntry {
				w.entries[index] = raft.Entry{
					Term:  term,
					Index: index,
					Data:  payload,
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

// TruncateSuffix removes all entries starting from fromIndex (used when follower encounters conflict).
func (w *SegmentedWAL) TruncateSuffix(fromIndex uint64) {
	for idx := fromIndex; idx <= w.lastIndex; idx++ {
		delete(w.entries, idx)
	}
	if fromIndex > 0 {
		w.lastIndex = fromIndex - 1
	} else {
		w.lastIndex = 0
	}
}

func (w *SegmentedWAL) Sync() error {
	if w.activeFile != nil {
		return w.activeFile.Sync()
	}
	return nil
}

func (w *SegmentedWAL) LastIndex() uint64 {
	return w.lastIndex
}

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
