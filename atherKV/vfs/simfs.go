package vfs

import (
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cutaret/aetherkv/sim"
)

const SectorSize = 512 // Standard disk sector boundary for torn writes

type simWriteRange struct {
	off int64
	len int64
}

type simFileRecord struct {
	name           string
	dir            string
	durableData    []byte
	volatileData   []byte
	unsyncedWrites []simWriteRange
	durableMeta    bool // False if creation/rename/remove has not been SyncDir'd
	isDeleted      bool
}

// SimFS is an in-memory crashable filesystem that models power-loss and torn writes.
type SimFS struct {
	files map[string]*simFileRecord
	dirs  map[string]bool
}

func NewSimFS() *SimFS {
	fs := &SimFS{
		files: make(map[string]*simFileRecord),
		dirs:  make(map[string]bool),
	}
	fs.dirs["."] = true
	return fs
}

func cleanPath(p string) string {
	cleaned := filepath.Clean(p)
	if strings.HasPrefix(cleaned, "/") {
		cleaned = strings.TrimPrefix(cleaned, "/")
	}
	if cleaned == "" {
		cleaned = "."
	}
	return cleaned
}

func (s *SimFS) MkdirAll(dir string) error {
	cleaned := cleanPath(dir)
	s.dirs[cleaned] = true
	return nil
}

func (s *SimFS) Open(name string) (File, error) {
	cleaned := cleanPath(name)
	rec, ok := s.files[cleaned]
	if !ok || rec.isDeleted {
		return nil, ErrNotFound
	}
	return &simFileHandle{fs: s, rec: rec, closed: false}, nil
}

func (s *SimFS) Create(name string) (File, error) {
	cleaned := cleanPath(name)
	dir := cleanPath(filepath.Dir(cleaned))
	s.dirs[dir] = true

	rec, ok := s.files[cleaned]
	if !ok || rec.isDeleted {
		rec = &simFileRecord{
			name:         cleaned,
			dir:          dir,
			durableData:  nil,
			volatileData: nil,
			durableMeta:  false, // Requires SyncDir to survive crash
			isDeleted:    false,
		}
		s.files[cleaned] = rec
	} else {
		// Truncate existing
		rec.volatileData = nil
		rec.unsyncedWrites = append(rec.unsyncedWrites, simWriteRange{off: 0, len: int64(len(rec.durableData))})
	}
	return &simFileHandle{fs: s, rec: rec, closed: false}, nil
}

func (s *SimFS) Rename(oldpath, newpath string) error {
	oldClean := cleanPath(oldpath)
	newClean := cleanPath(newpath)

	rec, ok := s.files[oldClean]
	if !ok || rec.isDeleted {
		return ErrNotFound
	}

	newDir := cleanPath(filepath.Dir(newClean))
	s.dirs[newDir] = true

	// Mark old deleted and assign to new path (uncommitted meta until SyncDir)
	rec.isDeleted = true
	rec.durableMeta = false

	s.files[newClean] = &simFileRecord{
		name:           newClean,
		dir:            newDir,
		durableData:    slices.Clone(rec.durableData),
		volatileData:   slices.Clone(rec.volatileData),
		unsyncedWrites: slices.Clone(rec.unsyncedWrites),
		durableMeta:    false,
		isDeleted:      false,
	}
	return nil
}

func (s *SimFS) Remove(name string) error {
	cleaned := cleanPath(name)
	rec, ok := s.files[cleaned]
	if !ok || rec.isDeleted {
		return ErrNotFound
	}
	rec.isDeleted = true
	rec.durableMeta = false
	return nil
}

func (s *SimFS) List(dir string) ([]string, error) {
	cleaned := cleanPath(dir)
	var names []string
	for path, rec := range s.files {
		if rec.isDeleted {
			continue
		}
		if cleanPath(filepath.Dir(path)) == cleaned {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names) // Maintain strict determinism
	return names, nil
}

// SyncDir commits directory metadata changes (file additions, renames, removals).
func (s *SimFS) SyncDir(dir string) error {
	cleaned := cleanPath(dir)
	for path, rec := range s.files {
		if cleanPath(filepath.Dir(path)) == cleaned {
			rec.durableMeta = true
			if rec.isDeleted {
				delete(s.files, path)
			}
		}
	}
	return nil
}

// Crash simulates immediate power loss and tears uncommitted writes.
func (s *SimFS) Crash(rng sim.Rand) {
	for path, rec := range s.files {
		// Revert metadata changes if the parent directory was not synced
		if !rec.durableMeta {
			if rec.isDeleted {
				// Undelete file
				rec.isDeleted = false
				rec.durableMeta = true
			} else if rec.durableData == nil {
				// Delete newly created uncommitted file
				delete(s.files, path)
				continue
			}
		}

		if len(rec.unsyncedWrites) == 0 {
			continue
		}

		// Process torn writes at SectorSize (512-byte) granularity
		for _, w := range rec.unsyncedWrites {
			startSector := w.off / SectorSize
			endSector := (w.off + w.len + SectorSize - 1) / SectorSize

			for sec := startSector; sec < endSector; sec++ {
				secOff := sec * SectorSize
				secEnd := secOff + SectorSize

				// Determine torn outcome:
				// 0 = survives cleanly
				// 1 = completely lost (reverts to durableData)
				// 2 = torn write (corrupted midway)
				fate := rng.Intn(3)
				switch fate {
				case 0:
					// Promoted to durable
					s.copyRange(rec, secOff, secEnd, true)
				case 1:
					// Reverted to durable (volatile restored from durable)
					s.copyRange(rec, secOff, secEnd, false)
				case 2:
					// Torn: partial bytes survive, remainder filled with garbage
					tearPoint := secOff + int64(rng.Intn(SectorSize))
					s.copyRange(rec, secOff, tearPoint, true)
					s.corruptRange(rec, tearPoint, secEnd)
				}
			}
		}

		// Realign volatile to durable after crash
		rec.volatileData = slices.Clone(rec.durableData)
		rec.unsyncedWrites = nil
	}
}

func (s *SimFS) copyRange(rec *simFileRecord, from, to int64, volatileToDurable bool) {
	if volatileToDurable {
		if int(to) > len(rec.durableData) {
			newDurable := make([]byte, to)
			copy(newDurable, rec.durableData)
			rec.durableData = newDurable
		}
		end := min(int(to), len(rec.volatileData))
		if int(from) < end {
			copy(rec.durableData[from:end], rec.volatileData[from:end])
		}
	} else {
		// Restore volatile from durable
		end := min(int(to), len(rec.durableData))
		if int(from) < end {
			copy(rec.volatileData[from:end], rec.durableData[from:end])
		}
	}
}

func (s *SimFS) corruptRange(rec *simFileRecord, from, to int64) {
	if int(to) > len(rec.durableData) {
		newDurable := make([]byte, to)
		copy(newDurable, rec.durableData)
		rec.durableData = newDurable
	}
	for i := from; i < to && i < int64(len(rec.durableData)); i++ {
		rec.durableData[i] = 0xAA // Distinctive bit corruption pattern
	}
}

type simFileHandle struct {
	fs     *SimFS
	rec    *simFileRecord
	closed bool
}

func (h *simFileHandle) ReadAt(p []byte, off int64) (int, error) {
	if h.closed {
		return 0, ErrClosed
	}
	if off >= int64(len(h.rec.volatileData)) {
		return 0, io.EOF
	}
	n := copy(p, h.rec.volatileData[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *simFileHandle) WriteAt(p []byte, off int64) (int, error) {
	if h.closed {
		return 0, ErrClosed
	}
	reqEnd := off + int64(len(p))
	if reqEnd > int64(len(h.rec.volatileData)) {
		newVol := make([]byte, reqEnd)
		copy(newVol, h.rec.volatileData)
		h.rec.volatileData = newVol
	}
	copy(h.rec.volatileData[off:], p)
	h.rec.unsyncedWrites = append(h.rec.unsyncedWrites, simWriteRange{off: off, len: int64(len(p))})
	return len(p), nil
}

func (h *simFileHandle) Truncate(size int64) error {
	if h.closed {
		return ErrClosed
	}
	if size < int64(len(h.rec.volatileData)) {
		h.rec.volatileData = h.rec.volatileData[:size]
		h.rec.unsyncedWrites = append(h.rec.unsyncedWrites, simWriteRange{off: size, len: 0})
	}
	return nil
}

func (h *simFileHandle) Sync() error {
	if h.closed {
		return ErrClosed
	}
	h.rec.durableData = slices.Clone(h.rec.volatileData)
	h.rec.unsyncedWrites = nil
	return nil
}

func (h *simFileHandle) Size() (int64, error) {
	if h.closed {
		return 0, ErrClosed
	}
	return int64(len(h.rec.volatileData)), nil
}

func (h *simFileHandle) Close() error {
	h.closed = true
	return nil
}
