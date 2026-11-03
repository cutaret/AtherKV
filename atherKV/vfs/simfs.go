package vfs

import (
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cutaret/aetherkv/sim"
)

const SectorSize = 512

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
	durableMeta    bool
	isDeleted      bool
	overwritten    *simFileRecord // Previous durable record if overwritten by an unsynced Rename
}

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
	cleaned := filepath.Clean(filepath.ToSlash(p))
	cleaned = strings.TrimPrefix(cleaned, "/")
	cleaned = strings.TrimPrefix(cleaned, "./")
	if cleaned == "" {
		cleaned = "."
	}
	return cleaned
}

func dirOf(p string) string {
	d := filepath.Dir(filepath.ToSlash(p))
	return cleanPath(d)
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
	d := dirOf(cleaned)
	s.dirs[d] = true

	rec, ok := s.files[cleaned]
	if !ok || rec.isDeleted {
		rec = &simFileRecord{
			name:        cleaned,
			dir:         d,
			durableMeta: false,
			isDeleted:   false,
		}
		s.files[cleaned] = rec
	} else {
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

	newDir := dirOf(newClean)
	s.dirs[newDir] = true

	var prevDurable *simFileRecord
	if existing, exists := s.files[newClean]; exists && existing.durableMeta && !existing.isDeleted {
		prevDurable = &simFileRecord{
			name:         existing.name,
			dir:          existing.dir,
			durableData:  slices.Clone(existing.durableData),
			volatileData: slices.Clone(existing.durableData),
			durableMeta:  true,
			isDeleted:    false,
		}
	}

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
		overwritten:    prevDurable,
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
		if dirOf(path) == cleaned {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (s *SimFS) SyncDir(dir string) error {
	cleaned := cleanPath(dir)
	for path, rec := range s.files {
		if dirOf(path) == cleaned {
			rec.durableMeta = true
			rec.overwritten = nil
			if rec.isDeleted {
				delete(s.files, path)
			}
		}
	}
	return nil
}

func (s *SimFS) Crash(rng sim.Rand) {
	for path, rec := range s.files {
		// Revert any file creation, rename, or deletion whose parent directory was not SyncDir'd
		if !rec.durableMeta {
			if rec.isDeleted {
				rec.isDeleted = false
				rec.durableMeta = true
			} else if rec.overwritten != nil {
				s.files[path] = rec.overwritten
				continue
			} else {
				delete(s.files, path)
				continue
			}
		}

		if len(rec.unsyncedWrites) == 0 {
			rec.volatileData = slices.Clone(rec.durableData)
			continue
		}

		for _, w := range rec.unsyncedWrites {
			if w.len <= 0 {
				continue
			}
			startSector := w.off / SectorSize
			endSector := (w.off + w.len + SectorSize - 1) / SectorSize

			for sec := startSector; sec < endSector; sec++ {
				secOff := sec * SectorSize
				secEnd := secOff + SectorSize

				writeStart := max(secOff, w.off)
				writeEnd := min(secEnd, w.off+w.len)
				if writeStart >= writeEnd {
					continue
				}

				fate := rng.Intn(3)
				switch fate {
				case 0:
					s.copyRange(rec, writeStart, writeEnd, true)
				case 1:
					// Unsynced write lost: durableData in [writeStart, writeEnd) remains unchanged
				case 2:
					span := int(writeEnd - writeStart)
					tearPoint := writeStart + int64(rng.Intn(span))
					s.copyRange(rec, writeStart, tearPoint, true)
					s.corruptRange(rec, tearPoint, writeEnd)
				}
			}
		}

		rec.volatileData = slices.Clone(rec.durableData)
		rec.unsyncedWrites = nil
	}
}

func (s *SimFS) copyRange(rec *simFileRecord, from, to int64, volatileToDurable bool) {
	if volatileToDurable {
		if int(to) > len(rec.durableData) {
			newD := make([]byte, to)
			copy(newD, rec.durableData)
			rec.durableData = newD
		}
		end := min(int(to), len(rec.volatileData))
		if int(from) < end {
			copy(rec.durableData[from:end], rec.volatileData[from:end])
		}
	} else {
		if int(to) > len(rec.volatileData) {
			newVol := make([]byte, to)
			copy(newVol, rec.volatileData)
			rec.volatileData = newVol
		}
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
		rec.durableData[i] = 0xAA
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
	if reqEnd > int64(cap(h.rec.volatileData)) {
		newCap := max(reqEnd, int64(cap(h.rec.volatileData))*2)
		newCap = max(newCap, 4096)
		newVol := make([]byte, reqEnd, newCap)
		copy(newVol, h.rec.volatileData)
		h.rec.volatileData = newVol
	} else if reqEnd > int64(len(h.rec.volatileData)) {
		h.rec.volatileData = h.rec.volatileData[:reqEnd]
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
		prevLen := int64(len(h.rec.volatileData))
		h.rec.volatileData = h.rec.volatileData[:size]
		h.rec.unsyncedWrites = append(h.rec.unsyncedWrites, simWriteRange{off: size, len: prevLen - size})
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
