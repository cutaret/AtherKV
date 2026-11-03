package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cutaret/aetherkv/vfs"
)

const MaxLevels = 3

// FileMeta tracks an immutable SSTable in a specific LSM level.
type FileMeta struct {
	FileNum uint64 `json:"file_num"`
	Level   int    `json:"level"`
	MinKey  []byte `json:"min_key"`
	MaxKey  []byte `json:"max_key"`
}

// DeletedFile identifies an SSTable removed during compaction.
type DeletedFile struct {
	Level   int    `json:"level"`
	FileNum uint64 `json:"file_num"`
}

// VersionEdit represents an atomic transition in the LSM version set logged to MANIFEST.
type VersionEdit struct {
	NextFileNum  uint64        `json:"next_file_num,omitempty"`
	LastSeq      uint64        `json:"last_seq,omitempty"`
	FlushedIndex uint64        `json:"flushed_index,omitempty"`
	AddedFiles   []FileMeta    `json:"added_files,omitempty"`
	DeletedFiles []DeletedFile `json:"deleted_files,omitempty"`
}

// VersionState holds the reconstructed active SSTables across levels.
type VersionState struct {
	NextFileNum  uint64
	LastSeq      uint64
	FlushedIndex uint64
	Levels       [MaxLevels][]FileMeta
}

func NewVersionState() *VersionState {
	return &VersionState{
		NextFileNum: 1,
		LastSeq:     0,
	}
}

func (vs *VersionState) ApplyEdit(edit VersionEdit) {
	if edit.NextFileNum > vs.NextFileNum {
		vs.NextFileNum = edit.NextFileNum
	}
	if edit.LastSeq > vs.LastSeq {
		vs.LastSeq = edit.LastSeq
	}
	if edit.FlushedIndex > vs.FlushedIndex {
		vs.FlushedIndex = edit.FlushedIndex
	}

	for _, del := range edit.DeletedFiles {
		if del.Level < 0 || del.Level >= MaxLevels {
			continue
		}
		var kept []FileMeta
		for _, fm := range vs.Levels[del.Level] {
			if fm.FileNum != del.FileNum {
				kept = append(kept, fm)
			}
		}
		vs.Levels[del.Level] = kept
	}

	for _, add := range edit.AddedFiles {
		if add.Level < 0 || add.Level >= MaxLevels {
			continue
		}
		vs.Levels[add.Level] = append(vs.Levels[add.Level], FileMeta{
			FileNum: add.FileNum,
			Level:   add.Level,
			MinKey:  bytes.Clone(add.MinKey),
			MaxKey:  bytes.Clone(add.MaxKey),
		})
	}

	slices.SortFunc(vs.Levels[0], func(a, b FileMeta) int {
		if a.FileNum < b.FileNum {
			return -1
		}
		if a.FileNum > b.FileNum {
			return 1
		}
		return 0
	})
	for lvl := 1; lvl < MaxLevels; lvl++ {
		slices.SortFunc(vs.Levels[lvl], func(a, b FileMeta) int {
			return bytes.Compare(a.MinKey, b.MinKey)
		})
	}
}

func (vs *VersionState) SnapshotEdit() VersionEdit {
	edit := VersionEdit{
		NextFileNum:  vs.NextFileNum,
		LastSeq:      vs.LastSeq,
		FlushedIndex: vs.FlushedIndex,
	}
	for lvl := 0; lvl < MaxLevels; lvl++ {
		for _, fm := range vs.Levels[lvl] {
			edit.AddedFiles = append(edit.AddedFiles, FileMeta{
				FileNum: fm.FileNum,
				Level:   fm.Level,
				MinKey:  bytes.Clone(fm.MinKey),
				MaxKey:  bytes.Clone(fm.MaxKey),
			})
		}
	}
	return edit
}

// LogAndApplyManifest writes a new MANIFEST file, points CURRENT to it via atomic Rename,
// and calls SyncDir (unless Bug09SkipManifestSync is enabled).
func LogAndApplyManifest(fs vfs.FS, dir string, vs *VersionState, edit VersionEdit, bugs LSMBugConfig) error {
	vs.ApplyEdit(edit)
	manifestNum := vs.NextFileNum
	vs.NextFileNum++

	manifestName := fmt.Sprintf("MANIFEST-%06d", manifestNum)
	manifestPath := filepath.Join(dir, manifestName)

	fullEdit := vs.SnapshotEdit()
	payload, err := json.Marshal(fullEdit)
	if err != nil {
		return err
	}

	mf, err := fs.Create(manifestPath)
	if err != nil {
		return err
	}
	if _, err := mf.WriteAt(append(payload, '\n'), 0); err != nil {
		_ = mf.Close()
		return err
	}
	if err := mf.Sync(); err != nil {
		_ = mf.Close()
		return err
	}
	_ = mf.Close()

	tmpCurrent := filepath.Join(dir, "CURRENT.tmp")
	cf, err := fs.Create(tmpCurrent)
	if err != nil {
		return err
	}
	if _, err := cf.WriteAt([]byte(manifestName+"\n"), 0); err != nil {
		_ = cf.Close()
		return err
	}
	if err := cf.Sync(); err != nil {
		_ = cf.Close()
		return err
	}
	_ = cf.Close()

	currentPath := filepath.Join(dir, "CURRENT")
	if err := fs.Rename(tmpCurrent, currentPath); err != nil {
		return err
	}

	if !bugs.Bug09SkipManifestSync {
		if err := fs.SyncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

// RecoverManifest reads CURRENT and reconstructs the VersionState from the active MANIFEST.
func RecoverManifest(fs vfs.FS, dir string) (*VersionState, bool, error) {
	currentPath := filepath.Join(dir, "CURRENT")
	cf, err := fs.Open(currentPath)
	if err != nil {
		return NewVersionState(), false, nil
	}
	defer cf.Close()

	sz, err := cf.Size()
	if err != nil || sz <= 0 {
		return nil, false, ErrCorruptBatch
	}
	buf := make([]byte, sz)
	n, _ := cf.ReadAt(buf, 0)
	manifestName := strings.TrimSpace(string(buf[:n]))
	if manifestName == "" {
		return nil, false, ErrCorruptBatch
	}

	mf, err := fs.Open(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, false, err
	}
	defer mf.Close()

	msz, err := mf.Size()
	if err != nil || msz <= 0 {
		return nil, false, ErrCorruptBatch
	}
	mbuf := make([]byte, msz)
	mn, _ := mf.ReadAt(mbuf, 0)

	vs := NewVersionState()
	lines := strings.Split(strings.TrimSpace(string(mbuf[:mn])), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var edit VersionEdit
		if err := json.Unmarshal([]byte(line), &edit); err != nil {
			return nil, false, err
		}
		vs.ApplyEdit(edit)
	}
	return vs, true, nil
}
