package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"slices"

	"github.com/cutaret/aetherkv/sim"
	"github.com/cutaret/aetherkv/statemachine"
	"github.com/cutaret/aetherkv/vfs"
)

const (
	sysAppliedIndexKey = "\x00sys:applied_index"
	sysSessionPrefix   = "\x00sys:sess:"
	userKeyPrefix      = "u:"
)

type lsmSessionRecord struct {
	ClientID   uint64              `json:"client_id"`
	LastSeqNum uint64              `json:"last_seq_num"`
	LastResult statemachine.Result `json:"last_result"`
}

type Options struct {
	MemtableFlushThreshold int    // Flush memtable when ApproximateSize >= threshold (default 16 KiB)
	L0CompactionTrigger    int    // Compact L0 -> L1 when len(L0) >= trigger (default 3)
	L1CompactionTrigger    int    // Compact L1 -> L2 when len(L1) >= trigger (default 4)
	BloomBitsPerKey        int    // Default 10
	ValueLogThreshold      int    // If > 0, values >= threshold are stored in WiscKey ValueLog
	ValueLogMaxSegBytes    uint64 // Max bytes per vlog segment before rotation
	DisableWALSync         bool   // Option B: skip LSM WAL sync and rely on Raft log up to FlushedIndex
	Bugs                   LSMBugConfig
}

func DefaultOptions() Options {
	return Options{
		MemtableFlushThreshold: 16 * 1024,
		L0CompactionTrigger:    3,
		L1CompactionTrigger:    4,
		BloomBitsPerKey:        10,
		ValueLogThreshold:      0,
		ValueLogMaxSegBytes:    16 * 1024 * 1024,
		DisableWALSync:         false, // Option A by default
	}
}

// LSMStore implements statemachine.StateMachine on top of a crash-safe LSM + WiscKey engine.
type LSMStore struct {
	fs              vfs.FS
	dir             string
	rng             sim.Rand
	opts            Options
	vs              *VersionState
	mem             *Memtable
	wal             *WAL
	vlog            *ValueLog
	lastSeq         uint64
	appliedIdx      uint64
	flushedIdx      uint64
	sessions        []lsmSessionRecord
	pinnedSnapshots []uint64

	logicalBytesWritten uint64
	lsmDiskBytesWritten uint64
	fsyncCount          uint64
}

func OpenLSMStore(fs vfs.FS, dir string, rng sim.Rand, opts Options) (*LSMStore, error) {
	if opts.MemtableFlushThreshold <= 0 {
		opts.MemtableFlushThreshold = 16 * 1024
	}
	if opts.L0CompactionTrigger <= 0 {
		opts.L0CompactionTrigger = 3
	}
	if opts.L1CompactionTrigger <= 0 {
		opts.L1CompactionTrigger = 4
	}
	if opts.BloomBitsPerKey <= 0 {
		opts.BloomBitsPerKey = 10
	}

	if err := fs.MkdirAll(dir); err != nil {
		return nil, err
	}
	_ = fs.SyncDir(".")

	vs, foundManifest, err := RecoverManifest(fs, dir)
	if err != nil {
		return nil, err
	}

	vl, err := OpenValueLog(fs, dir, opts.ValueLogMaxSegBytes, opts.Bugs)
	if err != nil {
		return nil, err
	}

	db := &LSMStore{
		fs:         fs,
		dir:        dir,
		rng:        rng,
		opts:       opts,
		vs:         vs,
		mem:        NewMemtable(rng),
		vlog:       vl,
		lastSeq:    vs.LastSeq,
		flushedIdx: vs.FlushedIndex,
	}

	if !foundManifest {
		initBugs := opts.Bugs
		initBugs.Bug09SkipManifestSync = false
		if err := LogAndApplyManifest(fs, dir, db.vs, VersionEdit{NextFileNum: 2, LastSeq: 0}, initBugs); err != nil {
			return nil, err
		}
	}

	for lvl := 0; lvl < MaxLevels; lvl++ {
		for _, fm := range db.vs.Levels[lvl] {
			if _, err := OpenSSTable(fs, db.sstPath(fm.FileNum)); err != nil {
				return nil, err
			}
		}
	}

	activeWALPath := filepath.Join(dir, "active.wal")
	recoveredUnflushed := false
	// Under Option B (DisableWALSync), the LSM relies on Raft log replay rather than active.wal
	if !opts.DisableWALSync {
		if batches, recErr := RecoverWAL(fs, activeWALPath, opts.Bugs); recErr == nil && len(batches) > 0 {
			for _, wb := range batches {
				endSeq := wb.StartSeq + uint64(len(wb.Entries)) - 1
				if endSeq <= db.vs.LastSeq && !opts.Bugs.Bug08AcceptTornWAL {
					continue
				}
				db.mem.ApplyBatch(wb)
				recoveredUnflushed = true
				if endSeq > db.lastSeq {
					db.lastSeq = endSeq
				}
			}
		}
	}

	if recoveredUnflushed && db.mem.Count() > 0 {
		if err := db.flushMemtableOnly(); err != nil {
			return nil, err
		}
	}

	w, err := CreateWAL(fs, activeWALPath)
	if err != nil {
		return nil, err
	}
	_ = w.Sync()
	if !opts.Bugs.Bug09SkipManifestSync {
		_ = fs.SyncDir(dir)
	}
	db.wal = w

	if err := db.loadSystemState(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *LSMStore) sstPath(fileNum uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", fileNum))
}

func encodeUserKey(k []byte) []byte {
	out := make([]byte, len(userKeyPrefix)+len(k))
	copy(out, userKeyPrefix)
	copy(out[len(userKeyPrefix):], k)
	return out
}

func isUserKey(internalUserKey []byte) (rawKey []byte, ok bool) {
	if bytes.HasPrefix(internalUserKey, []byte(userKeyPrefix)) {
		return bytes.Clone(internalUserKey[len(userKeyPrefix):]), true
	}
	return nil, false
}

func sessionKey(clientID uint64) []byte {
	return []byte(fmt.Sprintf("%s%020d", sysSessionPrefix, clientID))
}

func (db *LSMStore) findSession(clientID uint64) (int, bool) {
	return slices.BinarySearchFunc(db.sessions, clientID, func(e lsmSessionRecord, target uint64) int {
		if e.ClientID < target {
			return -1
		}
		if e.ClientID > target {
			return 1
		}
		return 0
	})
}

func (db *LSMStore) resolveValue(vType ValueType, rawVal []byte) ([]byte, error) {
	if vType == ValTypeValue {
		return bytes.Clone(rawVal), nil
	}
	if vType == ValTypeVLogPtr {
		ptr, err := DecodeVLogPointer(rawVal)
		if err != nil {
			return nil, err
		}
		return db.vlog.Read(ptr)
	}
	return nil, nil
}

func (db *LSMStore) loadSystemState() error {
	if val, ok, err := db.getRawKey([]byte(sysAppliedIndexKey), math.MaxUint64); err != nil {
		return err
	} else if ok && len(val) == 8 {
		db.appliedIdx = binary.LittleEndian.Uint64(val)
	}

	it, err := db.buildMergedIterator()
	if err != nil {
		return err
	}
	it.Seek([]byte(sysSessionPrefix))
	var lastSeenKey []byte
	for it.Valid() {
		ikey := it.Key()
		if !bytes.HasPrefix(ikey.UserKey, []byte(sysSessionPrefix)) {
			break
		}
		if lastSeenKey == nil || !bytes.Equal(lastSeenKey, ikey.UserKey) {
			lastSeenKey = bytes.Clone(ikey.UserKey)
			if ikey.Type == ValTypeValue {
				var rec lsmSessionRecord
				if err := json.Unmarshal(it.Value(), &rec); err == nil {
					idx, found := db.findSession(rec.ClientID)
					if found {
						db.sessions[idx] = rec
					} else {
						db.sessions = slices.Insert(db.sessions, idx, rec)
					}
				}
			}
		}
		it.Next()
	}
	return nil
}

func (db *LSMStore) putIntoBatch(batch *WriteBatch, encKey, val []byte) (usedVLog bool) {
	db.logicalBytesWritten += uint64(len(encKey) + len(val))
	if db.opts.ValueLogThreshold > 0 && len(val) >= db.opts.ValueLogThreshold {
		ptr, err := db.vlog.Append(encKey, val)
		if err == nil {
			batch.PutVLogPtr(encKey, ptr)
			return true
		}
	}
	batch.Put(encKey, val)
	return false
}

func (db *LSMStore) ApplyBatch(appliedIndex uint64, ops []statemachine.Op) []statemachine.Result {
	results := make([]statemachine.Result, len(ops))
	dataBatch := NewWriteBatch(db.lastSeq + 1)
	touchedVLog := false

	type batchOverlay struct {
		deleted bool
		val     []byte
	}
	var overlayKeys [][]byte
	var overlayVals []batchOverlay

	getOverlay := func(uKey []byte) ([]byte, bool, error) {
		for i, k := range overlayKeys {
			if bytes.Equal(k, uKey) {
				if overlayVals[i].deleted {
					return nil, false, nil
				}
				return bytes.Clone(overlayVals[i].val), true, nil
			}
		}
		return db.Get(uKey)
	}

	setOverlay := func(uKey, val []byte, deleted bool) {
		for i, k := range overlayKeys {
			if bytes.Equal(k, uKey) {
				overlayVals[i] = batchOverlay{deleted: deleted, val: bytes.Clone(val)}
				return
			}
		}
		overlayKeys = append(overlayKeys, bytes.Clone(uKey))
		overlayVals = append(overlayVals, batchOverlay{deleted: deleted, val: bytes.Clone(val)})
	}

	for i, op := range ops {
		if op.ClientID != 0 {
			sIdx, found := db.findSession(op.ClientID)
			if found && op.SeqNum <= db.sessions[sIdx].LastSeqNum {
				dupRes := db.sessions[sIdx].LastResult
				dupRes.Deduplicated = true
				results[i] = dupRes
				continue
			}
		}

		var res statemachine.Result
		encKey := encodeUserKey(op.Key)

		switch op.Type {
		case statemachine.OpPut:
			if db.putIntoBatch(dataBatch, encKey, op.Value) {
				touchedVLog = true
			}
			setOverlay(op.Key, op.Value, false)
			res = statemachine.Result{Applied: true, Value: bytes.Clone(op.Value)}

		case statemachine.OpDelete:
			_, existed, _ := getOverlay(op.Key)
			dataBatch.Delete(encKey)
			setOverlay(op.Key, nil, true)
			res = statemachine.Result{Applied: true, PreviousFound: existed}

		case statemachine.OpCAS:
			curVal, existed, _ := getOverlay(op.Key)
			if existed && bytes.Equal(curVal, op.Expected) {
				if db.putIntoBatch(dataBatch, encKey, op.Value) {
					touchedVLog = true
				}
				setOverlay(op.Key, op.Value, false)
				res = statemachine.Result{Applied: true, PreviousFound: true, Value: bytes.Clone(op.Value)}
			} else if !existed && len(op.Expected) == 0 {
				if db.putIntoBatch(dataBatch, encKey, op.Value) {
					touchedVLog = true
				}
				setOverlay(op.Key, op.Value, false)
				res = statemachine.Result{Applied: true, PreviousFound: false, Value: bytes.Clone(op.Value)}
			} else {
				res = statemachine.Result{Applied: false, PreviousFound: existed, Value: curVal}
			}
		}

		if op.ClientID != 0 && !db.opts.Bugs.Bug14SplitApplyIndex {
			rec := lsmSessionRecord{
				ClientID:   op.ClientID,
				LastSeqNum: op.SeqNum,
				LastResult: res,
			}
			sIdx, found := db.findSession(op.ClientID)
			if found {
				db.sessions[sIdx] = rec
			} else {
				db.sessions = slices.Insert(db.sessions, sIdx, rec)
			}
			recBytes, _ := json.Marshal(rec)
			dataBatch.Put(sessionKey(op.ClientID), recBytes)
		}

		results[i] = res
	}

	if touchedVLog && !db.opts.Bugs.Bug15DanglingVLogPtr {
		_ = db.vlog.Sync()
		db.fsyncCount++
	}

	var idxBuf [8]byte
	binary.LittleEndian.PutUint64(idxBuf[:], appliedIndex)

	if db.opts.Bugs.Bug14SplitApplyIndex {
		if len(dataBatch.Entries) > 0 {
			_ = db.commitBatch(dataBatch, true)
		}
		idxBatch := NewWriteBatch(db.lastSeq + 1)
		idxBatch.Put([]byte(sysAppliedIndexKey), idxBuf[:])
		_ = db.commitBatch(idxBatch, false)
		db.appliedIdx = appliedIndex
		return results
	}

	dataBatch.Put([]byte(sysAppliedIndexKey), idxBuf[:])
	syncWAL := !db.opts.DisableWALSync
	_ = db.commitBatch(dataBatch, syncWAL)
	db.appliedIdx = appliedIndex

	if db.mem.ApproximateSize() >= db.opts.MemtableFlushThreshold {
		_ = db.Flush()
	}
	return results
}

func (db *LSMStore) commitBatch(wb *WriteBatch, syncWAL bool) error {
	if len(wb.Entries) == 0 {
		return nil
	}
	if !db.opts.DisableWALSync || !syncWAL {
		// In Option A (or when testing unsynced WAL in Zoo #14), write to LSM WAL
		if !db.opts.DisableWALSync {
			encoded := wb.Encode()
			db.lsmDiskBytesWritten += uint64(len(encoded) + WALHeaderSize)
			if err := db.wal.AppendGroup([]*WriteBatch{wb}, syncWAL); err != nil {
				return err
			}
			if syncWAL {
				db.fsyncCount++
			}
		}
	}
	db.mem.ApplyBatch(wb)
	db.lastSeq = wb.StartSeq + uint64(len(wb.Entries)) - 1
	return nil
}

func (db *LSMStore) PutDirect(key, value []byte) error {
	db.ApplyBatch(db.appliedIdx+1, []statemachine.Op{
		{Type: statemachine.OpPut, Key: key, Value: value},
	})
	return nil
}

func (db *LSMStore) DeleteDirect(key []byte) error {
	db.ApplyBatch(db.appliedIdx+1, []statemachine.Op{
		{Type: statemachine.OpDelete, Key: key},
	})
	return nil
}

func (db *LSMStore) Get(key []byte) ([]byte, bool, error) {
	return db.getRawKey(encodeUserKey(key), math.MaxUint64)
}

func (db *LSMStore) getRawKey(rawKey []byte, snapshotSeq uint64) ([]byte, bool, error) {
	if val, vType, ok := db.mem.Get(rawKey, snapshotSeq); ok {
		if vType == ValTypeTombstone {
			return nil, false, nil
		}
		resolved, err := db.resolveValue(vType, val)
		if err != nil {
			return nil, false, err
		}
		return resolved, true, nil
	}

	l0 := db.vs.Levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		fm := l0[i]
		if bytes.Compare(rawKey, fm.MinKey) < 0 || bytes.Compare(rawKey, fm.MaxKey) > 0 {
			continue
		}
		r, err := OpenSSTable(db.fs, db.sstPath(fm.FileNum))
		if err != nil {
			return nil, false, err
		}
		val, vType, found, err := r.Get(rawKey, snapshotSeq)
		if err != nil {
			return nil, false, err
		}
		if found {
			if vType == ValTypeTombstone {
				return nil, false, nil
			}
			resolved, rErr := db.resolveValue(vType, val)
			if rErr != nil {
				return nil, false, rErr
			}
			return resolved, true, nil
		}
	}

	for lvl := 1; lvl < MaxLevels; lvl++ {
		for _, fm := range db.vs.Levels[lvl] {
			if bytes.Compare(rawKey, fm.MinKey) < 0 || bytes.Compare(rawKey, fm.MaxKey) > 0 {
				continue
			}
			r, err := OpenSSTable(db.fs, db.sstPath(fm.FileNum))
			if err != nil {
				return nil, false, err
			}
			val, vType, found, err := r.Get(rawKey, snapshotSeq)
			if err != nil {
				return nil, false, err
			}
			if found {
				if vType == ValTypeTombstone {
					return nil, false, nil
				}
				resolved, rErr := db.resolveValue(vType, val)
				if rErr != nil {
					return nil, false, rErr
				}
				return resolved, true, nil
			}
		}
	}

	return nil, false, nil
}

func (db *LSMStore) buildMergedIterator() (Iterator, error) {
	var iters []Iterator
	iters = append(iters, db.mem.NewIterator())

	for i := len(db.vs.Levels[0]) - 1; i >= 0; i-- {
		r, err := OpenSSTable(db.fs, db.sstPath(db.vs.Levels[0][i].FileNum))
		if err != nil {
			return nil, err
		}
		it, err := r.NewIterator()
		if err != nil {
			return nil, err
		}
		iters = append(iters, it)
	}

	for lvl := 1; lvl < MaxLevels; lvl++ {
		for _, fm := range db.vs.Levels[lvl] {
			r, err := OpenSSTable(db.fs, db.sstPath(fm.FileNum))
			if err != nil {
				return nil, err
			}
			it, err := r.NewIterator()
			if err != nil {
				return nil, err
			}
			iters = append(iters, it)
		}
	}
	return NewMergingIterator(iters), nil
}

func (db *LSMStore) Scan(startKey, endKey []byte, limit int) ([]statemachine.KV, error) {
	it, err := db.buildMergedIterator()
	if err != nil {
		return nil, err
	}

	encStart := encodeUserKey(startKey)
	var encEnd []byte
	if len(endKey) > 0 {
		encEnd = encodeUserKey(endKey)
	}

	it.Seek(encStart)
	var out []statemachine.KV
	var lastSeenUserKey []byte

	for it.Valid() {
		ikey := it.Key()
		uKey, isUser := isUserKey(ikey.UserKey)
		if !isUser {
			it.Next()
			continue
		}
		if len(encEnd) > 0 && bytes.Compare(ikey.UserKey, encEnd) >= 0 {
			break
		}

		if lastSeenUserKey == nil || !bytes.Equal(lastSeenUserKey, ikey.UserKey) {
			lastSeenUserKey = bytes.Clone(ikey.UserKey)
			if ikey.Type == ValTypeValue || ikey.Type == ValTypeVLogPtr {
				resolved, rErr := db.resolveValue(ikey.Type, it.Value())
				if rErr != nil {
					return nil, rErr
				}
				out = append(out, statemachine.KV{
					Key:   uKey,
					Value: resolved,
				})
				if limit > 0 && len(out) >= limit {
					break
				}
			}
		}
		it.Next()
	}
	return out, nil
}

func (db *LSMStore) VerifyAllPointers() error {
	_, err := db.Scan(nil, nil, 0)
	return err
}

func (db *LSMStore) RunVLogGC() (reclaimedSegments int, err error) {
	ids, err := db.vlog.listSegments()
	if err != nil || len(ids) <= 1 {
		return 0, err
	}

	oldSegments := ids[:len(ids)-1]
	for _, segID := range oldSegments {
		records, rErr := db.vlog.readSegmentRecords(segID)
		if rErr != nil {
			return reclaimedSegments, rErr
		}

		if !db.opts.Bugs.Bug16VLogGCDropLive {
			wb := NewWriteBatch(db.lastSeq + 1)
			for _, rec := range records {
				it, bErr := db.buildMergedIterator()
				if bErr != nil {
					return reclaimedSegments, bErr
				}
				it.Seek(rec.key)
				if it.Valid() && bytes.Equal(it.Key().UserKey, rec.key) && it.Key().Type == ValTypeVLogPtr {
					curPtr, dErr := DecodeVLogPointer(it.Value())
					if dErr == nil && curPtr.SegmentID == segID && curPtr.Offset == rec.ptr.Offset {
						newPtr, aErr := db.vlog.Append(rec.key, rec.val)
						if aErr != nil {
							return reclaimedSegments, aErr
						}
						wb.PutVLogPtr(rec.key, newPtr)
					}
				}
			}
			if len(wb.Entries) > 0 {
				_ = db.vlog.Sync()
				if err := db.commitBatch(wb, true); err != nil {
					return reclaimedSegments, err
				}
			}
		}

		_ = db.fs.Remove(db.vlog.segPath(segID))
		reclaimedSegments++
	}
	_ = db.fs.SyncDir(db.dir)
	return reclaimedSegments, nil
}

func (db *LSMStore) AppliedIndex() uint64 { return db.appliedIdx }
func (db *LSMStore) FlushedIndex() uint64 { return db.flushedIdx }
func (db *LSMStore) FsyncCount() uint64   { return db.fsyncCount }

func (db *LSMStore) PinSnapshot() uint64 {
	seq := db.lastSeq
	db.pinnedSnapshots = append(db.pinnedSnapshots, seq)
	slices.Sort(db.pinnedSnapshots)
	return seq
}

func (db *LSMStore) ReleaseSnapshot(seq uint64) {
	idx, found := slices.BinarySearch(db.pinnedSnapshots, seq)
	if found {
		db.pinnedSnapshots = slices.Delete(db.pinnedSnapshots, idx, idx+1)
	}
}

func (db *LSMStore) recordFileSizeWritten(path string) {
	if f, err := db.fs.Open(path); err == nil {
		if sz, sErr := f.Size(); sErr == nil && sz > 0 {
			db.lsmDiskBytesWritten += uint64(sz)
		}
		_ = f.Close()
	}
}

func (db *LSMStore) flushMemtableOnly() error {
	if db.mem.Count() == 0 {
		return nil
	}

	fileNum := db.vs.NextFileNum
	db.vs.NextFileNum++
	sstPath := db.sstPath(fileNum)

	minK, maxK, err := WriteSSTable(db.fs, sstPath, db.mem.NewIterator(), db.opts.BloomBitsPerKey)
	if err != nil {
		return err
	}
	db.recordFileSizeWritten(sstPath)
	db.fsyncCount++

	if val, _, ok := db.mem.Get([]byte(sysAppliedIndexKey), math.MaxUint64); ok && len(val) == 8 {
		idx := binary.LittleEndian.Uint64(val)
		if idx > db.flushedIdx {
			db.flushedIdx = idx
		}
	}

	edit := VersionEdit{
		NextFileNum:  db.vs.NextFileNum,
		LastSeq:      db.lastSeq,
		FlushedIndex: db.flushedIdx,
		AddedFiles: []FileMeta{
			{
				FileNum: fileNum,
				Level:   0,
				MinKey:  minK,
				MaxKey:  maxK,
			},
		},
	}
	if err := LogAndApplyManifest(db.fs, db.dir, db.vs, edit, db.opts.Bugs); err != nil {
		return err
	}
	db.mem = NewMemtable(db.rng)
	return nil
}

func (db *LSMStore) Flush() error {
	if db.mem.Count() == 0 {
		return nil
	}
	if err := db.flushMemtableOnly(); err != nil {
		return err
	}

	if db.wal != nil {
		_ = db.wal.Close()
	}
	w, err := CreateWAL(db.fs, filepath.Join(db.dir, "active.wal"))
	if err != nil {
		return err
	}
	_ = w.Sync()
	if !db.opts.Bugs.Bug09SkipManifestSync {
		_ = db.fs.SyncDir(db.dir)
	}
	db.wal = w

	if len(db.vs.Levels[0]) >= db.opts.L0CompactionTrigger {
		if err := db.CompactLevel(0); err != nil {
			return err
		}
	}
	if len(db.vs.Levels[1]) >= db.opts.L1CompactionTrigger {
		if err := db.CompactLevel(1); err != nil {
			return err
		}
	}
	return nil
}

func (db *LSMStore) CompactLevel(sourceLevel int) error {
	if sourceLevel < 0 || sourceLevel >= MaxLevels-1 {
		return nil
	}
	srcFiles := db.vs.Levels[sourceLevel]
	if len(srcFiles) == 0 {
		return nil
	}
	targetLevel := sourceLevel + 1
	dstFiles := db.vs.Levels[targetLevel]

	var iters []Iterator
	var deleted []DeletedFile

	if sourceLevel == 0 {
		for i := len(srcFiles) - 1; i >= 0; i-- {
			r, err := OpenSSTable(db.fs, db.sstPath(srcFiles[i].FileNum))
			if err != nil {
				return err
			}
			it, err := r.NewIterator()
			if err != nil {
				return err
			}
			iters = append(iters, it)
			deleted = append(deleted, DeletedFile{Level: 0, FileNum: srcFiles[i].FileNum})
		}
	} else {
		for _, fm := range srcFiles {
			r, err := OpenSSTable(db.fs, db.sstPath(fm.FileNum))
			if err != nil {
				return err
			}
			it, err := r.NewIterator()
			if err != nil {
				return err
			}
			iters = append(iters, it)
			deleted = append(deleted, DeletedFile{Level: sourceLevel, FileNum: fm.FileNum})
		}
	}

	for _, fm := range dstFiles {
		r, err := OpenSSTable(db.fs, db.sstPath(fm.FileNum))
		if err != nil {
			return err
		}
		it, err := r.NewIterator()
		if err != nil {
			return err
		}
		iters = append(iters, it)
		deleted = append(deleted, DeletedFile{Level: targetLevel, FileNum: fm.FileNum})
	}

	merged := NewMergingIterator(iters)
	oldestSnapshot := uint64(math.MaxUint64)
	if len(db.pinnedSnapshots) > 0 {
		oldestSnapshot = db.pinnedSnapshots[0]
	}

	deeperFilesExist := false
	for l := targetLevel + 1; l < MaxLevels; l++ {
		if len(db.vs.Levels[l]) > 0 {
			deeperFilesExist = true
			break
		}
	}

	var compactedEntries []sstKV
	var lastUserKey []byte

	merged.SeekToFirst()
	for merged.Valid() {
		ikey := merged.Key()
		val := bytes.Clone(merged.Value())

		isFirstForKey := lastUserKey == nil || !bytes.Equal(lastUserKey, ikey.UserKey)
		if isFirstForKey {
			lastUserKey = bytes.Clone(ikey.UserKey)

			if ikey.Type == ValTypeTombstone {
				if db.opts.Bugs.Bug10DropTombstoneEarly {
					merged.Next()
					continue
				}
				if !deeperFilesExist && ikey.SeqNum <= oldestSnapshot {
					merged.Next()
					continue
				}
			}
			compactedEntries = append(compactedEntries, sstKV{key: ikey, value: val})
		}
		merged.Next()
	}

	edit := VersionEdit{
		NextFileNum:  db.vs.NextFileNum,
		LastSeq:      db.lastSeq,
		FlushedIndex: db.flushedIdx,
		DeletedFiles: deleted,
	}

	if len(compactedEntries) > 0 {
		newFileNum := db.vs.NextFileNum
		db.vs.NextFileNum++
		edit.NextFileNum = db.vs.NextFileNum

		sliceIt := &SSTIterator{entries: compactedEntries, pos: 0}
		outPath := db.sstPath(newFileNum)
		minK, maxK, err := WriteSSTable(db.fs, outPath, sliceIt, db.opts.BloomBitsPerKey)
		if err != nil {
			return err
		}
		db.recordFileSizeWritten(outPath)
		db.fsyncCount++
		edit.AddedFiles = append(edit.AddedFiles, FileMeta{
			FileNum: newFileNum,
			Level:   targetLevel,
			MinKey:  minK,
			MaxKey:  maxK,
		})
	}

	if err := LogAndApplyManifest(db.fs, db.dir, db.vs, edit, db.opts.Bugs); err != nil {
		return err
	}

	for _, del := range deleted {
		_ = db.fs.Remove(db.sstPath(del.FileNum))
	}
	if !db.opts.Bugs.Bug09SkipManifestSync {
		_ = db.fs.SyncDir(db.dir)
	}
	return nil
}

func (db *LSMStore) WriteAmplification() float64 {
	if db.logicalBytesWritten == 0 {
		return 1.0
	}
	totalDisk := db.lsmDiskBytesWritten + db.vlog.bytesWritten
	return float64(totalDisk) / float64(db.logicalBytesWritten)
}

type lsmLogicalSnapshot struct {
	AppliedIdx uint64             `json:"applied_idx"`
	LastSeq    uint64             `json:"last_seq"`
	Items      []statemachine.KV  `json:"items"`
	Sessions   []lsmSessionRecord `json:"sessions,omitempty"`
}

func (db *LSMStore) Snapshot() ([]byte, error) {
	items, err := db.Scan(nil, nil, 0)
	if err != nil {
		return nil, err
	}
	snap := lsmLogicalSnapshot{
		AppliedIdx: db.appliedIdx,
		LastSeq:    db.lastSeq,
		Items:      items,
	}
	// Zoo #12: Omit client session deduplication table from snapshot!
	if !db.opts.Bugs.Bug12SnapshotOmitDedup {
		snap.Sessions = slices.Clone(db.sessions)
	}
	return json.Marshal(snap)
}

func (db *LSMStore) Restore(snapshot []byte) error {
	var snap lsmLogicalSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return err
	}

	wb := NewWriteBatch(db.lastSeq + 1)
	for _, kv := range snap.Items {
		db.putIntoBatch(wb, encodeUserKey(kv.Key), kv.Value)
	}
	_ = db.vlog.Sync()
	if !db.opts.Bugs.Bug12SnapshotOmitDedup {
		for _, s := range snap.Sessions {
			recBytes, _ := json.Marshal(s)
			wb.Put(sessionKey(s.ClientID), recBytes)
		}
		db.sessions = slices.Clone(snap.Sessions)
	} else {
		db.sessions = nil
	}
	var idxBuf [8]byte
	binary.LittleEndian.PutUint64(idxBuf[:], snap.AppliedIdx)
	wb.Put([]byte(sysAppliedIndexKey), idxBuf[:])

	if err := db.commitBatch(wb, true); err != nil {
		return err
	}
	db.appliedIdx = snap.AppliedIdx
	return db.Flush()
}

func (db *LSMStore) Close() error {
	if db.vlog != nil {
		_ = db.vlog.Close()
	}
	if db.wal != nil {
		return db.wal.Close()
	}
	return nil
}
