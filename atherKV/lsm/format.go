package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrCorruptBlock   = errors.New("lsm: block CRC32C checksum mismatch")
	ErrCorruptFooter  = errors.New("lsm: invalid SSTable magic number or footer CRC")
	ErrCorruptBatch   = errors.New("lsm: corrupted WriteBatch payload")
	ErrDanglingVLog   = errors.New("lsm: dangling value-log pointer beyond durable EOF")
	ErrCorruptVLogCRC = errors.New("lsm: value-log payload CRC32C mismatch (torn value write)")
)

// ValueType distinguishes live values, deletion tombstones, and WiscKey value-log pointers.
type ValueType uint8

const (
	ValTypeValue     ValueType = 1
	ValTypeTombstone ValueType = 2
	ValTypeVLogPtr   ValueType = 3
)

// LSMBugConfig holds switches for storage-layer, snapshot, and value-log Bug Zoo entries.
type LSMBugConfig struct {
	Bug08AcceptTornWAL      bool // Zoo #8: WAL recovery accepts torn tail record without CRC check
	Bug09SkipManifestSync   bool // Zoo #9: Rename CURRENT without SyncDir on parent directory
	Bug10DropTombstoneEarly bool // Zoo #10: Compaction drops tombstone while older value survives deeper
	Bug12SnapshotOmitDedup  bool // Zoo #12: InstallSnapshot omits client session deduplication table
	Bug14SplitApplyIndex    bool // Zoo #14: Write KV data batch and appliedIndex in separate batches
	Bug15DanglingVLogPtr    bool // Zoo #15: Sync LSM batch with VLogPointer before syncing ValueLog
	Bug16VLogGCDropLive     bool // Zoo #16: Value-log GC discards a segment containing a live key's value
	Bug17AcceptTornVLog     bool // Zoo #17: Value-log reader accepts torn tail without verifying CRC32C
}

// VLogPointer is a 24-byte pointer stored in the LSM tree when value separation is active.
type VLogPointer struct {
	SegmentID uint64
	Offset    uint64
	Length    uint32
	Checksum  uint32
}

const VLogPointerSize = 24

func (p VLogPointer) Encode() []byte {
	var buf [VLogPointerSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], p.SegmentID)
	binary.LittleEndian.PutUint64(buf[8:16], p.Offset)
	binary.LittleEndian.PutUint32(buf[16:20], p.Length)
	binary.LittleEndian.PutUint32(buf[20:24], p.Checksum)
	return buf[:]
}

func DecodeVLogPointer(b []byte) (VLogPointer, error) {
	if len(b) != VLogPointerSize {
		return VLogPointer{}, ErrCorruptBatch
	}
	return VLogPointer{
		SegmentID: binary.LittleEndian.Uint64(b[0:8]),
		Offset:    binary.LittleEndian.Uint64(b[8:16]),
		Length:    binary.LittleEndian.Uint32(b[16:20]),
		Checksum:  binary.LittleEndian.Uint32(b[20:24]),
	}, nil
}

// InternalKey orders keys by (UserKey ascending, SeqNum descending).
type InternalKey struct {
	UserKey []byte
	SeqNum  uint64
	Type    ValueType
}

// CompareInternalKey sorts UserKey ascending, then SeqNum descending so newer versions come first.
func CompareInternalKey(a, b InternalKey) int {
	if cmp := bytes.Compare(a.UserKey, b.UserKey); cmp != 0 {
		return cmp
	}
	if a.SeqNum > b.SeqNum {
		return -1
	}
	if a.SeqNum < b.SeqNum {
		return 1
	}
	if a.Type > b.Type {
		return -1
	}
	if a.Type < b.Type {
		return 1
	}
	return 0
}

// BatchEntry is a single mutation inside an atomic WriteBatch.
type BatchEntry struct {
	Type    ValueType
	UserKey []byte
	Value   []byte
}

// WriteBatch groups multiple mutations into one atomic WAL and Memtable operation.
type WriteBatch struct {
	StartSeq uint64
	Entries  []BatchEntry
}

func NewWriteBatch(startSeq uint64) *WriteBatch {
	return &WriteBatch{StartSeq: startSeq}
}

func (wb *WriteBatch) Put(key, value []byte) {
	wb.Entries = append(wb.Entries, BatchEntry{
		Type:    ValTypeValue,
		UserKey: bytes.Clone(key),
		Value:   bytes.Clone(value),
	})
}

func (wb *WriteBatch) PutVLogPtr(key []byte, ptr VLogPointer) {
	wb.Entries = append(wb.Entries, BatchEntry{
		Type:    ValTypeVLogPtr,
		UserKey: bytes.Clone(key),
		Value:   ptr.Encode(),
	})
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.Entries = append(wb.Entries, BatchEntry{
		Type:    ValTypeTombstone,
		UserKey: bytes.Clone(key),
		Value:   nil,
	})
}

func (wb *WriteBatch) Encode() []byte {
	var buf bytes.Buffer
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[0:8], wb.StartSeq)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(wb.Entries)))
	buf.Write(hdr[:])

	for _, e := range wb.Entries {
		buf.WriteByte(byte(e.Type))
		var lens [8]byte
		binary.LittleEndian.PutUint32(lens[0:4], uint32(len(e.UserKey)))
		binary.LittleEndian.PutUint32(lens[4:8], uint32(len(e.Value)))
		buf.Write(lens[:])
		buf.Write(e.UserKey)
		buf.Write(e.Value)
	}
	return buf.Bytes()
}

func DecodeWriteBatch(data []byte) (*WriteBatch, error) {
	if len(data) < 12 {
		return nil, ErrCorruptBatch
	}
	startSeq := binary.LittleEndian.Uint64(data[0:8])
	count := binary.LittleEndian.Uint32(data[8:12])
	wb := &WriteBatch{
		StartSeq: startSeq,
		Entries:  make([]BatchEntry, 0, min(int(count), 4096)),
	}
	off := 12
	for i := uint32(0); i < count; i++ {
		if off+9 > len(data) {
			return nil, ErrCorruptBatch
		}
		vType := ValueType(data[off])
		kLen := int(binary.LittleEndian.Uint32(data[off+1 : off+5]))
		vLen := int(binary.LittleEndian.Uint32(data[off+5 : off+9]))
		off += 9
		if kLen < 0 || vLen < 0 || off+kLen+vLen > len(data) {
			return nil, ErrCorruptBatch
		}
		key := bytes.Clone(data[off : off+kLen])
		off += kLen
		val := bytes.Clone(data[off : off+vLen])
		off += vLen
		wb.Entries = append(wb.Entries, BatchEntry{
			Type:    vType,
			UserKey: key,
			Value:   val,
		})
	}
	return wb, nil
}

// Iterator is the standard internal cursor over Memtables and SSTables.
type Iterator interface {
	SeekToFirst()
	Seek(targetUserKey []byte)
	Valid() bool
	Key() InternalKey
	Value() []byte
	Next()
}

// MergingIterator merges multiple sorted Iterators in InternalKey order (userKey ASC, seqNum DESC).
type MergingIterator struct {
	children []Iterator
	current  int
}

func NewMergingIterator(children []Iterator) *MergingIterator {
	return &MergingIterator{
		children: children,
		current:  -1,
	}
}

func (m *MergingIterator) findSmallest() {
	m.current = -1
	for i, child := range m.children {
		if !child.Valid() {
			continue
		}
		if m.current == -1 {
			m.current = i
		} else {
			cmp := CompareInternalKey(child.Key(), m.children[m.current].Key())
			if cmp < 0 {
				m.current = i
			}
		}
	}
}

func (m *MergingIterator) SeekToFirst() {
	for _, child := range m.children {
		child.SeekToFirst()
	}
	m.findSmallest()
}

func (m *MergingIterator) Seek(targetUserKey []byte) {
	for _, child := range m.children {
		child.Seek(targetUserKey)
	}
	m.findSmallest()
}

func (m *MergingIterator) Valid() bool {
	return m.current >= 0 && m.current < len(m.children) && m.children[m.current].Valid()
}

func (m *MergingIterator) Key() InternalKey {
	return m.children[m.current].Key()
}

func (m *MergingIterator) Value() []byte {
	return m.children[m.current].Value()
}

func (m *MergingIterator) Next() {
	if !m.Valid() {
		return
	}
	m.children[m.current].Next()
	m.findSmallest()
}
