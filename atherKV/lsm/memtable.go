package lsm

import (
	"bytes"

	"github.com/cutaret/aetherkv/sim"
)

const skipMaxHeight = 12

type skipNode struct {
	key   InternalKey
	value []byte
	next  [skipMaxHeight]*skipNode
}

// Memtable is a deterministic skiplist keyed by (userKey ASC, seqNum DESC).
type Memtable struct {
	head       *skipNode
	maxHeight  int
	rng        sim.Rand
	approxSize int
	count      int
}

func NewMemtable(rng sim.Rand) *Memtable {
	return &Memtable{
		head:      &skipNode{},
		maxHeight: 1,
		rng:       rng,
	}
}

func (m *Memtable) randomHeight() int {
	h := 1
	for h < skipMaxHeight && (m.rng.Uint64()&0x3) == 0 { // p = 1/4 branching factor
		h++
	}
	return h
}

// ApplyBatch inserts all entries in wb atomically with monotonically increasing sequence numbers.
func (m *Memtable) ApplyBatch(wb *WriteBatch) {
	seq := wb.StartSeq
	for _, entry := range wb.Entries {
		ikey := InternalKey{
			UserKey: bytes.Clone(entry.UserKey),
			SeqNum:  seq,
			Type:    entry.Type,
		}
		m.insert(ikey, bytes.Clone(entry.Value))
		seq++
	}
}

func (m *Memtable) insert(key InternalKey, val []byte) {
	var prev [skipMaxHeight]*skipNode
	x := m.head
	for i := m.maxHeight - 1; i >= 0; i-- {
		for x.next[i] != nil && CompareInternalKey(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		prev[i] = x
	}

	height := m.randomHeight()
	if height > m.maxHeight {
		for i := m.maxHeight; i < height; i++ {
			prev[i] = m.head
		}
		m.maxHeight = height
	}

	node := &skipNode{
		key:   key,
		value: val,
	}
	for i := 0; i < height; i++ {
		node.next[i] = prev[i].next[i]
		prev[i].next[i] = node
	}

	m.approxSize += len(key.UserKey) + len(val) + 16
	m.count++
}

// Get returns the newest version of userKey with SeqNum <= snapshotSeq.
func (m *Memtable) Get(userKey []byte, snapshotSeq uint64) (val []byte, vType ValueType, found bool) {
	target := InternalKey{
		UserKey: userKey,
		SeqNum:  snapshotSeq,
		Type:    ValTypeVLogPtr, // Highest ValueType so Seek lands at first entry <= snapshotSeq
	}

	x := m.head
	for i := m.maxHeight - 1; i >= 0; i-- {
		for x.next[i] != nil && CompareInternalKey(x.next[i].key, target) < 0 {
			x = x.next[i]
		}
	}
	cand := x.next[0]
	if cand != nil && bytes.Equal(cand.key.UserKey, userKey) {
		return bytes.Clone(cand.value), cand.key.Type, true
	}
	return nil, 0, false
}

func (m *Memtable) ApproximateSize() int { return m.approxSize }
func (m *Memtable) Count() int           { return m.count }

func (m *Memtable) NewIterator() Iterator {
	return &memIterator{head: m.head, curr: m.head.next[0]}
}

type memIterator struct {
	head *skipNode
	curr *skipNode
}

func (it *memIterator) SeekToFirst() {
	it.curr = it.head.next[0]
}

func (it *memIterator) Seek(targetUserKey []byte) {
	x := it.head
	for x.next[0] != nil && bytes.Compare(x.next[0].key.UserKey, targetUserKey) < 0 {
		x = x.next[0]
	}
	it.curr = x.next[0]
}

func (it *memIterator) Valid() bool {
	return it.curr != nil
}

func (it *memIterator) Key() InternalKey {
	return it.curr.key
}

func (it *memIterator) Value() []byte {
	return it.curr.value
}

func (it *memIterator) Next() {
	if it.curr != nil {
		it.curr = it.curr.next[0]
	}
}