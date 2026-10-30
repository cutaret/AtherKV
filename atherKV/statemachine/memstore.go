package statemachine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
)

type sessionEntry struct {
	ClientID   uint64 `json:"client_id"`
	LastSeqNum uint64 `json:"last_seq_num"`
	LastResult Result `json:"last_result"`
}

type memSnapshot struct {
	AppliedIdx uint64         `json:"applied_index"`
	Items      []KV           `json:"items"`
	Sessions   []sessionEntry `json:"sessions"`
}

// MemStore is the deterministic in-memory sorted reference StateMachine built in S0.
// It uses sorted slices rather than Go maps so state iteration and snapshots are 100% deterministic.
type MemStore struct {
	appliedIdx uint64
	items      []KV
	sessions   []sessionEntry
}

func NewMemStore() *MemStore {
	return &MemStore{}
}

func (m *MemStore) findKey(key []byte) (int, bool) {
	return slices.BinarySearchFunc(m.items, key, func(elem KV, target []byte) int {
		return bytes.Compare(elem.Key, target)
	})
}

func (m *MemStore) findSession(clientID uint64) (int, bool) {
	return slices.BinarySearchFunc(m.sessions, clientID, func(elem sessionEntry, target uint64) int {
		if elem.ClientID < target {
			return -1
		}
		if elem.ClientID > target {
			return 1
		}
		return 0
	})
}

func (m *MemStore) ApplyBatch(appliedIndex uint64, ops []Op) []Result {
	if appliedIndex <= m.appliedIdx && m.appliedIdx != 0 {
		panic(fmt.Sprintf("statemachine.MemStore: non-monotonic appliedIndex %d <= %d", appliedIndex, m.appliedIdx))
	}
	m.appliedIdx = appliedIndex

	results := make([]Result, len(ops))
	for i, op := range ops {
		// Check session deduplication table if ClientID > 0
		if op.ClientID != 0 {
			sIdx, found := m.findSession(op.ClientID)
			if found && op.SeqNum <= m.sessions[sIdx].LastSeqNum {
				dupRes := m.sessions[sIdx].LastResult
				dupRes.Deduplicated = true
				results[i] = dupRes
				continue
			}
		}

		var res Result
		switch op.Type {
		case OpPut:
			m.putInternal(op.Key, op.Value)
			res = Result{Applied: true, Value: bytes.Clone(op.Value)}

		case OpDelete:
			idx, found := m.findKey(op.Key)
			if found {
				m.items = slices.Delete(m.items, idx, idx+1)
			}
			res = Result{Applied: true, PreviousFound: found}

		case OpCAS:
			idx, found := m.findKey(op.Key)
			if found && bytes.Equal(m.items[idx].Value, op.Expected) {
				m.items[idx].Value = bytes.Clone(op.Value)
				res = Result{Applied: true, PreviousFound: true, Value: bytes.Clone(op.Value)}
			} else if !found && len(op.Expected) == 0 {
				m.putInternal(op.Key, op.Value)
				res = Result{Applied: true, PreviousFound: false, Value: bytes.Clone(op.Value)}
			} else {
				var cur []byte
				if found {
					cur = bytes.Clone(m.items[idx].Value)
				}
				res = Result{Applied: false, PreviousFound: found, Value: cur}
			}

		default:
			panic(fmt.Sprintf("statemachine.MemStore: unknown OpType %d", op.Type))
		}

		if op.ClientID != 0 {
			sIdx, found := m.findSession(op.ClientID)
			entry := sessionEntry{
				ClientID:   op.ClientID,
				LastSeqNum: op.SeqNum,
				LastResult: res,
			}
			if found {
				m.sessions[sIdx] = entry
			} else {
				m.sessions = slices.Insert(m.sessions, sIdx, entry)
			}
		}
		results[i] = res
	}
	return results
}

func (m *MemStore) putInternal(key, val []byte) {
	idx, found := m.findKey(key)
	kv := KV{Key: bytes.Clone(key), Value: bytes.Clone(val)}
	if found {
		m.items[idx] = kv
	} else {
		m.items = slices.Insert(m.items, idx, kv)
	}
}

func (m *MemStore) Get(key []byte) ([]byte, bool, error) {
	idx, found := m.findKey(key)
	if !found {
		return nil, false, nil
	}
	return bytes.Clone(m.items[idx].Value), true, nil
}

// Scan returns up to limit KV pairs in [startKey, endKey). If endKey is empty, scans to the end.
func (m *MemStore) Scan(startKey, endKey []byte, limit int) ([]KV, error) {
	startIdx, _ := m.findKey(startKey)
	var out []KV
	for i := startIdx; i < len(m.items); i++ {
		if len(endKey) > 0 && bytes.Compare(m.items[i].Key, endKey) >= 0 {
			break
		}
		out = append(out, KV{
			Key:   bytes.Clone(m.items[i].Key),
			Value: bytes.Clone(m.items[i].Value),
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *MemStore) AppliedIndex() uint64 {
	return m.appliedIdx
}

func (m *MemStore) Snapshot() ([]byte, error) {
	snap := memSnapshot{
		AppliedIdx: m.appliedIdx,
		Items:      make([]KV, len(m.items)),
		Sessions:   make([]sessionEntry, len(m.sessions)),
	}
	for i, it := range m.items {
		snap.Items[i] = KV{Key: bytes.Clone(it.Key), Value: bytes.Clone(it.Value)}
	}
	copy(snap.Sessions, m.sessions)
	return json.Marshal(snap)
}

func (m *MemStore) Restore(snapshot []byte) error {
	var snap memSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return err
	}
	m.appliedIdx = snap.AppliedIdx
	m.items = snap.Items
	m.sessions = snap.Sessions
	return nil
}
