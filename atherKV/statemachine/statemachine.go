package statemachine

type OpType uint8

const (
	OpPut OpType = iota + 1
	OpDelete
	OpCAS
)

// Op represents a single deterministic state machine mutation.
type Op struct {
	ClientID uint64 `json:"client_id"`
	SeqNum   uint64 `json:"seq_num"`
	Type     OpType `json:"type"`
	Key      []byte `json:"key"`
	Value    []byte `json:"value,omitempty"`
	Expected []byte `json:"expected,omitempty"` // Used by OpCAS
}

// Result is returned by ApplyBatch for each Op.
type Result struct {
	Applied       bool   `json:"applied"`
	Deduplicated  bool   `json:"deduplicated"`
	PreviousFound bool   `json:"previous_found,omitempty"`
	Value         []byte `json:"value,omitempty"`
}

// KV is a key-value pair returned by Scan.
type KV struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// StateMachine is the seam between the replicated KV layer and storage engines.
type StateMachine interface {
	ApplyBatch(appliedIndex uint64, ops []Op) []Result
	Get(key []byte) (val []byte, ok bool, err error)
	Scan(startKey, endKey []byte, limit int) ([]KV, error)
	AppliedIndex() uint64
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}
