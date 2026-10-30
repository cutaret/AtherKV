package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
)

// TraceEvent is a single canonical simulation step logged in JSON-lines format.
type TraceEvent struct {
	Seq     uint64 `json:"seq"`
	TimeNs  uint64 `json:"time_ns"`
	Node    string `json:"node,omitempty"`
	Kind    string `json:"kind"`
	Details string `json:"details,omitempty"`
}

// Tracer computes a rolling SHA-256 hash over every event and optionally emits JSONL.
type Tracer struct {
	rolling [32]byte
	out     io.Writer
	count   uint64
}

func NewTracer(out io.Writer) *Tracer {
	return &Tracer{out: out}
}

func (t *Tracer) Record(ev TraceEvent) {
	line, err := json.Marshal(ev)
	if err != nil {
		panic("sim.Tracer: failed to marshal TraceEvent: " + err.Error())
	}

	h := sha256.New()
	h.Write(t.rolling[:])
	h.Write(line)
	copy(t.rolling[:], h.Sum(nil))
	t.count++

	if t.out != nil {
		_, _ = t.out.Write(line)
		_, _ = t.out.Write([]byte("\n"))
	}
}

// HashHex returns the hex-encoded 256-bit rolling trace hash.
func (t *Tracer) HashHex() string {
	return hex.EncodeToString(t.rolling[:])
}

func (t *Tracer) EventCount() uint64 {
	return t.count
}
