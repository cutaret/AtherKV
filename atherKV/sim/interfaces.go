package sim

// Clock provides virtual or physical time in nanoseconds.
// Core packages must never call time.Now() directly.
type Clock interface {
	Now() uint64
}

// Rand provides deterministic pseudo-random numbers from an isolated stream.
// Core packages must never import math/rand or crypto/rand.
type Rand interface {
	Uint64() uint64
	Intn(n int) int
	Int63n(n int64) int64
	Float64() float64
}

// Scheduler replaces raw goroutines in core packages.
// Background work (flushes, compactions, timeouts) is scheduled as tasks.
type Scheduler interface {
	ScheduleAfter(delayNs uint64, taskName string, fn func())
}

// Message is the wire-level envelope routed across Transport.
type Message struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Type    string `json:"type"`
	Term    uint64 `json:"term,omitempty"`
	Payload []byte `json:"payload,omitempty"`
}

// Transport abstracts message delivery between cluster nodes.
type Transport interface {
	Send(msg Message)
}
