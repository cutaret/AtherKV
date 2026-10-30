package sim

import (
	"fmt"
	"slices"
)

// LinkConfig controls fault injection on a directed network link (From -> To).
type LinkConfig struct {
	MinLatencyNs uint64
	MaxLatencyNs uint64
	DropProb     float64
	DupProb      float64
	ReorderProb  float64
	Partitioned  bool
}

// SimNet simulates a message-passing network with per-link PRNG streams and faults.
type SimNet struct {
	kernel   *Kernel
	handlers map[string]func(Message)
	links    map[string]LinkConfig
	paused   map[string]bool
}

func NewSimNet(k *Kernel) *SimNet {
	return &SimNet{
		kernel:   k,
		handlers: make(map[string]func(Message)),
		links:    make(map[string]LinkConfig),
		paused:   make(map[string]bool),
	}
}

func linkKey(from, to string) string {
	return from + "->" + to
}

func (n *SimNet) RegisterNode(nodeID string, handler func(Message)) {
	n.handlers[nodeID] = handler
}

func (n *SimNet) SetLink(from, to string, cfg LinkConfig) {
	n.links[linkKey(from, to)] = cfg
}

func (n *SimNet) GetLink(from, to string) LinkConfig {
	cfg, ok := n.links[linkKey(from, to)]
	if !ok {
		return LinkConfig{MinLatencyNs: 1_000_000, MaxLatencyNs: 5_000_000} // Default 1-5ms
	}
	return cfg
}

func (n *SimNet) Partition(nodeA, nodeB string) {
	cfgAB := n.GetLink(nodeA, nodeB)
	cfgAB.Partitioned = true
	n.SetLink(nodeA, nodeB, cfgAB)

	cfgBA := n.GetLink(nodeB, nodeA)
	cfgBA.Partitioned = true
	n.SetLink(nodeB, nodeA, cfgBA)
}

func (n *SimNet) AsymmetricPartition(from, to string) {
	cfg := n.GetLink(from, to)
	cfg.Partitioned = true
	n.SetLink(from, to, cfg)
}

func (n *SimNet) HealAll(nodes []string) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a == b {
				continue
			}
			n.SetLink(a, b, LinkConfig{
				MinLatencyNs: 1_000_000,
				MaxLatencyNs: 3_000_000,
				DropProb:     0,
				DupProb:      0,
				ReorderProb:  0,
				Partitioned:  false,
			})
		}
		n.paused[a] = false
	}
}

func (n *SimNet) PauseNode(nodeID string, paused bool) {
	n.paused[nodeID] = paused
}

func (n *SimNet) IsPaused(nodeID string) bool {
	return n.paused[nodeID]
}

// Send implements the Transport interface using an isolated PRNG stream per directed link.
func (n *SimNet) Send(msg Message) {
	if n.paused[msg.From] || n.paused[msg.To] {
		n.kernel.RecordTrace(msg.From, "net_drop_paused", fmt.Sprintf("%s->%s type=%s", msg.From, msg.To, msg.Type))
		return
	}

	cfg := n.GetLink(msg.From, msg.To)
	rng := n.kernel.Stream("net:" + linkKey(msg.From, msg.To))

	if cfg.Partitioned {
		n.kernel.RecordTrace(msg.From, "net_drop_partition", fmt.Sprintf("%s->%s type=%s", msg.From, msg.To, msg.Type))
		return
	}
	if cfg.DropProb > 0 && rng.Float64() < cfg.DropProb {
		n.kernel.RecordTrace(msg.From, "net_drop_prob", fmt.Sprintf("%s->%s type=%s", msg.From, msg.To, msg.Type))
		return
	}

	deliver := func(extraDelay uint64, tag string) {
		spread := cfg.MaxLatencyNs - cfg.MinLatencyNs
		delay := cfg.MinLatencyNs
		if spread > 0 {
			delay += uint64(rng.Int63n(int64(spread)))
		}
		delay += extraDelay

		clonedPayload := slices.Clone(msg.Payload)
		outMsg := Message{
			From:    msg.From,
			To:      msg.To,
			Type:    msg.Type,
			Term:    msg.Term,
			Payload: clonedPayload,
		}

		n.kernel.NodeScheduler(msg.To).ScheduleAfter(delay, fmt.Sprintf("recv_%s_%s", tag, msg.Type), func() {
			if n.paused[outMsg.To] {
				return
			}
			if h, ok := n.handlers[outMsg.To]; ok {
				h(outMsg)
			}
		})
	}

	var reorderExtra uint64
	if cfg.ReorderProb > 0 && rng.Float64() < cfg.ReorderProb {
		reorderExtra = uint64(rng.Int63n(15_000_000) + 5_000_000) // +5ms to +20ms reorder delay
	}
	deliver(reorderExtra, "orig")

	if cfg.DupProb > 0 && rng.Float64() < cfg.DupProb {
		dupExtra := uint64(rng.Int63n(5_000_000) + 1_000_000)
		deliver(reorderExtra+dupExtra, "dup")
	}
}

// FaultType enumerates nemesis chaos actions.
type FaultType string

const (
	FaultPartitionSym  FaultType = "partition_sym"
	FaultPartitionAsym FaultType = "partition_asym"
	FaultLinkLossy     FaultType = "link_lossy"
	FaultNodeCrash     FaultType = "node_crash"
	FaultNodeRestart   FaultType = "node_restart"
	FaultNodePause     FaultType = "node_pause"
	FaultNodeResume    FaultType = "node_resume"
	FaultClockSkew     FaultType = "clock_skew"
	FaultHealAll       FaultType = "heal_all"
)

// FaultAction is a single scheduled chaos event that can be pruned during shrinking.
type FaultAction struct {
	ID      int
	TimeNs  uint64
	Type    FaultType
	TargetA string
	TargetB string
	SkewPPM int64
}

// BuildNemesisSchedule generates a phased fault schedule (Warm-up -> Chaos -> Heal)
// from the isolated "nemesis" PRNG stream.
func BuildNemesisSchedule(rng Rand, nodes []string, numFaults int) []FaultAction {
	var actions []FaultAction
	// Warm-up ends at 50ms; Chaos runs from 50ms to 450ms; Heal triggers at 460ms
	baseTimeNs := uint64(50_000_000)

	for i := 0; i < numFaults; i++ {
		stepDelay := uint64(rng.Int63n(25_000_000) + 5_000_000)
		baseTimeNs += stepDelay

		a := nodes[rng.Intn(len(nodes))]
		b := nodes[rng.Intn(len(nodes))]
		if a == b {
			b = nodes[(rng.Intn(len(nodes)-1)+1)%len(nodes)]
		}

		pick := rng.Intn(6)
		switch pick {
		case 0:
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultPartitionSym, TargetA: a, TargetB: b})
		case 1:
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultPartitionAsym, TargetA: a, TargetB: b})
		case 2:
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultLinkLossy, TargetA: a, TargetB: b})
		case 3:
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultNodeCrash, TargetA: a})
			// Schedule corresponding restart 20ms later
			actions = append(actions, FaultAction{ID: i + 1000, TimeNs: baseTimeNs + 20_000_000, Type: FaultNodeRestart, TargetA: a})
		case 4:
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultNodePause, TargetA: a})
			actions = append(actions, FaultAction{ID: i + 2000, TimeNs: baseTimeNs + 15_000_000, Type: FaultNodeResume, TargetA: a})
		case 5:
			skew := rng.Int63n(6000) - 3000 // +/- 3000 PPM
			actions = append(actions, FaultAction{ID: i + 1, TimeNs: baseTimeNs, Type: FaultClockSkew, TargetA: a, SkewPPM: skew})
		}
	}

	// Final Heal phase so cluster can converge for liveness verification
	actions = append(actions, FaultAction{
		ID:     9999,
		TimeNs: baseTimeNs + 40_000_000,
		Type:   FaultHealAll,
	})
	return actions
}

// ShrinkFaultSchedule uses delta-debugging to find a minimal subset of FaultActions
// that still reproduces a test failure.
func ShrinkFaultSchedule(initial []FaultAction, reproduces func([]FaultAction) bool) []FaultAction {
	current := slices.Clone(initial)
	granularity := 2

	for len(current) >= 1 {
		chunkSize := (len(current) + granularity - 1) / granularity
		if chunkSize == 0 {
			break
		}
		reduced := false
		for start := 0; start < len(current); start += chunkSize {
			end := min(start+chunkSize, len(current))
			// Never strip the final heal action if needed, or allow testing without it
			candidate := make([]FaultAction, 0, len(current)-(end-start))
			candidate = append(candidate, current[:start]...)
			candidate = append(candidate, current[end:]...)

			if reproduces(candidate) {
				current = candidate
				granularity = max(granularity-1, 2)
				reduced = true
				break
			}
		}
		if !reduced {
			if granularity >= len(current) {
				break
			}
			granularity = min(granularity*2, len(current))
		}
	}
	return current
}
