package sim

import (
	"container/heap"
	"fmt"
	"io"
)

type scheduledItem struct {
	timeNs   uint64
	seq      uint64
	nodeID   string
	taskName string
	fn       func()
}

type eventHeap []scheduledItem

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].timeNs != h[j].timeNs {
		return h[i].timeNs < h[j].timeNs
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(scheduledItem)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Kernel is the single-threaded deterministic simulation engine.
type Kernel struct {
	masterSeed uint64
	clock      *BaseClock
	nodeClocks map[string]*NodeClock
	streams    map[string]*StreamRand
	queue      eventHeap
	nextSeq    uint64
	tracer     *Tracer
}

func NewKernel(masterSeed uint64, traceOut io.Writer) *Kernel {
	k := &Kernel{
		masterSeed: masterSeed,
		clock:      NewBaseClock(0),
		nodeClocks: make(map[string]*NodeClock),
		streams:    make(map[string]*StreamRand),
		tracer:     NewTracer(traceOut),
	}
	heap.Init(&k.queue)
	k.tracer.Record(TraceEvent{
		Seq:     0,
		TimeNs:  0,
		Kind:    "sim_init",
		Details: fmt.Sprintf("seed=%d", masterSeed),
	})
	return k
}

// Stream returns an isolated PRNG stream for the given component name (e.g., "nemesis", "net:n1->n2").
func (k *Kernel) Stream(name string) Rand {
	if r, ok := k.streams[name]; ok {
		return r
	}
	r := NewStreamRand(k.masterSeed, name)
	k.streams[name] = r
	return r
}

// NodeClock returns or creates the virtual clock for a given node.
func (k *Kernel) NodeClock(nodeID string) *NodeClock {
	if nc, ok := k.nodeClocks[nodeID]; ok {
		return nc
	}
	nc := NewNodeClock(k.clock, 0, 0)
	k.nodeClocks[nodeID] = nc
	return nc
}

// NodeScheduler returns a Scheduler bound to a specific node ID for trace attribution.
func (k *Kernel) NodeScheduler(nodeID string) Scheduler {
	return &nodeScheduler{kernel: k, nodeID: nodeID}
}

type nodeScheduler struct {
	kernel *Kernel
	nodeID string
}

func (ns *nodeScheduler) ScheduleAfter(delayNs uint64, taskName string, fn func()) {
	ns.kernel.schedule(ns.nodeID, delayNs, taskName, fn)
}

func (k *Kernel) schedule(nodeID string, delayNs uint64, taskName string, fn func()) {
	k.nextSeq++
	targetNs := k.clock.Now() + delayNs
	heap.Push(&k.queue, scheduledItem{
		timeNs:   targetNs,
		seq:      k.nextSeq,
		nodeID:   nodeID,
		taskName: taskName,
		fn:       fn,
	})
}

// ScheduleAfter schedules a global (non-node-specific) task.
func (k *Kernel) ScheduleAfter(delayNs uint64, taskName string, fn func()) {
	k.schedule("global", delayNs, taskName, fn)
}

// Step executes the next scheduled event if one exists before maxTimeNs.
func (k *Kernel) Step(maxTimeNs uint64) bool {
	if k.queue.Len() == 0 {
		return false
	}
	if k.queue[0].timeNs > maxTimeNs {
		return false
	}
	item := heap.Pop(&k.queue).(scheduledItem)
	k.clock.AdvanceTo(item.timeNs)
	k.tracer.Record(TraceEvent{
		Seq:     item.seq,
		TimeNs:  item.timeNs,
		Node:    item.nodeID,
		Kind:    "task_exec",
		Details: item.taskName,
	})
	item.fn()
	return true
}

// RunUntil executes all scheduled events up to maxTimeNs or maxSteps.
func (k *Kernel) RunUntil(maxTimeNs uint64, maxSteps int) int {
	steps := 0
	for steps < maxSteps && k.Step(maxTimeNs) {
		steps++
	}
	return steps
}

func (k *Kernel) Now() uint64         { return k.clock.Now() }
func (k *Kernel) TraceHash() string   { return k.tracer.HashHex() }
func (k *Kernel) TraceEvents() uint64 { return k.tracer.EventCount() }
func (k *Kernel) RecordTrace(nodeID, kind, details string) {
	k.nextSeq++
	k.tracer.Record(TraceEvent{
		Seq:     k.nextSeq,
		TimeNs:  k.clock.Now(),
		Node:    nodeID,
		Kind:    kind,
		Details: details,
	})
}
