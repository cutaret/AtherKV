package sim

// BaseClock tracks global simulated time in nanoseconds.
type BaseClock struct {
	nowNs uint64
}

func NewBaseClock(initialNs uint64) *BaseClock {
	return &BaseClock{nowNs: initialNs}
}

func (c *BaseClock) Now() uint64 {
	return c.nowNs
}

func (c *BaseClock) AdvanceTo(targetNs uint64) {
	if targetNs < c.nowNs {
		panic("sim.BaseClock: time cannot move backwards")
	}
	c.nowNs = targetNs
}

// NodeClock wraps BaseClock with per-node static offset and linear drift (in PPM).
type NodeClock struct {
	base     *BaseClock
	anchorNs uint64
	offsetNs int64 // Static offset in nanoseconds (+ ahead, - behind)
	driftPPM int64 // Parts-per-million drift (+ fast clock, - slow clock)
}

func NewNodeClock(base *BaseClock, offsetNs int64, driftPPM int64) *NodeClock {
	return &NodeClock{
		base:     base,
		anchorNs: base.Now(),
		offsetNs: offsetNs,
		driftPPM: driftPPM,
	}
}

func (nc *NodeClock) Now() uint64 {
	baseNow := nc.base.Now()
	elapsed := int64(baseNow - nc.anchorNs)
	driftDelta := (elapsed * nc.driftPPM) / 1_000_000
	adjusted := int64(baseNow) + nc.offsetNs + driftDelta
	if adjusted < 0 {
		return 0
	}
	return uint64(adjusted)
}

func (nc *NodeClock) SetSkew(offsetNs int64, driftPPM int64) {
	nc.anchorNs = nc.base.Now()
	nc.offsetNs = offsetNs
	nc.driftPPM = driftPPM
}
