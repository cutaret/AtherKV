package raft

import (
	"bytes"
	"fmt"
	"slices"
)

// InvariantViolation describes the exact safety property that fired on a simulated step.
type InvariantViolation struct {
	Property string
	Details  string
}

func (v InvariantViolation) Error() string {
	return fmt.Sprintf("[%s] %s", v.Property, v.Details)
}

// NodeStateView captures a single node's instantaneous state for the global observer.
type NodeStateView struct {
	ID         string
	Crashed    bool
	Role       Role
	Term       uint64
	CommitIdx  uint64
	Log        []Entry
	AppliedLog []Entry
}

type termLeaderRecord struct {
	term     uint64
	leaderID string
	lastLog  []Entry
}

type committedRecord struct {
	index       uint64
	term        uint64
	committedAt uint64 // Term in which this entry first became committed
	data        []byte
}

// Observer tracks cluster history across all steps and verifies Raft Figure 3 safety properties.
type Observer struct {
	nodeIDs      []string
	highestTerm  map[string]uint64
	termLeaders  []termLeaderRecord
	committed    []committedRecord
	appliedByIdx map[uint64]Entry
}

func NewObserver(nodeIDs []string) *Observer {
	sorted := slices.Clone(nodeIDs)
	slices.Sort(sorted)
	return &Observer{
		nodeIDs:      sorted,
		highestTerm:  make(map[string]uint64),
		appliedByIdx: make(map[uint64]Entry),
	}
}

// RecordCommitted explicitly registers an entry acknowledged as committed by a leader or quorum.
func (o *Observer) RecordCommitted(entry Entry, committedInTerm uint64) *InvariantViolation {
	for _, c := range o.committed {
		if c.index == entry.Index {
			if c.term != entry.Term || !bytes.Equal(c.data, entry.Data) {
				return &InvariantViolation{
					Property: "StateMachineSafety",
					Details: fmt.Sprintf("conflicting entries committed at index %d: (term=%d, data=%q) vs (term=%d, data=%q)",
						entry.Index, c.term, c.data, entry.Term, entry.Data),
				}
			}
			return nil
		}
	}
	o.committed = append(o.committed, committedRecord{
		index:       entry.Index,
		term:        entry.Term,
		committedAt: committedInTerm,
		data:        slices.Clone(entry.Data),
	})
	return nil
}

// Check inspects all nodes after a step and returns the first InvariantViolation if any property fails.
func (o *Observer) Check(views []NodeStateView) *InvariantViolation {
	// 1. Term Monotonicity (even across crashes and restarts)
	for _, v := range views {
		if v.Crashed {
			continue
		}
		prev, seen := o.highestTerm[v.ID]
		if seen && v.Term < prev {
			return &InvariantViolation{
				Property: "TermMonotonicity",
				Details:  fmt.Sprintf("node %s term decreased from %d to %d after restart", v.ID, prev, v.Term),
			}
		}
		o.highestTerm[v.ID] = v.Term
	}

	// 2. Election Safety & 3. Leader Append-Only
	for _, v := range views {
		if v.Crashed || v.Role != RoleLeader {
			continue
		}
		foundTerm := false
		for i := range o.termLeaders {
			rec := &o.termLeaders[i]
			if rec.term == v.Term {
				foundTerm = true
				if rec.leaderID != v.ID {
					return &InvariantViolation{
						Property: "ElectionSafety",
						Details:  fmt.Sprintf("two leaders elected in term %d: %s and %s", v.Term, rec.leaderID, v.ID),
					}
				}
				// Leader Append-Only: previous leader log must be a prefix of current leader log in same term
				if len(v.Log) < len(rec.lastLog) {
					return &InvariantViolation{
						Property: "LeaderAppendOnly",
						Details:  fmt.Sprintf("leader %s in term %d shrunk its log from %d to %d", v.ID, v.Term, len(rec.lastLog), len(v.Log)),
					}
				}
				for idx := 0; idx < len(rec.lastLog); idx++ {
					if v.Log[idx].Term != rec.lastLog[idx].Term || !bytes.Equal(v.Log[idx].Data, rec.lastLog[idx].Data) {
						return &InvariantViolation{
							Property: "LeaderAppendOnly",
							Details:  fmt.Sprintf("leader %s mutated log entry at index %d in term %d", v.ID, idx+1, v.Term),
						}
					}
				}
				rec.lastLog = slices.Clone(v.Log)
			}
		}
		if !foundTerm {
			o.termLeaders = append(o.termLeaders, termLeaderRecord{
				term:     v.Term,
				leaderID: v.ID,
				lastLog:  slices.Clone(v.Log),
			})
		}
	}

	// 4. Log Matching across all pairs of live nodes
	for i := 0; i < len(views); i++ {
		for j := i + 1; j < len(views); j++ {
			a, b := views[i], views[j]
			if a.Crashed || b.Crashed {
				continue
			}
			minLen := min(len(a.Log), len(b.Log))
			for k := minLen - 1; k >= 0; k-- {
				if a.Log[k].Term == b.Log[k].Term {
					// All preceding entries [0..k] must match identically
					for p := 0; p <= k; p++ {
						if a.Log[p].Term != b.Log[p].Term || !bytes.Equal(a.Log[p].Data, b.Log[p].Data) {
							return &InvariantViolation{
								Property: "LogMatching",
								Details: fmt.Sprintf("nodes %s and %s match at index %d (term %d) but diverge at index %d",
									a.ID, b.ID, k+1, a.Log[k].Term, p+1),
							}
						}
					}
					break
				}
			}
		}
	}

	// Register newly committed entries from views
	for _, v := range views {
		if v.Crashed {
			continue
		}
		for i := uint64(1); i <= v.CommitIdx && i <= uint64(len(v.Log)); i++ {
			e := v.Log[i-1]
			if viol := o.RecordCommitted(e, v.Term); viol != nil {
				return viol
			}
		}
	}

	// 5. State Machine Safety across applied entries
	for _, v := range views {
		for _, app := range v.AppliedLog {
			if prev, exists := o.appliedByIdx[app.Index]; exists {
				if prev.Term != app.Term || !bytes.Equal(prev.Data, app.Data) {
					return &InvariantViolation{
						Property: "StateMachineSafety",
						Details: fmt.Sprintf("node %s applied (term=%d, data=%q) at index %d, conflicting with (term=%d, data=%q)",
							v.ID, app.Term, app.Data, app.Index, prev.Term, prev.Data),
					}
				}
			} else {
				o.appliedByIdx[app.Index] = app
			}
		}
	}

	// 6. Leader Completeness & Committed Durability
	for _, v := range views {
		if v.Crashed || v.Role != RoleLeader {
			continue
		}
		for _, c := range o.committed {
			// If entry was committed in an earlier or equal term, leader MUST have it in its log
			if v.Term >= c.committedAt {
				if uint64(len(v.Log)) < c.index {
					return &InvariantViolation{
						Property: "CommittedDurability",
						Details: fmt.Sprintf("leader %s (term %d) is missing committed entry at index %d (committed in term %d)",
							v.ID, v.Term, c.index, c.committedAt),
					}
				}
				got := v.Log[c.index-1]
				if got.Term != c.term || !bytes.Equal(got.Data, c.data) {
					return &InvariantViolation{
						Property: "LeaderCompleteness",
						Details: fmt.Sprintf("leader %s (term %d) has (term=%d, data=%q) at index %d instead of committed (term=%d, data=%q)",
							v.ID, v.Term, got.Term, got.Data, c.index, c.term, c.data),
					}
				}
			}
		}
	}

	return nil
}
