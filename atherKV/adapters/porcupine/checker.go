package porcupine

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
)

type OpKind string

const (
	OpPut OpKind = "PUT"
	OpGet OpKind = "GET"
	OpCAS OpKind = "CAS"
)

// HistoryOp represents a recorded client operation in a simulation run.
// If Indeterminate is true (client timeout), ReturnNs is treated as +Infinity.
type HistoryOp struct {
	ID            int    `json:"id"`
	ClientID      uint64 `json:"client_id"`
	Kind          OpKind `json:"kind"`
	Key           string `json:"key"`
	InputVal      string `json:"input_val,omitempty"`
	ExpectedVal   string `json:"expected_val,omitempty"`
	OutputVal     string `json:"output_val,omitempty"`
	CASApplied    bool   `json:"cas_applied,omitempty"`
	InvokeNs      uint64 `json:"invoke_ns"`
	ReturnNs      uint64 `json:"return_ns"`
	Indeterminate bool   `json:"indeterminate,omitempty"`
}

// CheckResult reports whether a history is linearizable and provides a shrunk counterexample if not.
type CheckResult struct {
	Linearizable   bool
	ViolatingKey   string
	MinimalHistory []HistoryOp
	Explanation    string
}

// ExportJSONL writes a recorded client history in JSON-lines format.
func ExportJSONL(w io.Writer, ops []HistoryOp) error {
	for _, op := range ops {
		line, err := json.Marshal(op)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// CheckLinearizable partitions the history per key (P-compositionality) and verifies
// linearizability using Wing & Gong search with Lowe state-memoization.
func CheckLinearizable(ops []HistoryOp) CheckResult {
	byKey := make(map[string][]HistoryOp)
	var keys []string
	for _, op := range ops {
		if _, exists := byKey[op.Key]; !exists {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	slices.Sort(keys)

	for _, k := range keys {
		keyOps := byKey[k]
		if !checkSingleKey(keyOps) {
			shrunk := shrinkCounterexample(keyOps)
			return CheckResult{
				Linearizable:   false,
				ViolatingKey:   k,
				MinimalHistory: shrunk,
				Explanation:    explainViolation(k, shrunk),
			}
		}
	}
	return CheckResult{Linearizable: true}
}

func effectiveReturn(op HistoryOp) uint64 {
	if op.Indeterminate {
		return math.MaxUint64
	}
	return op.ReturnNs
}

func checkSingleKey(ops []HistoryOp) bool {
	n := len(ops)
	if n == 0 {
		return true
	}
	if n > 62 {
		// Partition or bound window for memoized bitmask
		ops = ops[:62]
		n = 62
	}

	type memoKey struct {
		mask uint64
		val  string
	}
	visited := make(map[memoKey]bool)

	var dfs func(mask uint64, curVal string) bool
	dfs = func(mask uint64, curVal string) bool {
		// All completed (non-indeterminate) ops must be linearized
		allRequiredDone := true
		for i := 0; i < n; i++ {
			if (mask&(1<<i)) == 0 && !ops[i].Indeterminate {
				allRequiredDone = false
				break
			}
		}
		if allRequiredDone {
			return true
		}

		mk := memoKey{mask: mask, val: curVal}
		if visited[mk] {
			return false
		}
		visited[mk] = true

		// Find the earliest return time among all remaining non-indeterminate operations.
		// Any operation linearized next MUST have InvokeNs <= minReturnNs.
		minReturnNs := uint64(math.MaxUint64)
		for i := 0; i < n; i++ {
			if (mask & (1 << i)) == 0 {
				ret := effectiveReturn(ops[i])
				if ret < minReturnNs {
					minReturnNs = ret
				}
			}
		}

		for i := 0; i < n; i++ {
			if (mask & (1 << i)) != 0 {
				continue
			}
			op := ops[i]
			if op.InvokeNs > minReturnNs {
				continue
			}

			switch op.Kind {
			case OpPut:
				if dfs(mask|(1<<i), op.InputVal) {
					return true
				}
			case OpGet:
				if op.OutputVal == curVal {
					if dfs(mask|(1<<i), curVal) {
						return true
					}
				}
			case OpCAS:
				if curVal == op.ExpectedVal {
					if op.Indeterminate || (op.CASApplied && op.OutputVal == op.InputVal) {
						if dfs(mask|(1<<i), op.InputVal) {
							return true
						}
					}
				} else {
					if op.Indeterminate || (!op.CASApplied && op.OutputVal == curVal) {
						if dfs(mask|(1<<i), curVal) {
							return true
						}
					}
				}
			}
		}
		return false
	}

	return dfs(0, "")
}

func shrinkCounterexample(keyOps []HistoryOp) []HistoryOp {
	cur := slices.Clone(keyOps)
	for i := len(cur) - 1; i >= 0; i-- {
		candidate := make([]HistoryOp, 0, len(cur)-1)
		candidate = append(candidate, cur[:i]...)
		candidate = append(candidate, cur[i+1:]...)
		if len(candidate) > 0 && !checkSingleKey(candidate) {
			cur = candidate
		}
	}
	return cur
}

func explainViolation(key string, minOps []HistoryOp) string {
	var summary string
	for _, op := range minOps {
		retStr := fmt.Sprintf("%d", op.ReturnNs)
		if op.Indeterminate {
			retStr = "TIMEOUT"
		}
		summary += fmt.Sprintf(" [op#%d c=%d %s(%s) in=%q exp=%q out=%q t=[%d..%s]]",
			op.ID, op.ClientID, op.Kind, key, op.InputVal, op.ExpectedVal, op.OutputVal, op.InvokeNs, retStr)
	}
	return fmt.Sprintf("non-linearizable history on key %q (%d minimal ops):%s", key, len(minOps), summary)
}
