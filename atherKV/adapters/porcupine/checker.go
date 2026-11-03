package porcupine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
)

type OpKind string

const (
	OpPut  OpKind = "PUT"
	OpGet  OpKind = "GET"
	OpCAS  OpKind = "CAS"
	OpScan OpKind = "SCAN"
)

// HistoryOp represents a recorded client operation in a simulation run.
// If Indeterminate is true (client timeout), ReturnNs is treated as +Infinity.
type HistoryOp struct {
	ID            int    `json:"id"`
	ClientID      uint64 `json:"client_id"`
	Kind          OpKind `json:"kind"`
	Key           string `json:"key"`
	EndKey        string `json:"end_key,omitempty"`
	InputVal      string `json:"input_val,omitempty"`
	ExpectedVal   string `json:"expected_val,omitempty"`
	OutputVal     string `json:"output_val,omitempty"`
	ScanOut       string `json:"scan_out,omitempty"` // Formatted as "k1=v1;k2=v2"
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

// ImportJSONL parses a JSON-lines history file into a slice of HistoryOp.
func ImportJSONL(r io.Reader) ([]HistoryOp, error) {
	var ops []HistoryOp
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var op HistoryOp
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, scanner.Err()
}

// CheckLinearizable partitions the history per key when no SCAN operations are present,
// or checks the whole bounded keyspace when range scans span multiple keys.
func CheckLinearizable(ops []HistoryOp) CheckResult {
	hasScan := false
	for _, op := range ops {
		if op.Kind == OpScan {
			hasScan = true
			break
		}
	}

	if hasScan {
		if !checkMultiKeyScan(ops) {
			shrunk := shrinkMultiKeyCounterexample(ops)
			return CheckResult{
				Linearizable:   false,
				ViolatingKey:   "[multi-key-scan]",
				MinimalHistory: shrunk,
				Explanation:    explainViolation("[multi-key-scan]", shrunk),
			}
		}
		return CheckResult{Linearizable: true}
	}

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

// checkMultiKeyScan verifies linearizability over bounded multi-key histories containing range scans.
func checkMultiKeyScan(ops []HistoryOp) bool {
	n := len(ops)
	if n == 0 {
		return true
	}
	if n > 30 {
		ops = ops[:30]
		n = 30
	}

	encodeState := func(m map[string]string) string {
		var keys []string
		for k, v := range m {
			if v != "" {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, k+"="+m[k])
		}
		return strings.Join(parts, ";")
	}

	evalScan := func(m map[string]string, startKey, endKey string) string {
		var keys []string
		for k, v := range m {
			if v == "" {
				continue
			}
			if k >= startKey && (endKey == "" || k < endKey) {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, k+"="+m[k])
		}
		return strings.Join(parts, ";")
	}

	type memoKey struct {
		mask  uint64
		state string
	}
	visited := make(map[memoKey]bool)

	var dfs func(mask uint64, state map[string]string) bool
	dfs = func(mask uint64, state map[string]string) bool {
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

		mk := memoKey{mask: mask, state: encodeState(state)}
		if visited[mk] {
			return false
		}
		visited[mk] = true

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
				old := state[op.Key]
				state[op.Key] = op.InputVal
				if dfs(mask|(1<<i), state) {
					return true
				}
				state[op.Key] = old

			case OpGet:
				if state[op.Key] == op.OutputVal {
					if dfs(mask|(1<<i), state) {
						return true
					}
				}

			case OpScan:
				if evalScan(state, op.Key, op.EndKey) == op.ScanOut {
					if dfs(mask|(1<<i), state) {
						return true
					}
				}
			}
		}
		return false
	}

	return dfs(0, make(map[string]string))
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

func shrinkMultiKeyCounterexample(ops []HistoryOp) []HistoryOp {
	cur := slices.Clone(ops)
	for i := len(cur) - 1; i >= 0; i-- {
		candidate := make([]HistoryOp, 0, len(cur)-1)
		candidate = append(candidate, cur[:i]...)
		candidate = append(candidate, cur[i+1:]...)
		if len(candidate) > 0 && !checkMultiKeyScan(candidate) {
			cur = candidate
		}
	}
	return cur
}

func explainViolation(key string, minOps []HistoryOp) string {
	var summary string
	var latestWriteVal string
	var latestWriteReturn uint64

	for _, op := range minOps {
		retStr := fmt.Sprintf("%d", op.ReturnNs)
		if op.Indeterminate {
			retStr = "TIMEOUT"
		}
		summary += fmt.Sprintf(" [op#%d c=%d %s(%s) in=%q exp=%q out=%q scan=%q t=[%d..%s]]",
			op.ID, op.ClientID, op.Kind, op.Key, op.InputVal, op.ExpectedVal, op.OutputVal, op.ScanOut, op.InvokeNs, retStr)
		if op.Kind == OpPut && !op.Indeterminate && op.ReturnNs >= latestWriteReturn {
			latestWriteVal = op.InputVal
			latestWriteReturn = op.ReturnNs
		}
	}

	reason := "no valid sequential ordering satisfies real-time invocation/return constraints"
	for _, op := range minOps {
		if op.Kind == OpGet && op.InvokeNs > latestWriteReturn && latestWriteReturn > 0 && op.OutputVal != latestWriteVal {
			reason = fmt.Sprintf("stale read: GET(op#%d) invoked at t=%d after PUT(%q) completed at t=%d, but observed older value %q",
				op.ID, op.InvokeNs, latestWriteVal, latestWriteReturn, op.OutputVal)
			break
		}
	}

	return fmt.Sprintf("non-linearizable history on key %q (%d minimal ops; %s):%s", key, len(minOps), reason, summary)
}
