package porcupine_test

import (
	"bytes"
	"testing"

	"github.com/cutaret/aetherkv/adapters/porcupine"
)

func FuzzPorcupineChecker(f *testing.F) {
	// Seed 1: valid linearizable single-key history
	f.Add([]byte(
		`{"id":1,"client_id":1,"kind":"PUT","key":"k1","input_val":"v1","invoke_ns":10,"return_ns":20}` + "\n" +
			`{"id":2,"client_id":2,"kind":"GET","key":"k1","output_val":"v1","invoke_ns":25,"return_ns":30}` + "\n",
	))
	// Seed 2: non-linearizable stale read
	f.Add([]byte(
		`{"id":1,"client_id":1,"kind":"PUT","key":"k1","input_val":"v1","invoke_ns":10,"return_ns":20}` + "\n" +
			`{"id":2,"client_id":2,"kind":"GET","key":"k1","output_val":"","invoke_ns":25,"return_ns":30}` + "\n",
	))
	// Seed 3: multi-key range scan history
	f.Add([]byte(
		`{"id":1,"client_id":1,"kind":"PUT","key":"a","input_val":"1","invoke_ns":10,"return_ns":20}` + "\n" +
			`{"id":2,"client_id":2,"kind":"SCAN","key":"a","end_key":"z","scan_out":"a=1","invoke_ns":25,"return_ns":30}` + "\n",
	))
	// Seed 4: malformed JSONL
	f.Add([]byte("{not valid json\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ops, err := porcupine.ImportJSONL(bytes.NewReader(data))
		if err != nil || len(ops) == 0 {
			return
		}
		// Cap history length during fuzzing so pathological concurrent histories stay fast
		if len(ops) > 8 {
			ops = ops[:8]
		}
		_ = porcupine.CheckLinearizable(ops)
	})
}
