package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cutaret/aetherkv/adapters/porcupine"
)

func main() {
	inPath := flag.String("in", "", "Path to JSON-lines client history file")
	flag.Parse()

	if *inPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: aether-check -in <history.jsonl>")
		os.Exit(2)
	}

	f, err := os.Open(*inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aether-check: failed to open %s: %v\n", *inPath, err)
		os.Exit(2)
	}
	defer f.Close()

	ops, err := porcupine.ImportJSONL(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "aether-check: parse error: %v\n", err)
		os.Exit(2)
	}

	res := porcupine.CheckLinearizable(ops)
	if res.Linearizable {
		fmt.Printf("aether-check PASS: history (%d ops) is strictly linearizable.\n", len(ops))
		os.Exit(0)
	}

	fmt.Printf("aether-check FAIL: %s\n", res.Explanation)
	os.Exit(1)
}
