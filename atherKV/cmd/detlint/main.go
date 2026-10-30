package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/cutaret/aetherkv/internal/detlint"
)

var defaultCoreDirs = []string{"statemachine", "raft", "lsm", "kv"}

func main() {
	dirs := os.Args[1:]
	if len(dirs) == 0 {
		dirs = defaultCoreDirs
	}

	var allViolations []detlint.Violation
	checkedFiles := 0

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			continue // Skip directories not yet created in earlier milestones
		}
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			fset := token.NewFileSet()
			var files []*ast.File
			for _, entry := range entries {
				name := entry.Name()
				if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
					continue
				}
				fullPath := filepath.Join(path, name)
				parsed, parseErr := parser.ParseFile(fset, fullPath, nil, parser.ParseComments)
				if parseErr != nil {
					fmt.Fprintf(os.Stderr, "detlint parse error in %s: %v\n", fullPath, parseErr)
					os.Exit(2)
				}
				files = append(files, parsed)
				checkedFiles++
			}
			if len(files) > 0 {
				v := detlint.CheckFiles(fset, files, path)
				allViolations = append(allViolations, v...)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "detlint error walking %s: %v\n", dir, err)
			os.Exit(2)
		}
	}

	if len(allViolations) > 0 {
		for _, v := range allViolations {
			fmt.Fprintln(os.Stderr, v.String())
		}
		fmt.Fprintf(os.Stderr, "\ndetlint FAILED: %d determinism violation(s) across %d file(s).\n", len(allViolations), checkedFiles)
		os.Exit(1)
	}

	fmt.Printf("detlint PASSED: 0 violations across %d core file(s).\n", checkedFiles)
}
