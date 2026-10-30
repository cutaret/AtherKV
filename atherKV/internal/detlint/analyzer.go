package detlint

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
)

// Violation represents a single determinism contract breach in a core package.
type Violation struct {
	File    string
	Line    int
	Col     int
	Rule    string
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s:%d:%d: [%s] %s", v.File, v.Line, v.Col, v.Rule, v.Message)
}

var forbiddenImports = map[string]string{
	"math/rand":    "use injected sim.Rand stream instead of global math/rand",
	"math/rand/v2": "use injected sim.Rand stream instead of global math/rand/v2",
	"crypto/rand":  "use injected sim.Rand stream instead of nondeterministic crypto/rand",
}

var forbiddenTimeFuncs = map[string]bool{
	"Now":   true,
	"Since": true,
	"Until": true,
	"After": true,
	"Sleep": true,
	"Tick":  true,
}

// CheckFiles parses and type-checks a set of Go source files belonging to a package
// and returns any determinism violations.
func CheckFiles(fset *token.FileSet, files []*ast.File, pkgPath string) []Violation {
	info := &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}

	conf := types.Config{
		Importer:                 importer.Default(),
		IgnoreFuncBodies:         false,
		DisableUnusedImportCheck: true,
		Error:                    func(err error) {}, // Continue AST + partial type analysis even if external types are missing
	}
	_, _ = conf.Check(pkgPath, fset, files, info)

	var violations []Violation

	for _, file := range files {
		// 1. Check imports for math/rand or crypto/rand
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if reason, bad := forbiddenImports[importPath]; bad {
				pos := fset.Position(imp.Pos())
				violations = append(violations, Violation{
					File:    pos.Filename,
					Line:    pos.Line,
					Col:     pos.Column,
					Rule:    "forbidden-rand-import",
					Message: fmt.Sprintf("import %q is forbidden in core packages: %s", importPath, reason),
				})
			}
		}

		// 2. Walk AST for go statements, time.* calls, and map range loops
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				return false
			}
			switch stmt := n.(type) {
			case *ast.GoStmt:
				pos := fset.Position(stmt.Go)
				violations = append(violations, Violation{
					File:    pos.Filename,
					Line:    pos.Line,
					Col:     pos.Column,
					Rule:    "no-goroutines",
					Message: "raw 'go' statement is forbidden in core packages; schedule tasks via sim.Scheduler",
				})

			case *ast.CallExpr:
				if sel, ok := stmt.Fun.(*ast.SelectorExpr); ok {
					if pkgIdent, ok := sel.X.(*ast.Ident); ok && pkgIdent.Name == "time" {
						if forbiddenTimeFuncs[sel.Sel.Name] {
							pos := fset.Position(stmt.Pos())
							violations = append(violations, Violation{
								File:    pos.Filename,
								Line:    pos.Line,
								Col:     pos.Column,
								Rule:    "no-wall-clock",
								Message: fmt.Sprintf("call to time.%s() is forbidden; use injected sim.Clock", sel.Sel.Name),
							})
						}
					}
				}

			case *ast.RangeStmt:
				if isMapExpr(stmt.X, info) {
					pos := fset.Position(stmt.For)
					violations = append(violations, Violation{
						File:    pos.Filename,
						Line:    pos.Line,
						Col:     pos.Column,
						Rule:    "no-unsorted-map-range",
						Message: "direct 'for ... range' over a map is nondeterministic in Go; extract and sort keys first",
					})
				}
			}
			return true
		})
	}

	return violations
}

func isMapExpr(expr ast.Expr, info *types.Info) bool {
	if tv, ok := info.Types[expr]; ok && tv.Type != nil {
		if _, isMap := tv.Type.Underlying().(*types.Map); isMap {
			return true
		}
	}
	if ident, ok := expr.(*ast.Ident); ok {
		if obj, ok := info.Uses[ident]; ok && obj.Type() != nil {
			if _, isMap := obj.Type().Underlying().(*types.Map); isMap {
				return true
			}
		}
		if ident.Obj != nil {
			if assign, ok := ident.Obj.Decl.(*ast.AssignStmt); ok {
				for _, rhs := range assign.Rhs {
					if comp, ok := rhs.(*ast.CompositeLit); ok {
						if _, isMap := comp.Type.(*ast.MapType); isMap {
							return true
						}
					}
					if call, ok := rhs.(*ast.CallExpr); ok && len(call.Args) > 0 {
						if _, isMap := call.Args[0].(*ast.MapType); isMap {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// CheckSourceString is a helper used by unit/sanity tests to lint an in-memory Go file.
func CheckSourceString(filename, src string) ([]Violation, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	return CheckFiles(fset, []*ast.File{f}, "testpkg"), nil
}
