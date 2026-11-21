// Package shards holds the guards of the shard packages below it. Each of
// them is a test binary of its own, which go test runs as a parallel
// process: conformance1 and 2 and oneconn1 and 2 run one share each of a
// SQLite conformance suite (enginetest.ConformanceSQLite,
// enginetest.ConformanceOnOneConnection), and retention, cluster and
// childflows wrap the test functions of enginetest's files of those names.
package shards

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

// TestEveryShareHasAPackage: the number of shard packages matches the
// share tables, so a share added to a table is never left without a
// binary that runs it.
func TestEveryShareHasAPackage(t *testing.T) {
	for _, s := range []struct {
		prefix string
		shares int
	}{{"conformance", enginetest.ConformanceShares}, {"oneconn", enginetest.OneConnectionShares}} {
		for i := 1; i <= s.shares+1; i++ {
			_, err := os.Stat(fmt.Sprintf("%s%d", s.prefix, i))
			if exists := err == nil; exists != (i <= s.shares) {
				t.Errorf("%s%d: package exists %t, want %t (%d shares)", s.prefix, i, exists, i <= s.shares, s.shares)
			}
		}
	}
}

// TestEveryRunnerHasAWrapper: every exported func(*testing.T) of the
// enginetest package (a test moved out of a _test.go file) is called by a
// Test function of the same name in a shard package, so no moved test is
// left without a binary that runs it, and none runs under another name.
func TestEveryRunnerHasAWrapper(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "..", func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	var runners []string
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Recv != nil || !fd.Name.IsExported() || len(fd.Type.Params.List) != 1 || len(fd.Type.Params.List[0].Names) != 1 {
					continue
				}
				if star, ok := fd.Type.Params.List[0].Type.(*ast.StarExpr); ok {
					if sel, ok := star.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "T" {
						runners = append(runners, fd.Name.Name)
					}
				}
			}
		}
	}
	if len(runners) == 0 {
		t.Fatal("found no exported func(*testing.T) in enginetest: the parse is wrong")
	}
	files, err := filepath.Glob("*/*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var wrappers strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		wrappers.Write(b)
	}
	for _, r := range runners {
		want := regexp.MustCompile(`func Test` + r + `\(t \*testing\.T\)\s*\{\s*enginetest\.` + r + `\(t\)\s*\}`)
		if !want.MatchString(wrappers.String()) {
			t.Errorf("enginetest.%s has no wrapper func Test%s(t *testing.T) { enginetest.%s(t) } in a shard package", r, r, r)
		}
	}
}
