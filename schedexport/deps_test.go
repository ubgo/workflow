package schedexport_test

import (
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// allowedCore is the part of the core module schedexport may import.
var allowedCore = map[string]bool{
	"github.com/ubgo/workflow":       true,
	"github.com/ubgo/workflow/clock": true,
	"github.com/ubgo/workflow/cron":  true,
}

// TestGuard_StdlibAndCoreOnly reads every non-test file of the package
// and fails on an import that is neither the standard library nor the
// core. The module-wide guard (deps_test.go at the root) only sees go.mod
// require lines; this one also stops a reach into another core package
// (a driver, middleware) that would make writing a file pull in a
// scheduler. A standard-library path has no dot in its first element.
//
//ds:def id=schedexport-testguard-stdlibandcoreonly-xkunw5qa owner=@khanakia stability=stable
func TestGuard_StdlibAndCoreOnly(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			first, _, _ := strings.Cut(path, "/")
			if !strings.Contains(first, ".") || allowedCore[path] {
				continue
			}
			t.Errorf("%s imports %s; schedexport may import only the standard library and the core packages workflow, clock and cron", name, path)
		}
	}
	if checked == 0 {
		t.Fatal("no source files found; the guard is reading the wrong directory")
	}
}
