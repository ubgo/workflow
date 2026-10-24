package redis_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// allowedImport reports whether a package of this module may import
// path: the standard library (no dot in the first element), go-redis,
// or packages of the workflow core module (its root, clock, cron and
// middleware packages; middleware is imported for the DebounceStore
// type). All of those come with the core module this driver already
// requires, so go-redis stays the only third-party dependency. A driver,
// store or other ubgo module (sqlb, a SQL driver) would make every app on
// Redis download it.
func allowedImport(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	switch {
	case !strings.Contains(first, "."):
		return true
	case path == "github.com/redis/go-redis/v9", strings.HasPrefix(path, "github.com/redis/go-redis/v9/"):
		return true
	case path == "github.com/ubgo/workflow", path == "github.com/ubgo/workflow/clock", path == "github.com/ubgo/workflow/cron",
		path == "github.com/ubgo/workflow/middleware":
		return true
	}
	return false
}

// TestGuard_Imports parses every non-test Go file of the module (the
// parser reads multi-line import blocks, which a line match would miss)
// and fails on any import outside allowedImport.
func TestGuard_Imports(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range parsed.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			checked++
			if !allowedImport(path) {
				t.Errorf("%s imports %s; the redis module may import only the standard library, go-redis and the workflow core", f, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no imports checked; the guard is reading the wrong directory")
	}
}

// TestGuard_GoModRequires fails on a direct require other than go-redis
// and the workflow core.
func TestGuard_GoModRequires(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "module github.com/ubgo/workflow/redis\n") {
		t.Fatal("go.mod does not declare github.com/ubgo/workflow/redis; the guard is reading the wrong file")
	}
	inRequire := false
	for _, line := range strings.Split(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "require ("):
			inRequire = true
			continue
		case inRequire && trimmed == ")":
			inRequire = false
			continue
		case !inRequire && !strings.HasPrefix(trimmed, "require "):
			continue
		}
		if trimmed == "" || strings.HasSuffix(trimmed, "// indirect") {
			continue
		}
		mod := strings.Fields(strings.TrimPrefix(trimmed, "require "))[0]
		if mod != "github.com/redis/go-redis/v9" && mod != "github.com/ubgo/workflow" {
			t.Errorf("go.mod requires %s directly; the redis module may require only go-redis and the workflow core", mod)
		}
	}
}
