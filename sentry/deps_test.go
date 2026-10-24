package sentry_test

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
// path: the standard library (no dot in the first element), sentry-go,
// or the workflow core itself. A driver, store or other ubgo module would
// make every app that reports to Sentry download it.
func allowedImport(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	switch {
	case !strings.Contains(first, "."):
		return true
	case path == "github.com/getsentry/sentry-go", strings.HasPrefix(path, "github.com/getsentry/sentry-go/"):
		return true
	case path == "github.com/ubgo/workflow":
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
				t.Errorf("%s imports %s; the sentry module may import only the standard library, sentry-go and the workflow core", f, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no imports checked; the guard is reading the wrong directory")
	}
}

// TestGuard_GoModRequires fails on a direct require other than sentry-go
// and the workflow core.
func TestGuard_GoModRequires(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "module github.com/ubgo/workflow/sentry\n") {
		t.Fatal("go.mod does not declare github.com/ubgo/workflow/sentry; the guard is reading the wrong file")
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
		if mod != "github.com/getsentry/sentry-go" && mod != "github.com/ubgo/workflow" {
			t.Errorf("go.mod requires %s directly; the sentry module may require only sentry-go and the workflow core", mod)
		}
	}
}
