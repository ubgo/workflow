package workflow_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var requireLine = regexp.MustCompile(`^\s*(?:require\s+)?([a-zA-Z0-9.\-_/]+\.[a-zA-Z0-9.\-_/]+)\s+v\S+`)

// TestGuard_CoreModuleIsStdlibOnly reads go.mod and fails on any require
// line. The core module needs nothing beyond the standard library: drivers
// with an SDK (hatchet), a database store or an OTel bridge are separate
// modules, so an app that never uses one never downloads it. A package in
// the core that imports a third-party module makes go mod tidy add a
// require line, and this test fails with its name.
func TestGuard_CoreModuleIsStdlibOnly(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "module github.com/ubgo/workflow\n") {
		t.Fatal("go.mod does not declare github.com/ubgo/workflow; the guard is reading the wrong file")
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
		if m := requireLine.FindStringSubmatch(line); m != nil {
			t.Errorf("the core module requires %s; move the package that needs it into a module of its own", m[1])
		}
	}
}
