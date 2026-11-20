package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write creates the files of a throwaway module tree under root.
func write(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCheck is the rule both ways on local modules (no network): a library
// whose only use of a module is in a test is flagged, and the same library
// with that test moved out passes.
func TestCheck(t *testing.T) {
	root := t.TempDir()
	write(t, root, map[string]string{
		"helper/go.mod":    "module example.com/helper\n\ngo 1.24\n",
		"helper/helper.go": "package helper\n\nfunc Open() {}\n",
		"lib/go.mod":       "module example.com/lib\n\ngo 1.24\n\nrequire example.com/helper v0.0.0\n\nreplace example.com/helper => ../helper\n",
		"lib/lib.go":       "package lib\n\nfunc Do() {}\n",
		"lib/lib_test.go":  "package lib\n\nimport (\n\t\"testing\"\n\n\t\"example.com/helper\"\n)\n\nfunc TestDo(t *testing.T) { helper.Open(); Do() }\n",
	})
	lib := filepath.Join(root, "lib")
	problems, err := check(lib)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || !strings.Contains(problems[0], "requires example.com/helper, which no library package imports") {
		t.Fatalf("test-only require: problems = %q, want the helper flagged", problems)
	}

	// The fix: the test leaves, so the require goes too.
	if err := os.Remove(filepath.Join(lib, "lib_test.go")); err != nil {
		t.Fatal(err)
	}
	write(t, root, map[string]string{"lib/go.mod": "module example.com/lib\n\ngo 1.24\n"})
	if problems, err := check(lib); err != nil || len(problems) != 0 {
		t.Fatalf("clean module: problems = %q, err = %v", problems, err)
	}

	// A require the library does use passes, and an untidy go.mod is caught.
	write(t, root, map[string]string{
		"lib/go.mod": "module example.com/lib\n\ngo 1.24\n\nrequire example.com/helper v0.0.0\n\nreplace example.com/helper => ../helper\n",
		"lib/lib.go": "package lib\n\nimport \"example.com/helper\"\n\nfunc Do() { helper.Open() }\n",
	})
	if problems, err := check(lib); err != nil || len(problems) != 0 {
		t.Fatalf("used require: problems = %q, err = %v", problems, err)
	}
	write(t, root, map[string]string{"lib/go.mod": "module example.com/lib\n\ngo 1.24\n\nreplace example.com/helper => ../helper\n"})
	problems, err = check(lib)
	if err == nil && (len(problems) == 0 || !strings.Contains(problems[0], "not tidy")) {
		t.Fatalf("missing require: problems = %q, want go.mod not tidy", problems)
	}
}
