package cli

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestGuard_StdlibOnly fails when the module requires anything or any
// non-test source file imports a package outside the standard library.
// The CLI talks to the admin API over HTTP; importing the library (even
// its core, which a go.work would let compile without a require line)
// would make installing the CLI download what an operator's laptop does
// not need, and tie it to one version of the server.
func TestGuard_StdlibOnly(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mod), "module github.com/ubgo/workflow/cli\n") {
		t.Fatal("go.mod does not declare github.com/ubgo/workflow/cli; the guard is reading the wrong file")
	}
	if strings.Contains(string(mod), "require") {
		t.Errorf("go.mod has a require line; the CLI must stay stdlib-only")
	}
	files := 0
	err = filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != "." {
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir // a nested module (e2e) is not part of this one
			}
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		files++
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if first, _, _ := strings.Cut(p, "/"); strings.Contains(first, ".") && p != "github.com/ubgo/workflow/cli" {
				t.Errorf("%s imports %s; the CLI may import only the standard library", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 4 {
		t.Fatalf("scanned %d files; the guard is not seeing the package", files)
	}
}
