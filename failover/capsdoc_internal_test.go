package failover

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// TestCapabilitiesDocNamesEveryRule: the Capabilities doc comment names
// every capability the method treats specially (each one of scheduleCaps
// and primaryOnlyCaps, and CapRunFinished), so the comment cannot fall
// behind the sets again when one grows.
func TestCapabilitiesDocNamesEveryRule(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "failover.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var doc string
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "Capabilities" && fn.Recv != nil {
			doc = fn.Doc.Text()
		}
	}
	if doc == "" {
		t.Fatal("Client.Capabilities has no doc comment")
	}
	for _, set := range []workflow.Caps{scheduleCaps, primaryOnlyCaps, workflow.CapRunFinished} {
		for _, name := range strings.Split(set.String(), "|") {
			if !regexp.MustCompile(`\bCap` + name + `\b`).MatchString(doc) {
				t.Errorf("the Capabilities doc comment does not name Cap%s", name)
			}
		}
	}
}
