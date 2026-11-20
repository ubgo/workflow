package workflow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// capDecl is one capability as the source declares it.
type capDecl struct {
	ident, name string
	n           int
	pos         string
}

// declaredCaps reads every package-level var of the core (test files
// aside) and returns the capabilities declared with defineCap. It fails
// the test for an exported Cap var that bypasses defineCap or gives it a
// computed number, since the guard below could not see that one.
func declaredCaps(t *testing.T) []capDecl {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var out []capDecl
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || (g.Tok != token.VAR && g.Tok != token.CONST) {
				continue
			}
			for _, s := range g.Specs {
				vs := s.(*ast.ValueSpec)
				for i, id := range vs.Names {
					var call *ast.CallExpr
					if i < len(vs.Values) {
						call, _ = vs.Values[i].(*ast.CallExpr)
					}
					isDefine := call != nil && isIdent(call.Fun, "defineCap")
					if !isDefine {
						if strings.HasPrefix(id.Name, "Cap") && id.IsExported() && len(id.Name) > 3 {
							t.Errorf("%s: %s is not declared with defineCap", fset.Position(id.Pos()), id.Name)
						}
						continue
					}
					num, okN := call.Args[0].(*ast.BasicLit)
					name, okS := call.Args[1].(*ast.BasicLit)
					if !okN || num.Kind != token.INT || !okS || name.Kind != token.STRING {
						t.Errorf("%s: %s: defineCap needs a literal number and name", fset.Position(id.Pos()), id.Name)
						continue
					}
					n, _ := strconv.Atoi(num.Value)
					s, _ := strconv.Unquote(name.Value)
					out = append(out, capDecl{id.Name, s, n, fset.Position(id.Pos()).String()})
				}
			}
		}
	}
	return out
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

// TestCapsDistinct is the guard: it fails when two capabilities share a
// number or a name, when a capability's name is not its identifier
// without "Cap", or when the runtime table disagrees with the source.
func TestCapsDistinct(t *testing.T) {
	if len(capClashes) > 0 {
		t.Errorf("capabilities share a number: %s", strings.Join(capClashes, "; "))
	}
	decls := declaredCaps(t)
	if len(decls) < 61 {
		t.Fatalf("found %d capability declarations, want at least the 61 of v0.1.1", len(decls))
	}
	byNum := map[int]capDecl{}
	byName := map[string]capDecl{}
	for _, d := range decls {
		if prev, ok := byNum[d.n]; ok {
			t.Errorf("%s and %s share number %d (%s, %s)", prev.ident, d.ident, d.n, prev.pos, d.pos)
		}
		if prev, ok := byName[d.name]; ok {
			t.Errorf("%s and %s share name %q", prev.ident, d.ident, d.name)
		}
		byNum[d.n], byName[d.name] = d, d
		if d.ident != "Cap"+d.name {
			t.Errorf("%s is named %q, want %q", d.ident, d.name, strings.TrimPrefix(d.ident, "Cap"))
		}
		if d.n < 0 || d.n >= capBits {
			t.Errorf("%s has number %d, outside 0..%d", d.ident, d.n, capBits-1)
			continue
		}
		if capNames[d.n] != d.name {
			t.Errorf("number %d is %q at run time, %q in the source", d.n, capNames[d.n], d.name)
		}
		if c, err := ParseCaps(d.name); err != nil || c != capBit(d.n) {
			t.Errorf("ParseCaps(%q) = %s, %v; want number %d", d.name, c, err, d.n)
		}
	}
	if got := strings.Count(AllCaps().String(), "|") + 1; got != len(decls) {
		t.Errorf("AllCaps holds %d capabilities, the source declares %d", got, len(decls))
	}
}

// TestCapsDistinctCatchesAClash breaks the rule on purpose and checks the
// guard sees it, so the guard cannot pass vacuously.
func TestCapsDistinctCatchesAClash(t *testing.T) {
	saved := capClashes
	t.Cleanup(func() { capClashes = saved })
	c := defineCap(0, "Imposter")
	if len(capClashes) != len(saved)+1 || !strings.Contains(capClashes[len(capClashes)-1], "Durable and Imposter") {
		t.Fatalf("clash not recorded: %v", capClashes)
	}
	if capNames[0] != "Durable" || c != CapDurable {
		t.Fatalf("a clash renamed number 0 to %q", capNames[0])
	}
	defer func() {
		if recover() == nil {
			t.Fatal("defineCap past the last number did not panic")
		}
	}()
	defineCap(capBits, "TooFar")
}

// TestCapsSetOps checks the set operations at the word boundaries (63/64,
// 127/128, the last number), by number so it holds whatever names a
// release gives them.
func TestCapsSetOps(t *testing.T) {
	nums := []int{0, 63, 64, 127, 128, 200, capBits - 1}
	var all Caps
	for _, n := range nums {
		b := capBit(n)
		if b.IsZero() || all.HasAny(b) {
			t.Fatalf("bit %d is empty or aliases another", n)
		}
		all = all.With(b)
	}
	count := 0
	all.each(func(int) { count++ })
	if count != len(nums) {
		t.Fatalf("set of %d numbers holds %d", len(nums), count)
	}
	for _, n := range nums {
		b := capBit(n)
		if !all.Has(b) || all.Without(b).Has(b) || all.Without(b).With(b) != all {
			t.Fatalf("Has/Without/With wrong at %d", n)
		}
		if all.Intersect(b) != b || all.Missing(b) != (Caps{}) || (Caps{}).Missing(b) != b {
			t.Fatalf("Intersect/Missing wrong at %d", n)
		}
	}
	if !all.Has(Caps{}) || (Caps{}).HasAny(all) || !CapsOf().IsZero() || CapsOf(all) != all {
		t.Fatal("empty-set rules broken")
	}
	// Methods return new sets; the receiver is untouched.
	before := all
	_ = all.With(capBit(1))
	_ = all.Without(capBit(0))
	_ = all.Intersect(Caps{})
	if all != before {
		t.Fatal("a method changed its receiver")
	}
}
