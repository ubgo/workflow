package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const src = `package main

func main() {
	// snippet:outer
	a := 1
	// snippet:inner
	if a > 0 {
		b := 2
	}
	// snippet:end
	// snippet:end
}
`

func write(t *testing.T, dir, rel, body string) string {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRegion(t *testing.T) {
	dir := t.TempDir()
	p := write(t, dir, "x/main.go", src)
	got, err := region(p, "inner")
	if err != nil {
		t.Fatal(err)
	}
	if want := "if a > 0 {\n\tb := 2\n}"; strings.Join(got, "\n") != want {
		t.Errorf("inner = %q, want %q", strings.Join(got, "\n"), want)
	}
	got, err = region(p, "outer")
	if err != nil {
		t.Fatal(err)
	}
	if want := "a := 1\nif a > 0 {\n\tb := 2\n}"; strings.Join(got, "\n") != want {
		t.Errorf("outer = %q, want %q", strings.Join(got, "\n"), want)
	}
	if _, err := region(p, "nope"); err == nil {
		t.Error("missing region: want an error")
	}
}

func TestCheckAndSync(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "x/main.go", src)
	fresh := "Intro.\n\n<!-- snippet: x/main.go#inner -->\n```go\nif a > 0 {\n\tb := 2\n}\n```\n"
	stale := "Intro.\n\n<!-- snippet: x/main.go#inner -->\n```go\nif a > 1 {\n}\n```\n\nAfter.\n"
	write(t, dir, "docs/fresh.md", fresh)
	guide := write(t, dir, "docs/stale.md", stale)
	write(t, dir, "docs/broken.md", "<!-- snippet: x/main.go#gone -->\n```go\n```\n<!-- snippet: x/main.go -->\nno fence\n")

	var sb strings.Builder
	n, err := run(&sb, dir, false, []string{"docs"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("problems = %d, want 3 (missing region, no fence, stale):\n%s", n, sb.String())
	}
	if !strings.Contains(sb.String(), "docs/stale.md:3: stale snippet") {
		t.Errorf("output lacks the stale block:\n%s", sb.String())
	}

	sb.Reset()
	if _, err := run(&sb, dir, true, []string{"docs/stale.md"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(guide)
	if want := strings.Replace(stale, "if a > 1 {\n}", "if a > 0 {\n\tb := 2\n}", 1); string(b) != want {
		t.Errorf("synced guide =\n%s\nwant\n%s", b, want)
	}
	if n, _ := run(&sb, dir, false, []string{"docs/stale.md", "docs/fresh.md"}); n != 0 {
		t.Errorf("after sync: %d problems:\n%s", n, sb.String())
	}
}

// TestRegionTrailingBlank: gofmt leaves a blank line between a top-level
// func and the "snippet:end" after it; the snippet must not end with it.
func TestRegionTrailingBlank(t *testing.T) {
	dir := t.TempDir()
	p := write(t, dir, "x/main.go", "package main\n\n// snippet:fn\nfunc f() {}\n\n// snippet:end\n")
	got, err := region(p, "fn")
	if err != nil {
		t.Fatal(err)
	}
	if want := "func f() {}"; strings.Join(got, "\n") != want {
		t.Errorf("fn = %q, want %q", strings.Join(got, "\n"), want)
	}
}
