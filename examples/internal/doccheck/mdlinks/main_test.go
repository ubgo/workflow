package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSlug(t *testing.T) {
	cases := map[string]string{
		"Install":                           "install",
		"Step 1 — Pick plugins":             "step-1--pick-plugins",
		"Retries: `RetryPolicy` and jitter": "retries-retrypolicy-and-jitter",
		"[Link](x.md) text":                 "link-text",
		"snake_case and kebab-case":         "snake_case-and-kebab-case",
		"Trailing hashes ##":                "trailing-hashes",
	}
	for in, want := range cases {
		if got := Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("docs/a.md", "# A\n\n## Same\n\n## Same\n\n<a id=\"custom\"></a>\n")
	write("examples/x/main.go", "package main\n")
	write("docs/b.md", strings.Join([]string{
		"# B",
		"[ok](a.md) [ok](a.md#same) [ok](a.md#same-1) [ok](a.md#custom) [ok](#b)",
		"[ok](../examples/x/) [ok](https://example.com/nope) `[code](missing.md)`",
		"```",
		"[fenced](missing.md)",
		"```",
		"[bad](missing.md)",
		"[bad](a.md#nope)",
		"[bad](#nope)",
		"[ref]: gone.md",
	}, "\n"))
	c := &checker{root: dir, anchors: map[string]map[string]bool{}}
	var sb strings.Builder
	bad, err := c.run(&sb, []string{"docs"})
	if err != nil {
		t.Fatal(err)
	}
	if bad != 4 {
		t.Fatalf("bad=%d, want 4; output:\n%s", bad, sb.String())
	}
	for _, want := range []string{"docs/b.md:7: missing.md", "docs/b.md:8: a.md#nope", "docs/b.md:9: #nope", "docs/b.md:10: gone.md"} {
		if !strings.Contains(sb.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, sb.String())
		}
	}
	if _, err := c.run(&sb, nil); err == nil {
		t.Error("no paths: want an error")
	}
}
