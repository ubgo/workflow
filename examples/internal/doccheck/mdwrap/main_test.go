package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestWrapped(t *testing.T) {
	cases := map[string]struct {
		in   string
		want []int
	}{
		"one-line paragraphs": {"# H\n\nOne line.\n\nAnother line.\n", nil},
		"wrapped paragraph":   {"Para starts\nand continues\nand again.\n", []int{2, 3}},
		"fenced code":         {"```go\na := 1\nb := 2\n```\n", nil},
		"frontmatter":         {"---\nname: x\ndescription: y\n---\n\nText.\n", nil},
		"lists and tables":    {"- a\n- b\n1. one\n2. two\n| a |\n| b |\n", nil},
		"html and quotes":     {"<p>\n<b>x</b>\n> q\n> r\n", nil},
		"indented code":       {"    code\n    more\n", nil},
		"list continuation":   {"- item\n  continues here\n", nil},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := wrapped(strings.NewReader(c.in))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("wrapped = %v, want %v", got, c.want)
			}
		})
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
	write("ok.md", "Fine.\n")
	write("bad.md", "Wrapped\nline.\n")
	write("notes.txt", "Wrapped\nbut not markdown.\n")
	write("vendor/skip.md", "Wrapped\nin vendor.\n")
	var sb strings.Builder
	n, err := run(&sb, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || sb.String() != filepath.Join(dir, "bad.md")+":2\n" {
		t.Errorf("n=%d output=%q", n, sb.String())
	}
	if _, err := run(&sb, nil); err == nil {
		t.Error("no paths: want an error")
	}
	if _, err := run(&sb, []string{filepath.Join(dir, "missing")}); err == nil {
		t.Error("missing path: want an error")
	}
}
