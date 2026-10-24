// Command snippets keeps code blocks in the guides identical to the example
// programs they are copied from.
//
// Why it exists: a snippet pasted into a guide stops compiling the day the
// API changes, and nothing notices, because a guide is not built. The
// examples are built and tested, so a guide that copies a region of an
// example and is checked against it can only show code that runs today.
//
// A guide marks a block with a comment on the line before its fence:
//
//	<!-- snippet: examples/quickstart/main.go#enqueue -->
//	```go
//	…
//	```
//
// The path is relative to the repository root. Without #region the whole
// file is meant. In the source a region is delimited by marker comments,
// written in the file's own comment style (//, # or --):
//
//	// snippet:enqueue
//	…
//	// snippet:end
//
// Regions may nest; "snippet:end" closes the innermost open one. Marker
// lines of any region are left out of every snippet, and the region's
// common indentation is removed, so a region inside a function reads as
// top-level code.
//
// Usage: go run ./internal/doccheck/snippets [-root DIR] [-sync] FILE_OR_DIR...
// Without -sync it exits 1 and names every stale, missing or malformed
// snippet; with -sync it rewrites stale blocks in place and still fails on
// a missing file or region.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// skipDirs are never walked: dependencies, VCS data and generated output.
var skipDirs = map[string]bool{".git": true, "node_modules": true, "vendor": true, ".cover": true, ".lore": true}

// marker is the guide-side comment naming a snippet's source.
var marker = regexp.MustCompile(`^<!--\s*snippet:\s*([^#\s]+)(?:#(\S+))?\s*-->\s*$`)

// srcMarker is a source-side region marker in any of the comment styles.
var srcMarker = regexp.MustCompile(`^\s*(?://|#|--)\s*snippet:(\S+)\s*$`)

// regionEnd is the name that closes a region.
const regionEnd = "end"

func main() {
	root := flag.String("root", ".", "repository root that snippet paths are relative to")
	sync := flag.Bool("sync", false, "rewrite stale snippets instead of failing on them")
	flag.Parse()
	n, err := run(os.Stdout, *root, *sync, flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "snippets:", err)
		os.Exit(1)
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "snippets: %d problem(s); run task docs:snippets:sync for stale blocks\n", n)
		os.Exit(1)
	}
}

// run checks (or with sync, rewrites) every Markdown file under paths and
// returns the number of problems left.
func run(w io.Writer, root string, sync bool, paths []string) (int, error) {
	if len(paths) == 0 {
		return 0, errors.New("no paths given")
	}
	var files []string
	for _, p := range paths {
		err := filepath.WalkDir(filepath.Join(root, p), func(fp string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(fp) == ".md" {
				files = append(files, fp)
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	sort.Strings(files)
	problems := 0
	for _, f := range files {
		n, err := checkFile(w, root, f, sync)
		if err != nil {
			return problems, err
		}
		problems += n
	}
	return problems, nil
}

// checkFile handles one guide and returns its problem count.
func checkFile(w io.Writer, root, file string, sync bool) (int, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return 0, err
	}
	rel, _ := filepath.Rel(root, file)
	lines := strings.Split(string(b), "\n")
	problems, changed := 0, false
	report := func(line int, format string, a ...any) {
		problems++
		fmt.Fprintf(w, "%s:%d: %s\n", rel, line, fmt.Sprintf(format, a...))
	}
	var out []string
	for i := 0; i < len(lines); i++ {
		out = append(out, lines[i])
		m := marker.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		at := i + 1
		j := i + 1
		for j < len(lines) && strings.TrimSpace(lines[j]) == "" {
			j++
		}
		if j >= len(lines) || !strings.HasPrefix(strings.TrimSpace(lines[j]), "```") {
			report(at, "snippet marker is not followed by a fenced code block")
			continue
		}
		open := strings.TrimSpace(lines[j])
		fence := open[:len(open)-len(strings.TrimLeft(open, "`"))]
		end := j + 1
		for end < len(lines) && strings.TrimSpace(lines[end]) != fence {
			end++
		}
		if end >= len(lines) {
			report(at, "snippet code block is never closed")
			continue
		}
		want, err := region(filepath.Join(root, m[1]), m[2])
		if err != nil {
			report(at, "%s", err)
			continue
		}
		got := lines[j+1 : end]
		out = append(out, lines[i+1:j+1]...)
		if equal(got, want) {
			out = append(out, got...)
		} else if sync {
			out = append(out, want...)
			changed = true
			fmt.Fprintf(w, "%s:%d: synced from %s\n", rel, at, label(m[1], m[2]))
		} else {
			out = append(out, got...)
			report(at, "stale snippet: differs from %s (run task docs:snippets:sync)", label(m[1], m[2]))
		}
		out = append(out, lines[end])
		i = end
	}
	if changed {
		if err := os.WriteFile(file, []byte(strings.Join(out, "\n")), 0o644); err != nil {
			return problems, err
		}
	}
	return problems, nil
}

func label(file, name string) string {
	if name == "" {
		return file
	}
	return file + "#" + name
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// region returns the lines of a source file's named region (the whole file
// when name is empty), without marker lines and with the common indentation
// removed.
func region(file, name string) ([]string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	var body []string
	var stack []string
	found, closed := name == "", name == ""
	for _, l := range lines {
		if m := srcMarker.FindStringSubmatch(l); m != nil {
			if m[1] == regionEnd {
				if len(stack) == 0 {
					return nil, fmt.Errorf("%s: snippet:end without an open region", file)
				}
				if stack[len(stack)-1] == name && name != "" {
					closed = true
				}
				stack = stack[:len(stack)-1]
				continue
			}
			if m[1] == name {
				if found {
					return nil, fmt.Errorf("%s: region %q defined twice", file, name)
				}
				found = true
			}
			stack = append(stack, m[1])
			continue
		}
		if name == "" || (found && !closed && contains(stack, name)) {
			body = append(body, l)
		}
	}
	if !found {
		return nil, fmt.Errorf("%s: no region %q", file, name)
	}
	if !closed {
		return nil, fmt.Errorf("%s: region %q is never closed", file, name)
	}
	// gofmt puts a blank line between a top-level func and a following
	// "snippet:end" comment; it is layout, not part of the snippet.
	for len(body) > 0 && strings.TrimSpace(body[len(body)-1]) == "" {
		body = body[:len(body)-1]
	}
	return dedent(body), nil
}

func contains(s []string, v string) bool {
	return slices.Contains(s, v)
}

// dedent removes the indentation every non-blank line shares.
func dedent(lines []string) []string {
	prefix := ""
	first := true
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		ind := l[:len(l)-len(strings.TrimLeft(l, " \t"))]
		if first {
			prefix, first = ind, false
			continue
		}
		for !strings.HasPrefix(ind, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
	}
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = strings.TrimRight(strings.TrimPrefix(l, prefix), " \t")
	}
	return out
}
