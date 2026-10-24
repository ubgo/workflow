// Command mdwrap reports Markdown files with hard-wrapped prose: a paragraph
// line that continues on the next physical line.
//
// Why it exists: the repository's rule is that every paragraph, bullet and
// table row is one physical line and the renderer wraps it. A wrapped line
// reads fine on GitHub, so nobody notices until someone edits it and gets a
// diff of reflowed text; a check is the only thing that keeps the rule.
//
// Usage: go run ./internal/doccheck/mdwrap FILE_OR_DIR...
// `task docs:wrap` passes every Markdown file git tracks or would track
// (git ls-files --cached --others --exclude-standard), so ignored private
// overlays are skipped. It exits 1 and prints file:line for every wrapped
// line.
package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// fence opens and closes a code block; lines inside are left alone because
// their breaks are meaningful.
const fence = "```"

// frontmatter delimits YAML frontmatter, one key per line by design.
const frontmatter = "---"

// skipDirs are never walked: dependencies, VCS data and generated output.
var skipDirs = map[string]bool{".git": true, "node_modules": true, "vendor": true, ".cover": true, ".lore": true}

func main() {
	n, err := run(os.Stdout, os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "mdwrap:", err)
		os.Exit(1)
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "mdwrap: %d hard-wrapped line(s); join each paragraph onto one line\n", n)
		os.Exit(1)
	}
}

// run checks every .md file under the given paths and returns how many
// wrapped lines it reported.
func run(w io.Writer, paths []string) (int, error) {
	if len(paths) == 0 {
		return 0, fmt.Errorf("no paths given")
	}
	total := 0
	for _, root := range paths {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}
				return nil
			}
			if filepath.Ext(p) != ".md" {
				return nil
			}
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()
			lines, err := wrapped(f)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			for _, l := range lines {
				fmt.Fprintf(w, "%s:%d\n", p, l)
			}
			total += len(lines)
			return nil
		})
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// wrapped returns the numbers of lines that are the continuation of a prose
// line above them. A continuation is a non-empty line directly below a prose
// line, where neither is a block construct (heading, list item, table row,
// quote, HTML, fence) — the shape a reflowed paragraph leaves behind.
func wrapped(r io.Reader) ([]int, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var out []int
	inFence, inFront := false, false
	prevProse := false
	n := 0
	for sc.Scan() {
		n++
		line := sc.Text()
		t := strings.TrimSpace(line)
		switch {
		case n == 1 && t == frontmatter:
			inFront = true
			prevProse = false
			continue
		case inFront:
			if t == frontmatter {
				inFront = false
			}
			continue
		case strings.HasPrefix(t, fence):
			inFence = !inFence
			prevProse = false
			continue
		case inFence:
			continue
		}
		prose := isProse(line)
		if prose && prevProse {
			out = append(out, n)
		}
		prevProse = prose
	}
	return out, sc.Err()
}

// isProse reports whether a line is plain paragraph text rather than a block
// construct or a blank line. Indented lines count as prose only when they are
// not code (four spaces) — a continuation of a list item is prose too.
func isProse(line string) bool {
	t := strings.TrimSpace(line)
	if t == "" || strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t") {
		return false
	}
	for _, p := range []string{"#", "|", ">", "<", "- ", "* ", "+ ", "![", "[!", ":::"} {
		if strings.HasPrefix(t, p) {
			return false
		}
	}
	if i := strings.Index(t, ". "); i > 0 && i < 4 && strings.Trim(t[:i], "0123456789") == "" {
		return false // numbered list item
	}
	return true
}
