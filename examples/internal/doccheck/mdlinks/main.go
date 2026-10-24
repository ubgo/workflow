// Command mdlinks fails when a relative link or #anchor in the repository's
// Markdown does not resolve.
//
// Why it exists: the guides link to each other by heading anchor and to the
// example programs by path. GitHub renders a broken link as a working one
// until somebody clicks it, and a renamed heading silently breaks every
// link to it, so only a check keeps the cross-links honest.
//
// What it checks, for every Markdown file given (directories are walked):
//
//   - inline links and images [text](target) and reference definitions
//     [label]: target, outside fenced code and inline code;
//   - a relative target must exist (a file or a directory);
//   - a #fragment on a Markdown target (or a bare #fragment on the same
//     page) must name a heading anchor of that page, computed the way
//     GitHub does, or an explicit <a id="…"> / <a name="…">;
//   - absolute URLs (http, https, mailto) are not fetched.
//
// Usage: go run ./internal/doccheck/mdlinks [-root DIR] FILE_OR_DIR...
// Paths are relative to -root (default "."). It exits 1 on any broken link.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// skipDirs are never walked: dependencies, VCS data and generated output.
var skipDirs = map[string]bool{".git": true, "node_modules": true, "vendor": true, ".cover": true, ".lore": true}

// inlineLink matches [text](target) and ![alt](target); the target stops at
// whitespace (an optional "title" follows) or the closing parenthesis.
var inlineLink = regexp.MustCompile(`!?\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+"[^"]*")?\s*\)`)

// refDef matches a reference definition: [label]: target.
var refDef = regexp.MustCompile(`^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$`)

// htmlAnchor matches an explicit anchor: <a id="x"> or <a name="x">.
var htmlAnchor = regexp.MustCompile(`<a\s+(?:id|name)="([^"]+)"`)

// inlineCode matches a code span, whose content is not a link.
var inlineCode = regexp.MustCompile("`[^`]*`")

func main() {
	root := flag.String("root", ".", "repository root that paths are relative to")
	flag.Parse()
	c := &checker{root: *root, anchors: map[string]map[string]bool{}}
	bad, err := c.run(os.Stdout, flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "mdlinks:", err)
		os.Exit(1)
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr, "mdlinks: %d broken link(s)\n", bad)
		os.Exit(1)
	}
}

// checker holds the root and a cache of each page's anchors.
type checker struct {
	root    string
	anchors map[string]map[string]bool
}

// run checks every Markdown file under paths and returns the count of
// broken links.
func (c *checker) run(w io.Writer, paths []string) (bad int, err error) {
	if len(paths) == 0 {
		return 0, errors.New("no paths given")
	}
	var files []string
	for _, p := range paths {
		err := filepath.WalkDir(filepath.Join(c.root, p), func(fp string, d fs.DirEntry, err error) error {
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
				rel, err := filepath.Rel(c.root, fp)
				if err != nil {
					return err
				}
				files = append(files, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			return bad, err
		}
	}
	sort.Strings(files)
	for _, f := range files {
		links, err := c.links(f)
		if err != nil {
			return bad, err
		}
		for _, l := range links {
			if msg := c.resolve(f, l.target); msg != "" {
				bad++
				fmt.Fprintf(w, "%s:%d: %s: %s\n", f, l.line, l.target, msg)
			}
		}
	}
	return bad, nil
}

type link struct {
	line   int
	target string
}

// links returns every link target of a page with its line number, skipping
// fenced code and code spans.
func (c *checker) links(file string) ([]link, error) {
	b, err := os.ReadFile(filepath.Join(c.root, file))
	if err != nil {
		return nil, err
	}
	var out []link
	inFence := false
	for i, line := range strings.Split(string(b), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "```") || strings.HasPrefix(t, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if m := refDef.FindStringSubmatch(line); m != nil {
			out = append(out, link{i + 1, m[1]})
			continue
		}
		clean := inlineCode.ReplaceAllString(line, "")
		for _, m := range inlineLink.FindAllStringSubmatch(clean, -1) {
			out = append(out, link{i + 1, m[1]})
		}
	}
	return out, nil
}

// resolve returns "" when target resolves from the page at from, or why it
// does not.
func (c *checker) resolve(from, target string) string {
	if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
		return ""
	}
	file, frag, _ := strings.Cut(target, "#")
	dest := from
	if file != "" {
		if strings.HasPrefix(file, "/") {
			return "absolute path; use a relative link"
		}
		dest = path.Clean(path.Join(path.Dir(from), file))
		if strings.HasPrefix(dest, "../") {
			return "points outside the repository"
		}
		if _, err := os.Stat(filepath.Join(c.root, dest)); err != nil {
			return "no such file or directory"
		}
	}
	if frag == "" || !strings.HasSuffix(dest, ".md") {
		return ""
	}
	a, err := c.pageAnchors(dest)
	if err != nil {
		return err.Error()
	}
	if !a[strings.ToLower(frag)] {
		return "no heading with anchor #" + frag + " in " + dest
	}
	return ""
}

// pageAnchors returns the anchors of a page: one per heading, slugged the
// way GitHub does (duplicates get -1, -2 …), plus explicit HTML anchors.
func (c *checker) pageAnchors(file string) (map[string]bool, error) {
	if a, ok := c.anchors[file]; ok {
		return a, nil
	}
	b, err := os.ReadFile(filepath.Join(c.root, file))
	if err != nil {
		return nil, err
	}
	a := map[string]bool{}
	seen := map[string]int{}
	inFence := false
	for line := range strings.SplitSeq(string(b), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "```") || strings.HasPrefix(t, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		for _, m := range htmlAnchor.FindAllStringSubmatch(line, -1) {
			a[strings.ToLower(m[1])] = true
		}
		if !strings.HasPrefix(line, "#") {
			continue
		}
		h := strings.TrimLeft(line, "#")
		if len(line)-len(h) > 6 || (h != "" && h[0] != ' ') {
			continue
		}
		s := Slug(h)
		if n := seen[s]; n > 0 {
			a[fmt.Sprintf("%s-%d", s, n)] = true
		} else {
			a[s] = true
		}
		seen[s]++
	}
	c.anchors[file] = a
	return a, nil
}

// linkText matches a link inside a heading; only its text counts.
var linkText = regexp.MustCompile(`!?\[([^\]]*)\]\([^)]*\)`)

// Slug turns a heading into its GitHub anchor: link text kept, markup
// dropped, lower case, every character that is not a letter, digit, space,
// hyphen or underscore removed, and each space turned into a hyphen (so
// "Step 1 — Pick" becomes "step-1--pick").
func Slug(heading string) string {
	h := strings.TrimSpace(heading)
	h = strings.TrimRight(h, "#")
	h = strings.TrimSpace(h)
	h = linkText.ReplaceAllString(h, "$1")
	var sb strings.Builder
	for _, r := range strings.ToLower(h) {
		switch {
		case r == ' ':
			sb.WriteRune('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r):
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
