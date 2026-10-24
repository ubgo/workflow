// Command words reports files that contain a forbidden word, matched
// without regard to case.
//
// Why it exists: some names must never appear in the public docs, the
// examples or the code comments (a product the docs are not to compare
// against, tooling that is not part of the project). A grep run once by
// hand catches today's mentions; only a check in `task check` keeps them
// from coming back.
//
// The word list is given with -words and is never written in this source,
// so the checker does not trip over itself; `task docs:words` holds the
// list and passes the files to check (Markdown under docs/ and examples/,
// the top-level Markdown files and every Go file, from git ls-files, so
// ignored private overlays are skipped).
//
// Usage: go run ./examples/internal/doccheck/words -words a,b,c FILE...
// It exits 1 and prints file:line: word for every hit.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// maxLine is the longest line the scanner accepts; a generated file with a
// longer line is an error rather than a silent skip.
const maxLine = 1 << 20

func main() {
	fs := flag.NewFlagSet("words", flag.ContinueOnError)
	list := fs.String("words", "", "comma-separated words to refuse (case-insensitive)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	n, err := run(os.Stdout, parseWords(*list), fs.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "words:", err)
		os.Exit(1)
	}
	if n > 0 {
		fmt.Fprintf(os.Stderr, "words: %d forbidden word(s); describe the behaviour without the name\n", n)
		os.Exit(1)
	}
}

// parseWords splits the -words value into lower-case words, dropping empty
// entries.
func parseWords(list string) []string {
	var out []string
	for _, w := range strings.Split(list, ",") {
		if w = strings.ToLower(strings.TrimSpace(w)); w != "" {
			out = append(out, w)
		}
	}
	return out
}

// run checks every file in files for words and returns how many hits it
// printed. No words or no files is an error: a check that checks nothing
// must not pass quietly.
func run(w io.Writer, words, files []string) (int, error) {
	if len(words) == 0 {
		return 0, errors.New("no words given (-words)")
	}
	if len(files) == 0 {
		return 0, errors.New("no files given")
	}
	total := 0
	for _, p := range files {
		n, err := checkFile(w, p, words)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// checkFile prints file:line: word for each line of p that contains a word.
func checkFile(w io.Writer, p string, words []string) (int, error) {
	f, err := os.Open(p)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	n, line := 0, 0
	for sc.Scan() {
		line++
		low := strings.ToLower(sc.Text())
		for _, word := range words {
			if strings.Contains(low, word) {
				fmt.Fprintf(w, "%s:%d: %s\n", p, line, word)
				n++
			}
		}
	}
	if err := sc.Err(); err != nil {
		return n, fmt.Errorf("%s: %w", p, err)
	}
	return n, nil
}
