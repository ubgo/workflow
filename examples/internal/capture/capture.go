// Package capture runs an example program and returns what it printed, so
// the examples print with plain fmt.Println (as an app would) and their
// tests still check every line.
package capture

import (
	"errors"
	"io"
	"os"
	"strings"
)

// Stdout runs fn with os.Stdout pointed at a pipe and returns everything fn
// printed, along with fn's error and any error from the pipe itself. It
// swaps the process-wide os.Stdout, so callers must not run in parallel
// with anything else that prints.
func Stdout(fn func() error) (string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	type result struct {
		text string
		err  error
	}
	read := make(chan result, 1)
	go func() {
		var b strings.Builder
		_, err := io.Copy(&b, r)
		read <- result{b.String(), err}
	}()

	saved := os.Stdout
	os.Stdout = w
	runErr := fn()
	os.Stdout = saved

	closeErr := w.Close() // ends the copy above
	got := <-read
	return got.text, errors.Join(runErr, closeErr, got.err, r.Close())
}
