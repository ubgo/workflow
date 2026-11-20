package cli

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// failWriter fails every write, like stdout on a closed pipe.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

// TestRunReportsAFailedWrite pins that a command whose output could not be
// written exits with ExitError and says why, instead of exiting 0 with a
// partial or empty output.
func TestRunReportsAFailedWrite(t *testing.T) {
	var stderr strings.Builder
	code := Run(context.Background(), []string{"help"}, Options{Stdout: failWriter{}, Stderr: &stderr})
	if code != ExitError {
		t.Fatalf("Run = %d, want ExitError (%d)", code, ExitError)
	}
	if !strings.Contains(stderr.String(), "write output: broken pipe") {
		t.Errorf("stderr = %q, want the write error", stderr.String())
	}
}

// TestStickyWriterKeepsTheFirstError pins that writes after a failure fail
// fast with the first error.
func TestStickyWriterKeepsTheFirstError(t *testing.T) {
	s := &stickyWriter{w: failWriter{}}
	if _, err := s.Write([]byte("a")); err == nil {
		t.Fatal("first Write succeeded, want the writer error")
	}
	first := s.err
	if _, err := s.Write([]byte("b")); err != first {
		t.Fatalf("second Write = %v, want the first error %v", err, first)
	}
}
