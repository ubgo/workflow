package capture_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// TestStdout checks the text printed inside fn comes back whole (more than
// a pipe buffer holds), os.Stdout is restored afterwards, and fn's error is
// passed through along with what was printed before it.
func TestStdout(t *testing.T) {
	saved := os.Stdout
	big := strings.Repeat("x", 1<<20)
	out, err := capture.Stdout(func() error {
		fmt.Println("one")
		fmt.Print(big)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "one\n" + big; out != want {
		t.Errorf("captured %d bytes, want %d", len(out), len(want))
	}
	if os.Stdout != saved {
		t.Error("os.Stdout was not restored")
	}

	boom := errors.New("boom")
	out, err = capture.Stdout(func() error {
		fmt.Println("before the failure")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want %v", err, boom)
	}
	if out != "before the failure\n" {
		t.Errorf("output = %q", out)
	}
}
