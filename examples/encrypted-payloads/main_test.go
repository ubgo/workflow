package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `driver stores a sealed payload: true, card visible: false
capture got auth-o-1
sealed with the new key: succeeded, decode error: false
capture got auth-o-2
sealed with the old key: succeeded, decode error: false
plaintext: failed, decode error: true
old key dropped: failed, decode error: true, permanent: true
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	if got := out; got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
