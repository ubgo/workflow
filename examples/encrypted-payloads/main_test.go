package main

import (
	"context"
	"strings"
	"testing"
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
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
