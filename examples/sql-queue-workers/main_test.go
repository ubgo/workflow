package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `email while mail is paused: queued
email after resume: succeeded, on queue mail
failed: build-report at step render on queue default
after retry: gather ran 1 time(s), render 2
after delete: not found
mail worker drained: true
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
