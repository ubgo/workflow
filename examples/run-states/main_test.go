package main

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/examples/internal/capture"
)

// want is the program's whole output, the one its README and guide show.
const want = `memory declares CapRunStates: true
sync-invoices: retrying, attempt 2 at 1m0s, last error "upstream 503"
after the backoff: succeeded
ship-order: discarded, terminal true, ErrDiscarded true, ErrCancelled true
admin discarded: ship-order, "workflow: run cancelled: workflow: run discarded: step \"ship\": order o-9 no longer exists"
call-partner with MaxErrors 2: failed
batch with DiscardedAs "": batch-catch ran; succeeded 1, failed 1, discarded 1
batch with DiscardedAs "succeeded": batch-then ran; succeeded 2, failed 0, discarded 1
  retrying: waiting to retry
  discarded: dropped on purpose
  failed: needs a look
  succeeded: done
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
