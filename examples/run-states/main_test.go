package main

import (
	"context"
	"strings"
	"testing"
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
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
