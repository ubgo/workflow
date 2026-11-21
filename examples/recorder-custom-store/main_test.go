package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/recorder"
	"github.com/ubgo/workflow/recorder/recordertest"
	"github.com/ubgo/workflow/workflowtest"
)

// want is the program's whole output, the one its README and guide show.
const want = `input as stored: {"card":{"number":"[REDACTED]"},"order":"order-1"}
  step charge output=none (51 bytes before redaction)
  step notify output={"sent":true} (13 bytes before redaction)
    event "receipt sent" [{email a***@example.com}]
-- the history store goes into maintenance
history writes that failed: add-event end-step finish-run start-step upsert-run
run status from the driver: succeeded; in the history: true
-- maintenance over: one Reconciler pass
reconcile: checked=0 finished=0 backfilled=2 errors=0
second run in the history: succeeded source=api/order-2 steps=0
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// snippet:contract-store
// TestStoreContract holds docStore to the recorder.Store contract the
// bundled stores pass: fill-only upserts, attempts, events, outcome,
// finish and re-open, the join of a delayed run's two IDs, every filter,
// paging, Pending, copies on read, concurrent writes and the refusals.
// docStore is no recorder.Pruner, so the retention cases skip.
func TestStoreContract(t *testing.T) {
	recordertest.RunStore(t, func(t *testing.T, _ recordertest.StoreConfig) recorder.Store {
		return newDocStore()
	})
}

// snippet:end

// snippet:contract-driver
// TestRecorderOnMemoryDriver runs the recorder's driver cases (source,
// steps, events and outcome; retries and failure; redaction; a delayed run
// cancelled before it starts; the Reconciler's backfill and check) on the
// memory driver, recording into docStore instead of the default store.
func TestRecorderOnMemoryDriver(t *testing.T) {
	recordertest.RunDriver(t, workflowtest.Harness{New: func(t *testing.T) workflowtest.Instance {
		clk := clock.NewFake(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
		return workflowtest.Instance{Client: memory.New(memory.WithClock(clk)), Clock: clk}
	}}, recordertest.WithStore(func(t *testing.T, _ recordertest.StoreConfig) recorder.Store {
		return newDocStore()
	}))
}

// snippet:end
