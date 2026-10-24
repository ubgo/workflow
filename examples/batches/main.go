// Command batches runs groups of jobs as one unit on the in-memory driver,
// with Then, Catch and Finally callbacks: a batch that succeeds, one whose
// failing job cancels the rest, one cancelled by the app, labels copied
// onto the callbacks, batch IDs of the app's own, and a Finally whose
// enqueue failed and is retried.
//
// hatchet.go dispatches a batch on the Hatchet driver through the batch
// coordinator; it runs only when HATCHET_CLIENT_TOKEN is set.
//
// It is the program behind docs/batches/batches.md and
// docs/batches/completion.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// storePoll is how often the program re-reads the batch store while it
// waits for a callback's failed enqueue to be recorded.
const storePoll = 5 * time.Millisecond

var errBroken = errors.New("image is broken")

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "batches:", err)
		os.Exit(1)
	}
	if os.Getenv(envToken) != "" {
		if err := runHatchet(ctx, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "batches on Hatchet:", err)
			os.Exit(1)
		}
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The memory driver; WrapClient fails the first Finally enqueue, to
	// show a callback that is retried.
	var finallyFailed atomic.Bool // enqueues run on the driver's goroutines too
	c := memory.New()
	client := middleware.WrapClient(c, func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if job.Workflow == "finally" && job.Labels["tenant"] == "globex" && finallyFailed.CompareAndSwap(false, true) {
				return workflow.RunRef{}, errors.New("engine unavailable")
			}
			return next(ctx, job)
		}
	})

	callbacks := make(chan string, 16)
	if err := registerWorkflows(c, callbacks); err != nil {
		return err
	}

	// snippet:manager
	seq := 0
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := batch.NewMemoryStore()
	m := batch.New(client, store,
		batch.WithClock(clk),
		batch.WithIDFunc(func() (batch.ID, error) { // the default is 128 random bits as hex
			seq++
			return batch.ID("thumbs-" + strconv.Itoa(seq)), nil
		}),
		batch.WithManualCallbackRetry(), // this program calls RetryCallbacks itself
	)
	if err := m.Attach(ctx); err != nil { // before Start: it registers the run-finished hook
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	await := func(n int) {
		var got []string
		for range n {
			got = append(got, <-callbacks)
		}
		slices.Sort(got) // callbacks are separate runs and may finish in any order
		for _, line := range got {
			fmt.Fprintln(out, line)
		}
	}

	fmt.Fprintln(out, "-- every job succeeds")
	// snippet:dispatch
	id, err := m.Dispatch(ctx, batch.Batch{
		Name: "thumbnails",
		Jobs: []workflow.Job{
			{Workflow: "resize", Payload: []byte(`"a.png"`)},
			{Workflow: "resize", Payload: []byte(`"b.png"`)},
			{Workflow: "resize", Payload: []byte(`"c.png"`)},
		},
		Then:    "then",
		Catch:   "catch",
		Finally: "finally",
		Labels:  map[string]string{"tenant": "acme"}, // copied onto Then, Catch and Finally
	})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "dispatched", id)
	await(2)

	fmt.Fprintln(out, "-- a job fails")
	_, err = m.Dispatch(ctx, batch.Batch{
		Name:    "thumbnails",
		Jobs:    []workflow.Job{{Workflow: "resize", Payload: []byte(`"slow"`)}, {Workflow: "resize", Payload: []byte(`"broken"`)}},
		Then:    "then",
		Catch:   "catch",
		Finally: "finally",
		Labels:  map[string]string{"tenant": "acme"},
	})
	if err != nil {
		return err
	}
	await(2)

	fmt.Fprintln(out, "-- the app cancels")
	id, err = m.Dispatch(ctx, batch.Batch{
		Name:    "thumbnails",
		Jobs:    []workflow.Job{{Workflow: "resize", Payload: []byte(`"slow"`)}, {Workflow: "resize", Payload: []byte(`"slow"`)}},
		Then:    "then",
		Finally: "finally",
		Labels:  map[string]string{"tenant": "acme"},
	})
	if err != nil {
		return err
	}
	// snippet:cancel
	p, err := m.Progress(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "before cancel: %d of %d processed (%d%%)\n", p.Processed(), p.Total, p.Percent())
	if err := m.Cancel(ctx, id); err != nil {
		return err
	}
	// snippet:end
	await(1)

	fmt.Fprintln(out, "-- a Finally that failed to enqueue")
	id, err = m.Dispatch(ctx, batch.Batch{
		Name:    "thumbnails",
		Jobs:    []workflow.Job{{Workflow: "resize", Payload: []byte(`"a.png"`)}},
		Finally: "finally",
		Labels:  map[string]string{"tenant": "globex"},
	})
	if err != nil {
		return err
	}
	if err := waitFailedCallback(ctx, store, id); err != nil {
		return err
	}
	// snippet:retry
	st, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	for _, pc := range st.Pending {
		fmt.Fprintf(out, "pending %s: attempts=%d last error: %s\n", pc.Kind, pc.Attempts, pc.LastError)
	}
	clk.Advance(batch.DefaultCallbackRetry) // the failed attempt's claim ends
	if err := m.RetryCallbacks(ctx); err != nil {
		return err
	}
	// snippet:end
	await(1)

	fmt.Fprintln(out, "-- a reserved label")
	// snippet:reserved
	_, err = m.Dispatch(ctx, batch.Batch{
		Jobs: []workflow.Job{{Workflow: "resize", Labels: map[string]string{batch.LabelBatch: "mine"}}},
	})
	fmt.Fprintln(out, "refused:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// waitFailedCallback polls the store until the batch holds a callback
// whose enqueue failed.
func waitFailedCallback(ctx context.Context, store batch.Store, id batch.ID) error {
	for {
		st, err := store.Get(ctx, id)
		if err != nil {
			return err
		}
		if len(st.Pending) > 0 && st.Pending[0].LastError != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(storePoll):
		}
	}
}

// registerWorkflows registers the batch's job workflow and the three
// callbacks. Each callback reports what it received on callbacks.
func registerWorkflows(c *memory.Client, callbacks chan<- string) error {
	if err := c.Register(workflow.Workflow{
		Name: "resize",
		Steps: []workflow.Step{{Name: "run", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			switch string(in) {
			case `"broken"`:
				return nil, workflow.Permanent(errBroken)
			case `"slow"`:
				<-ctx.Done() // runs until the batch cancels it
				return nil, ctx.Err()
			}
			return nil, nil
		}}},
	}); err != nil {
		return err
	}
	for _, kind := range []string{"then", "catch", "finally"} {
		// snippet:callback
		cb := workflow.Workflow{
			Name: kind,
			Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(ctx context.Context, p batch.Progress) (string, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				callbacks <- fmt.Sprintf("%s %s tenant=%s succeeded=%d failed=%d cancelledJobs=%d cancelled=%v",
					kind, p.ID, info.Labels["tenant"], p.Succeeded, p.Failed, p.CancelledJobs, p.Cancelled)
				return "", nil
			})}},
		}
		// snippet:end
		if err := c.Register(cb); err != nil {
			return err
		}
	}
	return nil
}
