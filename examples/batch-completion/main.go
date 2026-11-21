// Command batch-completion shows where a batch hears that its jobs ended,
// on the in-memory driver: the status poller a Manager picks for a client
// without the run-finished hook (its interval set by WithStatusPoll and
// driven by a fake clock), an app's own feed of run ends written as a
// batch.Source that calls Manager.RunFinished, and RunFinished ignoring a
// run that is not a batch job.
//
// It is the program behind docs/batches/completion.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

const (
	// pollEvery is the status poller's interval. The poller runs on a fake
	// clock here, so the value costs no wall time; it is a production-like
	// interval, double batch.DefaultPollInterval, for an app that accepts
	// slower callbacks in exchange for half the Status calls.
	pollEvery = 10 * time.Second
	// statusPoll is how often the program asks the driver whether a job
	// has finished, while it waits to show the batch before the poll.
	statusPoll = 5 * time.Millisecond
	// busSize buffers the app's feed of run ends, so the run-finished hook
	// that fills it never blocks a worker.
	busSize = 16
	// fedCallbacks is how many callback lines the fed batch prints: its
	// Then and its Finally each run once.
	fedCallbacks = 2
)

// The workflow names: the batch job and the two batch callbacks.
const (
	wfResize  = "resize"
	wfThen    = "then"
	wfFinally = "finally"
)

// start is the fake clock's starting time; any fixed instant works.
var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "batch-completion:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and reads what it writes to w.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := memory.New()
	callbacks := make(chan string, busSize)
	if err := register(c, callbacks); err != nil {
		return err
	}

	// The app's feed of run ends. A real app reads a message bus or an
	// engine webhook; here the driver's run-finished hook fills a channel,
	// so the Source below is all that connects it to the batch.
	bus := make(chan workflow.RunEvent, busSize)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { bus <- ev }); err != nil {
		return err
	}

	clk := clock.NewFake(start)
	// snippet:status-poll
	// statusOnly hides the driver's run-finished hook: the Manager sees a
	// client that only answers Status, so Attach picks the poller.
	store := batch.NewMemoryStore()
	polled := batch.New(statusOnly{c}, store,
		batch.WithClock(clk),            // the poller ticks on the Manager's clock
		batch.WithStatusPoll(pollEvery), // the default poller's interval (DefaultPollInterval is 5s)
		batch.WithIDFunc(numbered("p")), // readable IDs for the output
	)
	if err := polled.Attach(ctx); err != nil { // starts the poller
		return err
	}
	// snippet:end

	// snippet:own-source
	fed := batch.New(c, batch.NewMemoryStore(),
		batch.WithCompletionSource(busSource{events: bus}), // replaces the defaults: no hook, no poller
		batch.WithIDFunc(numbered("f")),
	)
	if err := fed.Attach(ctx); err != nil { // before Start, as for every Manager
		return err
	}
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- a poller, on a client without the run-finished hook")
	if err := pollDemo(ctx, w, c, store, polled, clk, callbacks); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- an app's own feed of run ends")
	id, err := fed.Dispatch(ctx, batch.Batch{
		Jobs:    []workflow.Job{{Workflow: wfResize}, {Workflow: wfResize}},
		Then:    wfThen,
		Finally: wfFinally,
	})
	if err != nil {
		return err
	}
	printCallbacks(w, callbacks, fedCallbacks)
	p, err := fed.Progress(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s: %d of %d processed, finished=%v\n", id, p.Processed(), p.Total, p.Finished)

	// snippet:ignored
	// A run that is not a batch job carries no batch labels: ignored, nil.
	err = fed.RunFinished(ctx, map[string]string{"tenant": "acme"}, workflow.StatusSucceeded)
	fmt.Fprintln(w, "not a batch job, error:", err)
	// A job of a batch this Manager's store does not hold: ignored, nil.
	err = fed.RunFinished(ctx, batch.JobLabels("unknown", 0, 0), workflow.StatusFailed)
	fmt.Fprintln(w, "unknown batch, error:", err)
	// snippet:end
	return nil
}

// pollDemo dispatches a one-job batch on the polled Manager, waits until
// the driver reports the job succeeded, shows that the batch has not heard
// of it, then moves the fake clock one interval so the poller asks Status.
func pollDemo(ctx context.Context, w io.Writer, c workflow.Client, store batch.Store, polled *batch.Manager, clk *clock.Fake, callbacks <-chan string) error {
	id, err := polled.Dispatch(ctx, batch.Batch{Jobs: []workflow.Job{{Workflow: wfResize}}, Finally: wfFinally})
	if err != nil {
		return err
	}
	if err := waitJobsEnded(ctx, c, store, id); err != nil {
		return err
	}
	// snippet:poll-tick
	p, err := polled.Progress(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "the job ran; before the poll: %d of %d processed\n", p.Processed(), p.Total)
	if err := clk.BlockUntil(ctx, 1); err != nil { // the poller is waiting on its timer
		return err
	}
	clk.Advance(pollEvery) // one tick: one Status call per unfinished job
	// snippet:end
	printCallbacks(w, callbacks, 1)
	p, err = polled.Progress(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "after one poll: %d of %d processed, finished=%v\n", p.Processed(), p.Total, p.Finished)
	return nil
}

// waitJobsEnded asks the driver (not the batch) for the status of every
// job of batch id until each is terminal, so the output can show the
// batch lagging behind the engine until the poller's next tick. The refs
// come from the store: Dispatch saved them before it returned.
func waitJobsEnded(ctx context.Context, c workflow.Client, store batch.Store, id batch.ID) error {
	st, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	for _, j := range st.Jobs {
		for {
			status, err := c.Status(ctx, j.Ref)
			if err != nil {
				return err
			}
			if status.IsTerminal() {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(statusPoll):
			}
		}
	}
	return nil
}

// snippet:source
// busSource is a batch.Source fed by the app: every run end that arrives
// on events is handed to Manager.RunFinished, which ignores runs that are
// not batch jobs and repeats, so the feed may carry every run.
type busSource struct{ events <-chan workflow.RunEvent }

// Attach starts the reader; it stops when ctx, the context given to
// Manager.Attach, is cancelled.
func (s busSource) Attach(ctx context.Context, m *batch.Manager) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-s.events:
				if err := m.RunFinished(ctx, ev.Labels, ev.Status); err != nil {
					fmt.Fprintln(os.Stderr, "batch-completion: record run end:", err) // a real app logs and alerts
				}
			}
		}
	}()
	return nil
}

// snippet:end

// statusOnly is a client that only has the workflow.Client methods: the
// embedded interface hides memory.Client's OnRunFinished, the way a driver
// without a run-finished hook looks to a batch.
type statusOnly struct{ workflow.Client }

// numbered returns a batch ID function that numbers IDs after prefix, so
// the output is the same on every run.
func numbered(prefix string) func() (batch.ID, error) {
	n := 0
	return func() (batch.ID, error) {
		n++
		return batch.ID(prefix + strconv.Itoa(n)), nil
	}
}

// printCallbacks reads n callback lines and prints them sorted: callbacks
// are separate runs and may finish in any order.
func printCallbacks(w io.Writer, callbacks <-chan string, n int) {
	got := make([]string, 0, n)
	for range n {
		got = append(got, <-callbacks)
	}
	slices.Sort(got)
	for _, line := range got {
		fmt.Fprintln(w, line)
	}
}

// register registers the job workflow and the Then and Finally callbacks;
// each callback reports what it received on callbacks.
func register(c *memory.Client, callbacks chan<- string) error {
	if err := c.Register(workflow.Workflow{
		Name:  wfResize,
		Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	for _, kind := range []string{wfThen, wfFinally} {
		if err := c.Register(workflow.Workflow{
			Name: kind,
			Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
				callbacks <- fmt.Sprintf("%s %s succeeded=%d of %d", kind, p.ID, p.Succeeded, p.Total)
				return "", nil
			})}},
		}); err != nil {
			return err
		}
	}
	return nil
}
