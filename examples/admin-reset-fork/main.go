// Command admin-reset-fork acts on runs from Go through the admin module,
// on the in-memory driver with a fake clock: it resets a failed run in
// place from a step, resets a succeeded one with the explicit opt-in,
// forks a finished run into a new one, signals only the runs waiting now,
// withdraws one step, forces runs to an end, and edits and reschedules a
// delayed run. First it asks which of these a client wrapper really
// supports, through workflow.Supports and the wrapper's OpProber.
//
// It is the program behind steps 3 and 5 of docs/admin/operations.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-reset-fork:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant, so every time printed is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// The workflow and event names the example uses.
const (
	wfETL     = "etl"
	wfRefresh = "cache-refresh"
	wfEmail   = "send-email"
	eventName = "cache.refresh"
	// regionLabel keys each cache-refresh run's wait, so a signal can
	// reach one region's run and not the others.
	regionLabel = "region"
)

// delayedBy is how far ahead the delayed send-email run is due, so it has
// not started when it is edited and rescheduled.
const delayedBy = time.Hour

// snippet:wrapper
// audited is an app's own client wrapper (it would log each enqueue).
// Embedding workflow.Forward beside the wrapped client keeps the optional
// operations of the client underneath reachable; Forward's Supports
// method (workflow.OpProber) answers for that client, not for the
// wrapper, whose methods exist whatever it wraps.
type audited struct {
	workflow.Client
	workflow.Forward
}

// wrap returns c inside audited, with Forward pointing at the same client
// so the optional operations stay reachable.
func wrap(c workflow.Client) *audited {
	return &audited{Client: c, Forward: workflow.Forward{Inner: c}}
}

// plain hides every optional interface of the client it holds, like a
// driver that has none of them.
type plain struct{ workflow.Client }

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))
	var broken atomic.Bool // the etl transform step fails while true
	broken.Store(true)
	// pass is a step that hands its input on unchanged.
	pass := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }
	for _, wf := range []workflow.Workflow{
		{Name: wfETL, Steps: []workflow.Step{
			{Name: "extract", Run: func(context.Context, []byte) ([]byte, error) { return []byte("41"), nil }},
			{Name: "transform", After: []string{"extract"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				if broken.Load() {
					return nil, workflow.Permanent(errors.New("transform: bad date in row 7"))
				}
				return in, nil
			}},
			{Name: "publish", After: []string{"transform"}, Run: pass},
		}},
		{Name: wfRefresh, Steps: []workflow.Step{
			{Name: "await", Wait: &workflow.WaitSpec{
				Event:   eventName,
				KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[regionLabel] }),
			}},
			{Name: "reload", After: []string{"await"}, Run: pass},
		}},
		{Name: wfEmail, Steps: []workflow.Step{{Name: "send", Run: pass}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	// settle runs every run that is due until none is left.
	settle := func() error { _, err := c.RunAll(ctx); return err }
	// status reads one run's current state through the admin module.
	status := func(adm *admin.Admin, ref workflow.RunRef) (workflow.RunInfo, error) { return adm.Run(ctx, ref) }

	fmt.Fprintln(w, "-- what a wrapper supports")
	// snippet:support
	for _, cl := range []struct {
		name string
		c    workflow.Client
	}{
		{"memory client", c},
		{"wrapper over memory", wrap(c)},
		{"wrapper over a plain client", wrap(plain{c})},
	} {
		_, hasMethod := cl.c.(workflow.RunResetter) // true for both wrappers
		adm := admin.New(cl.c)
		fmt.Fprintf(w, "%-28s has ResetRun %-5v Supports(OpResetRun) %-5v admin reset_run %v\n",
			cl.name, hasMethod, workflow.Supports(cl.c, workflow.OpResetRun), adm.Supports(admin.OpResetRun))
	}
	// snippet:end
	for _, s := range admin.New(wrap(plain{c})).Support() {
		if s.Operation == admin.OpNodes {
			fmt.Fprintf(w, "wrapper over a plain client: %s supported=%v reason=%q\n", s.Operation, s.Supported, s.Reason)
		}
	}

	adm := admin.New(c, admin.WithClock(clk))
	// Every run is enqueued under a pause, so the IDs the driver hands out
	// do not depend on which runs its workers pick up first.
	if err := c.Pause(ctx, workflow.PauseAll()); err != nil {
		return err
	}
	failed, err := c.Enqueue(ctx, workflow.Job{Workflow: wfETL})
	if err != nil {
		return err
	}
	waiting := map[string]workflow.RunRef{}
	for _, region := range []string{"eu", "us", "apac", "latam"} {
		if waiting[region], err = c.Enqueue(ctx, workflow.Job{Workflow: wfRefresh, Labels: map[string]string{regionLabel: region}}); err != nil {
			return err
		}
	}
	if err := c.Resume(ctx, workflow.PauseAll()); err != nil {
		return err
	}
	if err := settle(); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- reset and fork")
	broken.Store(false) // the transform fix is deployed
	// snippet:reset
	ref, err := adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{})
	if err != nil {
		return err
	}
	// snippet:end
	if err := settle(); err != nil {
		return err
	}
	info, err := status(adm, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "reset %s from transform: same run %v, now %s\n", failed.RunID, ref == failed, info.Status)

	// snippet:reset-succeeded
	_, err = adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{})
	fmt.Fprintln(w, "reset a succeeded run without the opt-in:", errors.Is(err, workflow.ErrWrongState))
	ref, err = adm.ResetRun(ctx, failed, "transform", workflow.ResetOptions{Succeeded: true})
	if err != nil {
		return err
	}
	// snippet:end
	if err := settle(); err != nil {
		return err
	}
	if info, err = status(adm, ref); err != nil {
		return err
	}
	fmt.Fprintf(w, "reset with Succeeded: %s, redo label %q\n", info.Status, info.Labels[workflow.LabelRedo])

	// snippet:fork
	fork, err := adm.ForkRun(ctx, failed, "publish", workflow.ForkOptions{
		IdempotencyKey: "replay-" + failed.RunID.String(), // the fork's own key, never the original's
		Labels:         map[string]string{"ticket": "OPS-7"},
	})
	if err != nil {
		return err
	}
	// snippet:end
	if err := settle(); err != nil {
		return err
	}
	if info, err = status(adm, fork); err != nil {
		return err
	}
	fmt.Fprintf(w, "fork %s: new run %v, %s, fork-of %s from step %s, ticket %s\n", fork.RunID, fork.RunID != failed.RunID,
		info.Status, info.Labels[workflow.LabelForkOf], info.Labels[workflow.LabelForkStep], info.Labels["ticket"])
	_, err = adm.ForkRun(ctx, waiting["eu"], "reload", workflow.ForkOptions{})
	fmt.Fprintln(w, "fork an unfinished run:", errors.Is(err, workflow.ErrWrongState))

	fmt.Fprintln(w, "-- signal the runs waiting now")
	// snippet:signal
	n, err := adm.SignalWaiting(ctx, workflow.Event{Name: eventName, Key: "eu", Payload: []byte(`{"reason":"deploy"}`)})
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "resumed:", n)
	n, err = adm.SignalWaiting(ctx, workflow.Event{Name: eventName, Key: "eu"}) // nobody waits for eu now
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "resumed again:", n, "(zero is not an error)")
	// snippet:end
	_, err = adm.SignalWaiting(ctx, workflow.Event{})
	fmt.Fprintln(w, "an event without a name:", errors.Is(err, workflow.ErrInvalid))
	if err := settle(); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- withdraw a step, force an end")
	// snippet:cancel-step
	if err := adm.CancelStep(ctx, waiting["us"], "await"); err != nil {
		return err
	}
	// snippet:end
	if err := awaitEnd(ctx, c, waiting["us"]); err != nil {
		return err
	}
	if info, err = status(adm, waiting["us"]); err != nil {
		return err
	}
	fmt.Fprintf(w, "cancel step await of %s: run %s at step %s\n", waiting["us"].RunID, info.Status, info.FailedStep)

	// snippet:force
	if err := adm.ForceComplete(ctx, waiting["apac"]); err != nil { // someone refreshed apac by hand
		return err
	}
	if err := adm.ForceFail(ctx, waiting["latam"], "region retired"); err != nil {
		return err
	}
	// snippet:end
	for _, region := range []string{"apac", "latam"} {
		if info, err = status(adm, waiting[region]); err != nil {
			return err
		}
		line := fmt.Sprintf("forced %s: %s", region, info.Status)
		if info.Error != "" {
			line += ", error " + info.Error
		}
		fmt.Fprintln(w, line)
	}
	err = adm.ForceFail(ctx, waiting["eu"], strings.Repeat("x", admin.MaxForceReason+1))
	fmt.Fprintf(w, "a reason over %d bytes: %v\n", admin.MaxForceReason, errors.Is(err, workflow.ErrInvalid))

	fmt.Fprintln(w, "-- edit and move a delayed run")
	delayed, err := c.Enqueue(ctx, workflow.Job{Workflow: wfEmail, Payload: []byte(`"dan"`), RunAt: start.Add(delayedBy)})
	if err != nil {
		return err
	}
	// snippet:edit
	payload, priority := []byte(`"dana"`), 5 // higher goes first; zero is the default
	delayed, err = adm.EditRun(ctx, delayed, workflow.JobEdit{Payload: &payload, Priority: &priority})
	if err != nil {
		return err
	}
	delayed, err = adm.Reschedule(ctx, delayed, start.Add(2*delayedBy)) // one delayedBy later than first due
	if err != nil {
		return err
	}
	// snippet:end
	if info, err = status(adm, delayed); err != nil {
		return err
	}
	fmt.Fprintf(w, "edited and moved %s: %s, priority %d, due %s\n", delayed.ScheduledID, info.Status, info.Priority, info.RunAt.Format(time.TimeOnly))
	_, err = adm.Reschedule(ctx, delayed, time.Time{})
	fmt.Fprintln(w, "reschedule to the zero time:", errors.Is(err, workflow.ErrInvalid))
	return nil
}

// endPoll is how often awaitEnd looks at a run, and endTimeout how long it
// waits for one to end before giving up.
const (
	endPoll    = time.Millisecond
	endTimeout = 10 * time.Second
)

// awaitEnd waits until the run ref has ended. A step cancel on a parked
// step ends the run on the driver's own goroutine, after CancelStep has
// returned, so RunAll alone may come back before it.
func awaitEnd(ctx context.Context, c *memory.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(endTimeout)
	for {
		page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		if len(page.Runs) == 1 && page.Runs[0].Status.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s did not end within %s", ref.RunID, endTimeout)
		}
		time.Sleep(endPoll)
	}
}
