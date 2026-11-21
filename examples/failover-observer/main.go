// Command failover-observer shows the parts of failover.Client an operator
// tunes and watches, on two in-memory backends with a fake clock for the
// cool-off:
//
//   - which errors count as an outage: DefaultClassifier, StrictClassifier
//     and a Classifier of the app's own, passed with WithClassifier;
//   - an Observer (WithObserver) that logs every Event with its fields and
//     counts the events per EventKind, as a metric would, while WithCoolOff
//     skips the primary after repeated failovers;
//   - ResetRun, EditRun and SignalWaiting on runs that sit on either
//     backend, with workflow.Supports telling whether both backends can
//     do them;
//   - WithSchedulesOnBoth, which puts a schedule on both backends.
//
// It is the program behind docs/reliability/failover.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

const (
	// coolOffAfter is how many failovers in a row start a cool-off: two,
	// so the demo shows one plain failover before the cool-off starts.
	coolOffAfter = 2
	// coolOffFor is how long the primary is skipped once a cool-off
	// starts; the fake clock jumps over it at once.
	coolOffFor = time.Minute
	// statusPoll is how often the program re-reads a run's status: short,
	// so the demo finishes quickly; a real app waits on the run-finished
	// hook.
	statusPoll = 2 * time.Millisecond
	// eventGo is the event the waiting runs wait for.
	eventGo = "go"
	// timeOfDay formats the event times: the fake clock starts on a fixed
	// date, so the time alone is what changes.
	timeOfDay = "15:04:05"
	// remindDelay is how far ahead the remind run is due: a day, so it is
	// certainly still waiting when the program edits it.
	remindDelay = 24 * time.Hour
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "failover-observer:", err)
		os.Exit(1)
	}
}

// snippet:classifier
// outage is this deployment's rule for "the primary is unavailable": what a
// driver marked with workflow.Unavailable (StrictClassifier), plus a
// refused connection, which an older driver returns unmarked. Any other
// unknown error is treated as a bug and returned to the caller.
func outage(err error) bool {
	return failover.StrictClassifier(err) || errors.Is(err, syscall.ECONNREFUSED)
}

// snippet:end

// switchable wraps a memory client so it fails every enqueue with err
// while down is set: a stand-in for an engine outage. WrapClient keeps the
// memory client's optional interfaces (ResetRun, EditRun, SignalWaiting).
func switchable(down *atomic.Bool, err error) workflow.Client {
	return middleware.WrapClient(memory.New(), func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if down.Load() {
				return workflow.RunRef{}, err
			}
			return next(ctx, job)
		}
	})
}

// side is the backend a run or schedule ID names: the part before the
// first colon.
func side(id string) string {
	s, _, _ := strings.Cut(id, ":")
	return s
}

// refSide is the backend a RunRef names; a delayed run may have only its
// ScheduledID yet.
func refSide(ref workflow.RunRef) string {
	if !ref.RunID.IsZero() {
		return side(string(ref.RunID))
	}
	return side(string(ref.ScheduledID))
}

// run is main without the process exit, writing to w, so the test drives
// the same code and reads every line.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Fprintln(w, "-- Decide what counts as an outage")
	classify(w)

	fmt.Fprintln(w, "-- Watch failovers")
	return watch(ctx, w)
}

// classify asks each classifier about the same errors: what a driver
// marked, a transport error, an app bug and errors about the request.
func classify(w io.Writer) {
	// snippet:classify
	errs := []struct {
		name string
		err  error
	}{
		{"marked Unavailable", workflow.Unavailable(errors.New("engine timeout"))},
		{"connection refused", fmt.Errorf("dial primary: %w", syscall.ECONNREFUSED)},
		{"unknown error", errors.New("unexpected reply")},
		{"invalid job", fmt.Errorf("bad payload: %w", workflow.ErrInvalid)},
		{"permanent", workflow.Permanent(workflow.Unavailable(errors.New("engine timeout")))},
	}
	for _, e := range errs {
		fmt.Fprintf(w, "  %-18s default %-5t strict %-5t ours %t\n", e.name,
			failover.DefaultClassifier(e.err), failover.StrictClassifier(e.err), outage(e.err))
	}
	// snippet:end
}

// eventLog is an Observer that prints each event and counts them per
// kind. Enqueue calls the Observer synchronously and may be called from
// many goroutines, so the counts are guarded.
type eventLog struct {
	w      io.Writer
	mu     sync.Mutex
	counts map[failover.EventKind]int
}

// snippet:observer
// observe logs every failover event and counts it, as a metric would. It
// returns quickly and never calls Enqueue on the failover client.
func (l *eventLog) observe(_ context.Context, ev failover.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.counts[ev.Kind]++
	line := fmt.Sprintf("  event %s workflow=%s at=%s", ev.Kind, ev.Workflow, ev.At.Format(timeOfDay))
	switch ev.Kind {
	case failover.EventFailedOver:
		line += fmt.Sprintf(" err=%q", ev.Err) // the primary's error
	case failover.EventCoolOffStarted:
		line += " until=" + ev.Until.Format(timeOfDay) // when the primary is tried again
	case failover.EventPrimarySkipped, failover.EventCoolOffEnded:
		// Kind, Workflow and At say it all.
	}
	fmt.Fprintln(l.w, line)
}

// snippet:end

// watch runs jobs through an outage and back, prints the observer's
// events, then resets, edits and signals runs on both backends.
func watch(ctx context.Context, w io.Writer) error {
	clk := clock.NewFake(start)
	var down atomic.Bool
	primary := switchable(&down, fmt.Errorf("dial primary: %w", syscall.ECONNREFUSED))
	secondary := memory.New()
	log := &eventLog{w: w, counts: map[failover.EventKind]int{}}

	// snippet:new
	c := failover.New(primary, secondary,
		failover.WithClassifier(outage),
		failover.WithCoolOff(coolOffAfter, coolOffFor), // skip the primary for 1m after 2 failovers in a row
		failover.WithClock(clk),                        // the cool-off's time source
		failover.WithObserver(log.observe),
	)
	// snippet:end

	var fixed atomic.Bool // the bug in "report" is fixed and deployed
	report := workflow.Workflow{Name: "report", Steps: []workflow.Step{
		{Name: "fetch", Run: func(context.Context, []byte) ([]byte, error) { return json.Marshal(41) }}, // demo data: the report's figure
		{Name: "render", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			if !fixed.Load() {
				return nil, workflow.Permanent(errors.New("template missing"))
			}
			return in, nil
		}},
	}}
	remind := workflow.Workflow{Name: "remind", Steps: []workflow.Step{{Name: "mail",
		Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	approve := workflow.Workflow{Name: "approve", Steps: []workflow.Step{{Name: "wait", Wait: &workflow.WaitSpec{Event: eventGo}}}}
	for _, wf := range []workflow.Workflow{report, remind, approve} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	// enqueue enqueues job and prints which backend took it.
	enqueue := func(job workflow.Job) (workflow.RunRef, error) {
		ref, err := c.Enqueue(ctx, job)
		if err == nil {
			fmt.Fprintf(w, "%s %s -> %s\n", job.Workflow, job.IdempotencyKey, refSide(ref))
		}
		return ref, err
	}

	waitingOnPrimary, err := enqueue(workflow.Job{Workflow: "approve", IdempotencyKey: "a-1"})
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "-- the primary goes down")
	down.Store(true)
	failing, err := enqueue(workflow.Job{Workflow: "report", IdempotencyKey: "r-1"}) // failover 1
	if err != nil {
		return err
	}
	delayed, err := enqueue(workflow.Job{Workflow: "remind", IdempotencyKey: "m-1", // failover 2: the cool-off starts
		Payload: []byte(`"old@example.com"`), RunAt: time.Now().Add(remindDelay)})
	if err != nil {
		return err
	}
	waitingOnSecondary, err := enqueue(workflow.Job{Workflow: "approve", IdempotencyKey: "a-2"}) // cooling off: not tried
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "-- the primary is back and the cool-off runs out")
	down.Store(false)
	clk.Advance(coolOffFor)
	backOnPrimary, err := enqueue(workflow.Job{Workflow: "approve", IdempotencyKey: "a-3"})
	if err != nil {
		return err
	}
	log.mu.Lock()
	kinds := []failover.EventKind{failover.EventFailedOver, failover.EventCoolOffStarted, failover.EventPrimarySkipped, failover.EventCoolOffEnded}
	for _, k := range kinds {
		fmt.Fprintf(w, "  count %s: %d\n", k, log.counts[k])
	}
	log.mu.Unlock()

	fmt.Fprintln(w, "-- Reset, edit and signal runs on either backend")
	if err := manage(ctx, w, c, &fixed, failing, delayed, []workflow.RunRef{waitingOnPrimary, waitingOnSecondary, backOnPrimary}); err != nil {
		return err
	}

	fmt.Fprintln(w, "-- Schedules on both backends")
	return schedulesOnBoth(ctx, w)
}

// manage resets the failed run, edits the delayed one and signals the
// waiting ones, wherever each sits.
func manage(ctx context.Context, w io.Writer, c *failover.Client, fixed *atomic.Bool, failing, delayed workflow.RunRef, waiting []workflow.RunRef) error {
	// await waits for ref to end and returns its final status.
	await := func(ref workflow.RunRef) (workflow.Status, error) {
		return workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: statusPoll})
	}
	st, err := await(failing)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "report on the", side(string(failing.RunID)), "is", st)

	// snippet:supports
	for _, op := range []workflow.Op{workflow.OpResetRun, workflow.OpEditRun, workflow.OpSignalWaiting} {
		// True only when both backends do it, since a run may sit on either.
		fmt.Fprintf(w, "  supports %s: %t\n", op, workflow.Supports(c, op))
	}
	// snippet:end

	// snippet:reset
	fixed.Store(true)
	again, err := c.ResetRun(ctx, failing, "render") // routed by the ref's "secondary:" prefix
	if err != nil {
		return err
	}
	// snippet:end
	if st, err = await(again); err != nil {
		return err
	}
	fmt.Fprintln(w, "reset in place:", again == failing, st)

	// snippet:edit
	payload, now := []byte(`"new@example.com"`), time.Now()
	edited, err := c.EditRun(ctx, delayed, workflow.JobEdit{Payload: &payload, RunAt: &now})
	if err != nil {
		return err // workflow.ErrWrongState once the run has started
	}
	// snippet:end
	if _, err = await(edited); err != nil {
		return err
	}
	out, err := c.Result(ctx, edited)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "edited remind on the", refSide(edited), "sent to", string(out))

	for _, ref := range waiting {
		if err := awaitStatus(ctx, c, ref, workflow.StatusWaiting); err != nil {
			return err
		}
	}
	// snippet:signal-waiting
	n, err := c.SignalWaiting(ctx, workflow.Event{Name: eventGo}) // both backends' waiting runs
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(w, "signal-waiting resumed", n, "run(s)")
	for _, ref := range waiting {
		if st, err = await(ref); err != nil {
			return err
		}
		fmt.Fprintln(w, "  approve on the", side(string(ref.RunID)), "is", st)
	}

	// snippet:supports-missing
	// A backend that hides the optional interfaces (here a bare
	// workflow.Client) makes the failover client refuse them.
	partial := failover.New(memory.New(), struct{ workflow.Client }{memory.New()})
	fmt.Fprintf(w, "  with a bare secondary, supports %s: %t\n", workflow.OpSignalWaiting, workflow.Supports(partial, workflow.OpSignalWaiting))
	_, err = partial.SignalWaiting(ctx, workflow.Event{Name: eventGo})
	fmt.Fprintln(w, "  SignalWaiting refused:", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}

// awaitStatus polls a run until it reaches want.
func awaitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}

// schedulesOnBoth makes one schedule with WithSchedulesOnBoth and lists
// its copies.
func schedulesOnBoth(ctx context.Context, w io.Writer) error {
	digest := workflow.Workflow{Name: "digest", Steps: []workflow.Step{{Name: "send",
		Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}

	// snippet:schedules-on-both
	// Every tick fires on both backends while both are up: only for
	// scheduled work that tolerates running twice per tick.
	c := failover.New(memory.New(), memory.New(), failover.WithSchedulesOnBoth())
	if err := c.Register(digest); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "digest", Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	infos, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	// snippet:end
	sides := make([]string, 0, len(infos))
	for _, s := range infos {
		sides = append(sides, side(string(s.ID)))
	}
	slices.Sort(sides)
	fmt.Fprintln(w, "schedule copies:", strings.Join(sides, ", "))
	fmt.Fprintln(w, "Schedule returned an ID for:", side(string(id))) // pauses both copies
	return nil
}
