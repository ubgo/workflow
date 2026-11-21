// Command flow-cancel withdraws work that is no longer wanted without
// failing the run that started it: CancelChildren cancels every child a
// step spawned (found by the labels ChildLabels builds), CancelSteps
// withdraws several steps of one run while the rest of the run goes on,
// a TransientSignaller nudges only the runs waiting at that moment, and
// an order waits for "paid and packed, or cancelled": the "all of" part
// runs as a child that answers the parent's race, and the cancel branch
// withdraws that child. It runs on the in-memory driver, so nothing needs to be installed.
//
// It is the program behind the "Cancel every child of a step" section of
// docs/workflows/joins.md, "Withdraw several steps of a run" in
// docs/workflows/promises.md and "Deliver only to runs waiting now" in
// docs/workflows/signals.md, and "Wait for all of, any of, or a mix" in
// docs/workflows/multi-choice-and-race.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

const (
	// spawnStep is the step of the "album" workflow that spawns the
	// renders; CancelChildren and ChildLabels name it to pick its
	// children only.
	spawnStep = "render-all"

	// pages is how many children the album spawns: enough to show that
	// one call cancels them all.
	pages = 3

	// statusPolls bounds how long the program polls for a run to reach a
	// status (a millisecond each), so a broken driver fails the program
	// instead of hanging it.
	statusPolls = 5000

	// refreshEvent and refreshKey name the transient nudge: a hint whose
	// late delivery would be wrong, so it is not kept for later runs.
	refreshEvent = "cache.refresh"
	refreshKey   = "catalogue"

	// watchStep is the step of the "order" workflow that spawns the
	// "all of" child; the cancel branch names it to cancel that child only.
	watchStep = "watch"

	// orderLabel is the job label carrying the order ID: the parent's race
	// and the child's waits key on it, so one order's events reach only
	// that order's runs.
	orderLabel = "order"

	// decidedEvent is the event the race of the "order" workflow waits
	// for; the child's last step and the cancel request both send it.
	decidedEvent = "order.decided"

	// feeds is how many price feeds a quote asks in parallel; the program
	// waits until both are fetching before it withdraws them.
	feeds = 2

	// allOf is how many events the "all of" child waits for (paid and
	// packed); an order sent fewer is cancelled instead.
	allOf = 2

	// awaitPoll is how often the program looks at a run it waits for: the
	// memory driver answers at once, so a short poll keeps the run fast.
	awaitPoll = 2 * time.Millisecond
)

// await is how the program waits for a run to end.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "flow-cancel:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, writing to w, so the test drives
// the same code and checks every line.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Spawn memoises the children it started, so the client needs the
	// Journal plugin.
	m := memory.New()
	defer m.Stop()
	c, err := workflow.Use(m, workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	// rendering and fetching tell the program a step's handler is running,
	// so it cancels running work rather than racing the worker.
	rendering := make(chan struct{}, pages)
	fetching := make(chan string, feeds)
	order, err := orderWorkflow(c)
	if err != nil {
		return err
	}
	wfs := []workflow.Workflow{
		renderWorkflow(rendering),
		albumWorkflow(c),
		quoteWorkflow(fetching),
		refreshWorkflow(),
		readyWorkflow(c),
		order,
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	if err := cancelChildren(ctx, w, c, rendering); err != nil {
		return err
	}
	if err := cancelSteps(ctx, w, c, fetching); err != nil {
		return err
	}
	if err := transient(ctx, w, c); err != nil {
		return err
	}
	return mix(ctx, w, c)
}

// renderWorkflow is the child: one long render that runs until its ctx
// ends, so the children are still running when they are cancelled.
func renderWorkflow(rendering chan<- struct{}) workflow.Workflow {
	return workflow.Workflow{Name: "render", Steps: []workflow.Step{{Name: "render", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		rendering <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}}}
}

// albumWorkflow spawns one render per page and moves on without waiting.
func albumWorkflow(c workflow.Client) workflow.Workflow {
	return workflow.Workflow{Name: "album", Steps: []workflow.Step{{Name: spawnStep, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		jobs := make([]workflow.Job, pages)
		for i := range jobs {
			jobs[i] = workflow.Job{Workflow: "render", Payload: fmt.Appendf(nil, "%d", i+1)}
		}
		_, err := workflow.Spawn(ctx, c, "render", jobs)
		return nil, err
	}}}}
}

// cancelChildren starts an album, lets its renders start, then cancels
// every child of its render-all step from outside the run, as an
// operator's tool would.
func cancelChildren(ctx context.Context, w io.Writer, c workflow.Client, rendering <-chan struct{}) error {
	fmt.Fprintln(w, "cancel every child of a step:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "album"})
	if err != nil {
		return err
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	for range pages {
		<-rendering
	}
	fmt.Fprintf(w, "  album %s, %d renders running\n", workflow.StatusSucceeded, pages)

	// snippet:cancel-children
	n, err := workflow.CancelChildren(ctx, c, ref.RunID, "render-all")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  cancelled %d children\n", n)
	// snippet:end

	// snippet:child-labels
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{
		Labels: workflow.ChildLabels(ref.RunID, "render-all"),
	})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(w, "  child %s of step %s: %s\n", r.Payload, r.Labels[workflow.LabelParentStep], r.Status)
	}
	// snippet:end

	n, err = workflow.CancelChildren(ctx, c, ref.RunID, spawnStep)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  cancel again: %d children (only unfinished ones count)\n", n)
	return nil
}

// quoteWorkflow asks two price feeds in parallel and combines what they
// return. A feed that is withdrawn falls back to a cached price, so the
// combine step still runs.
func quoteWorkflow(fetching chan<- string) workflow.Workflow {
	// feed builds one price-feed step that hangs until it is withdrawn,
	// then answers with a cached price.
	feed := func(name string) workflow.Step {
		return workflow.Step{Name: name, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			fetching <- name
			<-ctx.Done() // the feed hangs
			if errors.Is(context.Cause(ctx), workflow.ErrStepCancelled) {
				return json.Marshal("cached " + name) // withdrawn: fall back
			}
			return nil, context.Cause(ctx)
		}}
	}
	return workflow.Workflow{Name: "quote", Steps: []workflow.Step{
		feed("feed-eu"),
		feed("feed-us"),
		{Name: "combine", After: []string{"feed-eu", "feed-us"}, Run: workflow.Define(func(_ context.Context, in map[string]string) (string, error) {
			return in["feed-eu"] + " + " + in["feed-us"], nil
		})},
	}}
}

// cancelSteps withdraws both hanging feeds of one quote run with one call;
// the run goes on with their fallbacks.
func cancelSteps(ctx context.Context, w io.Writer, c workflow.Client, fetching <-chan string) error {
	fmt.Fprintln(w, "withdraw several steps:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "quote"})
	if err != nil {
		return err
	}
	<-fetching
	<-fetching

	// snippet:cancel-steps
	if err := workflow.CancelSteps(ctx, c, ref, "feed-eu", "feed-us"); err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  quote succeeded with %s\n", out)
	// snippet:end

	// The run has ended, so each step answers ErrWrongState and
	// CancelSteps joins both answers.
	err = workflow.CancelSteps(ctx, c, ref, "feed-eu", "feed-us")
	fmt.Fprintf(w, "  withdraw again: ErrWrongState %t, both steps named %t\n",
		errors.Is(err, workflow.ErrWrongState),
		err != nil && strings.Contains(err.Error(), `step "feed-eu"`) && strings.Contains(err.Error(), `step "feed-us"`))
	return nil
}

// refreshWorkflow waits for a refresh nudge, then succeeds.
func refreshWorkflow() workflow.Workflow {
	return workflow.Workflow{Name: "refresh", Steps: []workflow.Step{{
		Name: "await-refresh",
		Wait: &workflow.WaitSpec{Event: refreshEvent, Key: refreshKey},
	}}}
}

// transient sends a nudge before any run waits (it reaches nobody and is
// not kept), then again once a run waits (it resumes that run).
func transient(ctx context.Context, w io.Writer, c workflow.Client) error {
	fmt.Fprintln(w, "deliver only to runs waiting now:")
	ts, ok := c.(workflow.TransientSignaller)
	if !ok {
		return errors.New("client has no SignalWaiting")
	}
	nudge := workflow.Event{Name: refreshEvent, Key: refreshKey}
	n, err := ts.SignalWaiting(ctx, nudge)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  before any run waits: resumed %d\n", n)

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "refresh"})
	if err != nil {
		return err
	}
	if err := until(ctx, c, ref, workflow.StatusWaiting); err != nil {
		return err
	}
	fmt.Fprintln(w, "  the run started later is still waiting: the first nudge was not kept")

	// snippet:signal-waiting
	n, err = c.(workflow.TransientSignaller).SignalWaiting(ctx, workflow.Event{Name: "cache.refresh", Key: "catalogue"})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  while it waits: resumed %d\n", n)
	// snippet:end

	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	st, err := c.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  run %s\n", st)
	return nil
}

// until polls the run's status: a step moves on its own goroutine.
func until(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	for range statusPolls {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("run %s never reached %s", ref.RunID, want)
}

// byOrder keys a wait on the run's order label, so a signal for one order
// reaches only that order's runs.
var byOrder = workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[orderLabel] })

// readyWorkflow is the "all of" half of "paid and packed, or cancelled":
// one wait per condition and a step after both that answers the parent's
// race with the "ship" branch.
func readyWorkflow(c workflow.Client) workflow.Workflow {
	// snippet:mix-ready
	return workflow.Workflow{Name: "order.ready", Steps: []workflow.Step{
		{Name: "paid", Wait: &workflow.WaitSpec{Event: "order.paid", KeyFrom: byOrder}},
		{Name: "packed", Wait: &workflow.WaitSpec{Event: "order.packed", KeyFrom: byOrder}},
		{Name: "both", After: []string{"paid", "packed"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			return nil, workflow.SignalChoice(ctx, c, decidedEvent, info.Labels[orderLabel], "ship", nil)
		}},
	}}
	// snippet:end
}

// orderWorkflow spawns the "all of" child, then races: the child's
// "ship" or a "cancel" request, whichever comes first. The cancel branch
// cancels the half-met child so it does not linger.
func orderWorkflow(c workflow.Client) (workflow.Workflow, error) {
	// snippet:mix-race
	watch := workflow.Step{Name: watchStep, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		_, err := workflow.Spawn(ctx, c, "ready", []workflow.Job{{
			Workflow: "order.ready",
			Labels:   map[string]string{orderLabel: info.Labels[orderLabel]},
		}})
		return nil, err
	}}
	race, err := workflow.Race(workflow.RaceSpec{
		Name: "decided", After: []string{watchStep}, Event: decidedEvent, KeyFrom: byOrder,
		Branches: []workflow.Branch{
			{Name: "ship", Run: func(context.Context, []byte) ([]byte, error) {
				return json.Marshal("shipped")
			}},
			{Name: "cancel", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				info, _ := workflow.StepInfoFrom(ctx)
				n, err := workflow.CancelChildren(ctx, c, info.RunID, watchStep) // the half-met "all of"
				if err != nil {
					return nil, err
				}
				return json.Marshal(fmt.Sprintf("cancelled, %d child withdrawn", n))
			}},
		},
	})
	if err != nil {
		return workflow.Workflow{}, err
	}
	// snippet:end
	return workflow.Workflow{Name: "order", Steps: append([]workflow.Step{watch}, race...)}, nil
}

// mix runs two orders: A-1 is paid and packed, so its child answers the
// race with "ship"; A-2 is only paid before the customer cancels, so the
// cancel branch wins and withdraws A-2's waiting child.
func mix(ctx context.Context, w io.Writer, c workflow.Client) error {
	fmt.Fprintln(w, "wait for all of, or any of:")
	for _, o := range []struct {
		id     string
		events []string
	}{
		{"A-1", []string{"order.paid", "order.packed"}},
		{"A-2", []string{"order.paid"}},
	} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "order", Labels: map[string]string{orderLabel: o.id}})
		if err != nil {
			return err
		}
		child, err := childOf(ctx, c, ref.RunID)
		if err != nil {
			return err
		}
		// Each wait must have started before its event is sent: a signal
		// reaches only runs that wait for it.
		if err := until(ctx, c, child, workflow.StatusWaiting); err != nil {
			return err
		}
		for _, ev := range o.events {
			if err := c.Signal(ctx, workflow.Event{Name: ev, Key: o.id}); err != nil {
				return err
			}
		}
		if len(o.events) < allOf {
			// snippet:mix-cancel
			if err := workflow.SignalChoice(ctx, c, decidedEvent, o.id, "cancel", nil); err != nil {
				return err
			}
			// snippet:end
		}
		out, err := workflow.AwaitResultOf[string](ctx, c, ref, await)
		if err != nil {
			return err
		}
		// The child's last step may still be finishing after the parent
		// ended, so wait for the child to end too before printing it.
		st, err := ended(ctx, c, child)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s (%s): %s; child %s\n", o.id, strings.Join(o.events, " + "), out, st)
	}
	return nil
}

// childOf polls until the order run has spawned its child and returns
// the child's ref, found by the parent labels.
func childOf(ctx context.Context, c workflow.Client, parent workflow.RunID) (workflow.RunRef, error) {
	q, ok := c.(workflow.RunQuerier)
	if !ok {
		return workflow.RunRef{}, errors.New("client cannot list runs")
	}
	for range statusPolls {
		page, err := q.Runs(ctx, workflow.RunQuery{Labels: workflow.ChildLabels(parent, watchStep)})
		if err != nil {
			return workflow.RunRef{}, err
		}
		if len(page.Runs) > 0 {
			return page.Runs[0].Ref, nil
		}
		time.Sleep(time.Millisecond)
	}
	return workflow.RunRef{}, errors.New("no child spawned")
}

// ended polls the run's status until it is terminal and returns it.
func ended(ctx context.Context, c workflow.Client, ref workflow.RunRef) (workflow.Status, error) {
	for range statusPolls {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return "", err
		}
		if st.IsTerminal() {
			return st, nil
		}
		time.Sleep(time.Millisecond)
	}
	return "", fmt.Errorf("run %s never ended", ref.RunID)
}
