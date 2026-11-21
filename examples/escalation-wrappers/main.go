// Command escalation-wrappers shows run-addressed signals behind the
// client wrappers and a plugin that binds the client it wraps. An order
// waits on its own address (WaitSpec.OwnRun) behind regional, failover,
// cutover and hybrid, and workflow.SignalRun reaches it through each one
// although the ref the app holds is prefixed with a region or a backend.
// Then a plugin with the workflow.ClientBinder hook keeps the client's
// workflow.RunSignalReader and reports a run's pending signals before each
// step, while the program reads and takes one of them itself with
// PendingSignals and TakeSignal through the regional wrapper. Every
// backend is the in-memory driver, so nothing needs to be installed.
//
// It is the program behind the "Behind wrappers" and "Bind the client
// in a plugin" sections of docs/workflows/escalation.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/hybrid"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/regional"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "escalation-wrappers:", err)
		os.Exit(1)
	}
}

const (
	// pollEvery is how often the program looks at a run it waits for. The
	// memory driver answers at once, so a short poll keeps the run fast.
	pollEvery = 2 * time.Millisecond

	// waitLimit bounds every wait of the program, so a run that never
	// reaches the state it should fails the program instead of hanging it.
	waitLimit = 10 * time.Second

	// regionLabel is the job label the regional router reads.
	regionLabel = "region"

	// paymentEvent is the signal an order waits for on its own address.
	paymentEvent = "payment"

	// noteEvent is the signal the inbox run is sent while it works; the
	// gauge plugin counts it and the program takes one by hand.
	noteEvent = "note"

	// orderFlow and inboxFlow are the workflow names.
	orderFlow = "order"
	inboxFlow = "inbox"
)

// await is how the program waits for a run's result.
var await = workflow.AwaitOptions{Poll: pollEvery}

// order is a workflow that waits for a payment addressed to its own run
// and ships what it was paid with.
func order() workflow.Workflow {
	return workflow.Workflow{Name: orderFlow, Steps: []workflow.Step{
		{Name: "await-payment", Wait: &workflow.WaitSpec{Event: paymentEvent, OwnRun: true}},
		{Name: "ship", After: []string{"await-payment"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			return in, nil
		}},
	}}
}

// wrapper is one client wrapper the order runs behind.
type wrapper struct {
	name  string
	build func() (workflow.Client, error)
}

// snippet:wrappers
// newRegional joins two memory clients as the regions "eu" and "us",
// picking the region from the job's region label ("eu" without one).
func newRegional() (workflow.Client, error) {
	return regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()},
		regional.ByLabel(regionLabel, "eu"))
}

// wrappers lists the four wrappers, each over two fresh memory clients.
func wrappers() []wrapper {
	return []wrapper{
		{"regional", newRegional},
		{"failover", func() (workflow.Client, error) {
			return failover.New(memory.New(), memory.New(), failover.AllowWithoutKey()), nil
		}},
		{"cutover", func() (workflow.Client, error) { return cutover.New(memory.New(), memory.New()), nil }},
		{"hybrid", func() (workflow.Client, error) { return hybrid.New(memory.New(), memory.New()), nil }},
	}
}

// snippet:end

// run is main without the process exit, so the test drives the same code
// and checks every line it writes to w.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, waitLimit)
	defer cancel()

	fmt.Fprintln(w, "signal a run by its ID behind a wrapper:")
	for _, wr := range wrappers() {
		if err := payOrder(ctx, w, wr); err != nil {
			return fmt.Errorf("%s: %w", wr.name, err)
		}
	}

	fmt.Fprintln(w, "a plugin that binds the client:")
	if err := gaugeInbox(ctx, w); err != nil {
		return err
	}
	return refuseBare(w)
}

// payOrder starts two orders behind wr, pays the second by its ref and
// checks that the first still waits.
func payOrder(ctx context.Context, w io.Writer, wr wrapper) error {
	base, err := wr.build()
	if err != nil {
		return err
	}
	c := workflow.Client(base)
	if err := c.Register(order()); err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	if err := c.Start(ctx); err != nil {
		return err
	}
	job := workflow.Job{Workflow: orderFlow, Labels: map[string]string{regionLabel: "us"}}
	first, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, ref, workflow.StatusWaiting); err != nil {
		return err
	}
	// snippet:wrapped-send
	// ref.RunID starts with "us:" behind regional, "primary:" behind
	// failover and "new:" behind cutover: SignalRun strips the prefix and
	// signals the run's own address on the backend that holds it.
	if err := workflow.SignalRun(ctx, c, ref, paymentEvent, []byte(`"card 4242"`)); err != nil {
		return err
	}
	// snippet:end
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	st, err := c.Status(ctx, first)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  %-8s ref prefix %-10q paid %s, other order %s\n", wr.name, refPrefix(ref), out, st)
	return nil
}

// refPrefix is the part of a ref that names the backend holding the run:
// the text before the first colon, or nothing for a ref without one.
func refPrefix(ref workflow.RunRef) string {
	prefix, _, ok := strings.Cut(string(ref.RunID), ":")
	if !ok {
		return ""
	}
	return prefix
}

// gauge is a plugin that reports how many signals of its names a run has
// pending before each of its steps, without taking them. It reads them
// through the client it wraps, which Use hands it through BindClient.
type gauge struct {
	names  []string
	reader workflow.RunSignalReader
	log    *lines
}

// snippet:binder
// Name implements workflow.Plugin: it names the plugin in Use's refusals.
func (g *gauge) Name() string { return "signal-gauge" }

// BindClient implements workflow.ClientBinder: it keeps the client's RunSignalReader, or refuses a client
// without one, so Use fails before anything is registered.
func (g *gauge) BindClient(c workflow.Client) error {
	r, ok := c.(workflow.RunSignalReader)
	if !ok || !workflow.Supports(c, workflow.OpRunSignals) {
		return workflow.Unsupported("signal-gauge", "the client cannot read a run's signals")
	}
	g.reader = r
	return nil
}

// StepMiddleware implements workflow.StepMiddlewareProvider: it counts
// the run's pending signals before each step.
func (g *gauge) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if info, ok := workflow.StepInfoFrom(ctx); ok {
				sigs, err := g.reader.PendingSignals(ctx, info.RunID, g.names, 0)
				if err != nil {
					return nil, err
				}
				g.log.add(fmt.Sprintf("before %s: %d pending", info.Step, len(sigs)))
			}
			return next(ctx, in)
		}
	}}
}

// snippet:end

// The compiler checks that gauge has the BindClient hook Use looks for.
var _ workflow.ClientBinder = (*gauge)(nil)

// gaugeInbox runs the inbox behind the regional wrapper with the gauge,
// and reads and takes one of its signals by hand while its first step
// works.
func gaugeInbox(ctx context.Context, w io.Writer) error {
	log := &lines{}
	working, release := make(chan struct{}), make(chan struct{})
	inbox := workflow.Workflow{Name: inboxFlow, Steps: []workflow.Step{
		{Name: "work", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			close(working)
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
		{Name: "report", After: []string{"work"}, Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
	}}

	base, err := newRegional()
	if err != nil {
		return err
	}
	// snippet:use-binder
	c, err := workflow.Use(base, &gauge{names: []string{noteEvent}, log: log})
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Register(inbox); err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: inboxFlow, Labels: map[string]string{regionLabel: "eu"}})
	if err != nil {
		return err
	}
	select {
	case <-working:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, note := range []string{`"first"`, `"second"`} {
		if err := workflow.SignalRun(ctx, c, ref, noteEvent, []byte(note)); err != nil {
			return err
		}
	}

	// snippet:read-take
	reader, ok := c.(workflow.RunSignalReader)
	if !ok || !workflow.Supports(c, workflow.OpRunSignals) {
		return errors.New("the client cannot read a run's signals")
	}
	pending, err := reader.PendingSignals(ctx, ref.RunID, []string{noteEvent}, 0)
	if err != nil {
		return err
	}
	took, err := reader.TakeSignal(ctx, ref.RunID, pending[0].ID)
	if err != nil {
		return err
	}
	again, err := reader.TakeSignal(ctx, ref.RunID, pending[0].ID)
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(w, "  pending while working: %d, took %s: %v, took it again: %v\n", len(pending), pending[0].Payload, took, again)

	close(release)
	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	for _, l := range log.all() {
		fmt.Fprintf(w, "  gauge %s\n", l)
	}
	return nil
}

// bare hides every optional interface of the client it holds, as a
// driver without RunSignalReader would.
type bare struct{ workflow.Client }

// refuseBare shows Use refusing the gauge on a client it cannot read.
func refuseBare(w io.Writer) error {
	_, err := workflow.Use(bare{memory.New()}, &gauge{names: []string{noteEvent}, log: &lines{}})
	if err == nil {
		return errors.New("Use accepted a client without RunSignalReader")
	}
	fmt.Fprintf(w, "  without RunSignalReader: unsupported %v\n", errors.As(err, new(*workflow.UnsupportedError)))
	return nil
}

// lines is a log steps write from worker goroutines.
type lines struct {
	mu   sync.Mutex
	seen []string
}

// add appends one line; steps call it from worker goroutines.
func (l *lines) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, s)
}

// all returns a copy of the lines so far, safe to range over while
// workers still write.
func (l *lines) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

// waitFor polls until ref reaches want or ctx ends.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st == want {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %s is %s, want %s: %w", ref.RunID, st, want, ctx.Err())
		case <-time.After(pollEvery):
		}
	}
}
