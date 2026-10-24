// Command concurrency caps how many runs of a workflow execute at once per
// key (one report per tenant at a time), shows what each strategy does to
// the run over the cap, rate-limits starts per key, orders waiting work by
// priority, and shows which key form each driver refuses.
//
// It runs on the in-memory driver with a fake clock, so a one-minute rate
// window passes when the program moves the clock. It is the program behind
// docs/jobs/concurrency.md and the priority section of docs/jobs/enqueue.md:
// the guides copy its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

// gated is a step whose runs report when they start and then hold until
// the program releases them, so the program can look at a full bucket.
type gated struct {
	started chan string
	gates   map[string]chan struct{} // run name -> closed to let it finish
}

// handler reports the run's "name" label and waits for its gate, or for
// the run to be cancelled.
func (g gated) handler(ctx context.Context, _ []byte) ([]byte, error) {
	info, _ := workflow.StepInfoFrom(ctx)
	name := info.Labels["name"]
	g.started <- name
	select {
	case <-g.gates[name]:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "concurrency:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	g := gated{started: make(chan string, 16), gates: map[string]chan struct{}{}}
	for _, n := range []string{"acme-1", "acme-2", "globex-1", "sync-1", "preview-1", "preview-2", "mail-1", "mail-2", "mail-3"} {
		g.gates[n] = make(chan struct{})
	}
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))

	// snippet:per-key
	tenant := workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] })
	report := workflow.Workflow{
		Name: "report",
		Concurrency: &workflow.Concurrency{
			Key:      tenant, // one bucket per tenant
			MaxRuns:  1,      // one report per tenant at a time
			Strategy: workflow.ConcurrencyQueueNewest,
		},
		Steps: []workflow.Step{{Name: "build", Run: g.handler}},
	}
	// snippet:end

	// snippet:strategies
	sync := workflow.Workflow{
		Name:        "sync",
		Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyDropNewest},
		Steps:       []workflow.Step{{Name: "pull", Run: g.handler}},
	}
	preview := workflow.Workflow{
		Name:        "preview",
		Concurrency: &workflow.Concurrency{MaxRuns: 1, Strategy: workflow.ConcurrencyCancelInProgress},
		Steps:       []workflow.Step{{Name: "render", Run: g.handler}},
	}
	// snippet:end

	// snippet:rate-limit
	mail := workflow.Workflow{
		Name: "mail",
		RateLimits: []workflow.RateLimit{{
			Key:   workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["domain"] }),
			Limit: 2,           // at most two starts
			Per:   time.Minute, // per minute, per recipient domain
		}},
		Steps: []workflow.Step{{Name: "send", Run: g.handler}},
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{report, sync, preview, mail} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	enqueue := func(wf, name string, labels map[string]string) (workflow.RunRef, error) {
		l := map[string]string{"name": name}
		maps.Copy(l, labels)
		return c.Enqueue(ctx, workflow.Job{Workflow: wf, Labels: l})
	}
	// status is for printing only: an error reads as an empty status,
	// which the expected output in main_test.go catches.
	status := func(ref workflow.RunRef) workflow.Status {
		st, _ := c.Status(ctx, ref)
		return st
	}

	// One run per tenant: acme's second report waits, globex's runs.
	fmt.Fprintln(out, "per key:")
	// An enqueue error returns at once: the receives from g.started below
	// would otherwise wait forever for a run that was never created.
	a1, err := enqueue("report", "acme-1", map[string]string{"tenant": "acme"})
	if err != nil {
		return err
	}
	a2, err := enqueue("report", "acme-2", map[string]string{"tenant": "acme"})
	if err != nil {
		return err
	}
	g1, err := enqueue("report", "globex-1", map[string]string{"tenant": "globex"})
	if err != nil {
		return err
	}
	<-g.started
	<-g.started
	fmt.Fprintf(out, "  acme-1 %s, acme-2 %s, globex-1 %s\n", status(a1), status(a2), status(g1))
	close(g.gates["acme-1"])
	fmt.Fprintln(out, "  acme-1 finished, started:", <-g.started)
	close(g.gates["acme-2"])
	close(g.gates["globex-1"])

	fmt.Fprintln(out, "drop newest:")
	if _, err := enqueue("sync", "sync-1", nil); err != nil {
		return err
	}
	<-g.started
	// snippet:dropped
	s2, err := enqueue("sync", "sync-2", nil) // the bucket is full
	fmt.Fprintf(out, "  second sync: error %v, status %s\n", err, status(s2))
	// snippet:end
	close(g.gates["sync-1"])

	fmt.Fprintln(out, "cancel in progress:")
	p1, err := enqueue("preview", "preview-1", nil)
	if err != nil {
		return err
	}
	<-g.started
	p2, err := enqueue("preview", "preview-2", nil)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  started %s; preview-1 %s, preview-2 %s\n", <-g.started, status(p1), status(p2))
	close(g.gates["preview-2"])

	fmt.Fprintln(out, "rate limit:")
	d := map[string]string{"domain": "example.com"}
	for _, n := range []string{"mail-1", "mail-2"} {
		if _, err := enqueue("mail", n, d); err != nil {
			return err
		}
	}
	m3, err := enqueue("mail", "mail-3", d)
	if err != nil {
		return err
	}
	first := []string{<-g.started, <-g.started}
	slices.Sort(first) // the two start on two workers, in either order
	fmt.Fprintf(out, "  started %s; mail-3 %s\n", strings.Join(first, " and "), status(m3))
	clk.Advance(time.Minute)
	fmt.Fprintf(out, "  a minute later, started %s\n", <-g.started)
	for _, n := range []string{"mail-1", "mail-2", "mail-3"} {
		close(g.gates[n])
	}

	if err := priority(ctx, out); err != nil {
		return err
	}
	return refusals(ctx, out)
}

// priority runs one worker, holds it busy, enqueues three jobs with
// different priorities and prints the order they run in.
func priority(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "priority:")
	busy := make(chan struct{})
	holding := make(chan struct{})
	ran := make(chan string, 4)
	c := memory.New(memory.WithWorkers(1))
	err := c.Register(workflow.Workflow{
		Name: "task",
		Steps: []workflow.Step{{Name: "do", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if string(in) == "blocker" {
				close(holding)
				<-busy
				return nil, nil
			}
			ran <- string(in)
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "task", Payload: []byte("blocker")}); err != nil {
		return err
	}
	<-holding // the only worker is busy, so the next jobs wait in the queue
	// snippet:priority
	for _, j := range []workflow.Job{
		{Workflow: "task", Payload: []byte("backfill"), Priority: -10},
		{Workflow: "task", Payload: []byte("normal")}, // Priority 0 is the default
		{Workflow: "task", Payload: []byte("password-reset"), Priority: 10},
	} {
		if _, err := c.Enqueue(ctx, j); err != nil {
			return err
		}
	}
	// snippet:end
	close(busy)
	fmt.Fprintf(out, "  %s, %s, %s\n", <-ran, <-ran, <-ran)
	return nil
}

// refusals shows the key form each driver cannot evaluate: the memory
// driver runs Go functions only and refuses an expression at Start; the
// Hatchet driver evaluates CEL only and refuses a Go function at Register
// (it connects only at Start, so Register runs without a server).
func refusals(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "refusals:")
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	// snippet:key-forms
	byExpr := workflow.Workflow{
		Name:        "report",
		Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
		Steps:       []workflow.Step{{Name: "build", Run: noop}},
	}
	byFunc := byExpr
	byFunc.Concurrency = &workflow.Concurrency{
		Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }),
		MaxRuns: 1,
	}

	m := memory.New()
	if err := m.Register(byExpr); err != nil { // accepted here; Start validates the key
		return err
	}
	err := m.Start(ctx)
	fmt.Fprintln(out, "  memory, KeyExpr:", err)
	m.Stop()

	err = hatchet.New(hatchet.Config{}).Register(byFunc)
	fmt.Fprintln(out, "  hatchet, KeyFunc:", err, "| unsupported:", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end
	return nil
}
