// Command step-slots shows heavy steps holding several worker slots
// (Step.Slots, CapSlotCost) on the in-memory driver: a render that holds
// three of a four-slot pool runs beside one webhook at most, a step that
// needs more slots than the whole pool is refused at Register, and the
// shared SlotPool a driver of your own uses is driven by hand.
//
// It is the program behind docs/jobs/slots.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// poolSize is the client's worker pool: four slots, so a three-slot render
// leaves room for exactly one ordinary attempt beside it.
const poolSize = 4

// renderSlots is what one render attempt costs: three ordinary attempts'
// worth of memory and CPU.
const renderSlots = 3

// webhooks is how many light jobs are enqueued while the render runs.
const webhooks = 3

// awaitPoll is how often the program polls a run's status while waiting
// for it to end; short, because every run here ends in milliseconds.
const awaitPoll = 2 * time.Millisecond

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "step-slots:", err)
		os.Exit(1)
	}
}

// meter counts the slots the running handlers hold, by their step's cost,
// and remembers the highest count seen. The driver's own pool is not
// exported; the handlers report to the meter instead.
type meter struct {
	inUse, peak atomic.Int64
}

// enter adds a handler's slots to the count and raises the peak if the
// count passed it.
func (m *meter) enter(slots int) {
	now := m.inUse.Add(int64(slots))
	for {
		old := m.peak.Load()
		if now <= old || m.peak.CompareAndSwap(old, now) {
			return
		}
	}
}

// leave takes a handler's slots off the count.
func (m *meter) leave(slots int) { m.inUse.Add(-int64(slots)) }

// run is main without the process exit, so the test drives the same code
// and checks every line it writes to w.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var m meter
	var rendering atomic.Bool // true while the render handler runs
	var beside atomic.Int64   // webhooks that started while it ran
	renderStarted := make(chan struct{})
	webhookStarted := make(chan struct{}, webhooks)
	renderDone := make(chan struct{}) // closed when the render handler is about to return

	// encode is the render step: it holds its slots until one webhook has
	// started beside it.
	encode := func(context.Context, []byte) ([]byte, error) {
		m.enter(renderSlots)
		defer m.leave(renderSlots)
		rendering.Store(true)
		close(renderStarted)
		<-webhookStarted // hold the slots until one webhook runs beside us
		rendering.Store(false)
		close(renderDone)
		return nil, nil
	}
	// deliver is the webhook step: it notes whether the render was running
	// and keeps its slot until the render is done.
	deliver := func(context.Context, []byte) ([]byte, error) {
		m.enter(1)
		defer m.leave(1)
		if rendering.Load() {
			beside.Add(1)
		}
		webhookStarted <- struct{}{}
		<-renderDone // keep this slot until the render is done, so the count is exact
		return nil, nil
	}

	// snippet:cost
	c := memory.New(memory.WithWorkers(poolSize))
	render := workflow.Workflow{Name: "render", Steps: []workflow.Step{
		{Name: "encode", Slots: renderSlots, Run: encode}, // holds 3 of the 4 slots while it runs
	}}
	webhook := workflow.Workflow{Name: "webhook", Steps: []workflow.Step{
		{Name: "deliver", Run: deliver}, // zero means one slot
	}}
	for _, wf := range []workflow.Workflow{render, webhook} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	// snippet:end
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	fmt.Fprintln(w, "memory declares CapSlotCost:", c.Capabilities().Has(workflow.CapSlotCost))

	refs := []workflow.RunRef{}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "render"})
	if err != nil {
		return err
	}
	refs = append(refs, ref)
	<-renderStarted
	for range webhooks {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "webhook"})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	for _, ref := range refs {
		if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: awaitPoll}); err != nil {
			return err
		}
	}
	fmt.Fprintf(w, "webhooks that ran beside the render: %d of %d\n", beside.Load(), webhooks)
	fmt.Fprintf(w, "most slots held at once: %d of %d\n", m.peak.Load(), poolSize)

	// snippet:refuse
	huge := workflow.Workflow{Name: "transcode-8k", Steps: []workflow.Step{
		{Name: "transcode", Slots: poolSize + 1, Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
	}}
	err = c.Register(huge)
	fmt.Fprintln(w, "register a 5-slot step on a 4-slot pool:", errors.Is(err, workflow.ErrInvalid))
	fmt.Fprintln(w, " ", err)
	// snippet:end

	// snippet:pool
	pool := workflow.NewSlotPool(poolSize)
	cost := workflow.StepSlots(render.Steps[0]) // 3; a step with Slots 0 costs 1
	if err := pool.Acquire(ctx, cost); err != nil {
		return err
	}
	fmt.Fprintf(w, "pool: %d of %d in use after one render\n", pool.InUse(), pool.Size())
	short, stop := context.WithTimeout(ctx, 10*time.Millisecond) // long enough to show it waits, short for the demo
	defer stop()
	err = pool.Acquire(short, cost) // a second render must wait for the first
	fmt.Fprintln(w, "pool: a second render waits until its context ends:", errors.Is(err, context.DeadlineExceeded))
	pool.Release(cost)
	fmt.Fprintf(w, "pool: %d in use after Release\n", pool.InUse())
	fmt.Fprintln(w, "pool: asking for more than the pool:", errors.Is(pool.Acquire(ctx, poolSize+1), workflow.ErrInvalid))
	// snippet:end

	return nil
}
