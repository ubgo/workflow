package workflowtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
)

// Queue limit conformance (QueueConfig.Limit, CapQueueLimitGlobal).
//
// The cases need Harness.NewWithQueues, because a queue's configuration is a
// driver option the suite cannot name. Each row of the matrix builds a
// client (or two, over one store) with a limited queue, parks `jobs` runs
// in a handler that holds its slot until a gate opens, and checks the three
// things a cap promises:
//
//  1. never above: the in-flight count of the queue reaches the expected
//     value and stays there while more work waits (checked over a quiet
//     window, and over the whole run by a peak counter);
//  2. never below: it reaches exactly that value, so a cap that throttles
//     harder than asked fails too;
//  3. freed: when the holders finish, the waiting runs take the slots and
//     every run succeeds.
//
// Matrix dimensions: Limit (1..3) x per-client Workers (unset or 1) x number
// of clients (1 or 2 over one store) x sibling queue (none, or an unlimited
// queue whose runs must neither be throttled by nor consume the limited
// queue's slots) x which queue a run lands on (declared, default, Job.Queue
// override, two workflows on one queue) x node crash (slots held by a dead
// client free only when its lease expires).

// limitPoolFloor is the smallest worker pool a client is assumed to have
// when QueueConfig.Workers is unset: the expected in-flight count of a row
// with unset Workers is min(Limit, jobs), which holds only if the pool is
// at least Limit. Every driver's default pool is larger than the largest
// Limit in the matrix.
const limitPoolFloor = 3

// limitEnqueueWait is the least time one Enqueue of a queue-limit row may
// take. Every row holds its in-flight slots with workers parked on a gate
// while more jobs are enqueued, and the enqueue transaction counts the
// queue's slots under the same write lock the busy workers and a peer client
// use, so on a loaded machine (race detector, a database container beside
// the run) an Enqueue can wait past the fake-clock harness's 5s Wait and
// fail with "admit ... context deadline exceeded" though nothing is wrong
// (seen on sqlstore/SQLite in the full suite). 30s is far above any healthy
// admit and well below the 60s Wait of the real-clock harness, so a genuine
// hang still fails.
const limitEnqueueWait = 30 * time.Second

// siblingJobs is how many runs of the unlimited sibling queue each row parks
// beside the limited queue's.
const siblingJobs = 2

// limitRow is one row of the queue-limit matrix.
type limitRow struct {
	name    string
	limit   int
	workers int // QueueConfig.Workers of every client; 0 = unset
	clients int // 1, or 2 over one store (needs Instance.Peer)
	jobs    int // runs enqueued on the limited queue
	sibling bool
	// route says how the runs reach the limited queue.
	route limitRoute
}

// limitRoute is how a row's runs land on the limited queue.
type limitRoute int

const (
	routeDeclared limitRoute = iota // Workflow.Queue
	routeDefault                    // no queue named: DefaultQueue is the limited one
	routeOverride                   // Workflow.Queue is the sibling; Job.Queue moves it onto the limited queue
	routeTwoFlows                   // two workflows share the limited queue; the cap counts the queue, not the workflow
)

// expected is the in-flight count a row must settle at: the tightest of the
// cluster Limit, the runs available and the clients' own worker pools.
func (r limitRow) expected() int {
	pool := r.clients * limitPoolFloor // at least; never binds below the Limit
	if r.workers > 0 {
		pool = r.clients * r.workers
	}
	return min(r.limit, r.jobs, pool)
}

var limitMatrix = []limitRow{
	{name: "limit 1, one client", limit: 1, clients: 1, jobs: 4},
	{name: "limit 2, one client", limit: 2, clients: 1, jobs: 5},
	{name: "limit 3, fewer runs than the limit", limit: 3, clients: 1, jobs: 2},
	{name: "limit 3, Workers 1 binds first", limit: 3, workers: 1, clients: 1, jobs: 5},
	{name: "limit 1, two clients", limit: 1, clients: 2, jobs: 4},
	{name: "limit 2, two clients", limit: 2, clients: 2, jobs: 6},
	{name: "limit 2, Workers 1 per client, two clients", limit: 2, workers: 1, clients: 2, jobs: 5},
	{name: "limit 3, Workers 1 per client binds at 2", limit: 3, workers: 1, clients: 2, jobs: 5},
	{name: "limit 2 beside an unlimited queue", limit: 2, clients: 1, jobs: 5, sibling: true},
	{name: "limit 1 beside an unlimited queue, two clients", limit: 1, clients: 2, jobs: 4, sibling: true},
	{name: "limit on the default queue", limit: 2, clients: 1, jobs: 4, route: routeDefault},
	{name: "limit counts Job.Queue overrides", limit: 2, clients: 1, jobs: 5, sibling: true, route: routeOverride},
	{name: "limit counts the queue, not the workflow", limit: 2, clients: 1, jobs: 6, route: routeTwoFlows},
}

var queueLimitCases = func() []testCase {
	var out []testCase
	for _, row := range limitMatrix {
		out = append(out, testCase{"Limit: " + row.name, func(e *env) { e.runLimitRow(row) }})
	}
	return append(out, testCase{"a crashed client's slots free when its lease expires, not before", func(e *env) { e.runLimitCrash() }})
}()

// peakGauge is an in-flight counter that remembers its high-water mark.
type peakGauge struct {
	now, peak, started atomic.Int64
}

// enter registers an attempt and returns the func that releases its slot.
func (g *peakGauge) enter() (leave func()) {
	n := g.now.Add(1)
	g.started.Add(1)
	for p := g.peak.Load(); n > p && !g.peak.CompareAndSwap(p, n); p = g.peak.Load() {
	}
	return func() { g.now.Add(-1) }
}

// holdUntil is a handler that holds its slot in g until gate opens.
func (g *peakGauge) holdUntil(gt *gate) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		leave := g.enter()
		defer leave()
		return in, gt.wait(ctx)
	}
}

// startPeer registers wfs on a second client over the same store and starts
// it with a context cancelled when the subtest ends.
func (e *env) startPeer(c workflow.Client, wfs ...workflow.Workflow) {
	e.t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			e.t.Fatalf("peer Register(%s): %v", wf.Name, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		e.t.Fatalf("peer Start: %v", err)
	}
}

func (e *env) runLimitRow(r limitRow) {
	e.t.Helper()
	e.need(workflow.CapQueue | workflow.CapQueueLimitGlobal)
	if r.route == routeOverride {
		e.need(workflow.CapQueueOverride)
	}
	limited := queueA
	if r.route == routeDefault {
		limited = workflow.DefaultQueue
	}
	qs := []workflow.QueueConfig{{Name: limited, Workers: r.workers, Limit: r.limit}}
	if r.sibling {
		qs = append(qs, workflow.QueueConfig{Name: queueB})
	}
	e.withQueues(qs...)
	if r.clients > 1 && e.peer == nil {
		e.t.Skip("Instance.Peer is nil: this driver has no second node over one store")
	}

	var main, side peakGauge
	hold, holdSide := newGate(e.t), newGate(e.t)
	wfMain := oneStep(e.name("main"), main.holdUntil(hold))
	wfs := []workflow.Workflow{wfMain}
	switch r.route {
	case routeDeclared:
		wfMain.Queue = limited
	case routeOverride:
		wfMain.Queue = queueB // declared on the unlimited queue; each Job moves it
	case routeTwoFlows:
		wfMain.Queue = limited
	}
	wfs[0] = wfMain
	var wfOther workflow.Workflow
	if r.route == routeTwoFlows {
		wfOther = oneStep(e.name("other"), main.holdUntil(hold))
		wfOther.Queue = limited
		wfs = append(wfs, wfOther)
	}
	var wfSide workflow.Workflow
	if r.sibling {
		wfSide = oneStep(e.name("side"), side.holdUntil(holdSide))
		wfSide.Queue = queueB
		wfs = append(wfs, wfSide)
	}
	e.up(wfs...)
	if r.clients > 1 {
		e.startPeer(e.peer(e.t), wfs...)
	}

	var refs []workflow.RunRef
	for i := range r.jobs {
		job := workflow.Job{Workflow: wfMain.Name}
		if r.route == routeOverride {
			job.Queue = limited
		}
		if r.route == routeTwoFlows && i%2 == 1 {
			job.Workflow = wfOther.Name
		}
		refs = append(refs, e.enqueueWithin(job, limitEnqueueWait))
	}

	want := int64(r.expected())
	e.eventually(fmt.Sprintf("%d attempts of the limited queue in flight", want), 0, func() bool { return main.now.Load() >= want })
	e.quiet()
	e.quiet()
	if got := main.now.Load(); got != want {
		e.t.Fatalf("%d attempts in flight with the slots held, want exactly %d (limit %d, workers %d, clients %d, jobs %d)", got, want, r.limit, r.workers, r.clients, r.jobs)
	}
	if got := main.started.Load(); got != want {
		e.t.Fatalf("%d attempts started while %d slots were held, want %d", got, want, want)
	}

	var sideRefs []workflow.RunRef
	if r.sibling {
		// The limited queue is full and holding: the sibling must still run
		// (its own queue is not throttled), and must not take or give slots.
		for range siblingJobs {
			sideRefs = append(sideRefs, e.enqueue(workflow.Job{Workflow: wfSide.Name}))
		}
		e.eventually("the unlimited sibling queue to run beside a full limited one", 0, func() bool { return side.now.Load() == siblingJobs })
		e.quiet()
		if got := main.now.Load(); got != want {
			e.t.Fatalf("limited queue in flight = %d after the sibling started, want %d (the sibling must not consume or release its slots)", got, want)
		}
	}

	hold.open()
	holdSide.open()
	for _, ref := range append(refs, sideRefs...) {
		e.waitStatus(ref, workflow.StatusSucceeded, 0)
	}
	if got := main.started.Load(); got != int64(r.jobs) {
		e.t.Fatalf("%d attempts started for %d runs, want one each", got, r.jobs)
	}
	if got := main.peak.Load(); got != want {
		e.t.Fatalf("peak in flight = %d over the whole run, want %d (never above the cap, and it must reach it)", got, want)
	}
}

// runLimitCrash: Limit 2, two attempts hold the slots, their client dies
// without releasing; a replacement must not start anything while the dead
// leases are live, and must run everything once they expire, never above 2.
func (e *env) runLimitCrash() {
	e.t.Helper()
	e.need(workflow.CapQueue | workflow.CapQueueLimitGlobal | workflow.CapDurable)
	e.withQueues(workflow.QueueConfig{Name: queueA, Limit: 2})
	if e.crash == nil {
		e.t.Skip("Instance.Crash is nil")
	}
	if e.clk == nil {
		e.t.Skip("lease expiry needs a fake clock")
	}
	const jobs = 5
	var execs atomic.Int64
	var live peakGauge
	zombie, release := newGate(e.t), newGate(e.t)
	wf := oneStep(e.name("w"), func(ctx context.Context, in []byte) ([]byte, error) {
		if execs.Add(1) <= 2 {
			// The doomed worker's attempts: stuck forever, as if their
			// process were gone. Only the subtest's end frees them.
			_ = zombie.wait(context.Background())
			return nil, errBoom
		}
		leave := live.enter()
		defer leave()
		return in, release.wait(ctx)
	})
	wf.Queue = queueA
	// A crashed attempt may spend an attempt; one retry keeps the case about
	// slots, not about accounting.
	wf.Retry = workflow.RetryPolicy{Max: 1}
	e.up(wf)
	var refs []workflow.RunRef
	for range jobs {
		refs = append(refs, e.enqueue(workflow.Job{Workflow: wf.Name}))
	}
	e.eventually("both slots held", 0, func() bool { return execs.Load() == 2 })
	e.quiet()
	if got := execs.Load(); got != 2 {
		e.t.Fatalf("%d attempts started with Limit 2, want 2", got)
	}

	e.doCrash()
	e.quiet()
	e.quiet()
	if got := execs.Load(); got != 2 {
		e.t.Fatalf("%d attempts started after the crash with the dead leases still live, want 2 (a slot is freed by lease expiry only)", got)
	}

	// A lease is measured on whatever clock the driver chooses (the sqlstore
	// default is wall time), so move the scheduling clock once, far past any
	// lease, and otherwise wait: never keep advancing, or a live attempt
	// would lose its own lease on a driver that reads this clock.
	e.clk.Advance(e.tm.unit)
	e.eventuallyWithin(4*e.tm.wait, "both freed slots taken", 0, func() bool { return live.now.Load() == 2 })
	e.quiet()
	if got := live.now.Load(); got != 2 {
		e.t.Fatalf("%d attempts in flight after the dead leases expired, want 2 (the cap, not the pool)", got)
	}
	release.open()
	for _, ref := range refs {
		e.waitStatusWithin(4*e.tm.wait, ref, workflow.StatusSucceeded, 0)
	}
	if got := live.peak.Load(); got != 2 {
		e.t.Fatalf("peak live attempts = %d, want 2", got)
	}
}
