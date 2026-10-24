package workflow

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"
)

// Drainer stops a client gracefully, as before a deploy. A client
// implements it when it declares CapDrain.
//
// Drain stops claiming new step attempts at once, waits until the running
// attempts return or ctx ends, then cancels what is left and shuts the
// client down exactly as cancelling the Start ctx would. Unfinished runs
// stay queued on a durable driver, so another client resumes them, and
// become StatusCancelled on an in-memory one. It returns nil when every
// running attempt finished, and ctx's error when it had to cut some off.
// Calling it again, or after the Start ctx ended, waits for the same
// shutdown. Before Start it returns ErrNotStarted.
//
// Done is closed once the client has fully stopped: after Drain, after
// the Start ctx was cancelled, or after a driver's stop-when-idle option
// fired. It is the one way a process waits for its client to finish.
type Drainer interface {
	Drain(ctx context.Context) error
	Done() <-chan struct{}
}

// DrainDisposition says what became of an attempt a drain cut off. It is
// a closed set, so a caller can switch on it.
type DrainDisposition string

const (
	// DrainRequeued: the attempt was handed back to a durable store as
	// ready and was not charged an attempt (CapShutdownRequeue), so
	// another client runs it at once.
	DrainRequeued DrainDisposition = "requeued"
	// DrainCancelled: nothing outlives the client (an in-memory driver),
	// so the run became StatusCancelled.
	DrainCancelled DrainDisposition = "cancelled"
	// DrainEngineOwned: the engine, not this client, decides what happens
	// to the cut-off attempt (a Hatchet worker stopping mid-task: the
	// engine reclaims it after its own timeout). The driver cannot say
	// more without lying.
	DrainEngineOwned DrainDisposition = "engine"
)

// DrainCut is one step attempt that was still running when a drain ran
// out of time. Attempt is the number the attempt had (StepInfo.Attempt);
// on a driver that requeues, the same number runs again, because the cut
// attempt was not charged.
type DrainCut struct {
	RunID       RunID
	Workflow    string
	Step        string
	Attempt     int
	Disposition DrainDisposition
}

// DrainReport is what a drain did, for the log line a deploy script
// writes. FinishedSteps counts attempts that returned after the drain began
// and before its deadline; FinishedRuns counts runs that reached a terminal
// status from the drain's start until the client stopped, except runs with
// a cut-off attempt (a driver that cannot see runs end leaves it 0, see
// its doc); Cut lists the attempts still running at the deadline. A drain
// that waited for everything has an empty Cut.
type DrainReport struct {
	FinishedRuns  int
	FinishedSteps int
	Cut           []DrainCut
}

// Clean reports whether the drain cut nothing off.
func (r DrainReport) Clean() bool { return len(r.Cut) == 0 }

// CutRuns is the number of distinct runs with a cut-off attempt (one run
// can have several attempts running at once).
func (r DrainReport) CutRuns() int {
	seen := make(map[RunID]struct{}, len(r.Cut))
	for _, c := range r.Cut {
		seen[c.RunID] = struct{}{}
	}
	return len(seen)
}

// ReportingDrainer is a Drainer that also says what it cut off. A client
// implements it when it declares CapDrainReport. DrainWithReport behaves
// exactly as Drain (same shutdown, same errors) and additionally returns
// the report. Called again, or after the client stopped, it returns the
// same report as the first call: the report describes one drain.
type ReportingDrainer interface {
	Drainer
	DrainWithReport(ctx context.Context) (DrainReport, error)
}

// DrainOutcome is the one decision a drain makes about its result: given
// what was still running at the deadline and why the deadline ended, did
// the drain succeed? It is nil when nothing was cut off, even if ctx also
// ended (the work finished in the same instant), and ctxErr otherwise.
// Every driver's Drain returns this, so they cannot disagree.
func DrainOutcome(report DrainReport, ctxErr error) error {
	if report.Clean() {
		return nil
	}
	return ctxErr
}

// DrainContext returns ctx limited to a grace period: the running
// attempts get that long to finish, then the drain cuts the rest off. A
// grace of zero or less means no waiting at all (the returned ctx is
// already ended), never "wait forever": a shutdown that can hang is the
// bug a grace period exists to prevent. A tighter deadline already on ctx
// wins. The caller must call the returned cancel.
func DrainContext(ctx context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	if grace <= 0 {
		c, cancel := context.WithCancel(ctx)
		cancel()
		return c, cancel
	}
	return context.WithTimeout(ctx, grace)
}

// DrainTimeout drains d, giving running attempts at most grace to finish
// (see DrainContext), so a caller does not hand-roll context.WithTimeout.
// It returns what d.Drain returns.
func DrainTimeout(ctx context.Context, d Drainer, grace time.Duration) error {
	dctx, cancel := DrainContext(ctx, grace)
	defer cancel()
	return d.Drain(dctx)
}

// DrainTimeoutReport is DrainTimeout that also returns the report. When d
// does not implement ReportingDrainer it still drains and returns an empty
// report with detailed false: the caller learns the report is unknown, not
// that nothing was cut off.
func DrainTimeoutReport(ctx context.Context, d Drainer, grace time.Duration) (report DrainReport, detailed bool, err error) {
	dctx, cancel := DrainContext(ctx, grace)
	defer cancel()
	if rd, ok := d.(ReportingDrainer); ok {
		report, err = rd.DrainWithReport(dctx)
		return report, true, err
	}
	return DrainReport{}, false, d.Drain(dctx)
}

// ShutdownHook adapts a Drainer to the one shape every phased shutdown
// manager accepts: a func(context.Context) error to register in a phase.
// The hook drains d within grace, or sooner if the phase's own ctx ends
// first, and returns Drain's error (the ctx error when attempts were cut
// off). It is a plain func so any manager (ubgo/shutdown, errgroup-style
// stoppers, signal.NotifyContext loops) can call it; this package imports
// none of them. Register it in the phase that runs before the database and
// the HTTP listener close, since running attempts still use both.
func ShutdownHook(d Drainer, grace time.Duration) func(context.Context) error {
	return func(ctx context.Context) error { return DrainTimeout(ctx, d, grace) }
}

// ShutdownHookReport is ShutdownHook that hands the report to onReport
// (when non-nil) before returning, so the phase can log what was cut off.
// onReport also runs when the report is not detailed (see
// DrainTimeoutReport), with a zero report and detailed false.
func ShutdownHookReport(d Drainer, grace time.Duration, onReport func(report DrainReport, detailed bool)) func(context.Context) error {
	return func(ctx context.Context) error {
		report, detailed, err := DrainTimeoutReport(ctx, d, grace)
		if onReport != nil {
			onReport(report, detailed)
		}
		return err
	}
}

// DrainTracker is the bookkeeping every driver's drain report needs, as a
// mechanism a driver embeds: it knows which attempts are running, counts
// what finishes once a drain began, and fixes the report in two steps. The
// zero value is ready to use and safe for concurrent use.
//
// Freeze runs when the drain's deadline ends, before the client's context
// is cancelled: the attempts still running are the cuts, and the steps
// that ended before are the finished steps. Seal runs once the client has
// stopped: it adds the runs that reached a terminal status meanwhile
// (a run's last result is often recorded just after its attempt returns,
// so counting at Freeze would miss it), minus any run with a cut-off
// attempt. Invariants: Begin's returned func may be called more than once
// and only the first call counts; Freeze and Seal are idempotent and
// their first results are the ones every later call returns; nothing is
// counted as a finished step after Freeze, so attempts the shutdown
// itself ends never look like finished work.
type DrainTracker struct {
	mu       sync.Mutex
	next     uint64
	running  map[uint64]DrainCut
	draining bool
	frozen   bool
	sealed   bool
	runs     map[RunID]struct{}
	steps    int
	report   DrainReport
}

// Begin registers a running attempt and returns the func that ends it.
func (t *DrainTracker) Begin(a DrainCut) (end func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running == nil {
		t.running = map[uint64]DrainCut{}
	}
	t.next++
	id := t.next
	t.running[id] = a
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if _, ok := t.running[id]; !ok {
			return
		}
		delete(t.running, id)
		if t.draining && !t.frozen {
			t.steps++
		}
	}
}

// StartDrain marks that a drain began, so finishes are counted from now.
func (t *DrainTracker) StartDrain() {
	t.mu.Lock()
	t.draining = true
	t.mu.Unlock()
}

// RunFinished notes that run id reached a terminal status. Before
// StartDrain and after Seal it does nothing.
func (t *DrainTracker) RunFinished(id RunID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.draining || t.sealed {
		return
	}
	if t.runs == nil {
		t.runs = map[RunID]struct{}{}
	}
	t.runs[id] = struct{}{}
}

// Freeze fixes the cuts and the finished steps: every attempt still
// running is a cut, with disposition. Call it when the drain's deadline
// ended and before the client's context is cancelled. The returned
// report has no FinishedRuns yet; Seal adds them.
func (t *DrainTracker) Freeze(disposition DrainDisposition) DrainReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.frozen {
		return t.report
	}
	t.frozen = true
	cut := make([]DrainCut, 0, len(t.running))
	for _, a := range t.running {
		a.Disposition = disposition
		cut = append(cut, a)
	}
	slices.SortFunc(cut, func(a, b DrainCut) int {
		return cmp.Or(cmp.Compare(a.RunID, b.RunID), cmp.Compare(a.Step, b.Step), cmp.Compare(a.Attempt, b.Attempt))
	})
	t.report = DrainReport{FinishedSteps: t.steps, Cut: cut}
	return t.report
}

// Seal completes the report once the client has stopped; call it after
// Freeze. A run with a cut-off attempt is not counted as finished, even
// if the shutdown ended it.
func (t *DrainTracker) Seal() DrainReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sealed || !t.frozen {
		return t.report
	}
	t.sealed = true
	for _, c := range t.report.Cut {
		delete(t.runs, c.RunID)
	}
	t.report.FinishedRuns = len(t.runs)
	return t.report
}
