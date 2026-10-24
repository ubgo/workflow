package hatchet

import (
	"context"
	"sync"
	"time"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.Drainer          = (*Client)(nil)
	_ workflow.ReportingDrainer = (*Client)(nil)
)

// inflight counts the handlers running on this client and tells Drain
// when none is left. A WaitGroup does not fit: handlers keep starting
// while Drain waits (attempts the engine assigned before the pause).
type inflight struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed while n is zero; replaced when n leaves zero
	// track records the attempts a drain finished and cut off.
	track workflow.DrainTracker
	// finished holds the runs whose attempt returned once a drain began
	// (recording): the candidates for the report's FinishedRuns, whose
	// status the engine, not this client, decides. Nil until a drain starts.
	finished map[workflow.RunID]struct{}
}

// startRecording begins collecting the runs of attempts that return.
func (f *inflight) startRecording() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finished == nil {
		f.finished = map[workflow.RunID]struct{}{}
	}
}

// noteReturned records that an attempt of run id returned, when a drain is
// collecting (a no-op before one starts, so a long-lived client holds nothing).
func (f *inflight) noteReturned(id workflow.RunID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finished != nil {
		f.finished[id] = struct{}{}
	}
}

// returnedRuns is a copy of the recorded run IDs.
func (f *inflight) returnedRuns() []workflow.RunID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workflow.RunID, 0, len(f.finished))
	for id := range f.finished {
		out = append(out, id)
	}
	return out
}

func (f *inflight) add() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == 0 {
		f.idle = make(chan struct{})
	}
	f.n++
}

func (f *inflight) done() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n--
	if f.n == 0 {
		close(f.idle)
	}
}

// wait returns a channel closed once no handler is running.
func (f *inflight) wait() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return f.idle
}

// Drain stops this client gracefully (workflow.Drainer): it pauses every
// worker Start registered, so the engine assigns them no new step
// attempt, waits until the step and on-failure handlers running here
// return or ctx ends, then stops the workers as cancelling the Start ctx
// would. Afterwards every call that needs the engine (Enqueue, Status,
// Runs, ...) returns ErrNotStarted; query runs from another client. Runs it did not finish stay on the engine (queued, or running
// until the engine reclaims an attempt that was cut off, see doc.go), and
// any other worker serving the workflow takes them. Wait and sleep steps
// do not hold it up: they are durable and resume on any worker. It
// returns nil when every handler finished and ctx's error otherwise.
func (c *Client) Drain(ctx context.Context) error {
	_, err := c.drain(ctx, false)
	return err
}

// drainRunSettle bounds how long a drain waits for the engine to record the
// terminal status of the runs whose attempts returned: the engine writes a
// run's final state a moment after the worker reports the task, so reading
// it at once would miss runs that did finish. A run still unfinished after
// this (a multi-step run whose next step the engine has yet to hand out) is
// not counted, so a drain costs at most this much more than the handlers.
const drainRunSettle = 2 * time.Second

// drainRunPoll is the pause between status reads while settling.
const drainRunPoll = 100 * time.Millisecond

// drainJoinGrace is how long a second Drain whose own ctx has ended waits
// for the first drain to publish its report.
const drainJoinGrace = 5 * time.Second

// DrainWithReport is Drain that also returns what the drain finished and
// cut off (workflow.ReportingDrainer). What the engine allows: this client
// sees only the handlers it runs, so FinishedSteps counts handlers that
// returned during the drain and each cut-off handler is listed with
// DrainEngineOwned, because whether the engine requeues it (and when) is
// the engine's decision, not something this driver can promise. A worker is
// not told when a run ends, so FinishedRuns is read back from the engine:
// the runs of the handlers that returned are looked up (for up to
// drainRunSettle) and those the engine reports terminal are counted; a run
// that only reached terminal through another worker's step is not, because
// this client never saw it. A second call, concurrent or after Done, waits
// for the first drain and returns its report and its error.
func (c *Client) DrainWithReport(ctx context.Context) (workflow.DrainReport, error) {
	return c.drain(ctx, true)
}

// drain is Drain and DrainWithReport. Only a reporting drain settles
// FinishedRuns, which reads run statuses back from the engine and can cost
// up to drainRunSettle; a plain Drain (also what worker recycling calls)
// never pays it. The first call decides: a report fetched after a plain
// Drain has FinishedRuns 0.
func (c *Client) drain(ctx context.Context, settle bool) (workflow.DrainReport, error) {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return workflow.DrainReport{}, workflow.ErrNotStarted
	}
	if c.draining {
		done := c.drainDone
		c.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			// This call's own deadline is over, but the report belongs to
			// the first drain, which ends within moments of its own ctx
			// (a shared one included): give it a short while rather than
			// return an empty report that reads as "nothing was cut".
			select {
			case <-done:
			case <-time.After(drainJoinGrace):
				return workflow.DrainReport{}, ctx.Err()
			}
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.drainReport, c.drainErr
	}
	c.draining = true
	c.drainDone = make(chan struct{})
	sdk, ids, stop := c.sdk, c.workerIDs, c.stop
	c.mu.Unlock()
	if settle {
		c.inflight.startRecording()
	}
	c.inflight.track.StartDrain()
	for _, id := range ids {
		if _, err := sdk.Workers().Pause(ctx, id); err != nil {
			c.cfg.Logger.Error("hatchet driver: drain: pause worker", "worker", id, "error", err)
		}
	}
	// Idle first: with the ctx already ended (a grace of zero) a select
	// would pick either ready case at random and call a clean drain cut.
	var ctxErr error
	select {
	case <-c.inflight.wait():
	default:
		select {
		case <-c.inflight.wait():
		case <-ctx.Done():
			ctxErr = ctx.Err()
		}
	}
	cut := c.inflight.track.Freeze(workflow.DrainEngineOwned)
	if settle {
		c.settleFinishedRuns(cut)
	}
	// Marked drained before the workers stop, so a caller woken by Done
	// already gets ErrNotStarted from every call.
	c.mu.Lock()
	c.drained = true
	c.mu.Unlock()
	stop()
	report := c.inflight.track.Seal()
	err := workflow.DrainOutcome(report, ctxErr)
	select {
	case <-c.done:
	case <-ctx.Done():
	}
	c.mu.Lock()
	c.drainReport, c.drainErr = report, err
	close(c.drainDone)
	c.mu.Unlock()
	return report, err
}

// settleFinishedRuns reads back, from the engine, which of the runs whose
// attempts returned during the drain reached a terminal status, and tells
// the tracker (workflow.DrainTracker.RunFinished). A run with a cut-off
// attempt is skipped (Seal drops it anyway). It runs before the client
// marks itself drained, because Status refuses once it has.
func (c *Client) settleFinishedRuns(cut workflow.DrainReport) {
	cutIDs := make(map[workflow.RunID]struct{}, len(cut.Cut))
	for _, x := range cut.Cut {
		cutIDs[x.RunID] = struct{}{}
	}
	var pending []workflow.RunID
	for _, id := range c.inflight.returnedRuns() {
		if _, isCut := cutIDs[id]; !isCut {
			pending = append(pending, id)
		}
	}
	// Detached from the drain's ctx: it may have ended already (that is how
	// a deadline cuts), and these are short reads, bounded by drainRunSettle.
	ctx, cancel := context.WithTimeout(context.Background(), drainRunSettle)
	defer cancel()
	for len(pending) > 0 {
		rest := pending[:0]
		for _, id := range pending {
			st, err := c.Status(ctx, workflow.RunRef{RunID: id})
			if err == nil && st.IsTerminal() {
				c.inflight.track.RunFinished(id)
				continue
			}
			rest = append(rest, id)
		}
		pending = rest
		if len(pending) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(drainRunPoll):
		}
	}
}

// Done is closed once every worker of this client has stopped: after
// Drain, or after the Start ctx was cancelled. Before Start it is open.
func (c *Client) Done() <-chan struct{} { return c.done }
