package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/clock"
)

// pollEvery is the poll interval in these tests; the fake clock makes its
// length irrelevant to the run time.
const pollEvery = time.Second

// noHook hides the memory client's OnRunFinished: embedding the interface,
// not the concrete type, leaves only the Client methods, so the default
// sources fall back to the poller alone.
type noHook struct{ workflow.Client }

// waitRunsEnded waits, in real time, until every job of the batch has a
// RunRef whose run is terminal on the client.
func (e *env) waitRunsEnded(id batch.ID) {
	e.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		st, err := e.store.Get(e.ctx, id)
		if err != nil {
			e.t.Fatal(err)
		}
		done := true
		for _, j := range st.Jobs {
			s, err := e.client.Status(e.ctx, j.Ref)
			done = done && err == nil && s.IsTerminal()
		}
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
	e.t.Fatal("the batch's runs never ended")
}

// waitPollTimer waits until the poller has armed its timer on the fake
// clock, failing instead of hanging when no poller is running.
func (e *env) waitPollTimer(fake *clock.Fake) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, waitTimeout)
	defer cancel()
	if err := fake.BlockUntil(ctx, 1); err != nil {
		e.t.Fatalf("no poller armed a timer: %v", err)
	}
}

// TestPollAloneCompletesBatch: on a client with no run-finished hook the
// default poller is the only source, and nothing is recorded until its
// timer fires on the fake clock.
func TestPollAloneCompletesBatch(t *testing.T) {
	fake := clock.NewFake(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC))
	e := newEnvWith(t, func(c *counting) workflow.Client { return noHook{c} }, batch.WithClock(fake), batch.WithStatusPoll(pollEvery))
	id := e.dispatch(false, wfOK, wfOK, wfFail)
	e.waitRunsEnded(id)
	if p, _ := e.mgr.Progress(e.ctx, id); p.Pending != 3 {
		t.Fatalf("recorded before any poll, so something other than the poller reported: %+v", p)
	}
	e.waitPollTimer(fake)
	fake.Advance(pollEvery)
	p := e.waitFinally()
	if p.Succeeded != 2 || p.Failed != 1 || !p.Finished {
		t.Errorf("Finally payload = %+v", p)
	}
	e.wantCounts(0, 1, 1)
}

// lateHook is a Source that holds the run-finished hook's events instead of
// delivering them, so a test can replay them after the poller has already
// reported the same jobs.
type lateHook struct {
	n      workflow.RunNotifier
	events chan workflow.RunEvent
}

func (h lateHook) Attach(_ context.Context, _ *batch.Manager) error {
	return h.n.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		if ev.Labels[batch.LabelBatch] != "" {
			h.events <- ev
		}
	})
}

// TestHookAndPollReportTheSameJobs: the poller records every job first,
// then the hook delivers the same ends; no job is counted twice and no
// callback is enqueued twice.
func TestHookAndPollReportTheSameJobs(t *testing.T) {
	fake := clock.NewFake(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC))
	hook := lateHook{events: make(chan workflow.RunEvent, 8)}
	e := newEnvWith(t, func(c *counting) workflow.Client { hook.n = c; return c },
		batch.WithClock(fake), batch.WithCompletionSource(&hook, batch.PollSource(pollEvery)))
	id := e.dispatch(true, wfOK, wfOK, wfFail)
	var held []workflow.RunEvent
	for range 3 {
		select {
		case ev := <-hook.events:
			held = append(held, ev)
		case <-time.After(waitTimeout):
			t.Fatal("the hook never saw the batch's runs end")
		}
	}
	e.waitPollTimer(fake)
	fake.Advance(pollEvery)
	e.waitFinally()
	for _, ev := range held {
		if err := e.mgr.RunFinished(e.ctx, ev.Labels, ev.Status); err != nil {
			t.Fatal(err)
		}
	}
	if p, _ := e.mgr.Progress(e.ctx, id); p.Succeeded != 2 || p.Failed != 1 || p.Total != 3 {
		t.Errorf("Progress after both sources = %+v", p)
	}
	e.wantCounts(0, 1, 1)
}

// lossyHook is a RunNotifier that does not declare CapRunFinished and
// loses every event, the worst a best-effort hook can do. The default
// sources must add the poller, which alone completes the batch.
type lossyHook struct{ *counting }

func (l lossyHook) Capabilities() workflow.Caps {
	return l.counting.Capabilities() &^ workflow.CapRunFinished
}

func (lossyHook) OnRunFinished(workflow.RunFinishedFunc) error { return nil }

func TestBestEffortHookGetsPollSafetyNet(t *testing.T) {
	fake := clock.NewFake(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC))
	e := newEnvWith(t, func(c *counting) workflow.Client { return lossyHook{c} }, batch.WithClock(fake), batch.WithStatusPoll(pollEvery))
	id := e.dispatch(false, wfOK)
	e.waitRunsEnded(id)
	e.waitPollTimer(fake)
	fake.Advance(pollEvery)
	if p := e.waitFinally(); p.Succeeded != 1 {
		t.Errorf("Finally payload = %+v", p)
	}
	e.wantCounts(1, 0, 1)
}
