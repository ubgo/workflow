package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
)

// DefaultPollInterval is how often the status poller checks a batch's
// unfinished jobs. Each tick costs one Client.Status call per unfinished
// job, so it trades engine load against callback latency; five seconds
// keeps a 1,000-job batch at 200 calls a second at worst, while a Then or
// Finally that runs a few seconds late is acceptable for work batched in
// the background. Change it with WithStatusPoll.
const DefaultPollInterval = 5 * time.Second

// Source tells a Manager that batch jobs ended. It is the seam between the
// batch and however a driver reports run ends: the run-finished hook, a
// status poller, or a native engine feed. A Source reports through
// Manager.RunFinished, which ignores repeats, so several Sources may report
// the same job: the first terminal status recorded wins.
type Source interface {
	// Attach connects the source to m. It is called once, by
	// Manager.Attach, before the client starts. A source that works in the
	// background starts its goroutine here and stops it when ctx is
	// cancelled. An error means the source cannot work with this client.
	Attach(ctx context.Context, m *Manager) error
}

// HookSource reports run ends from the client's run-finished hook
// (workflow.RunNotifier). Its Attach fails with an error wrapping
// workflow.ErrUnsupported when the client is not a RunNotifier.
func HookSource() Source { return hookSource{} }

type hookSource struct{}

func (hookSource) Attach(_ context.Context, m *Manager) error {
	n, ok := m.client.(workflow.RunNotifier)
	if !ok {
		return &workflow.UnsupportedError{Field: "batch.HookSource", Reason: "the client does not implement workflow.RunNotifier"}
	}
	return n.OnRunFinished(m.observe)
}

// PollSource reports run ends by calling Client.Status for every
// unfinished job of every open batch each interval, on the Manager's
// clock, and retries the batch's callbacks that failed to enqueue. It
// works on every driver, so it is the safety net when hook delivery is best-effort. A job's RunRef is saved once Enqueue returns, so
// a job that ended before that is found on the next tick. A Status error
// for one job (an engine that pruned the run returns workflow.ErrNotFound)
// is logged and retried on the next tick, never guessed into a status. Its
// Attach fails with workflow.ErrInvalid for an interval that is not
// positive.
func PollSource(interval time.Duration) Source { return pollSource{interval: interval} }

type pollSource struct{ interval time.Duration }

func (p pollSource) Attach(ctx context.Context, m *Manager) error {
	if p.interval <= 0 {
		return fmt.Errorf("%w: batch.PollSource interval must be positive, got %s", workflow.ErrInvalid, p.interval)
	}
	go p.loop(ctx, m)
	return nil
}

func (p pollSource) loop(ctx context.Context, m *Manager) {
	for {
		t := m.clk.NewTimer(p.interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
		m.poll(ctx)
	}
}

// poll is one tick of PollSource.
func (m *Manager) poll(ctx context.Context) {
	open, err := m.store.Unfinished(ctx)
	if err != nil {
		m.log.ErrorContext(ctx, "batch: poll: list unfinished batches", "error", err)
		return
	}
	for _, st := range open {
		for i, j := range st.Jobs {
			if j.Ref.IsZero() || j.Status.IsTerminal() {
				continue
			}
			status, err := m.client.Status(ctx, j.Ref)
			if err != nil {
				m.log.ErrorContext(ctx, "batch: poll: run status", "batch", string(st.ID), "job", i, "error", err)
				continue
			}
			if err := m.record(ctx, st.ID, i, j.Attempt, status); err != nil {
				m.log.ErrorContext(ctx, "batch: poll: record job end", "batch", string(st.ID), "job", i, "error", err)
			}
		}
		// A batch whose jobs all ended may still hold a callback that
		// failed to enqueue; the claim inside retryBatch re-checks it, so
		// a callback a record above just claimed is not tried twice.
		if hasDue(st, m.clk.Now()) {
			if err := m.retryBatch(ctx, st.ID); err != nil {
				m.log.ErrorContext(ctx, "batch: poll: retry callbacks", "batch", string(st.ID), "error", err)
			}
		}
	}
	if err := m.ResumeDispatches(ctx); err != nil {
		m.log.ErrorContext(ctx, "batch: poll: resume dispatches", "error", err)
	}
}

// defaultSources picks the sources by what the client can do:
//
//   - CapRunFinished: the hook alone, because its delivery is guaranteed;
//   - a RunNotifier without CapRunFinished: the hook for speed plus the
//     poller as the safety net for events it loses;
//   - no RunNotifier: the poller alone.
func (m *Manager) defaultSources() []Source {
	_, notifier := m.client.(workflow.RunNotifier)
	switch {
	case notifier && m.client.Capabilities().Has(workflow.CapRunFinished):
		return []Source{HookSource()}
	case notifier:
		return []Source{HookSource(), PollSource(m.pollEvery)}
	}
	return []Source{PollSource(m.pollEvery)}
}
