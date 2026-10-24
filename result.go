package workflow

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// ResultReader returns a succeeded run's result. A client implements it
// when it declares CapResult.
//
// The result is RunOutput of the run's steps: kept with the run until the
// run is deleted or pruned by retention. A run that is not
// StatusSucceeded returns ErrWrongState (ErrNotFound when unknown), so a
// caller can tell "not done" from "done with an empty result".
type ResultReader interface {
	Result(ctx context.Context, ref RunRef) ([]byte, error)
}

// Sinks returns the names of wf's final steps: the steps no other step
// lists in After, in declaration order.
func Sinks(wf Workflow) []string {
	parent := make(map[string]bool, len(wf.Steps))
	for _, s := range wf.Steps {
		for _, p := range s.After {
			parent[p] = true
		}
	}
	var out []string
	for _, s := range wf.Steps {
		if !parent[s.Name] {
			out = append(out, s.Name)
		}
	}
	return out
}

// RunOutput is the result of a succeeded run of wf whose step outputs are
// outs: with one final step (Sinks), its output unchanged; with several,
// a JSON object keyed by step name, built by the StepInput rule. Every
// driver with CapResult computes the result with it, so a one-step job's
// result is simply its handler's return value everywhere.
func RunOutput(wf Workflow, outs map[string][]byte) []byte {
	sinks := Sinks(wf)
	out := StepInput(nil, sinks, outs)
	if len(sinks) == 1 && out != nil {
		out = slices.Clone(out)
	}
	return out
}

// RunFailedError is what AwaitResult returns for a run that ended without
// succeeding. It matches ErrCancelled for a cancelled or discarded run.
type RunFailedError struct {
	Ref    RunRef
	Status Status
	// Err is RunInfo.Error when the client could say why; empty otherwise.
	Err string
}

// Error names the run and why it ended.
func (e *RunFailedError) Error() string {
	msg := fmt.Sprintf("workflow: run %s ended %s", refString(e.Ref), e.Status)
	if e.Err != "" {
		msg += ": " + e.Err
	}
	return msg
}

// Is makes a cancelled or discarded run match ErrCancelled.
func (e *RunFailedError) Is(target error) bool {
	return target == ErrCancelled && (e.Status == StatusCancelled || e.Status == StatusDiscarded)
}

func refString(r RunRef) string {
	if !r.RunID.IsZero() {
		return r.RunID.String()
	}
	return r.ScheduledID.String()
}

// DefaultAwaitPoll is how often Await asks Status when no AwaitOptions.Poll
// is given.
const DefaultAwaitPoll = 500 * time.Millisecond

// AwaitOptions tunes Await.
type AwaitOptions struct {
	// Poll is the wait between Status calls; zero means DefaultAwaitPoll.
	Poll time.Duration

	// Sleep waits d or until ctx ends; nil means a real timer. A test
	// passes one driven by a fake clock.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (o AwaitOptions) sleep(ctx context.Context) error {
	d := o.Poll
	if d <= 0 {
		d = DefaultAwaitPoll
	}
	if o.Sleep != nil {
		return o.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// Await blocks until the run is terminal and returns its status, or
// ctx's error (give ctx a deadline for a timeout). It polls
// Client.Status, so it works on every driver; it holds no worker, but it
// does hold the caller, so call it from a request handler or a CLI, never
// from a step (use AwaitChild there).
func Await(ctx context.Context, c Client, ref RunRef, opts AwaitOptions) (Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return "", err
		}
		if st.IsTerminal() {
			return st, nil
		}
		if err := opts.sleep(ctx); err != nil {
			return st, err
		}
	}
}

// AwaitResult waits like Await and returns the run's result: Result when
// the run succeeded (c must implement ResultReader, else an
// UnsupportedError), a *RunFailedError when it ended otherwise.
func AwaitResult(ctx context.Context, c Client, ref RunRef, opts AwaitOptions) ([]byte, error) {
	st, err := Await(ctx, c, ref, opts)
	if err != nil {
		return nil, err
	}
	return resultOf(ctx, c, ref, st)
}

// resultOf turns a terminal status into the run's result or a
// RunFailedError.
func resultOf(ctx context.Context, c Client, ref RunRef, st Status) ([]byte, error) {
	if st != StatusSucceeded {
		return nil, &RunFailedError{Ref: ref, Status: st, Err: runError(ctx, c, ref)}
	}
	rr, ok := c.(ResultReader)
	if !ok {
		return nil, &UnsupportedError{Field: "Result", Missing: CapResult, Reason: "the client does not keep run results"}
	}
	return rr.Result(ctx, ref)
}

// runError reads RunInfo.Error when the client can list runs.
func runError(ctx context.Context, c Client, ref RunRef) string {
	q, ok := c.(RunQuerier)
	if !ok {
		return ""
	}
	page, err := q.Runs(ctx, RunQuery{Ref: ref, Limit: 1})
	if err != nil || len(page.Runs) == 0 {
		return ""
	}
	return page.Runs[0].Error
}
