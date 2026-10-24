package workflow

import (
	"context"
	"fmt"
	"time"
)

// DefaultChildPoll is how long AwaitChild snoozes the parent step between
// looks at an unfinished child.
const DefaultChildPoll = 5 * time.Second

// ChildOptions tunes AwaitChild and AwaitChildren.
type ChildOptions struct {
	// Poll is the snooze between looks at unfinished children; zero
	// means DefaultChildPoll.
	Poll time.Duration

	// Spawn are passed to Spawn (SpawnAllowDuplicates).
	Spawn []SpawnOption

	// Parent is the client the calling step's workflow runs on, whose
	// driver must honour Snooze. Nil means the client the children run
	// on, the usual case of one backend.
	Parent Client
}

// AwaitChild runs job as a child run of the calling step and returns the
// child's result: the parent waits for the child without holding a
// worker. The first call spawns the child (Spawn, so the Journal plugin
// is needed and a retried or snoozed step never spawns it twice); while
// the child is unfinished it returns Snooze(Poll), so the parent's step
// is put back without spending an attempt and asks again later; once the
// child succeeded it returns the child's result (client must implement
// ResultReader). A child that ended any other way fails the parent's step
// with a Permanent *RunFailedError, since asking again would see the
// same end.
//
// The parent's driver needs CapSnooze: without it each look would spend an
// attempt of the parent step, which could fail before its child ends, so
// AwaitChild refuses with a Permanent UnsupportedError instead. client is
// the client the child runs on: the parent's own, or another backend
// (then set ChildOptions.Parent to the parent's client).
func AwaitChild(ctx context.Context, client Client, name string, job Job, opts ChildOptions) ([]byte, error) {
	outs, err := AwaitChildren(ctx, client, name, []Job{job}, opts)
	if err != nil {
		return nil, err
	}
	return outs[0], nil
}

// AwaitChildren is AwaitChild for several children at once (multiple
// instances whose number is known when the step runs, results joined
// back in job order): it spawns them all, snoozes until every one is
// terminal, and returns their results in the order of jobs. The first
// child that did not succeed fails the step, as AwaitChild does.
func AwaitChildren(ctx context.Context, client Client, name string, jobs []Job, opts ChildOptions) ([][]byte, error) {
	parent := opts.Parent
	if parent == nil {
		parent = client
	}
	if parent == nil || !parent.Capabilities().Has(CapSnooze) {
		return nil, Permanent(&UnsupportedError{Field: "AwaitChild", Missing: CapSnooze,
			Reason: "the parent's driver would spend an attempt on every look at an unfinished child"})
	}
	refs, err := Spawn(ctx, client, name, jobs, opts.Spawn...)
	if err != nil {
		return nil, err
	}
	poll := opts.Poll
	if poll <= 0 {
		poll = DefaultChildPoll
	}
	sts := make([]Status, len(refs))
	for i, ref := range refs {
		st, err := client.Status(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("workflow: await child %q #%d: %w", name, i, err)
		}
		if !st.IsTerminal() {
			return nil, Snooze(poll)
		}
		sts[i] = st
	}
	outs := make([][]byte, len(refs))
	for i, ref := range refs {
		out, err := resultOf(ctx, client, ref, sts[i])
		if err != nil {
			return nil, Permanent(fmt.Errorf("workflow: await child %q #%d: %w", name, i, err))
		}
		outs[i] = out
	}
	return outs, nil
}
