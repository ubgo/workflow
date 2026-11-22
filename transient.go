package workflow

import "context"

// TransientSignaller delivers an event only to the runs waiting for it at
// the moment of the call (WC-23, transient trigger), where Signal keeps
// every event for a run whose wait step has not begun yet. Use it for
// hints whose loss is harmless and whose late delivery would be wrong: a
// "refresh now" nudge, a cache invalidation, a progress poke. Anything
// that must not be lost goes through Signal.
//
// SignalWaiting resumes every run whose wait step is suspended on ev's name
// and key when the call is made, and returns how many it resumed; a run
// that reaches its wait step a moment later never sees ev. Zero resumed is
// not an error. A client implements it when its driver can; the call
// answers for itself, so there is no capability bit (UnsupportedError from
// a wrapper whose inner client lacks it).
//
//ds:def id=transientsignaller-cww6hkhu owner=@khanakia stability=stable
type TransientSignaller interface {
	SignalWaiting(ctx context.Context, ev Event) (int, error)
}

// SignalWaiting forwards to Inner's TransientSignaller.
//
//ds:def id=forward-signalwaiting-swx7yvja owner=@khanakia stability=stable
func (f Forward) SignalWaiting(ctx context.Context, ev Event) (int, error) {
	if i, ok := f.Inner.(TransientSignaller); ok {
		return i.SignalWaiting(ctx, ev)
	}
	return 0, lacks("SignalWaiting", Caps{})
}
