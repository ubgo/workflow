package workflow

import (
	"context"
	"fmt"
	"time"
)

// suspendKey is the journal key of a run's suspend flag. It starts with
// the run data prefix's sibling namespace "ubgo.workflow/", which Memo
// keys (a step name's length and a colon) and run data ("data/") never
// use.
//
//ds:def id=suspendkey-agfhj5t4 owner=@khanakia stability=stable
const suspendKey = "ubgo.workflow/suspended"

// DefaultSuspendPoll is how often a suspended run's parked step looks
// again whether it was resumed.
//
//ds:def id=defaultsuspendpoll-z8mrjrqz owner=@khanakia stability=stable
const DefaultSuspendPoll = 5 * time.Second

// suspendPlugin is Suspension.
type suspendPlugin struct {
	store JournalStore
	poll  time.Duration
}

// Suspension returns the plugin that lets an operator suspend one run and
// resume it later (WP-18), unlike a pause, which holds a whole workflow
// or queue. A suspended run parks at the start of its next step: the step
// returns a Snooze of poll (DefaultSuspendPoll when zero) without running
// its handler, holding no worker and spending no attempt, and looks again
// after each snooze; steps already running finish. ResumeRun clears the
// flag and the parked step runs at its next look.
//
// The flag lives in store, a JournalStore: the in-process MemoryJournal,
// or a durable one (the sqlstore and Redis drivers ship them) so every
// node sees it. Pass the same store the Journal plugin uses, so a
// finished run's flag is forgotten with its journal. It needs a driver
// with CapSnooze (memory, sqlstore, Redis); on one without it the parked
// step would spend attempts, so do not use it there.
//
//ds:def id=suspension-tvpj5mcy owner=@khanakia stability=stable
func Suspension(store JournalStore, poll time.Duration) Plugin {
	if poll <= 0 {
		poll = DefaultSuspendPoll
	}
	return suspendPlugin{store: store, poll: poll}
}

func (p suspendPlugin) Name() string { return "suspension" }

func (p suspendPlugin) StepMiddleware() []Middleware {
	return []Middleware{func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := StepInfoFrom(ctx)
			if !ok || p.store == nil {
				return next(ctx, in)
			}
			v, found, err := p.store.Load(ctx, info.RunID, suspendKey)
			if err != nil {
				return nil, fmt.Errorf("workflow: suspension: read run %s: %w", info.RunID, err)
			}
			if found && string(v) == "1" {
				return nil, Snooze(p.poll)
			}
			return next(ctx, in)
		}
	}}
}

// SuspendRun marks ref suspended in store (see Suspension): its next step
// parks until ResumeRun. A run that has not started yet (only a
// ScheduledID) cannot be marked, since its flag is keyed by RunID; move
// it with RunRescheduler instead. Suspending a finished run has no
// effect.
//
//ds:def id=suspendrun-6dhpewn2 owner=@khanakia stability=stable
func SuspendRun(ctx context.Context, store JournalStore, ref RunRef) error {
	if ref.RunID.IsZero() {
		return invalidf("SuspendRun needs a RunID; a run that has not started is moved with Reschedule")
	}
	return store.Save(ctx, ref.RunID, suspendKey, []byte("1"))
}

// ResumeRun clears ref's suspension; its parked step runs at its next
// look (within the plugin's poll).
//
//ds:def id=resumerun-td25p96u owner=@khanakia stability=stable
func ResumeRun(ctx context.Context, store JournalStore, ref RunRef) error {
	if ref.RunID.IsZero() {
		return invalidf("ResumeRun needs a RunID")
	}
	return store.Save(ctx, ref.RunID, suspendKey, []byte("0"))
}

// RunSuspended reports whether ref is marked suspended in store.
//
//ds:def id=runsuspended-wjxk9sqy owner=@khanakia stability=stable
func RunSuspended(ctx context.Context, store JournalStore, ref RunRef) (bool, error) {
	if ref.RunID.IsZero() {
		return false, nil
	}
	v, found, err := store.Load(ctx, ref.RunID, suspendKey)
	return found && string(v) == "1", err
}
