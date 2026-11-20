package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// RaceSpec is a deferred choice (WC-16): the outside world decides which
// branch runs, and the first decision to arrive wins. Every party that
// can decide sends SignalChoice with its branch; the run takes the first
// one and later ones never reach it.
type RaceSpec struct {
	// Name is the name of the join step carrying the chosen branch's
	// output; later steps put it in After. The wait step is
	// "<Name>.wait".
	Name string

	// After lists the steps the race waits for before it starts waiting.
	After []string

	// Event is the event name every competitor signals.
	Event string

	// Key or KeyFrom pick the run, as in WaitSpec.
	Key     string
	KeyFrom ConcurrencyKey

	// Timeout bounds the wait (WaitSpec.Timeout); zero waits forever. A
	// timeout fails the run with ErrWaitTimeout. To give the race a
	// default branch instead, have a delayed job send SignalChoice for it
	// at the deadline: it simply loses when someone decided first.
	Timeout time.Duration

	// Branches are the paths. A branch receives the payload its signal
	// carried.
	Branches []Branch
}

// raceSignal is the payload SignalChoice sends.
type raceSignal struct {
	Branch  string `json:"branch"`
	Payload []byte `json:"payload,omitempty"`
}

// Race expands r into a wait step ("<Name>.wait") followed by a Choice
// named Name whose Decide reads the branch from the signal. It needs
// CapWaitForEvent (memory, sqlstore, Redis, Hatchet); the drivers' Signal
// contract makes the first signal the one the wait step takes, and a
// signal sent before the run reaches its wait is kept, so a quick answer
// is not lost. A signal naming no branch fails the run (Permanent,
// ErrInvalid).
func Race(r RaceSpec) ([]Step, error) {
	if r.Name == "" || r.Event == "" {
		return nil, invalidf("race %q needs a Name and an Event", r.Name)
	}
	branches := make([]Branch, len(r.Branches))
	for i, b := range r.Branches {
		run := b.Run
		if run != nil {
			run = func(ctx context.Context, in []byte) ([]byte, error) {
				var s raceSignal
				if err := json.Unmarshal(in, &s); err != nil {
					return nil, Permanent(fmt.Errorf("workflow: race %q: signal does not decode: %w", r.Name, err))
				}
				return b.Run(ctx, s.Payload)
			}
		}
		branches[i] = Branch{Name: b.Name, Run: run, Retry: b.Retry, Timeout: b.Timeout}
	}
	waitName := r.Name + ".wait"
	choice, err := Choice(ChoiceSpec{
		Name:  r.Name,
		After: []string{waitName},
		Decide: func(_ context.Context, in []byte) (string, error) {
			var s raceSignal
			if err := json.Unmarshal(in, &s); err != nil {
				return "", Permanent(fmt.Errorf("workflow: race %q: signal does not decode: %w", r.Name, err))
			}
			return s.Branch, nil
		},
		Branches: branches,
	})
	if err != nil {
		return nil, err
	}
	wait := Step{Name: waitName, After: slices.Clone(r.After), Wait: &WaitSpec{Event: r.Event, Key: r.Key, KeyFrom: r.KeyFrom, Timeout: r.Timeout}}
	return append([]Step{wait}, choice...), nil
}

// SignalChoice sends branch, with payload for the branch, to the runs
// racing on event and key (Race). The first signal a run takes decides;
// a later one is dropped by that run.
func SignalChoice(ctx context.Context, c Client, event, key, branch string, payload []byte) error {
	if branch == "" {
		return invalidf("SignalChoice needs a branch")
	}
	b, err := json.Marshal(raceSignal{Branch: branch, Payload: payload})
	if err != nil {
		return err
	}
	return c.Signal(ctx, Event{Name: event, Key: key, Payload: b})
}
