package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ubgo/workflow/clock"
)

// Deterministic replay (DE-08, XM-01): orchestration code that re-runs
// from the top on every wake, against the history of what it did before.
//
// A replayed handler is ordinary Go with local variables that live across
// waits of any length. Every call that touches the world goes through the
// ReplayCtx (ReplayStep, Sleep, SleepUntil, Now, AwaitPromise) and is
// recorded in order in the run's journal: on a later run of the handler
// (after a wait, a retry or a crash) each recorded call returns its
// recorded result instead of running again, so the code reaches the same
// point with the same locals and carries on. A wait that is not over
// suspends the handler: the step snoozes (CapSnooze), holding no worker,
// and runs again when the wait may be over.
//
// The rule the code keeps is determinism: given the same recorded
// results, it makes the same calls in the same order. Branching on a
// recorded result is fine; branching on time.Now, a random number, map
// order or a global without recording it is not. A call that does not
// match the recorded one at its position (another kind or another name),
// or a run that ends with recorded calls left over, fails the run with a
// Permanent *NondeterminismError, the replay engines' determinism check,
// instead of mixing two versions of the code.
//
// History lives in the JournalStore the Journal plugin puts on the step's
// ctx (durable on sqlstore and Redis), under keys of the step's own, and
// is forgotten with the rest of the run's journal when the run finishes.

// ErrNondeterminism matches a *NondeterminismError.
var ErrNondeterminism = errors.New("workflow: replay does not match the recorded history")

// NondeterminismError reports a replayed handler that did not repeat its
// recorded history: the code changed under a running run, or it depends
// on something it did not record.
type NondeterminismError struct {
	// Position is the index of the call, from 0.
	Position int

	// Recorded is the call recorded at Position ("step charge"); empty
	// when the history ended there.
	Recorded string

	// Got is the call the code made ("sleep cool-off"); empty when the
	// code returned with recorded calls left over.
	Got string
}

func (e *NondeterminismError) Error() string {
	switch {
	case e.Recorded == "":
		return fmt.Sprintf("workflow: replay: call %d is %s, but the history ended before it", e.Position, e.Got)
	case e.Got == "":
		return fmt.Sprintf("workflow: replay: the code returned before call %d, but the history has %s there", e.Position, e.Recorded)
	}
	return fmt.Sprintf("workflow: replay: call %d is %s, but the history has %s", e.Position, e.Got, e.Recorded)
}

// Is makes errors.Is(err, ErrNondeterminism) true.
func (e *NondeterminismError) Is(target error) bool { return target == ErrNondeterminism }

// ReplayFunc is a replayed handler's body: it gets the replay context and
// the step's input and returns the step's output. Return the error of a
// ReplayCtx call as it is (it may be the suspension of a wait).
type ReplayFunc func(rc *ReplayCtx, in []byte) ([]byte, error)

// ReplayOptions configures NewReplay.
type ReplayOptions struct {
	// Client, when set, is checked for CapSnooze, which waits need (a
	// suspended wait is a snooze). Pass the client the workflow is
	// registered on; nil skips the check.
	Client Client

	// Clock is the time source of Sleep, SleepUntil and Now; nil means
	// clock.Real.
	Clock clock.Clock

	// Poll is how long a wait on an unsettled promise sleeps between
	// looks; zero means DefaultPromisePoll.
	Poll time.Duration
}

// NewReplay returns a step handler that runs fn as replayed orchestration
// code. The step's client needs the Journal plugin (a Permanent
// ErrNoJournal otherwise) and, for waits, CapSnooze: memory, sqlstore and
// Redis; with opts.Client set, a client without CapSnooze is refused here
// with an UnsupportedError.
func NewReplay(fn ReplayFunc, opts ReplayOptions) (Handler, error) {
	if fn == nil {
		return nil, invalidf("NewReplay needs a function")
	}
	if opts.Client != nil && !opts.Client.Capabilities().Has(CapSnooze) {
		return nil, &UnsupportedError{Field: "NewReplay", Missing: CapSnooze, Reason: "a replayed wait suspends the step with a snooze"}
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.Poll <= 0 {
		opts.Poll = DefaultPromisePoll
	}
	return func(ctx context.Context, in []byte) ([]byte, error) {
		j, err := journalFrom(ctx)
		if err != nil {
			return nil, err
		}
		rc := &ReplayCtx{ctx: ctx, j: j, opts: opts, prefix: strconv.Itoa(len(j.step)) + ":" + j.step + "#replay/"}
		out, err := fn(rc, in)
		if rc.suspend != nil {
			return nil, rc.suspend // whatever fn returned after a wait suspended it
		}
		if rc.fault != nil {
			return nil, rc.fault
		}
		if err != nil {
			return nil, err
		}
		// Recorded calls past the last one the code made: the code took a
		// shorter path than the recorded run did.
		if e, ok, err := rc.load(rc.pos); err != nil {
			return nil, err
		} else if ok {
			return nil, Permanent(&NondeterminismError{Position: rc.pos, Recorded: e.Kind + " " + e.Name})
		}
		return out, nil
	}, nil
}

// ReplayCtx is what a replayed handler records its calls through. It is
// not safe for concurrent use: calls are recorded in the order they are
// made, so make them from the handler's goroutine.
type ReplayCtx struct {
	ctx     context.Context
	j       *journalScope
	opts    ReplayOptions
	prefix  string
	pos     int
	replay  bool  // the current call was found in the history
	suspend error // set once a wait suspends this run of the handler
	fault   error // set once a call failed for good (nondeterminism, a store error)
}

// Context returns the step's context.
func (rc *ReplayCtx) Context() context.Context { return rc.ctx }

// Replaying reports whether the last call returned a recorded result,
// for code that logs only on the first execution.
func (rc *ReplayCtx) Replaying() bool { return rc.replay }

// replayEntry is one recorded call.
type replayEntry struct {
	Kind  string          `json:"k"`
	Name  string          `json:"n"`
	Value json.RawMessage `json:"v,omitempty"`
	Err   string          `json:"e,omitempty"`
}

func (rc *ReplayCtx) key(pos int) string { return rc.prefix + strconv.Itoa(pos) }

func (rc *ReplayCtx) load(pos int) (replayEntry, bool, error) {
	var e replayEntry
	raw, ok, err := rc.j.store.Load(rc.ctx, rc.j.run, rc.key(pos))
	if err != nil || !ok {
		if err != nil {
			err = fmt.Errorf("workflow: replay: load call %d: %w", pos, err)
		}
		return e, false, err
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, false, Permanent(fmt.Errorf("workflow: replay: recorded call %d does not decode: %w", pos, err))
	}
	return e, true, nil
}

// next looks up the call at the current position: the recorded entry
// when there is one (checked against kind and name), and false for a new
// call. Once the handler is suspended or failed every call returns that
// error again, so code that ignores an error cannot run on.
func (rc *ReplayCtx) next(kind, name string) (replayEntry, bool, error) {
	if rc.suspend != nil {
		return replayEntry{}, false, rc.suspend
	}
	if rc.fault != nil {
		return replayEntry{}, false, rc.fault
	}
	if name == "" {
		return replayEntry{}, false, rc.fail(Permanent(invalidf("replay: %s needs a name", kind)))
	}
	e, ok, err := rc.load(rc.pos)
	if err != nil {
		return e, false, err
	}
	if ok && (e.Kind != kind || e.Name != name) {
		return e, false, rc.fail(Permanent(&NondeterminismError{Position: rc.pos, Recorded: e.Kind + " " + e.Name, Got: kind + " " + name}))
	}
	rc.replay = ok
	if ok {
		rc.pos++ // a recorded call is consumed here; a new one by record
	}
	return e, ok, nil
}

func (rc *ReplayCtx) fail(err error) error {
	rc.fault = err
	return err
}

// record saves the call at the current position and moves on.
func (rc *ReplayCtx) record(e replayEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return rc.fail(Permanent(fmt.Errorf("workflow: replay: encode call %d: %w", rc.pos, err)))
	}
	if err := rc.j.store.Save(rc.ctx, rc.j.run, rc.key(rc.pos), b); err != nil {
		return fmt.Errorf("workflow: replay: save call %d: %w", rc.pos, err)
	}
	rc.pos++
	return nil
}

// suspendFor suspends the handler until d has passed.
func (rc *ReplayCtx) suspendFor(d time.Duration) error {
	rc.suspend = Snooze(d)
	return rc.suspend
}

// ReplayStep runs fn once per run as the call named name and records its
// result; a replay returns the recorded result without calling fn, so the
// code after it sees the same value. Results are stored as JSON.
//
// A Permanent error from fn is recorded too and returned (Permanent, as
// a plain error with the same text) on every replay, so the code takes
// the same branch: return it to fail the step, or handle it and carry on.
// Any other error is not recorded and is returned as it is: return it,
// and the step's retry runs the code, and fn, again. fn is at least once:
// the process can die after fn returns and before its result is saved.
func ReplayStep[T any](rc *ReplayCtx, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	e, ok, err := rc.next("step", name)
	if err != nil {
		return zero, err
	}
	if ok {
		if e.Err != "" {
			return zero, Permanent(errors.New(e.Err))
		}
		var v T
		if err := json.Unmarshal(e.Value, &v); err != nil {
			return zero, rc.fail(Permanent(fmt.Errorf("workflow: replay: step %q: recorded result does not decode: %w", name, err)))
		}
		return v, nil
	}
	v, err := fn(rc.ctx)
	if err != nil {
		if !IsPermanent(err) {
			return zero, err // not recorded: the step's retry runs it again
		}
		if rerr := rc.record(replayEntry{Kind: "step", Name: name, Err: err.Error()}); rerr != nil {
			return zero, rerr
		}
		return zero, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return zero, rc.fail(Permanent(fmt.Errorf("workflow: replay: step %q: encode result: %w", name, err)))
	}
	if err := rc.record(replayEntry{Kind: "step", Name: name, Value: raw}); err != nil {
		return zero, err
	}
	return v, nil
}

// Now returns the time on the replay's clock, recorded under name: a
// replay returns the recorded instant, so code may branch on it.
func (rc *ReplayCtx) Now(name string) (time.Time, error) {
	return ReplayStep(rc, name, func(context.Context) (time.Time, error) { return rc.opts.Clock.Now().UTC(), nil })
}

// Sleep waits d from the first time the code reached it, without holding
// a worker: the wake time is recorded under name, and until it passes the
// handler is suspended (a snooze) and runs again at the wake time.
func (rc *ReplayCtx) Sleep(name string, d time.Duration) error {
	return rc.sleep("sleep", name, func() time.Time { return rc.opts.Clock.Now().Add(d) })
}

// SleepUntil waits until t, as Sleep does.
func (rc *ReplayCtx) SleepUntil(name string, t time.Time) error {
	return rc.sleep("sleep", name, func() time.Time { return t })
}

func (rc *ReplayCtx) sleep(kind, name string, wake func() time.Time) error {
	e, ok, err := rc.next(kind, name)
	if err != nil {
		return err
	}
	var at time.Time
	if ok {
		if err := json.Unmarshal(e.Value, &at); err != nil {
			return rc.fail(Permanent(fmt.Errorf("workflow: replay: sleep %q: recorded wake time does not decode: %w", name, err)))
		}
	} else {
		at = wake().UTC()
		raw, _ := json.Marshal(at)
		if err := rc.record(replayEntry{Kind: kind, Name: name, Value: raw}); err != nil {
			return err
		}
	}
	if d := at.Sub(rc.opts.Clock.Now()); d > 0 {
		return rc.suspendFor(d)
	}
	return nil
}

// AwaitPromise waits for promise id in store (resolved from outside with
// ResolvePromise) and returns its value, recorded under name once it is
// settled. While it is not, the handler is suspended and looks again
// every ReplayOptions.Poll. A rejected promise returns a Permanent
// *PromiseRejectedError, recorded like a value.
func (rc *ReplayCtx) AwaitPromise(name string, store PromiseStore, id string) ([]byte, error) {
	e, ok, err := rc.next("promise", name)
	if err != nil {
		return nil, err
	}
	if ok {
		if e.Err != "" {
			return nil, Permanent(&PromiseRejectedError{ID: id, Reason: e.Err})
		}
		var v []byte
		if err := json.Unmarshal(e.Value, &v); err != nil {
			return nil, rc.fail(Permanent(fmt.Errorf("workflow: replay: promise %q: recorded value does not decode: %w", name, err)))
		}
		return v, nil
	}
	v, settled, err := PromiseResult(rc.ctx, store, id)
	var rej *PromiseRejectedError
	switch {
	case errors.As(err, &rej):
		if rerr := rc.record(replayEntry{Kind: "promise", Name: name, Err: rej.Reason}); rerr != nil {
			return nil, rerr
		}
		return nil, Permanent(err)
	case err != nil:
		return nil, err
	case !settled:
		return nil, rc.suspendFor(rc.opts.Poll)
	}
	raw, _ := json.Marshal(v)
	if err := rc.record(replayEntry{Kind: "promise", Name: name, Value: raw}); err != nil {
		return nil, err
	}
	return v, nil
}
