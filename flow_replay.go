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
//
//ds:def id=errnondeterminism-4uctguxs owner=@khanakia stability=stable
var ErrNondeterminism = errors.New("workflow: replay does not match the recorded history")

// NondeterminismError reports a replayed handler that did not repeat its
// recorded history: the code changed under a running run, or it depends
// on something it did not record.
//
//ds:def id=nondeterminismerror-8x8pksxb owner=@khanakia stability=stable
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
//
//ds:def id=replayfunc-r6m9n633 owner=@khanakia stability=stable
type ReplayFunc func(rc *ReplayCtx, in []byte) ([]byte, error)

// ReplayOptions configures NewReplay.
//
//ds:def id=replayoptions-6vpwfkpn owner=@khanakia stability=stable
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
//
//ds:def id=newreplay-4kar3evc owner=@khanakia stability=stable
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
//
//ds:def id=replayctx-7t4tsdfs owner=@khanakia stability=stable
type ReplayCtx struct {
	ctx     context.Context
	j       *journalScope
	opts    ReplayOptions
	prefix  string
	pos     int
	peeked  int   // the position Replaying last looked at, plus one; 0: none
	peekOK  bool  // whether a call is recorded at peeked-1
	suspend error // set once a wait suspends this run of the handler
	fault   error // set once a call failed for good (nondeterminism, a store error)

	// children counts the ReplayChild calls per name made so far in this
	// run of the body, recorded or live, so each call gets a spawn name
	// of its own (childSpawnName).
	children map[string]int
}

// Context returns the step's context.
func (rc *ReplayCtx) Context() context.Context { return rc.ctx }

// Replaying reports whether the code is still inside its recorded
// history: true while the next call is one the history holds (a run that
// re-traces what an earlier run did), false once the code has gone past
// the history (it reaches this point for the first time). Code that logs
// or reports only once does it when Replaying is false. It reads the
// store once per position; a read error counts as not replaying.
//
//ds:def id=replayctx-replaying-fp9hjkng owner=@khanakia stability=stable
func (rc *ReplayCtx) Replaying() bool {
	if rc.peeked != rc.pos+1 {
		_, ok, err := rc.load(rc.pos)
		rc.peeked, rc.peekOK = rc.pos+1, ok && err == nil
	}
	return rc.peekOK
}

// replayEntry is one recorded call.
type replayEntry struct {
	Kind  string          `json:"k"`
	Name  string          `json:"n"`
	Value json.RawMessage `json:"v,omitempty"`
	Err   string          `json:"e,omitempty"`
}

func (rc *ReplayCtx) key(pos int) string { return rc.prefix + strconv.Itoa(pos) }

//ds:def id=replayctx-load-3kmgcg3a owner=@khanakia stability=stable
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
//
//ds:def id=replayctx-next-suvs8w6w owner=@khanakia stability=stable
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
		return e, false, rc.fail(err)
	}
	if ok && (e.Kind != kind || e.Name != name) {
		return e, false, rc.fail(Permanent(&NondeterminismError{Position: rc.pos, Recorded: e.Kind + " " + e.Name, Got: kind + " " + name}))
	}
	if ok {
		rc.pos++ // a recorded call is consumed here; a new one by record
	}
	return e, ok, nil
}

// fail marks the handler failed with err: every later call returns it,
// and the handler returns it whatever fn returns, so code that ignores an
// error can never record a call at a position it does not own. A
// Permanent err fails the run; any other one retries the step, which
// re-runs the code from the top.
//
//ds:def id=replayctx-fail-m5hxy5p4 owner=@khanakia stability=stable
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
		return rc.fail(fmt.Errorf("workflow: replay: save call %d: %w", rc.pos, err))
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
// The code sees exactly what every replay will see: the value decoded
// back from its JSON, and for a Permanent error from fn, a Permanent
// error with the same text (recorded, so the code can branch on it with
// IsPermanent or the text, and take the same branch on every run;
// errors.Is against fn's own sentinel does not survive the record, so
// return a decision as a value when the code must tell errors apart).
// Any other error is not recorded: it fails this run of the handler (every
// later call returns it too) and the step's retry runs the code, and fn,
// again. fn is at least once: the process can die after fn returns and
// before its result is saved.
//
//ds:def id=replaystep-7ga7k5mw owner=@khanakia stability=stable
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
			return zero, rc.fail(err) // not recorded: the step's retry runs it again
		}
		// The error the code sees is the one every replay will see: its
		// text, Permanent, so the first run and the replays branch alike.
		if rerr := rc.record(replayEntry{Kind: "step", Name: name, Err: err.Error()}); rerr != nil {
			return zero, rerr
		}
		return zero, Permanent(errors.New(err.Error()))
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return zero, rc.fail(Permanent(fmt.Errorf("workflow: replay: step %q: encode result: %w", name, err)))
	}
	// The value the code sees is the decoded one, as on every replay (a
	// time's zone name, unexported fields and the like do not survive JSON).
	var back T
	if err := json.Unmarshal(raw, &back); err != nil {
		return zero, rc.fail(Permanent(fmt.Errorf("workflow: replay: step %q: result does not decode back: %w", name, err)))
	}
	if err := rc.record(replayEntry{Kind: "step", Name: name, Value: raw}); err != nil {
		return zero, err
	}
	return back, nil
}

// Now returns the time on the replay's clock, recorded under name: a
// replay returns the recorded instant, so code may branch on it.
//
//ds:def id=replayctx-now-d769pxvb owner=@khanakia stability=stable
func (rc *ReplayCtx) Now(name string) (time.Time, error) {
	return ReplayStep(rc, name, func(context.Context) (time.Time, error) { return rc.opts.Clock.Now().UTC(), nil })
}

// Sleep waits d from the first time the code reached it, without holding
// a worker: the wake time is recorded under name, and until it passes the
// handler is suspended (a snooze) and runs again at the wake time.
//
//ds:def id=replayctx-sleep-25nbyyt6 owner=@khanakia stability=stable
func (rc *ReplayCtx) Sleep(name string, d time.Duration) error {
	return rc.sleep("sleep", name, func() time.Time { return rc.opts.Clock.Now().Add(d) })
}

// SleepUntil waits until t, as Sleep does.
//
//ds:def id=replayctx-sleepuntil-xff2xf57 owner=@khanakia stability=stable
func (rc *ReplayCtx) SleepUntil(name string, t time.Time) error {
	return rc.sleep("sleep", name, func() time.Time { return t })
}

//ds:def id=replayctx-sleep-xgbfeav4 owner=@khanakia stability=stable
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
//
//ds:def id=replayctx-awaitpromise-y7k6qya8 owner=@khanakia stability=stable
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
		return nil, rc.fail(err)
	case !settled:
		return nil, rc.suspendFor(rc.opts.Poll)
	}
	raw, _ := json.Marshal(v)
	if err := rc.record(replayEntry{Kind: "promise", Name: name, Value: raw}); err != nil {
		return nil, err
	}
	return v, nil
}

// ReplayChild runs job as a child run and returns its result, recorded
// under name (AwaitChild inside replayed code): the first run spawns the
// child (memoised, so it is spawned once), later runs look at it while it
// is unfinished, suspending the handler between looks (opts.Poll), and
// once it succeeded its result is recorded, so replays do not ask the
// client again. A child that ended any other way returns a Permanent
// *RunFailedError, recorded like a Permanent step error. client is the
// client the child runs on; the parent's driver needs CapSnooze, and
// the child's client needs results (ResultReader). As with the other
// calls the name need not be unique: each call with a name spawns its
// own child (a loop calling ReplayChild(rc, "invoice", ...) starts one
// per pass).
//
//ds:def id=replaychild-45qnbe3j owner=@khanakia stability=stable
func ReplayChild(rc *ReplayCtx, name string, client Client, job Job, opts ChildOptions) ([]byte, error) {
	e, ok, err := rc.next("child", name)
	if err != nil {
		return nil, err
	}
	spawnName := rc.childSpawnName(name)
	if ok {
		if e.Err != "" {
			return nil, Permanent(errors.New(e.Err))
		}
		var v []byte
		if err := json.Unmarshal(e.Value, &v); err != nil {
			return nil, rc.fail(Permanent(fmt.Errorf("workflow: replay: child %q: recorded result does not decode: %w", name, err)))
		}
		return v, nil
	}
	out, err := AwaitChild(rc.ctx, client, spawnName, job, opts)
	if d, snoozed := SnoozeDelay(err); snoozed {
		return nil, rc.suspendFor(d)
	}
	if err != nil {
		if !IsPermanent(err) {
			return nil, rc.fail(err)
		}
		if rerr := rc.record(replayEntry{Kind: "child", Name: name, Err: err.Error()}); rerr != nil {
			return nil, rerr
		}
		return nil, Permanent(errors.New(err.Error()))
	}
	raw, _ := json.Marshal(out)
	if err := rc.record(replayEntry{Kind: "child", Name: name, Value: raw}); err != nil {
		return nil, err
	}
	return out, nil
}

// childSpawnName is the name ReplayChild spawns its child under, counted
// on every call (a recorded one too) so the same call gets the same name
// on every run of the body: "replay-child/<name>" for the first call with
// name, as it always was (a run already waiting on that child keeps it),
// and "replay-child#<n>/<name>" for the n-th call with the same name, so
// a loop may reuse a name, as with the other calls. The "#<n>" form
// cannot be produced by a name in the first form, so the two never meet.
//
//ds:def id=replayctx-childspawnname-7dvcm2sd owner=@khanakia stability=stable
func (rc *ReplayCtx) childSpawnName(name string) string {
	if rc.children == nil {
		rc.children = map[string]int{}
	}
	rc.children[name]++
	if n := rc.children[name]; n > 1 {
		return "replay-child#" + strconv.Itoa(n) + "/" + name
	}
	return "replay-child/" + name
}
