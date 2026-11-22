package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
)

// LabelObjectKey is the label every call of a virtual object carries: the
// object's key. Calls are serialised on it (Concurrency MaxRuns 1). It is
// under the reserved internal prefix, set by CallObject through
// AllowReservedLabels, so a job cannot claim another object's key by
// accident.
//
//ds:def id=labelobjectkey-cn6pa8c2 owner=@khanakia stability=stable
const LabelObjectKey = LabelPrefixInternal + "object-key"

// ObjectMethod is one operation of a virtual object: it receives the
// object's current state (the zero S for a new key) and the call's
// arguments, and returns the new state and the call's result. It runs
// alone for its key: no other call of the same key runs at the same time.
//
//ds:def id=objectmethod-4fjnrbyb owner=@khanakia stability=stable
type ObjectMethod[S any] func(ctx context.Context, state S, args []byte) (next S, result []byte, err error)

// ObjectSpec defines a keyed virtual object (DE-15, XM-07): a named
// entity per key (a cart, an account, a device) whose calls run one at a
// time per key and share durable state, without a table of the app's own.
//
//ds:def id=objectspec-3me8q3hk owner=@khanakia stability=stable
type ObjectSpec[S any] struct {
	// Name is the object type; it is also the name of the workflow
	// DefineObject returns.
	Name string

	// Methods are the operations, by name.
	Methods map[string]ObjectMethod[S]

	// Store keeps each key's state. Use the driver's durable
	// JournalStore (sqlstore, Redis) so state survives restarts and is
	// shared by every worker; MemoryJournal keeps it in this process.
	Store JournalStore

	// Ordered runs a key's calls in the order they were enqueued
	// (Concurrency.FIFO, CapKeyFIFO); without it calls of one key still
	// run one at a time, in whatever order the driver claims them.
	Ordered bool

	// Retry is the calls' retry policy, as Workflow.Retry.
	Retry RetryPolicy
}

// objectCall is a call's payload: the method and its arguments.
type objectCall struct {
	Method string `json:"method"`
	Args   []byte `json:"args,omitempty"`
}

// objectRecord is a key's stored state plus the last call applied, so a
// call retried after its state was saved returns its result without
// applying it twice.
type objectRecord struct {
	State   json.RawMessage `json:"state"`
	LastRun RunID           `json:"last_run,omitempty"`
	LastOut []byte          `json:"last_out,omitempty"`
}

// objectStateKey is the journal key a key's record is saved under.
const objectStateKey = "state"

// objectRun is the journal "run" an object key's record lives under: the
// reserved internal prefix, which no driver's run id starts with, and a
// hash of the object and the key, so it has a fixed width (61 bytes) that
// fits every JournalStore's run column whatever the key's length.
//
//ds:def id=objectrun-5ayyvf97 owner=@khanakia stability=stable
func objectRun(name, key string) RunID {
	sum := sha256.Sum256([]byte(name + "\x00" + key))
	return RunID(LabelPrefixInternal + "object/" + hex.EncodeToString(sum[:20]))
}

// DefineObject returns the workflow that runs spec's calls: one step that
// loads the key's state, runs the method, and saves the new state with
// the call's run id, all while no other call of the key runs
// (Concurrency{Key: LabelObjectKey, MaxRuns: 1}). Register it like any
// workflow; CallObject enqueues a call.
//
// A call is applied once: the stored record names the last run applied,
// so a call retried after its state was saved (the process died before
// the step's end was recorded) returns the saved result without calling
// the method again. A method error leaves the state unchanged and goes to
// the retry policy. The driver needs CapConcurrencyKey (and CapKeyFIFO
// with Ordered); Register and Start check them as for any workflow.
//
//ds:def id=defineobject-2tt5zh4c owner=@khanakia stability=stable
func DefineObject[S any](spec ObjectSpec[S]) (Workflow, error) {
	if spec.Name == "" || len(spec.Methods) == 0 || spec.Store == nil {
		return Workflow{}, invalidf("object needs a Name, Methods and a Store")
	}
	for name, m := range spec.Methods {
		if name == "" || m == nil {
			return Workflow{}, invalidf("object %q: every method needs a name and a function", spec.Name)
		}
	}
	return Workflow{
		Name:  spec.Name,
		Retry: spec.Retry,
		Concurrency: &Concurrency{
			Key:     KeyFunc(func(j Job) string { return j.Labels[LabelObjectKey] }),
			MaxRuns: 1,
			FIFO:    spec.Ordered,
		},
		Steps: []Step{{Name: "call", Run: objectStep(spec)}},
	}, nil
}

// objectStep is the body of an object's one step.
//
//ds:def id=objectstep-u2x9g4k3 owner=@khanakia stability=stable
func objectStep[S any](spec ObjectSpec[S]) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, ok := StepInfoFrom(ctx)
		if !ok {
			return nil, Permanent(invalidf("object %q: not called from a step", spec.Name))
		}
		key := info.Labels[LabelObjectKey]
		if key == "" {
			return nil, Permanent(invalidf("object %q: the call has no object key; enqueue it with CallObject", spec.Name))
		}
		var call objectCall
		if err := json.Unmarshal(in, &call); err != nil {
			return nil, Permanent(fmt.Errorf("%w: object %q: call payload: %v", ErrInvalid, spec.Name, err))
		}
		method, ok := spec.Methods[call.Method]
		if !ok {
			return nil, Permanent(invalidf("object %q has no method %q (methods: %v)", spec.Name, call.Method, methodNames(spec.Methods)))
		}
		run := objectRun(spec.Name, key)
		rec, err := loadObject(ctx, spec.Store, run)
		if err != nil {
			return nil, fmt.Errorf("workflow: object %q key %q: %w", spec.Name, key, err)
		}
		if rec.LastRun == info.RunID {
			return rec.LastOut, nil // applied already; a retry after the save
		}
		var state S
		if len(rec.State) > 0 {
			if err := json.Unmarshal(rec.State, &state); err != nil {
				return nil, Permanent(fmt.Errorf("workflow: object %q key %q: stored state does not decode: %w", spec.Name, key, err))
			}
		}
		next, out, err := method(ctx, state, call.Args)
		if err != nil {
			return nil, err
		}
		raw, err := encodeJSON("object "+spec.Name+" state", next)
		if err != nil {
			return nil, err
		}
		b, err := encodeJSON("object "+spec.Name+" record", objectRecord{State: raw, LastRun: info.RunID, LastOut: out})
		if err != nil {
			return nil, err
		}
		if err := spec.Store.Save(ctx, run, objectStateKey, b); err != nil {
			return nil, fmt.Errorf("workflow: object %q key %q: save: %w", spec.Name, key, err)
		}
		return out, nil
	}
}

func loadObject(ctx context.Context, store JournalStore, run RunID) (objectRecord, error) {
	var rec objectRecord
	raw, ok, err := store.Load(ctx, run, objectStateKey)
	if err != nil || !ok {
		return rec, err
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, Permanent(fmt.Errorf("stored record does not decode: %w", err))
	}
	return rec, nil
}

func methodNames[S any](m map[string]ObjectMethod[S]) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// CallObject enqueues a call of method on the object named object with
// key, and returns the call's run; AwaitResult gives the method's result.
// job may carry the other fields of the run (IdempotencyKey, RunAt,
// Labels, Tenant); its Workflow and Payload are set here.
//
//ds:def id=callobject-2zxmsz9k owner=@khanakia stability=stable
func CallObject(ctx context.Context, client Client, object, key, method string, args []byte, job Job) (RunRef, error) {
	if client == nil || object == "" || key == "" || method == "" {
		return RunRef{}, invalidf("CallObject needs a client, an object, a key and a method")
	}
	payload, err := json.Marshal(objectCall{Method: method, Args: args})
	if err != nil {
		return RunRef{}, fmt.Errorf("workflow: call object %q: %w", object, err)
	}
	job.Workflow, job.Payload = object, payload
	labels := make(map[string]string, len(job.Labels)+1)
	for k, v := range job.Labels {
		labels[k] = v
	}
	labels[LabelObjectKey] = key
	job.Labels = labels
	return client.Enqueue(AllowReservedLabels(ctx, LabelObjectKey), job)
}

// ObjectState reads the state of object key from store, outside any call
// (a query endpoint, a test), and false for a key never called. It reads
// the last saved state; a call running now may replace it a moment later.
//
//ds:def id=objectstate-rwhd8g9m owner=@khanakia stability=stable
func ObjectState[S any](ctx context.Context, store JournalStore, object, key string) (S, bool, error) {
	var state S
	if store == nil || object == "" || key == "" {
		return state, false, invalidf("ObjectState needs a store, an object and a key")
	}
	rec, err := loadObject(ctx, store, objectRun(object, key))
	if err != nil {
		return state, false, fmt.Errorf("workflow: object %q key %q: %w", object, key, err)
	}
	if len(rec.State) == 0 {
		return state, false, nil
	}
	if err := json.Unmarshal(rec.State, &state); err != nil {
		return state, false, fmt.Errorf("workflow: object %q key %q: stored state does not decode: %w", object, key, err)
	}
	return state, true, nil
}
