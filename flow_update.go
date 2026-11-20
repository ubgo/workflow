package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
)

// UpdateRequest is what Update signals: the caller's arguments and the
// promise the run answers through. A wait step's output is the signal's
// payload, so the step after it decodes this with DecodeUpdate (or wraps
// its body in UpdateHandler).
type UpdateRequest struct {
	// Reply is the promise id the run settles with its answer.
	Reply string `json:"reply"`

	// Payload is the caller's arguments.
	Payload []byte `json:"payload,omitempty"`
}

// updateEnvelope is the signal payload of an update: a JSON object whose
// one key is under the reserved internal prefix, so an update is never
// mistaken for an app's own payload.
type updateEnvelope struct {
	Update *UpdateRequest `json:"ubgo.workflow/update"`
}

// Update sends ev to the runs waiting on its name and key, as Signal
// does, and waits for the run's answer: a signal that returns a result
// (DE-07, synchronous update). It wraps ev.Payload in an UpdateRequest
// carrying a fresh promise id, signals it, and blocks until the run
// settles that promise (UpdateHandler or ReplyUpdate) or ctx ends. The
// answer travels through store, which the run's side must share (a
// durable PromiseStore when they run in different processes).
//
// The update is a wait step plus a step that answers, so it needs a
// driver with CapWaitForEvent. Signals are kept from a run's enqueue
// (see Client.Signal), so an update sent before the run reaches its wait
// step is still taken. One signal reaches every run waiting with that name
// and key; the first answer wins and later ones are ignored. Give the
// caller a ctx with a deadline: a run that never reaches the wait step
// never answers. The promise is deleted once read.
func Update(ctx context.Context, client Client, store PromiseStore, ev Event, opts AwaitOptions) ([]byte, error) {
	if client == nil || store == nil || ev.Name == "" {
		return nil, invalidf("Update needs a client, a promise store and an event name")
	}
	if !client.Capabilities().Has(CapWaitForEvent) {
		return nil, &UnsupportedError{Field: "Update", Missing: CapWaitForEvent, Reason: "the run receives an update through a wait step"}
	}
	id, err := NewPromiseID()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(updateEnvelope{Update: &UpdateRequest{Reply: id, Payload: ev.Payload}})
	if err != nil {
		return nil, fmt.Errorf("workflow: update: encode: %w", err)
	}
	ev.Payload = raw
	if err := client.Signal(ctx, ev); err != nil {
		return nil, fmt.Errorf("workflow: update: signal: %w", err)
	}
	out, err := WaitPromise(ctx, store, id, opts)
	if err == nil || errors.As(err, new(*PromiseRejectedError)) {
		if derr := store.Delete(context.WithoutCancel(ctx), id); derr != nil && err == nil {
			err = fmt.Errorf("workflow: update: delete reply: %w", derr)
		}
	}
	return out, err
}

// DecodeUpdate decodes the UpdateRequest a wait step received, and false
// when the input is not an update (a plain Signal's payload).
func DecodeUpdate(in []byte) (UpdateRequest, bool) {
	var env updateEnvelope
	if err := json.Unmarshal(in, &env); err != nil || env.Update == nil || env.Update.Reply == "" {
		return UpdateRequest{}, false
	}
	return *env.Update, true
}

// ReplyUpdate answers an update: it resolves req's reply promise with
// out, or rejects it with failure's message when failure is not nil. The
// caller of Update returns out, or a *PromiseRejectedError. A repeated
// reply with the same outcome is a no-op, so a retried step may answer
// again.
func ReplyUpdate(ctx context.Context, store PromiseStore, req UpdateRequest, out []byte, failure error) error {
	if failure != nil {
		return RejectPromise(ctx, nil, store, req.Reply, failure.Error())
	}
	return ResolvePromise(ctx, nil, store, req.Reply, out)
}

// UpdateHandler is the body of the step after an update's wait step: it
// decodes the UpdateRequest, calls fn with the caller's arguments, and
// answers with fn's output. An input that is not an update (the wait was
// resumed by a plain Signal or SignalWithStart) is passed to fn as it is
// and nobody is answered, so one run takes both. A Permanent error from fn rejects the update
// with its message (the caller of Update gets a *PromiseRejectedError);
// any other error is returned for the step's retry policy while the
// caller keeps waiting, so a step that exhausts its retries leaves the
// caller to its ctx deadline. The step's own output is fn's output.
func UpdateHandler(store PromiseStore, fn Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if store == nil || fn == nil {
			return nil, Permanent(invalidf("UpdateHandler needs a promise store and a handler"))
		}
		req, ok := DecodeUpdate(in)
		if !ok {
			return fn(ctx, in)
		}
		out, err := fn(ctx, req.Payload)
		if err != nil && !IsPermanent(err) {
			return nil, err
		}
		if rerr := ReplyUpdate(ctx, store, req, out, err); rerr != nil {
			return nil, rerr
		}
		return out, err
	}
}

// SignalWithStart delivers ev to the run job starts, starting it first if
// it is not running (WP-08): it enqueues job, which needs an
// IdempotencyKey so a run already holding that key is reused rather than
// a second one started, and then signals ev. Signals are kept from a
// run's enqueue (see Client.Signal), so a run just started still takes ev
// when it reaches its wait step. It needs CapIdempotencyKey and
// CapWaitForEvent. The signal goes to every run waiting on ev's name and
// key, as Signal does; give each run its own key (WaitSpec.KeyFrom) to
// reach only the one started.
func SignalWithStart(ctx context.Context, client Client, job Job, ev Event) (RunRef, error) {
	if client == nil || ev.Name == "" {
		return RunRef{}, invalidf("SignalWithStart needs a client and an event name")
	}
	if job.IdempotencyKey == "" {
		return RunRef{}, invalidf("SignalWithStart needs Job.IdempotencyKey, so an existing run is reused")
	}
	if missing := client.Capabilities().Missing(CapIdempotencyKey | CapWaitForEvent); missing != 0 {
		return RunRef{}, &UnsupportedError{Field: "SignalWithStart", Missing: missing, Reason: "it reuses a run by key and resumes it with a wait step"}
	}
	ref, err := client.Enqueue(ctx, job)
	if err != nil {
		return RunRef{}, fmt.Errorf("workflow: signal with start: enqueue: %w", err)
	}
	if err := client.Signal(ctx, ev); err != nil {
		return ref, fmt.Errorf("workflow: signal with start: signal: %w", err)
	}
	return ref, nil
}

// LabelContinuedFrom is the label ContinueAsNew sets on the run it starts:
// the RunID of the run it continues. It is under the reserved internal
// prefix, set through AllowReservedLabels like the parent labels of
// Spawn.
const LabelContinuedFrom = LabelPrefixInternal + "continued-from"

// ContinueAsNew starts the next run of the calling step's workflow with
// payload, carrying the run's app labels, tenant and LabelContinuedFrom,
// and returns its ref (DE-10): a run that loops for ever (a subscription
// renewed every month, a poller) ends after each round and continues as a
// new run, so its journal, step history and error history stay bounded.
// Return from the step after it; the current run then ends as usual.
//
// The next run's IdempotencyKey is derived from the current run, so a
// retried step never starts two; it needs CapIdempotencyKey. Labels under
// a reserved prefix (schedule, tick, parent) are not carried, since the
// new run was not fired by a schedule or spawned by a parent. client is
// the client the workflow is registered on.
func ContinueAsNew(ctx context.Context, client Client, payload []byte) (RunRef, error) {
	if client == nil {
		return RunRef{}, Permanent(invalidf("ContinueAsNew: client is nil"))
	}
	if !client.Capabilities().Has(CapIdempotencyKey) {
		return RunRef{}, Permanent(&UnsupportedError{Field: "ContinueAsNew", Missing: CapIdempotencyKey, Reason: "a retried step could start two next runs"})
	}
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return RunRef{}, Permanent(invalidf("ContinueAsNew: not called from a step"))
	}
	labels := make(map[string]string, len(info.Labels)+1)
	for k, v := range maps.All(info.Labels) {
		if _, reserved := ReservedLabel(k); !reserved {
			labels[k] = v
		}
	}
	labels[LabelContinuedFrom] = info.RunID.String()
	job := Job{
		Workflow:       info.Workflow,
		Payload:        payload,
		Labels:         labels,
		Tenant:         info.Tenant,
		IdempotencyKey: "continue/" + info.RunID.String(),
	}
	ref, err := client.Enqueue(AllowReservedLabels(ctx, LabelContinuedFrom), job)
	if err != nil {
		return RunRef{}, fmt.Errorf("workflow: continue as new: %w", err)
	}
	return ref, nil
}

// ContinuedFrom returns the run the calling step's run continues
// (LabelContinuedFrom), and false for a run not started by ContinueAsNew.
func ContinuedFrom(ctx context.Context) (RunID, bool) {
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return "", false
	}
	id, ok := info.Labels[LabelContinuedFrom]
	return RunID(id), ok && id != ""
}

// CancelSteps withdraws several steps of one run (WC-25, cancel region):
// each listed step that is waiting or running is cancelled through
// StepCanceller, as CancelStep does for one, and the rest of the run goes
// on. It tries every step and returns their errors joined; a step already
// finished or not started is the driver's answer for that step (see
// StepCanceller). A client without StepCanceller is refused with an
// UnsupportedError.
func CancelSteps(ctx context.Context, client Client, ref RunRef, steps ...string) error {
	sc, ok := client.(StepCanceller)
	if client == nil || !ok {
		return &UnsupportedError{Field: "CancelSteps", Reason: "the driver cannot cancel one step of a run"}
	}
	if len(steps) == 0 {
		return invalidf("CancelSteps: no steps")
	}
	var errs []error
	for _, s := range steps {
		if err := sc.CancelStep(ctx, ref, s); err != nil {
			errs = append(errs, fmt.Errorf("step %q: %w", s, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("workflow: cancel steps: %w", err)
	}
	return nil
}
