package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ubgo/workflow"
)

// ResetRun re-runs a finished run in place from step (DE-14): step and
// every step downstream of it run again, the other steps that succeeded
// keep their outputs; see workflow.RunResetter. With opts.Succeeded a run
// that succeeded is reset too (workflow.RunResetterWith), which needs
// OpResetSucceeded; without it only a failed run is accepted, which needs
// OpResetRun. It refuses an empty ref or step (ErrInvalid) and a client
// without the interface (UnsupportedError); a run in another state
// returns ErrWrongState.
//
//ds:def id=admin-admin-resetrun-exh5rehg owner=@khanakia stability=stable
func (a *Admin) ResetRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ResetOptions) (workflow.RunRef, error) {
	op := OpResetRun
	if opts.Succeeded {
		op = OpResetSucceeded
	}
	if err := a.check(op); err != nil {
		return workflow.RunRef{}, err
	}
	if ref.IsZero() {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if step == "" {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: reset needs a step", workflow.ErrInvalid)
	}
	if opts.Succeeded {
		return a.c.(workflow.RunResetterWith).ResetRunWith(ctx, ref, step, opts)
	}
	return a.c.(workflow.RunResetter).ResetRun(ctx, ref, step)
}

// ForkRun starts a new run from a finished one at step, with the outputs
// of the steps before it, and returns the new run's ref (DE-14); see
// workflow.RunForker. The original is not changed. It refuses an empty
// ref or step and reserved labels in opts (ErrInvalid), and a client
// without RunForker (UnsupportedError); a run that has not finished
// returns ErrWrongState.
//
//ds:def id=admin-admin-forkrun-zxc24dj7 owner=@khanakia stability=stable
func (a *Admin) ForkRun(ctx context.Context, ref workflow.RunRef, step string, opts workflow.ForkOptions) (workflow.RunRef, error) {
	if err := a.check(OpForkRun); err != nil {
		return workflow.RunRef{}, err
	}
	if ref.IsZero() {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if err := workflow.CheckFork(step, opts); err != nil {
		return workflow.RunRef{}, err
	}
	return a.c.(workflow.RunForker).ForkRun(ctx, ref, step, opts)
}

// SignalWaiting delivers ev only to the runs waiting for it now and
// returns how many it resumed (WC-23); see workflow.TransientSignaller.
// Zero is not an error. It refuses an event without a name (ErrInvalid)
// and a client without TransientSignaller (UnsupportedError).
//
//ds:def id=admin-admin-signalwaiting-nu8n857v owner=@khanakia stability=stable
func (a *Admin) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	if err := a.check(OpSignalWaiting); err != nil {
		return 0, err
	}
	if ev.Name == "" {
		return 0, fmt.Errorf("%w: admin: event name is empty", workflow.ErrInvalid)
	}
	return a.c.(workflow.TransientSignaller).SignalWaiting(ctx, ev)
}

// resetJSON is the body of POST /runs/{id}/reset.
type resetJSON struct {
	Step      string `json:"step"`
	Succeeded bool   `json:"succeeded,omitempty"`
}

func (h *handler[P]) resetRun(w http.ResponseWriter, r *http.Request) {
	var body resetJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	ref := pathRef(r)
	opts := workflow.ResetOptions{Succeeded: body.Succeeded}
	if !h.authorize(w, r, ActionResetRun, Resource{Run: ref, Step: body.Step, Reset: &opts}) {
		return
	}
	out, err := h.a.ResetRun(r.Context(), ref, body.Step, opts)
	if err != nil {
		h.fail(w, ActionResetRun, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
	}{toRefJSON(out)})
}

// forkJSON is the body of POST /runs/{id}/fork.
type forkJSON struct {
	Step           string            `json:"step"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

func (h *handler[P]) forkRun(w http.ResponseWriter, r *http.Request) {
	var body forkJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	ref := pathRef(r)
	opts := workflow.ForkOptions{IdempotencyKey: body.IdempotencyKey, Labels: body.Labels}
	if !h.authorize(w, r, ActionForkRun, Resource{Run: ref, Step: body.Step, Fork: &opts}) {
		return
	}
	ctx, outcome := workflow.WithEnqueueOutcome(r.Context())
	out, err := h.a.ForkRun(ctx, ref, body.Step, opts)
	if err != nil {
		h.fail(w, ActionForkRun, err)
		return
	}
	if tenant, scoped := TenantFrom(r.Context()); scoped {
		// The fork keeps the original's tenant, but an idempotency key is
		// not per tenant: a key another tenant's unfinished run holds
		// returns that run, which a scoped caller must not learn.
		info, err := h.a.Run(r.Context(), out)
		if err == nil && info.Tenant != tenant {
			err = fmt.Errorf("%w: admin: the idempotency key is held by a run outside this tenant; nothing was started", workflow.ErrDuplicate)
		}
		if err != nil {
			h.fail(w, ActionForkRun, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
		// Duplicate is true when the idempotency key was held by an
		// unfinished run, which ref then names: nothing new started.
		Duplicate bool `json:"duplicate,omitempty"`
	}{toRefJSON(out), outcome.Duplicate})
}

// signalWaitingJSON is the body of POST /signals/waiting. Payload is the
// event's payload as JSON (a payload that is not JSON cannot be sent over
// HTTP); left out, the event has none.
type signalWaitingJSON struct {
	Name    string          `json:"name"`
	Key     string          `json:"key,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (h *handler[P]) signalWaiting(w http.ResponseWriter, r *http.Request) {
	var body signalWaitingJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	ev := workflow.Event{Name: body.Name, Key: body.Key}
	if len(body.Payload) > 0 {
		ev.Payload = []byte(body.Payload)
	}
	if !h.authorize(w, r, ActionSignalWaiting, Resource{Event: &ev}) {
		return
	}
	n, err := h.a.SignalWaiting(r.Context(), ev)
	if err != nil {
		h.fail(w, ActionSignalWaiting, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Resumed int `json:"resumed"`
	}{n})
}
