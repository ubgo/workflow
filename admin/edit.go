package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ubgo/workflow"
)

// EditRun changes a scheduled run's payload, priority or start; see
// workflow.RunEditor. It refuses an empty ref or an edit that changes
// nothing (ErrInvalid), and a client without RunEditor
// (UnsupportedError).
//
//ds:def id=admin-admin-editrun-yxk3t4zu owner=@khanakia stability=stable
func (a *Admin) EditRun(ctx context.Context, ref workflow.RunRef, edit workflow.JobEdit) (workflow.RunRef, error) {
	if err := a.check(OpEditRun); err != nil {
		return workflow.RunRef{}, err
	}
	if ref.IsZero() {
		return workflow.RunRef{}, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if err := workflow.CheckJobEdit(edit); err != nil {
		return workflow.RunRef{}, err
	}
	return a.c.(workflow.RunEditor).EditRun(ctx, ref, edit)
}

// editJSON is the body of POST /runs/{id}/edit. Payload is the new
// payload as JSON (a JSON payload is the common case; a payload that is
// not JSON cannot be set over HTTP); a field left out is not changed.
type editJSON struct {
	Payload  json.RawMessage `json:"payload,omitempty"`
	Priority *int            `json:"priority,omitempty"`
	RunAt    *time.Time      `json:"run_at,omitempty"`
}

//ds:def id=admin-handler-editrun-bzau6dfy owner=@khanakia stability=stable
func (h *handler[P]) editRun(w http.ResponseWriter, r *http.Request) {
	var body editJSON
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalid, err.Error())
		return
	}
	edit := workflow.JobEdit{Priority: body.Priority, RunAt: body.RunAt}
	if len(body.Payload) > 0 {
		p := []byte(body.Payload)
		edit.Payload = &p
	}
	ref := pathRef(r)
	if !h.authorize(w, r, ActionEditRun, Resource{Run: ref, Edit: &edit}) {
		return
	}
	out, err := h.a.EditRun(r.Context(), ref, edit)
	if err != nil {
		h.fail(w, ActionEditRun, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Ref refJSON `json:"ref"`
	}{toRefJSON(out)})
}
