package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestHandler_EditRun: POST /runs/{id}/edit changes a scheduled run's
// payload and priority through RunEditor, the Authorizer sees the edit,
// and a run that is not scheduled is a 409.
func TestHandler_EditRun(t *testing.T) {
	c := memory.New()
	got := make(chan string, 1)
	if err := c.Register(workflow.Workflow{Name: "remind", Steps: []workflow.Step{{Name: "mail", Run: func(_ context.Context, in []byte) ([]byte, error) {
		got <- string(in)
		return nil, nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Payload: []byte(`{"to":"old"}`), RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	var seen *workflow.JobEdit
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error {
		if req.Action == ActionEditRun {
			seen = req.Resource.Edit
		}
		return nil
	}
	h := NewHandler(New(c), auth)
	path := "/runs/" + string(ref.ScheduledID) + "/edit?scheduled=1" // a delayed run is named by its ScheduledID
	if rec := do(t, h, "POST", path, `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty edit = %d %s, want 400", rec.Code, rec.Body)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rec := do(t, h, "POST", path, `{"payload":{"to":"new"},"priority":5,"run_at":"`+now+`"}`)
	if rec.Code != 200 {
		t.Fatalf("edit = %d %s", rec.Code, rec.Body)
	}
	if seen == nil || seen.Priority == nil || *seen.Priority != 5 || string(*seen.Payload) != `{"to":"new"}` {
		t.Fatalf("Authorizer saw %+v", seen)
	}
	var out struct {
		Ref refJSON `json:"ref"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	select {
	case in := <-got:
		if in != `{"to":"new"}` {
			t.Fatalf("ran with %s", in)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the edited run did not run")
	}
	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, h, "POST", path, `{"priority":1}`); rec.Code != http.StatusConflict {
		t.Fatalf("edit after it ran = %d %s, want 409", rec.Code, rec.Body)
	}
	if _, err := New(bareClient{caps: allCaps}).EditRun(ctx, ref, workflow.JobEdit{Priority: new(int)}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("edit on a bare client = %v", err)
	}
}
