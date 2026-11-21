// Command edit-run shows workflow.RunEditor: a reminder scheduled for
// tomorrow is fixed before it runs (a new address, a higher priority) and
// moved to now, on the in-memory driver. A second reminder is then edited
// through the admin module: Admin.EditRun in Go, and POST /runs/{id}/edit
// behind an Authorizer that lets support staff move a run but not change
// its payload.
//
// It is the program behind docs/jobs/edit-runs.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/memory"
)

// operatorHeader names the caller in this example's admin requests. A real
// app identifies callers from a session cookie or a bearer token.
const operatorHeader = "X-Operator"

// roleLead is the only operator allowed to change a scheduled run's
// payload over the admin API; everyone else may move it or reprioritise it.
const roleLead = "lead"

// remindIn is how far ahead each reminder is scheduled: a day, so it is
// still waiting (and so editable) when the example edits it.
const remindIn = 24 * time.Hour

// awaitPoll is how often Await re-reads the run: a few milliseconds keeps
// the in-memory demo fast without spinning.
const awaitPoll = 2 * time.Millisecond

// snippet:admin-auth
// auth lets every named operator edit a scheduled run's time and priority,
// and only a lead change its payload: the Authorizer sees the edit itself
// in Resource.Edit, so it can judge each field.
var auth = admin.Auth[string]{
	Identify: func(r *http.Request) (string, error) {
		if who := r.Header.Get(operatorHeader); who != "" {
			return who, nil
		}
		return "", errors.New("no operator")
	},
	Authorize: func(_ context.Context, req admin.Request[string]) error {
		if req.Action == admin.ActionEditRun && req.Resource.Edit.Payload != nil && req.Who != roleLead {
			return fmt.Errorf("%s may move this run but not change its payload", req.Who)
		}
		return nil
	},
}

// snippet:end

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "edit-run:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()
	sent := make(chan string, 1)
	if err := c.Register(workflow.Workflow{Name: "remind", Steps: []workflow.Step{{Name: "mail", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		sent <- string(in)
		return nil, nil
	}}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Payload: []byte(`{"to":"old@example.com"}`), RunAt: time.Now().Add(remindIn)})
	if err != nil {
		return err
	}

	// snippet:edit
	editor, ok := workflow.Client(c).(workflow.RunEditor)
	if !ok {
		return errors.New("this driver cannot edit runs")
	}
	payload := []byte(`{"to":"new@example.com"}`)
	priority := 10 // above the default 0: it runs ahead of waiting work
	now := time.Now()
	ref, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload, Priority: &priority, RunAt: &now})
	if err != nil {
		return err // ErrWrongState once the run has started
	}
	// snippet:end
	fmt.Println("sent to", <-sent)

	if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: awaitPoll}); err != nil {
		return err
	}
	_, err = editor.EditRun(ctx, ref, workflow.JobEdit{Payload: &payload})
	fmt.Println("editing it after it ran:", errors.Is(err, workflow.ErrWrongState))

	tomorrow := time.Now().Add(remindIn)
	second, err := c.Enqueue(ctx, workflow.Job{Workflow: "remind", Payload: []byte(`{"to":"ann@example.com"}`), RunAt: tomorrow})
	if err != nil {
		return err
	}
	// snippet:admin-go
	ops := admin.New(c)
	urgent := 20 // above the first reminder's 10
	second, err = ops.EditRun(ctx, second, workflow.JobEdit{Priority: &urgent})
	if err != nil {
		return err // UnsupportedError on a client without RunEditor
	}
	// snippet:end
	info, err := ops.Run(ctx, second)
	if err != nil {
		return err
	}
	fmt.Println("admin edit: priority", info.Priority, "status", info.Status)

	srv := httptest.NewServer(admin.NewHandler(ops, auth))
	defer srv.Close()
	// snippet:admin-http
	// edit posts one edit to a run as operator who and returns the status.
	edit := func(who string, ref workflow.RunRef, body string) (int, error) {
		path := "/runs/" + string(ref.RunID) + "/edit"
		if ref.RunID.IsZero() { // a delayed run not yet started has only a ScheduledID
			path = "/runs/" + string(ref.ScheduledID) + "/edit?scheduled=1"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, strings.NewReader(body))
		if err != nil {
			return 0, err
		}
		req.Header.Set(operatorHeader, who)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		defer res.Body.Close()
		_, err = io.Copy(io.Discard, res.Body)
		return res.StatusCode, err
	}
	in48h := tomorrow.Add(24 * time.Hour).UTC().Format(time.RFC3339) // one more day out
	moved, err := edit("support", second, `{"run_at":"`+in48h+`"}`)
	if err != nil {
		return err
	}
	readdressed, err := edit("support", second, `{"payload":{"to":"bob@example.com"}}`)
	if err != nil {
		return err
	}
	byLead, err := edit(roleLead, second, `{"payload":{"to":"bob@example.com"}}`)
	if err != nil {
		return err
	}
	finished, err := edit(roleLead, ref, `{"priority":1}`) // the first reminder already ran
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Println("POST /runs/{id}/edit: support moves it", moved, "support readdresses it", readdressed,
		"lead readdresses it", byLead, "a finished run", finished)
	info, err = ops.Run(ctx, second)
	if err != nil {
		return err
	}
	fmt.Println("after the HTTP edits: due in", info.RunAt.Sub(tomorrow).Round(time.Hour), "more, priority", info.Priority, "same ref", info.Ref == second)
	return nil
}
