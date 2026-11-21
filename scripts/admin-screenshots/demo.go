package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/memory"
)

// demoRuns are the runs the screenshots open.
type demoRuns struct {
	Succeeded workflow.RunRef // an invoice-sync run that finished
	Waiting   workflow.RunRef // an order-approval run parked on its wait step
}

// operator is the demo's principal: everyone who logs in is an admin.
type operator struct{ Name string }

// sessionCookie is the cookie the demo's login route sets and Identify
// reads, as an app's own session would be.
const sessionCookie = "session"

// demo starts an in-memory client with dummy data and returns the app's
// mux (the admin handler under /admin/ and a stand-in /login), the runs
// the screenshots show, and a stop function. Nothing in it is real: the
// tenants, invoice and order numbers are made up.
func demo(ctx context.Context) (http.Handler, demoRuns, func(), error) {
	// A fixed node name, so no screenshot shows the capturing machine's host.
	c := memory.New(memory.WithNodeID("worker-1"))
	step := func(out string) workflow.Handler {
		return func(context.Context, []byte) ([]byte, error) { return []byte(out), nil }
	}
	wfs := []workflow.Workflow{
		{
			Name: "invoice-sync",
			Steps: []workflow.Step{
				{Name: "fetch", Run: step(`{"lines":12}`)},
				{Name: "transform", After: []string{"fetch"}, Run: step(`{"total":"1840.00"}`)},
				{Name: "upload", After: []string{"transform"}, Run: step(`{"uploaded":true}`)},
			},
		},
		{
			Name: "order-approval",
			Steps: []workflow.Step{
				{Name: "reserve", Run: step(`{"reserved":true}`)},
				{Name: "approval", After: []string{"reserve"}, Wait: &workflow.WaitSpec{Event: "order.approved", Key: "order-7731"}},
				{Name: "ship", After: []string{"approval"}, Run: step(`{"shipped":true}`)},
			},
		},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return nil, demoRuns{}, nil, err
		}
	}
	if err := c.Start(ctx); err != nil {
		return nil, demoRuns{}, nil, err
	}
	fail := func(err error) (http.Handler, demoRuns, func(), error) {
		c.Stop()
		return nil, demoRuns{}, nil, err
	}

	var runs demoRuns
	for i, tenant := range []string{"acme", "acme-eu", "globex"} {
		ref, err := c.Enqueue(ctx, workflow.Job{
			Workflow: "invoice-sync",
			Payload:  fmt.Appendf(nil, `{"invoice":"INV-%d"}`, 1040+i),
			Labels:   map[string]string{"tenant": tenant},
		})
		if err != nil {
			return fail(err)
		}
		if i == 0 {
			runs.Succeeded = ref
		}
	}
	ref, err := c.Enqueue(ctx, workflow.Job{
		Workflow: "order-approval",
		Payload:  []byte(`{"order":"order-7731"}`),
		Labels:   map[string]string{"tenant": "acme"},
	})
	if err != nil {
		return fail(err)
	}
	runs.Waiting = ref
	if err := awaitStatus(ctx, c, runs.Succeeded, workflow.StatusSucceeded); err != nil {
		return fail(err)
	}
	if err := awaitStatus(ctx, c, runs.Waiting, workflow.StatusWaiting); err != nil {
		return fail(err)
	}

	auth := admin.Auth[operator]{
		Identify: func(r *http.Request) (operator, error) {
			ck, err := r.Cookie(sessionCookie)
			if err != nil || ck.Value == "" {
				return operator{}, errors.New("no session")
			}
			return operator{Name: ck.Value}, nil
		},
		Authorize: func(context.Context, admin.Request[operator]) error { return nil },
	}
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth, admin.WithPayloads())))
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "demo", Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		http.Redirect(w, r, "/admin/", http.StatusFound)
	})
	return mux, runs, c.Stop, nil
}

// awaitStatus polls the run until it reaches want, for up to ten seconds.
func awaitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		got, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if got == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %v is %s, want %s", ref.RunID, got, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
