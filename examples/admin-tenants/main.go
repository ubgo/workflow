// Command admin-tenants serves the admin JSON API to an app's customers,
// each confined to its own tenant with admin.WithTenantScope: a
// customer's run list holds only its runs, another tenant's run answers
// 404, a filter naming another tenant and the system-wide routes answer
// 403, an Authorize that reads admin.TenantFrom lets a customer cancel
// but not delete, a bulk cancel touches only the caller's runs, and an
// operator given admin.AllTenants sees everything.
//
// It is the program behind docs/admin/tenants.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
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

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "admin-tenants:", err)
		os.Exit(1)
	}
}

// snippet:scope
// user is the app's own principal type: a customer's user carries its
// tenant, an operator none.
type user struct {
	Name   string
	Tenant string
}

// tokens stands in for the app's session or API-token store.
var tokens = map[string]user{
	"t-acme":   {"ann", "acme"},
	"t-globex": {"gil", "globex"},
	"t-ops":    {"oz", ""},
}

// auth identifies a request's user by its bearer token and checks each
// action with authorize.
var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: authorize,
}

// scope confines a customer to its tenant and lets an operator see all.
var scope = admin.WithTenantScope(func(_ *http.Request, u user) (string, error) {
	if u.Tenant == "" {
		return admin.AllTenants, nil
	}
	return u.Tenant, nil
})

// snippet:end

// snippet:authorize
// authorize lets a customer's users read their tenant's runs and cancel
// them, and nothing else; an operator may do anything. The scope has
// already confined the request to the tenant: TenantFrom says which one,
// and false for an operator given AllTenants.
func authorize(_ context.Context, req admin.Request[user]) error {
	tenant, scoped := admin.TenantFrom(req.HTTP.Context())
	if !scoped || req.Action.ReadOnly() {
		return nil
	}
	switch req.Action {
	case admin.ActionCancelRun, admin.ActionCancelRuns:
		return nil
	}
	return fmt.Errorf("%s users may not %s", tenant, req.Action)
}

// snippet:end

// pollInterval is how often waitWaiting re-reads the run list: the
// in-memory driver parks these runs within milliseconds.
const pollInterval = 5 * time.Millisecond

// export waits for an event, so its runs stay open while the program
// calls the API.
var export = workflow.Workflow{Name: "export", Steps: []workflow.Step{{
	Name: "wait", Wait: &workflow.WaitSpec{Event: "approved"},
}}}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := memory.New() // declares workflow.CapTenant, which the scope needs
	if err := c.Register(export); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	refs := map[string]workflow.RunRef{}
	tenants := []string{"acme", "acme", "globex"} // one run per entry
	for _, t := range tenants {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Tenant: t})
		if err != nil {
			return err
		}
		refs[t] = ref
	}
	if err := waitWaiting(ctx, c, len(tenants)); err != nil {
		return err
	}

	// snippet:mount
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth, scope)))
	// snippet:end
	srv := httptest.NewServer(mux)
	defer srv.Close()
	// call sends one admin API request as the user of token and returns
	// the status code and the trimmed body.
	call := func(method, path, token, body string) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		if method != http.MethodGet {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			return 0, err.Error()
		}
		b, err := io.ReadAll(resp.Body)
		if err = errors.Join(err, resp.Body.Close()); err != nil {
			return 0, err.Error()
		}
		return resp.StatusCode, strings.TrimSpace(string(b))
	}
	// listed lists runs as the user of token and returns the status code
	// and the tenant of each run listed, comma-separated.
	listed := func(token string) (int, string) {
		code, body := call(http.MethodGet, "/admin/runs", token, "")
		var page struct {
			Runs []struct {
				Tenant string `json:"tenant"`
			} `json:"runs"`
		}
		_ = json.Unmarshal([]byte(body), &page)
		var ts []string
		for _, r := range page.Runs {
			ts = append(ts, r.Tenant)
		}
		return code, strings.Join(ts, ",")
	}

	fmt.Println("-- A customer sees its own tenant")
	code, ts := listed("t-acme")
	fmt.Printf("acme GET /runs: %d, tenants %s\n", code, ts)
	code, _ = call(http.MethodGet, "/admin/runs/"+string(refs["globex"].RunID), "t-acme", "")
	fmt.Println("acme GET globex's run:", code)
	code, _ = call(http.MethodGet, "/admin/runs?tenant=globex", "t-acme", "")
	fmt.Println("acme GET /runs?tenant=globex:", code)
	code, _ = call(http.MethodGet, "/admin/queues", "t-acme", "")
	fmt.Println("acme GET /queues:", code)
	_, body := call(http.MethodGet, "/admin/capabilities", "t-acme", "")
	var caps struct {
		Tenant string `json:"tenant"`
	}
	if err := json.Unmarshal([]byte(body), &caps); err != nil {
		return err
	}
	fmt.Println("acme GET /capabilities: tenant", caps.Tenant)

	code, body = call(http.MethodDelete, "/admin/runs/"+string(refs["acme"].RunID), "t-acme", "")
	fmt.Println("acme DELETE its own run:", code, body)
	fmt.Println("-- A bulk action stays in the tenant")
	code, body = call(http.MethodPost, "/admin/runs/cancel", "t-acme", `{"workflow":"export","statuses":["waiting"]}`)
	fmt.Println("acme POST /runs/cancel:", code, body)
	st, err := c.Status(ctx, refs["globex"])
	if err != nil {
		return err
	}
	fmt.Println("globex's run:", st)

	fmt.Println("-- An operator sees every tenant")
	code, ts = listed("t-ops")
	fmt.Printf("ops GET /runs: %d, tenants %s\n", code, ts)
	code, _ = call(http.MethodGet, "/admin/queues", "t-ops", "")
	fmt.Println("ops GET /queues:", code)
	return nil
}

// waitWaiting polls until n runs are waiting.
func waitWaiting(ctx context.Context, c *memory.Client, n int) error {
	for {
		page, err := c.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusWaiting}})
		if err != nil || len(page.Runs) == n {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}
