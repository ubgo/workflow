// Command admin-tenants serves the admin JSON API to an app's customers,
// each confined to its own tenant with admin.WithTenantScope: a
// customer's run list holds only its runs, another tenant's run answers
// 404, a filter naming another tenant and the system-wide routes answer
// 403, a bulk cancel touches only the caller's runs, and an operator
// given admin.AllTenants sees everything.
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

var auth = admin.Auth[user]{
	Identify: func(r *http.Request) (user, error) {
		u, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			return user{}, errors.New("no session")
		}
		return u, nil
	},
	Authorize: func(context.Context, admin.Request[user]) error { return nil },
}

// scope confines a customer to its tenant and lets an operator see all.
var scope = admin.WithTenantScope(func(_ *http.Request, u user) (string, error) {
	if u.Tenant == "" {
		return admin.AllTenants, nil
	}
	return u.Tenant, nil
})

// snippet:end

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
	for _, t := range []string{"acme", "acme", "globex"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Tenant: t})
		if err != nil {
			return err
		}
		refs[t] = ref
	}
	if err := waitWaiting(ctx, c, 3); err != nil {
		return err
	}

	// snippet:mount
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth, scope)))
	// snippet:end
	srv := httptest.NewServer(mux)
	defer srv.Close()
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
	listed := func(token string) (int, string) {
		code, body := call("GET", "/admin/runs", token, "")
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
	code, _ = call("GET", "/admin/runs/"+string(refs["globex"].RunID), "t-acme", "")
	fmt.Println("acme GET globex's run:", code)
	code, _ = call("GET", "/admin/runs?tenant=globex", "t-acme", "")
	fmt.Println("acme GET /runs?tenant=globex:", code)
	code, _ = call("GET", "/admin/queues", "t-acme", "")
	fmt.Println("acme GET /queues:", code)
	_, body := call("GET", "/admin/capabilities", "t-acme", "")
	var caps struct {
		Tenant string `json:"tenant"`
	}
	if err := json.Unmarshal([]byte(body), &caps); err != nil {
		return err
	}
	fmt.Println("acme GET /capabilities: tenant", caps.Tenant)

	fmt.Println("-- A bulk action stays in the tenant")
	code, body = call("POST", "/admin/runs/cancel", "t-acme", `{"workflow":"export","statuses":["waiting"]}`)
	fmt.Println("acme POST /runs/cancel:", code, body)
	st, err := c.Status(ctx, refs["globex"])
	if err != nil {
		return err
	}
	fmt.Println("globex's run:", st)

	fmt.Println("-- An operator sees every tenant")
	code, ts = listed("t-ops")
	fmt.Printf("ops GET /runs: %d, tenants %s\n", code, ts)
	code, _ = call("GET", "/admin/queues", "t-ops", "")
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
		case <-time.After(5 * time.Millisecond):
		}
	}
}
