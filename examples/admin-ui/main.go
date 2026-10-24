// Command admin-ui mounts the admin web UI in an app: Identify reads the
// app's session cookie (a browser sends no Authorization header when it
// opens a page), a viewer sees the screens and is refused the buttons,
// and the page carries a strict Content-Security-Policy. It checks the
// behaviour through HTTP calls; set ADMIN_UI_ADDR (for example
// 127.0.0.1:8090) to keep the server running and open it in a browser.
//
// It is the program behind docs/admin/ui.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Getenv("ADMIN_UI_ADDR")); err != nil {
		fmt.Fprintln(os.Stderr, "admin-ui:", err)
		os.Exit(1)
	}
}

// snippet:auth
// user is the app's own principal type.
type user struct {
	Name string
	Role string // "admin" may do anything, "viewer" may only read
}

// sessions stands in for the app's session store, keyed by cookie value.
var sessions = map[string]user{"s-ann": {"ann", "admin"}, "s-vic": {"vic", "viewer"}}

var auth = admin.Auth[user]{
	// The browser opening the UI sends the app's session cookie, not an
	// Authorization header, so Identify reads the cookie.
	Identify: func(r *http.Request) (user, error) {
		ck, err := r.Cookie("session")
		if err != nil {
			return user{}, errors.New("no session")
		}
		u, ok := sessions[ck.Value]
		if !ok {
			return user{}, errors.New("unknown session")
		}
		return u, nil
	},
	// ui.read (admin.ActionUI) is read-only, so a viewer can open the UI;
	// every button the UI shows is a JSON request authorized on its own.
	Authorize: func(_ context.Context, req admin.Request[user]) error {
		if req.Who.Role == "admin" || req.Action.ReadOnly() {
			return nil
		}
		return fmt.Errorf("%s may not %s", req.Who.Name, req.Action)
	},
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
// A non-empty addr keeps the server up on that address until interrupted.
func run(ctx context.Context, out io.Writer, addr string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := memory.New()
	if err := c.Register(workflow.Workflow{
		Name:  "nightly-report",
		Cron:  []string{"0 2 * * *"},
		Steps: []workflow.Step{{Name: "build", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	for _, tenant := range []string{"acme", "acme-eu", "globex"} {
		job := workflow.Job{Workflow: "nightly-report", Labels: map[string]string{"tenant": tenant}}
		if _, err := c.Enqueue(ctx, job); err != nil {
			return err
		}
	}

	// snippet:mount
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth)))
	// Open /admin/ in a browser: it redirects to /admin/ui/.
	// snippet:end
	// A stand-in for the app's login page, for trying the UI in a browser:
	// /login?as=s-ann (admin) or /login?as=s-vic (viewer).
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: r.URL.Query().Get("as"), Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		http.Redirect(w, r, "/admin/", http.StatusFound)
	})

	srv := httptest.NewUnstartedServer(mux)
	if addr != "" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		srv.Listener = l
	}
	srv.Start()
	defer srv.Close()
	client := srv.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	call := func(method, path, session string) (*http.Response, string, error) {
		req, err := http.NewRequestWithContext(ctx, method, srv.URL+path, nil)
		if err != nil {
			return nil, "", err
		}
		if session != "" {
			req.AddCookie(&http.Cookie{Name: "session", Value: session})
		}
		if method != http.MethodGet {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp, string(b), err
	}

	resp, _, err := call("GET", "/admin/ui/", "")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "anonymous GET /admin/ui/:", resp.StatusCode)

	resp, _, err = call("GET", "/admin/", "s-vic")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "viewer GET /admin/: %d to %s\n", resp.StatusCode, resp.Header.Get("Location"))

	resp, body, err := call("GET", "/admin/ui/", "s-vic")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "viewer GET /admin/ui/: %d %s, page %v\n", resp.StatusCode, resp.Header.Get("Content-Type"), strings.Contains(body, "<html"))
	fmt.Fprintln(out, "CSP:", resp.Header.Get("Content-Security-Policy"))
	fmt.Fprintln(out, "X-Frame-Options:", resp.Header.Get("X-Frame-Options"))

	// snippet:count
	// What the runs screen asks for: a label prefix and a total for the pages.
	_, body, err = call("GET", "/admin/runs?label=tenant=acme*&count=1&limit=50", "s-vic")
	// snippet:end
	if err != nil {
		return err
	}
	var page struct {
		Runs  []json.RawMessage `json:"runs"`
		Total *int              `json:"total"`
	}
	if err := json.Unmarshal([]byte(body), &page); err != nil || page.Total == nil {
		return fmt.Errorf("runs: %s", body)
	}
	fmt.Fprintf(out, "viewer runs tenant=acme*: %d runs, total %d\n", len(page.Runs), *page.Total)

	_, body, err = call("GET", "/admin/schedules", "s-vic")
	if err != nil {
		return err
	}
	var scheds struct {
		Schedules []struct {
			ID string `json:"id"`
		} `json:"schedules"`
	}
	if err := json.Unmarshal([]byte(body), &scheds); err != nil || len(scheds.Schedules) != 1 {
		return fmt.Errorf("schedules: %s", body)
	}
	id := scheds.Schedules[0].ID
	resp, _, err = call("POST", "/admin/schedules/"+id+"/pause", "s-vic")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "viewer pauses the schedule:", resp.StatusCode)
	resp, _, err = call("POST", "/admin/schedules/"+id+"/pause", "s-ann")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "admin pauses the schedule:", resp.StatusCode)

	if addr != "" {
		fmt.Fprintf(out, "open %s/login?as=s-ann (or s-vic for a viewer); Ctrl-C to stop\n", srv.URL)
		sig, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()
		<-sig.Done()
	}
	return nil
}
