// Command admin-cli drives the workflow command line client against a real
// admin JSON API: an httptest server runs admin.NewHandler over a memory
// client behind a bearer token, and cli.Run, the whole CLI as a function,
// sends it commands: a bulk retry previewed and done, the health report,
// the online nodes as JSON, a drain, and the exit codes of a refused call
// and a usage error.
//
// It is the program behind docs/admin/cli.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/cli"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-cli:", err)
		os.Exit(1)
	}
}

// opsToken is the one token the example server accepts.
const opsToken = "ops-token"

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	c := memory.New(memory.WithClock(clk), memory.WithNodeID("worker-1"), memory.WithWorkers(4))
	var down atomic.Bool
	down.Store(true)
	if err := c.Register(workflow.Workflow{Name: "send-email", Steps: []workflow.Step{{Name: "send",
		Run: func(context.Context, []byte) ([]byte, error) {
			if down.Load() {
				return nil, workflow.Permanent(errors.New("smtp: connection refused"))
			}
			return nil, nil
		}}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	for range 3 {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email"}); err != nil {
			return err
		}
	}
	if _, err := c.RunAll(ctx); err != nil {
		return err
	}
	down.Store(false)

	auth := admin.Auth[string]{
		Identify: func(r *http.Request) (string, error) {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(opsToken)) != 1 {
				return "", errors.New("no session")
			}
			return "ops", nil
		},
		Authorize: func(context.Context, admin.Request[string]) error { return nil },
	}
	srv := httptest.NewServer(http.StripPrefix("/admin", admin.NewHandler(admin.New(c), auth)))
	defer srv.Close()

	// snippet:embed
	env := map[string]string{
		cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
		cli.EnvToken: opsToken,           // sent as a bearer token, never printed
	}
	workflowCmd := func(args ...string) int {
		return cli.Run(ctx, args, cli.Options{
			Stdout: out,
			Stderr: out,
			Getenv: func(k string) string { return env[k] },
			Name:   "workflow",
		})
	}
	// snippet:end

	cmd := func(args ...string) {
		fmt.Fprintln(out, "$ workflow", strings.Join(args, " "))
		code := workflowCmd(args...)
		fmt.Fprintln(out, "exit", code)
	}
	cmd("retry", "--all", "--workflow", "send-email", "--dry-run")
	cmd("retry", "--all", "--workflow", "send-email")
	if _, err := c.RunAll(ctx); err != nil {
		return err
	}
	cmd("health")
	cmd("nodes", "--json")
	cmd("drain", "--wait", "1m")
	env[cli.EnvToken] = "wrong"
	cmd("failures")
	cmd("retyr")
	return nil
}
