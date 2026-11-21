// Command cli-tour runs every command of the workflow command line client
// once against a real admin JSON API: an httptest server runs
// admin.NewHandler over a memory client with a fake clock, and cli.Run,
// the whole CLI as a function, is called the way an app embeds it as a
// subcommand of its own binary. It shows the environment variables the CLI
// reads (a bearer token and a cookie header) and each exit code.
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
	"github.com/ubgo/workflow/middleware"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "cli-tour:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant, so every time printed is fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// The credentials the example's Identify accepts: a bearer token for
// scripts and a session cookie, sent through WORKFLOW_ADMIN_HEADER.
const (
	opsToken      = "ops-token"
	sessionCookie = "session=s-ops"
)

// The workflow, queue, step and event names the commands use.
const (
	wfETL       = "etl"
	wfEmail     = "send-email"
	wfCleanup   = "cleanup"
	wfRefresh   = "cache-refresh"
	wfReport    = "nightly-report"
	queueEmails = "emails"
	eventName   = "cache.refresh"
	// regionLabel keys each cache-refresh run's wait, so a signal can
	// reach one region's run and not the others.
	regionLabel = "region"
)

// delayedBy is how far ahead the delayed send-email run is due, so it is
// still waiting to start when it is rescheduled.
const delayedBy = time.Hour

// etlRuns is how many failed etl runs the fixture leaves: one for reset
// and fork, one for inspect.
const etlRuns = 2

// defaultWorkers sizes the default queue's pool: more than one, so the
// queues command shows a real pool next to the one-worker emails queue.
const defaultWorkers = 2

// metricsWindow is the span the metrics command charts: two of its 5m
// steps from the fake clock's start.
const metricsWindow = 10 * time.Minute

// snippet:subcommand
// defaultAdminURL is where this app serves its admin API when
// WORKFLOW_ADMIN_URL is not set.
const defaultAdminURL = "http://127.0.0.1:8080/admin"

// runApp is an app binary's main with the admin CLI as its "admin"
// subcommand: "myapp admin failures" runs "workflow failures" against
// the app's own admin API. It returns the exit code for os.Exit.
func runApp(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 || args[0] != "admin" {
		fmt.Fprintln(stderr, "usage: myapp admin <command> [flags]")
		return cli.ExitUsage
	}
	return cli.Run(ctx, args[1:], cli.Options{
		Stdout: stdout,
		Stderr: stderr,
		Getenv: func(k string) string {
			if v := getenv(k); v != "" {
				return v
			}
			if k == cli.EnvURL {
				return defaultAdminURL
			}
			return ""
		},
		Name: "myapp admin",
	})
}

// snippet:end

// snippet:exit-codes
// exitNames names the CLI's exit codes, so a script's log says what
// happened: 0 done, 1 refused or failed (a 4xx or 5xx answer, a failed
// health check, a drain that timed out), 2 a wrong command line.
var exitNames = map[int]string{
	cli.ExitOK:    "cli.ExitOK",
	cli.ExitError: "cli.ExitError",
	cli.ExitUsage: "cli.ExitUsage",
}

// snippet:end

// fixture is the memory client the routes act on, with the runs each
// route needs, and the admin handlers in front of it.
type fixture struct {
	c        *memory.Client
	clk      *clock.Fake
	broken   *atomic.Bool // the etl transform step fails while true
	smtpDown *atomic.Bool // send-email fails for "bob" while true
	etl      [etlRuns]workflow.RunRef
	emails   map[string]workflow.RunRef // by recipient
	waiting  map[string]workflow.RunRef // cache-refresh runs by region
	schedule workflow.ScheduleID
}

// newFixture registers the workflows, starts the client and leaves it
// with two failed etl runs, three send-email runs (bob's failed), one
// discarded cleanup run and four cache-refresh runs waiting for their
// region's event. Steps log through middleware.OutputLogger, which
// CaptureOutput hands to outs.
func newFixture(ctx context.Context, clk *clock.Fake, outs *admin.MemoryOutputs, mm *admin.MemoryMetrics) (*fixture, error) {
	f := &fixture{
		clk:      clk,
		broken:   &atomic.Bool{},
		smtpDown: &atomic.Bool{},
		emails:   map[string]workflow.RunRef{},
		waiting:  map[string]workflow.RunRef{},
	}
	f.broken.Store(true)
	f.smtpDown.Store(true)
	capture := middleware.CaptureOutput(outs, middleware.CaptureClock(f.clk))
	f.c = memory.New(memory.WithClock(f.clk), memory.WithNodeID("worker-1"), memory.WithWorkers(defaultWorkers),
		// Declared queues are the ones the memory driver reports workers for.
		memory.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}, workflow.QueueConfig{Name: queueEmails, Workers: 1}),
		memory.WithMiddleware(capture))
	if err := f.c.OnRunFinished(mm.Observe); err != nil {
		return nil, err
	}
	// pass is a step that hands its input on unchanged.
	pass := func(_ context.Context, in []byte) ([]byte, error) { return in, nil }
	workflows := []workflow.Workflow{
		{Name: wfETL, Steps: []workflow.Step{
			{Name: "extract", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
				middleware.OutputLogger(ctx, nil).Info("extracted", "rows", 42) // demo data: the rows the step read
				return []byte("42"), nil
			}},
			{Name: "transform", After: []string{"extract"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
				if f.broken.Load() {
					middleware.OutputLogger(ctx, nil).Error("bad date column", "row", 7) // demo data: the row that fails
					return nil, workflow.Permanent(errors.New("transform: bad date in row 7"))
				}
				return in, nil
			}},
			{Name: "publish", After: []string{"transform"}, Run: pass},
		}},
		{Name: wfEmail, Queue: queueEmails, Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
			if string(in) == `"bob"` && f.smtpDown.Load() {
				return nil, workflow.Permanent(errors.New("smtp: connection refused"))
			}
			return nil, nil
		}}}},
		{Name: wfCleanup, Steps: []workflow.Step{{Name: "sweep", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, workflow.Discard(errors.New("nothing to clean"))
		}}}},
		{Name: wfRefresh, Steps: []workflow.Step{
			{Name: "await", Wait: &workflow.WaitSpec{
				Event:   eventName,
				KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels[regionLabel] }),
			}},
			{Name: "reload", After: []string{"await"}, Run: pass},
		}},
		{Name: wfReport, Steps: []workflow.Step{{Name: "build", Run: pass}}},
	}
	for _, wf := range workflows {
		if err := f.c.Register(wf); err != nil {
			return nil, err
		}
	}
	if err := f.c.Start(ctx); err != nil {
		return nil, err
	}
	// Every run is enqueued under a pause, so the IDs the driver hands out
	// do not depend on which runs its workers pick up first.
	if err := f.c.Pause(ctx, workflow.PauseAll()); err != nil {
		return nil, err
	}
	var err error
	if f.schedule, err = f.c.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: wfReport, Cron: "0 2 * * *", Location: time.UTC}); err != nil {
		return nil, err
	}
	for i := range f.etl {
		if f.etl[i], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfETL, Labels: map[string]string{"team": "data-eng"}}); err != nil {
			return nil, err
		}
	}
	for _, to := range []string{"ann", "bob", "cat"} {
		if f.emails[to], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfEmail, Payload: []byte(`"` + to + `"`)}); err != nil {
			return nil, err
		}
	}
	if _, err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfCleanup}); err != nil {
		return nil, err
	}
	for _, region := range []string{"eu", "us", "apac", "latam"} {
		if f.waiting[region], err = f.c.Enqueue(ctx, workflow.Job{Workflow: wfRefresh, Labels: map[string]string{regionLabel: region}}); err != nil {
			return nil, err
		}
	}
	if err := f.c.Resume(ctx, workflow.PauseAll()); err != nil {
		return nil, err
	}
	return f, f.settle(ctx)
}

// settle runs every run that can move until none can, so the next route
// sees a fixed state. The memory driver runs steps on its own goroutines.
func (f *fixture) settle(ctx context.Context) error {
	_, err := f.c.RunAll(ctx)
	return err
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	// The CLI prints table times in the terminal's zone; a fixed zero
	// offset keeps the guide's output the same on every machine. (Not
	// time.UTC itself: a schedule refuses a Location equal to time.Local.)
	time.Local = time.FixedZone("UTC", 0)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clk := clock.NewFake(start)
	outs := admin.NewMemoryOutputs(admin.MemoryOutputLimits{})
	mm := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{Clock: clk})
	f, err := newFixture(ctx, clk, outs, mm)
	if err != nil {
		return err
	}
	defer f.c.Stop()

	auth := admin.Auth[string]{
		// Scripts send the bearer token; a person's shell may send the
		// app's session cookie instead.
		Identify: func(r *http.Request) (string, error) {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(opsToken)) == 1 {
				return "ops", nil
			}
			if ck, err := r.Cookie("session"); err == nil && "session="+ck.Value == sessionCookie {
				return "ops (cookie)", nil
			}
			return "", errors.New("no session")
		},
		Authorize: func(context.Context, admin.Request[string]) error { return nil },
	}
	adm := admin.New(f.c, admin.WithClock(clk), admin.WithOutputs(outs), admin.WithMetrics(mm))
	srv := httptest.NewServer(http.StripPrefix("/admin", admin.NewHandler(adm, auth)))
	defer srv.Close()

	// snippet:env
	env := map[string]string{
		cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
		cli.EnvToken: opsToken,           // sent as a bearer token, never printed
	}
	getenv := func(k string) string { return env[k] } // the CLI reads env here instead of os.Getenv
	// snippet:end

	// cmd prints a command line, runs it through the app's subcommand and
	// prints its exit code with the code's name.
	cmd := func(args ...string) {
		fmt.Fprintln(w, "$ myapp admin", shellJoin(args))
		code := runApp(ctx, append([]string{"admin"}, args...), w, w, getenv)
		fmt.Fprintln(w, "exit", code, exitNames[code])
	}
	// id is the run ID the CLI takes as an argument.
	id := func(ref workflow.RunRef) string { return ref.RunID.String() }
	// settle runs every run that can move until none can.
	settle := func() error { return f.settle(ctx) }

	fmt.Fprintln(w, "## read")
	cmd("caps")
	cmd("runs", "--workflow", wfETL, "--count")
	cmd("failures")
	cmd("discarded")
	cmd("inspect", id(f.etl[1]), "--output")

	fmt.Fprintln(w, "## act on many runs")
	cmd("retry", "--all", "--workflow", wfEmail, "--dry-run")
	cmd("cancel", "--all", "--workflow", wfRefresh, "--status", "waiting", "--dry-run")
	cmd("delete", "--all", "--workflow", wfEmail, "--status", "succeeded", "--dry-run")

	fmt.Fprintln(w, "## act on one run")
	f.smtpDown.Store(false) // the mail server is back
	cmd("retry", id(f.emails["bob"]))
	cmd("cancel", id(f.waiting["us"]))
	cmd("delete", id(f.emails["ann"]))
	cmd("cancel-step", id(f.waiting["apac"]), "await")
	if err := awaitEnd(ctx, f.c, f.waiting["apac"]); err != nil {
		return err
	}
	f.broken.Store(false) // the transform fix is deployed
	cmd("reset", id(f.etl[0]), "--step", "transform")
	if err := settle(); err != nil {
		return err
	}
	cmd("fork", id(f.etl[0]), "--step", "publish", "--key", "replay-etl", "--label", "ticket=OPS-7")
	cmd("signal-waiting", eventName, "--key", "eu", "--payload", `{"reason":"deploy"}`)
	if err := settle(); err != nil {
		return err
	}
	delayed, err := f.c.Enqueue(ctx, workflow.Job{Workflow: wfEmail, Payload: []byte(`"dan"`), RunAt: start.Add(delayedBy)})
	if err != nil {
		return err
	}
	cmd("reschedule", string(delayed.ScheduledID), "--scheduled", "--at", start.Add(2*delayedBy).Format(time.RFC3339)) // one delayedBy later than first due

	fmt.Fprintln(w, "## pauses, queues and schedules")
	cmd("pause", "--queue", queueEmails)
	env[cli.EnvToken] = "" // the next call authenticates with the cookie header only
	env[cli.EnvHeader] = "Cookie: " + sessionCookie
	cmd("pauses")
	env[cli.EnvToken], env[cli.EnvHeader] = opsToken, ""
	cmd("resume", "--queue", queueEmails)
	cmd("queues")
	cmd("schedules")
	cmd("schedule", string(f.schedule))
	cmd("trigger", string(f.schedule), "--dry-run")
	cmd("trigger", string(f.schedule))
	if err := settle(); err != nil {
		return err
	}

	fmt.Fprintln(w, "## charts and system")
	cmd("drain", "--wait", "1m")
	cmd("metrics", "--from", start.Format(time.RFC3339), "--to", start.Add(metricsWindow).Format(time.RFC3339), "--step", "5m")
	cmd("health")
	cmd("nodes")

	fmt.Fprintln(w, "## exit codes")
	cmd("reset", id(f.etl[0]), "--step", "transform")             // it succeeded now: the server answers 409
	cmd("reschedule", string(delayed.ScheduledID), "--scheduled") // --at is required
	return nil
}

// shellJoin prints args as a shell would need them typed: an argument
// with a quote, a brace or a space is wrapped in single quotes.
func shellJoin(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		if strings.ContainsAny(a, ` "{}'`) {
			a = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
		}
		out[i] = a
	}
	return strings.Join(out, " ")
}

// endPoll is how often awaitEnd looks at a run, and endTimeout how long it
// waits for one to end before giving up.
const (
	endPoll    = time.Millisecond
	endTimeout = 10 * time.Second
)

// awaitEnd waits until the run ref has ended. A step cancel on a parked
// step ends the run on the driver's own goroutine, after CancelStep has
// returned, so RunAll alone may come back before it.
func awaitEnd(ctx context.Context, c *memory.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(endTimeout)
	for {
		page, err := c.Runs(ctx, workflow.RunQuery{Ref: ref})
		if err != nil {
			return err
		}
		if len(page.Runs) == 1 && page.Runs[0].Status.IsTerminal() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run %s did not end within %s", ref.RunID, endTimeout)
		}
		time.Sleep(endPoll)
	}
}
