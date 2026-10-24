// Command sentry-reporting sends workflow failures to Sentry with the
// sentry module, on the in-memory driver: one event per run that ends
// failed, with the workflow, step and status as tags, a chosen label as a
// tag (labels are opt-in), and never the payload. A transport that keeps
// events in memory stands in for sentry.io, so nothing leaves the
// machine.
//
// It is the program behind the "Report to Sentry" section of
// docs/observability/failure-alerts.md: the guide copies its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	sentrygo "github.com/getsentry/sentry-go"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sentry"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sentry-reporting:", err)
		os.Exit(1)
	}
}

// memTransport keeps the events sentry-go would send.
type memTransport struct {
	mu     sync.Mutex
	events []*sentrygo.Event
}

func (t *memTransport) Configure(sentrygo.ClientOptions)      {}
func (t *memTransport) Flush(time.Duration) bool              { return true }
func (t *memTransport) FlushWithContext(context.Context) bool { return true }
func (t *memTransport) Close()                                {}
func (t *memTransport) SendEvent(e *sentrygo.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, e)
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	tr := &memTransport{}
	// snippet:init
	// In an app: sentrygo.Init(sentrygo.ClientOptions{Dsn: os.Getenv("SENTRY_DSN")})
	// once at start-up, then sentry.New(nil) uses the current hub.
	client, err := sentrygo.NewClient(sentrygo.ClientOptions{Dsn: "https://key@sentry.invalid/1", Transport: tr})
	if err != nil {
		return err
	}
	hub := sentrygo.NewHub(client, sentrygo.NewScope())

	// The tenant label becomes a tag; no other label and never the
	// payload leaves the process.
	reporter := sentry.New(hub, sentry.WithLabels("tenant"))
	mem := memory.New()
	c, err := workflow.Use(mem, middleware.Report(reporter, middleware.ReportConfig{}))
	// snippet:end
	if err != nil {
		return err
	}

	err = c.Register(workflow.Workflow{
		Name: "charge",
		Steps: []workflow.Step{{Name: "capture", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("card declined")
		}}},
	})
	if err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 1)
	if err := mem.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer mem.Stop()

	_, err = c.Enqueue(ctx, workflow.Job{
		Workflow: "charge",
		Payload:  []byte(`{"card":"4242 4242 4242 4242"}`),
		Labels:   map[string]string{"tenant": "acme", "user": "u-7"},
	})
	if err != nil {
		return err
	}
	<-ends
	// snippet:flush
	hub.Flush(2 * time.Second) // before the process exits, as with any sentry-go event
	// snippet:end

	deadline := time.Now().Add(5 * time.Second)
	for {
		tr.mu.Lock()
		n := len(tr.events)
		tr.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("no event was sent")
		}
		time.Sleep(time.Millisecond)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	fmt.Fprintln(out, "events sent:", len(tr.events))
	e := tr.events[0]
	fmt.Fprintln(out, "level:", e.Level)
	fmt.Fprintln(out, "error:", e.Exception[len(e.Exception)-1].Value)
	for _, k := range slices.Sorted(maps.Keys(e.Tags)) {
		fmt.Fprintf(out, "tag %s=%s\n", k, e.Tags[k])
	}
	fmt.Fprintln(out, "fingerprint:", strings.Join(e.Fingerprint, " "))
	body, err := json.Marshal(e) // what would go over the wire
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "payload sent:", strings.Contains(string(body), "4242"), "user label sent:", strings.Contains(string(body), "u-7"))
	return nil
}
