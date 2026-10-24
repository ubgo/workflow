// Command client-wrapper writes its own workflow.Client wrapper, one that
// logs every Enqueue, and keeps the optional interfaces of the client
// inside (run query, pause, schedule list, drain, ...) reachable by
// embedding workflow.Forward. A wrapper that embeds only the Client hides
// them: a type assertion on it fails, although the driver inside has them.
// It runs on the in-memory driver.
//
// It is the program behind "The Client interface" in
// docs/concepts/client.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// waitTimeout bounds the wait for the run to finish; the memory driver
// takes well under a millisecond.
const waitTimeout = 5 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "client-wrapper:", err)
		os.Exit(1)
	}
}

// snippet:wrapper
// audited logs every Enqueue, then passes it on. Embedding Forward next to
// the Client makes it implement every optional interface of the run query,
// pause, schedule and drain families, each forwarded to the same inner
// client.
type audited struct {
	workflow.Client
	workflow.Forward
	out io.Writer
}

func newAudited(inner workflow.Client, out io.Writer) *audited {
	return &audited{Client: inner, Forward: workflow.Forward{Inner: inner}, out: out}
}

// Enqueue is the one method the wrapper changes.
func (a *audited) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	fmt.Fprintf(a.out, "audit: enqueue %s\n", job.Workflow)
	return a.Client.Enqueue(ctx, job)
}

// snippet:end

// bare is the same wrapper without Forward: it hides every optional
// interface of the client inside.
type bare struct {
	workflow.Client
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mem := memory.New()
	if err := mem.Register(workflow.Workflow{Name: "email", Steps: []workflow.Step{{
		Name: "send",
		Run:  func(_ context.Context, in []byte) ([]byte, error) { return in, nil },
	}}}); err != nil {
		return err
	}
	if err := mem.Start(ctx); err != nil {
		return err
	}
	defer mem.Stop()

	// snippet:use
	var c workflow.Client = newAudited(mem, out)
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "email"})
	if err != nil {
		return err
	}
	// The optional interfaces are found by type assertion, through the wrapper.
	_, isQuerier := c.(workflow.RunQuerier)
	_, isPauser := c.(workflow.Pauser)
	fmt.Fprintf(out, "audited: RunQuerier=%v Pauser=%v\n", isQuerier, isPauser)
	// snippet:end
	if err := waitFor(ctx, c, ref); err != nil {
		return err
	}
	page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "email"})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "runs through the wrapper: %d %s\n", len(page.Runs), page.Runs[0].Status)
	if err := c.(workflow.Pauser).Pause(ctx, workflow.PauseWorkflow("email")); err != nil {
		return err
	}
	paused, err := c.(workflow.Pauser).Paused(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "paused through the wrapper: %v\n", paused[0].Workflow)

	// snippet:bare
	var hidden workflow.Client = bare{Client: mem}
	_, isQuerier = hidden.(workflow.RunQuerier)
	fmt.Fprintf(out, "bare: RunQuerier=%v, although the driver has CapRunQuery=%v\n", isQuerier, hidden.Capabilities().Has(workflow.CapRunQuery))
	// snippet:end

	// snippet:refuse
	// Forward over a client that lacks an interface refuses with an
	// UnsupportedError naming the capability; it never pretends.
	stacked := newAudited(hidden, out)
	_, err = stacked.Runs(ctx, workflow.RunQuery{})
	var unsupported *workflow.UnsupportedError
	fmt.Fprintf(out, "Forward over bare: ErrUnsupported=%v missing=%v\n", errors.Is(err, workflow.ErrUnsupported), errors.As(err, &unsupported) && unsupported.Missing == workflow.CapRunQuery)
	// snippet:end
	return nil
}

// waitFor polls ref until its run has finished.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	deadline := time.Now().Add(waitTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st.IsTerminal() {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run is %s after %s", st, waitTimeout)
		}
		time.Sleep(time.Millisecond)
	}
}
