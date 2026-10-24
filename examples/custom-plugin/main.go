// Command custom-plugin bundles a feature as a workflow.Plugin: enqueue
// middleware that requires a tenant label and keeps one job per tenant
// and workflow, step middleware that logs every attempt, and a
// run-finished observer, all added with one workflow.Use call. The
// uniqueness locks live in a store written for this program (store.go),
// the shape of a store over Redis or any key-value service.
//
// It is the program behind docs/extending/writing-a-plugin.md,
// docs/extending/writing-middleware.md, docs/extending/custom-stores.md
// and docs/concepts/plugins.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "custom-plugin:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	log := &lineLog{out: out}
	finished := make(chan struct{}, 1)

	// snippet:use
	base := memory.New()
	plugin := &tenancy{log: log, unique: kvUnique{kv: newMemKV(clock.Real{}), prefix: "unique:"}}
	c, err := workflow.Use(base, plugin)
	if err != nil {
		return err
	}
	// snippet:end
	// Observers run in registration order, so this one, registered after
	// the plugin's, tells the program the plugin has logged the run end.
	if err := base.OnRunFinished(func(context.Context, workflow.RunEvent) { finished <- struct{}{} }); err != nil {
		return err
	}
	// snippet:register
	// Register and Start through c, the client Use returned, so the
	// plugin's step middleware wraps the workflow's steps.
	err = c.Register(workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{
		Name: "send",
		Run:  func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil },
	}}})
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	// snippet:enqueue-calls
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice"})
	log.Printf("no tenant: %v", err)

	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"tenant": "acme"}})
	if err != nil {
		return err
	}
	<-finished

	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"tenant": "acme"}})
	log.Printf("second job for acme: %v", err)
	// snippet:end
	return nil
}
