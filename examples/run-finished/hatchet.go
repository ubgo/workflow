package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// envToken turns the Hatchet part on. The Hatchet SDK reads it, and
// HATCHET_CLIENT_HOST_PORT and HATCHET_CLIENT_TLS_STRATEGY, itself.
const envToken = "HATCHET_CLIENT_TOKEN"

// runHatchet registers the same kind of observer on the Hatchet driver.
// Hatchet implements RunNotifier by polling the engine's run list, best
// effort: it does not declare CapRunFinished, so a consumer keeps a
// fallback (a TTL, a status poll) for an event that is lost.
func runHatchet(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// snippet:hatchet
	c := hatchet.New(hatchet.Config{
		WorkerName:         "run-finished-example",
		RunFinishedPoll:    2 * time.Second,  // how often the engine is asked for ended runs
		RunFinishedWindow:  6 * time.Hour,    // runs older than this are no longer watched
		RunFinishedCatchUp: 10 * time.Minute, // replay the ends of runs created this long before Start
	})
	fmt.Fprintln(out, "hatchet: every run end is delivered:", c.Capabilities().Has(workflow.CapRunFinished))
	ends := make(chan workflow.RunEvent, 1)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	// snippet:end

	name := fmt.Sprintf("run-finished-example-%d", time.Now().UnixNano())
	if err := c.Register(workflow.Workflow{
		Name:  name,
		Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}},
	}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: name})
	if err != nil {
		return err
	}
	for {
		select {
		case ev := <-ends:
			if ev.Ref.RunID != ref.RunID {
				continue // catch-up replays older runs' ends too
			}
			fmt.Fprintf(out, "hatchet: %s %s\n", "run", ev.Status)
			return nil
		case <-time.After(time.Minute):
			return fmt.Errorf("no run-finished event for %s within a minute", ref.RunID)
		}
	}
}
