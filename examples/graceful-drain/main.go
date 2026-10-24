// Command graceful-drain shows the two graceful ways a client stops on the
// in-memory driver: Drain, which stops claiming work and lets the running
// step finish, as before a deploy, and WithStopWhenIdle, which stops a
// batch-style process once its work is done. Both
// close Done, the one channel a process waits on.
//
// It is the program behind the "Stop gracefully" section of
// docs/concepts/client.md.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// start is the fake clock's first instant; any fixed time works.
var start = time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := drain(ctx, out); err != nil {
		return err
	}
	return stopWhenIdle(ctx, out)
}

func drain(ctx context.Context, out io.Writer) error {
	started, finish := make(chan struct{}), make(chan struct{})
	c := memory.New()
	err := c.Register(workflow.Workflow{Name: "resize-image", Steps: []workflow.Step{{Name: "resize", Run: func(context.Context, []byte) ([]byte, error) {
		close(started)
		<-finish // a long step; Drain lets it end on its own
		fmt.Fprintln(out, "resize finished")
		return nil, nil
	}}}})
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "resize-image"}); err != nil {
		return err
	}
	<-started
	go func() { time.Sleep(10 * time.Millisecond); close(finish) }()

	// snippet:drain
	// On SIGTERM: stop taking work, give running steps up to 30 seconds.
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := c.Drain(dctx); err != nil {
		fmt.Fprintln(out, "some steps were cut off:", err)
	}
	<-c.Done()
	// snippet:end
	fmt.Fprintln(out, "drained and stopped")
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "resize-image"})
	fmt.Fprintln(out, "enqueue after Drain:", err)
	return nil
}

func stopWhenIdle(ctx context.Context, out io.Writer) error {
	clk := clock.NewFake(start)
	// snippet:idle
	c := memory.New(memory.WithClock(clk), memory.WithStopWhenIdle(time.Minute))
	// snippet:end
	err := c.Register(workflow.Workflow{Name: "backfill", Steps: []workflow.Step{{Name: "copy", Run: func(_ context.Context, in []byte) ([]byte, error) {
		fmt.Fprintf(out, "backfill %s\n", in)
		return nil, nil
	}}}})
	if err != nil {
		return err
	}
	ends := make(chan workflow.RunEvent, 3)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	for _, day := range []string{"2026-01-01", "2026-01-02", "2026-01-03"} {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "backfill", Payload: []byte(day)}); err != nil {
			return err
		}
		<-ends
	}
	// The last run ended; a minute later with nothing new, the client stops.
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	clk.Advance(time.Minute)
	<-c.Done()
	fmt.Fprintln(out, "idle for a minute: stopped")
	return nil
}
