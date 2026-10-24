// Command task-output captures what a scheduled task logs and returns and
// sends it on after each run: appended to a file every night, and to a
// mail function when a run fails. The schedule runs on the in-memory
// driver with a fake clock, so two nights pass in an instant.
//
// It is the program behind docs/schedules/output.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// start is the fake clock's first instant: just before the first 02:00.
var start = time.Date(2026, 1, 5, 1, 0, 0, 0, time.UTC)

// runTimeout bounds each wait for a run end, generous for a slow CI host.
const runTimeout = 10 * time.Second

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "task-output:", err)
		os.Exit(1)
	}
}

// snippet:step
// report is the nightly task. It logs through middleware.OutputLogger, so
// its lines are captured when CaptureOutput wraps it and go to the app's
// logger otherwise.
func report(night *int) workflow.Handler {
	return func(ctx context.Context, _ []byte) ([]byte, error) {
		log := middleware.OutputLogger(ctx, nil)
		*night++
		log.Info("exporting orders", "night", *night)
		if *night == 2 {
			log.Error("warehouse API refused the export", "status", 503)
			return nil, workflow.Permanent(errors.New("export refused"))
		}
		log.Debug("not captured: below the default Info level")
		return []byte(`{"rows":42}`), nil
	}
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "task-output-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "nightly-report.log")
	var mailbox strings.Builder
	clk := clock.NewFake(start)

	// snippet:sink
	appendToFile := middleware.FileSink(path, middleware.FileAppend)
	mailOnFailure := middleware.OutputSinkFunc(func(ctx context.Context, o middleware.Output) error {
		if o.Err == nil {
			return nil
		}
		// Your mailer here: the body is the same text the file gets.
		fmt.Fprintf(&mailbox, "To: ops@example.com\nSubject: %s failed\n\n", o.Info.Workflow)
		return middleware.FormatOutput(&mailbox, o)
	})
	capture := middleware.CaptureOutput(
		middleware.OutputSinkFunc(func(ctx context.Context, o middleware.Output) error {
			return errors.Join(appendToFile.Deliver(ctx, o), mailOnFailure.Deliver(ctx, o))
		}),
		middleware.CaptureResult(),   // include the step's result; off by default
		middleware.CaptureClock(clk), // record times from the driver's clock
	)
	c := memory.New(memory.WithClock(clk), memory.WithMiddleware(capture))
	// snippet:end
	defer c.Stop()

	ends := make(chan workflow.RunEvent, 2)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { ends <- ev }); err != nil {
		return err
	}
	night := 0
	if err := c.Register(workflow.Workflow{
		Name:  "nightly-report",
		Cron:  []string{"0 2 * * *"},
		Steps: []workflow.Step{{Name: "export", Run: report(&night)}},
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	for i, step := range []time.Duration{time.Hour, 24 * time.Hour} {
		clk.Advance(step) // to 02:00 on the next night
		select {
		case ev := <-ends:
			fmt.Fprintf(out, "night %d: %s\n", i+1, ev.Status)
		case <-time.After(runTimeout):
			return fmt.Errorf("night %d did not finish within %s", i+1, runTimeout)
		}
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "--- %s\n%s", filepath.Base(path), b)
	fmt.Fprintf(out, "--- mail\n%s", mailbox.String())
	return nil
}
