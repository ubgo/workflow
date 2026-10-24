// Command claim-check moves payloads and step outputs above a size threshold
// out of the queue into a blob store, so only a short reference travels
// through the driver, and deletes a run's blobs when the run finishes.
//
// It is the program behind docs/reliability/claim-check.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "claim-check:", err)
		os.Exit(1)
	}
}

// threshold is the largest payload or output, in bytes, that stays inline.
// A real deployment sets it below the broker's cap (SQS: 1 MiB).
const threshold = 64

// describe says what a step's raw input is, as the driver delivered it.
func describe(in []byte) string {
	if strings.HasPrefix(string(in), `"wfclaim1:`) {
		return "a reference"
	}
	return fmt.Sprintf("%d bytes inline", len(in))
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))

	// raw sees each step's input before the claim check resolves it, to
	// show what travelled through the queue.
	var attempts int
	raw := func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			attempts++
			fmt.Fprintf(out, "%s: queue carried %s\n", info.Step, describe(in))
			return next(ctx, in)
		}
	}

	// snippet:wire
	store := middleware.NewMemoryBlobStore() // S3, a filesystem or a table in production
	cc, err := middleware.NewClaimCheck(store, threshold)
	if err != nil {
		return err
	}
	// The step half resolves references before the handler runs and
	// claims a large output after it.
	mc := memory.New(memory.WithClock(clk), memory.WithMiddleware(raw, cc.Step()))
	// The enqueue half stamps the run's token and claims a large payload.
	c := middleware.WrapClient(mc, cc.Enqueue())
	// snippet:end

	if err := mc.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{
		{Name: "load", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			fmt.Fprintf(out, "load: handler got %d bytes\n", len(in))
			return bytes.Repeat([]byte("r"), 200), nil // a large output
		}},
		{Name: "summarise", After: []string{"load"}, Run: func(ctx context.Context, in []byte) ([]byte, error) {
			fmt.Fprintf(out, "summarise: handler got %d bytes; blobs held: %d\n", len(in), store.Len())
			return []byte("ok"), nil
		}},
	}}); err != nil {
		return err
	}
	if err := mc.Register(workflow.Workflow{Name: "late", Retry: workflow.RetryPolicy{Max: 3},
		Steps: []workflow.Step{{Name: "use", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return nil, nil
		}}}}); err != nil {
		return err
	}

	// snippet:cleanup
	// Before Start: deletes every blob under the run's token once the run
	// is terminal (MemoryBlobStore is a BlobPrefixDeleter).
	if err := cc.CleanupOnFinish(mc); err != nil {
		return err
	}
	// snippet:end
	finished := make(chan workflow.RunEvent, 2)
	if err := mc.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { finished <- ev }); err != nil {
		return err
	}
	if err := mc.Start(ctx); err != nil {
		return err
	}

	fmt.Fprintln(out, "-- Move big payloads out of the queue")
	// snippet:enqueue
	big := fmt.Appendf(nil, `{"rows":%q}`, strings.Repeat("x", 500))
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "report", Payload: big}); err != nil {
		return err
	}
	// snippet:end
	ev := <-finished
	fmt.Fprintln(out, "run:", ev.Status, "token label set:", ev.Labels[middleware.ClaimCheckLabel] != "")

	fmt.Fprintln(out, "-- Delete blobs when the run finishes")
	fmt.Fprintln(out, "blobs held after the run:", store.Len())

	// A blob that is gone before its step runs fails the step for good.
	attempts = 0
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "late", Payload: big, RunAt: clk.Now().Add(time.Minute)}); err != nil {
		return err
	}
	if err := store.DeletePrefix(ctx, ""); err != nil { // an expiry that came too soon
		return err
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		return err
	}
	clk.Advance(time.Minute)
	ev = <-finished
	fmt.Fprintln(out, "late run:", ev.Status, "ErrBlobNotFound:", errors.Is(ev.Err, middleware.ErrBlobNotFound), "attempts:", attempts)
	return nil
}
