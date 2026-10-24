// Command admin-bulk runs the admin module's bulk actions and charts on the
// memory driver: every failed run a filter matches previewed with a dry
// run, then retried; a delete without a filter refused; a delete previewed
// and done; and the run ends of the last ten minutes read from
// MemoryMetrics, the in-process MetricsSource, in five-minute steps.
//
// It is the program behind the bulk-action and charts sections of
// docs/admin/: the guides copy its regions (marked "snippet:") and task
// docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

var errSMTP = errors.New("smtp: connection refused")

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "admin-bulk:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant, so the chart's times are fixed.
var start = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	c := memory.New(memory.WithClock(clk))

	var smtpDown atomic.Bool
	smtpDown.Store(true)
	err := c.Register(workflow.Workflow{
		Name: "send-email",
		Steps: []workflow.Step{{Name: "send", Run: func(_ context.Context, in []byte) ([]byte, error) {
			if string(in) == `"newsletter"` && smtpDown.Load() {
				return nil, workflow.Permanent(errSMTP)
			}
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}

	// snippet:metrics
	mm := admin.NewMemoryMetrics(admin.MemoryMetricsConfig{Clock: clk}) // one-minute buckets, kept a day
	if err := c.OnRunFinished(mm.Observe); err != nil {                 // before Start
		return err
	}
	adm := admin.New(c, admin.WithMetrics(mm), admin.WithClock(clk))
	// snippet:end

	ends := make(chan struct{}, 64)
	if err := c.OnRunFinished(func(context.Context, workflow.RunEvent) { ends <- struct{}{} }); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	enqueue := func(payload string, n int) error {
		for range n {
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte(`"` + payload + `"`)}); err != nil {
				return err
			}
			<-ends // observers ran: MemoryMetrics counted it
		}
		return nil
	}
	if err := enqueue("newsletter", 5); err != nil { // fail while SMTP is down
		return err
	}
	if err := enqueue("receipt", 2); err != nil {
		return err
	}
	clk.Advance(5 * time.Minute)
	smtpDown.Store(false)

	// snippet:retry
	failed := workflow.RunQuery{Workflow: "send-email"} // RetryAll adds statuses=[failed]
	preview, err := adm.RetryAll(ctx, failed, admin.BulkOptions{DryRun: true})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "dry run: would retry %d, sample %d, done %d\n", preview.Matched, len(preview.Sample), preview.Done)

	res, err := adm.RetryAll(ctx, failed, admin.BulkOptions{})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "retried %d of %d, skipped %d, more to do: %t\n", res.Done, res.Matched, res.Skipped, res.Truncated)
	// snippet:end
	for range res.Done {
		<-ends
	}

	// snippet:delete
	_, err = adm.DeleteAll(ctx, workflow.RunQuery{}, admin.BulkOptions{DryRun: true})
	fmt.Fprintln(out, "delete without a filter refused:", errors.Is(err, workflow.ErrInvalid))

	done := workflow.RunQuery{Workflow: "send-email", Statuses: []workflow.Status{workflow.StatusSucceeded}}
	preview, err = adm.DeleteAll(ctx, done, admin.BulkOptions{DryRun: true, Max: 3})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "dry run: would delete %d (Max 3), more match: %t\n", preview.Matched, preview.Truncated)

	res, err = adm.DeleteAll(ctx, done, admin.BulkOptions{})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "deleted %d of %d\n", res.Done, res.Matched)
	// snippet:end

	// snippet:chart
	series, err := adm.Metrics(ctx, admin.MetricsQuery{
		From: start,
		To:   start.Add(10 * time.Minute),
		Step: 5 * time.Minute,
	})
	if err != nil {
		return err
	}
	for _, p := range series.Points {
		fmt.Fprintf(out, "%s succeeded=%d failed=%d total=%d\n", p.At.Format("15:04"), p.Succeeded, p.Failed, p.Total())
	}
	// snippet:end
	return nil
}
