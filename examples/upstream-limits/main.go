// Command upstream-limits shows UpstreamLimit, a leaky-bucket, cost-based
// rate limit on calls a step makes to an outside API, driven by a fake
// clock; and PeriodKey and OncePerPeriod, which give work that must
// happen once per customer per month a stable idempotency key. It runs in
// process on the in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/jobs/upstream-limits.md and the "Once per
// period" section of docs/jobs/idempotency.md: the guides copy its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "upstream-limits:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := bucket(ctx, out); err != nil {
		return err
	}
	return perPeriod(ctx, out)
}

func bucket(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "bucket:")
	clk := clock.NewFake(time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC))
	// snippet:limit
	limit, err := workflow.NewUpstreamLimit(&workflow.MemoryBuckets{}, workflow.BucketSpec{
		Capacity:      4, // Shopify REST is 40; small here so the output stays short
		LeakPerSecond: 2,
	}, clk)
	if err != nil {
		return err
	}
	// snippet:end
	// snippet:take
	for i := 1; i <= 5; i++ {
		err := limit.Take(ctx, "shop:acme", 1) // one bucket per credential
		wait, _ := workflow.RetryAfterDelay(err, 0)
		fmt.Fprintf(out, "  call %d: throttled=%v retry after %s\n", i, errors.Is(err, workflow.ErrThrottled), wait)
	}
	// snippet:end
	clk.Advance(500 * time.Millisecond)
	fmt.Fprintf(out, "  after 500ms: %v\n", limit.Take(ctx, "shop:acme", 1))
	fmt.Fprintf(out, "  another key has its own bucket: %v\n", limit.Take(ctx, "shop:globex", 1))

	// snippet:observe
	// The response said "X-Shopify-Shop-Api-Call-Limit: 1/4": other apps
	// on the credential use less than the bucket assumed.
	if err := limit.Observe(ctx, "shop:acme", 1); err != nil {
		return err
	}
	wait, err := limit.Wait(ctx, "shop:acme", 3) // reserves 3 now, or reports the wait
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  after Observe(1), a call of cost 3 waits %s\n", wait)
	err = limit.Take(ctx, "shop:acme", 5)
	fmt.Fprintf(out, "  cost above capacity: ErrInvalid=%v permanent=%v\n", errors.Is(err, workflow.ErrInvalid), workflow.IsPermanent(err))
	err = limit.TakeOrSnooze(ctx, "shop:acme", 1)
	d, snoozed := workflow.SnoozeDelay(err)
	fmt.Fprintf(out, "  TakeOrSnooze on a full bucket: snooze=%v for %s\n", snoozed, d)
	return nil
}

func perPeriod(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "period:")
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return err
	}
	// 23:30 UTC on 31 October is already 1 November in Berlin.
	at := time.Date(2026, 10, 31, 23, 30, 0, 0, time.UTC)
	for _, p := range []workflow.Period{workflow.PeriodDay, workflow.PeriodWeek, workflow.PeriodMonth, workflow.PeriodQuarter, workflow.PeriodYear} {
		k, err := workflow.PeriodKey(at, p, berlin)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "  %-7s %s\n", p, k)
	}
	_, err = workflow.PeriodKey(at, workflow.PeriodMonth, time.Local)
	fmt.Fprintf(out, "  time.Local refused: %v\n", errors.Is(err, workflow.ErrInvalid))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := make(chan struct{}) // the charge holds until gate closes, so the first run is unfinished
	defer close(gate)
	c := memory.New()
	err = c.Register(workflow.Workflow{Name: "monthly-charge", Steps: []workflow.Step{{Name: "charge",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			select {
			case <-gate:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}}})
	if err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:period
	job, err := workflow.OncePerPeriod(workflow.Job{Workflow: "monthly-charge"}, "customer-42", workflow.PeriodMonth, at, berlin)
	if err != nil {
		return err
	}
	first, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	second, err := c.Enqueue(ctx, job) // a second fire in the same month, from any node
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  key %s\n", job.IdempotencyKey)
	fmt.Fprintf(out, "  second enqueue in the month returned the first run: %v\n", second == first)
	_, err = workflow.OncePerPeriod(job, "customer-42", workflow.PeriodMonth, at, berlin)
	fmt.Fprintf(out, "  a job that already has a key is refused: %v\n", errors.Is(err, workflow.ErrInvalid))
	return nil
}
