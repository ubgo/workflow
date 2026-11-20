// Command job-guards shows the guards that keep a job from running when
// it should not: an expiry for work that must not start late, a sweep for
// runs no worker takes, a bounded queue and a per-tenant quota, quiet
// hours in the recipient's zone, payload validation at enqueue, an
// explicit retry ladder, invariants and step conditions, a resource pool,
// a critical section and an endpoint switch that turns itself off.
// Everything runs on the in-memory driver, most of it on a fake clock, so
// nothing needs to be installed and the output is the same on every run.
//
// It is the program behind docs/jobs/guards.md and
// docs/reliability/pools.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"
	_ "time/tzdata" // the zones the quiet-hours part uses, whatever the host has

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "job-guards:", err)
		os.Exit(1)
	}
}

// start is the fake clock's first instant.
var start = time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

// await waits for a run to end.
var await = workflow.AwaitOptions{Poll: time.Millisecond}

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, part := range []struct {
		name string
		fn   func(context.Context) error
	}{
		{"expiry", expiry}, {"sweep", sweep}, {"bounds", bounds}, {"quiet hours", quiet},
		{"validation", validation}, {"ladder", ladder}, {"conditions", conditions},
		{"pool", pool}, {"critical section", section}, {"auto-disable", autoDisable},
	} {
		fmt.Println(part.name + ":")
		if err := part.fn(ctx); err != nil {
			return fmt.Errorf("%s: %w", part.name, err)
		}
	}
	return nil
}

// noop is a step that does nothing.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// started starts c and stops it when ctx ends.
func started(ctx context.Context, c *memory.Client, wfs ...workflow.Workflow) error {
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	context.AfterFunc(ctx, c.Stop)
	return nil
}

func expiry(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// snippet:expire
	clk := clock.NewFake(start)
	// The worker ends a run that starts after its expiry, instead of running it.
	mc := memory.New(memory.WithClock(clk), memory.WithMiddleware(middleware.Expire(clk)))
	// The enqueuing side stamps every job with an expiry 15 minutes after its start.
	c := middleware.WrapClient(mc, middleware.ExpireAfter(15*time.Minute, clk))
	// snippet:end
	var sent atomic.Int32
	err := started(ctx, mc, workflow.Workflow{Name: "send-code", Steps: []workflow.Step{{Name: "send", Run: func(context.Context, []byte) ([]byte, error) {
		sent.Add(1)
		return nil, nil
	}}}})
	if err != nil {
		return err
	}
	// snippet:expire-at
	// A one-time code mailed later than 12:05 is useless: expire it then.
	job := middleware.ExpireAt(workflow.Job{Workflow: "send-code", RunAt: start.Add(10 * time.Minute)}, start.Add(5*time.Minute))
	ref, err := c.Enqueue(ctx, job)
	// snippet:end
	if err != nil {
		return err
	}
	clk.Advance(10 * time.Minute) // the delayed start comes after the expiry
	st, err := workflow.Await(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Printf("  late run: %s, code sent %d time(s)\n", st, sent.Load())
	return nil
}

func sweep(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start)
	mc := memory.New(memory.WithClock(clk), memory.WithWorkers(1))
	busy, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	err := started(ctx, mc, workflow.Workflow{Name: "export", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
		busy <- struct{}{}
		<-release // the only worker stays busy
		return nil, nil
	}}}})
	if err != nil {
		return err
	}
	for range 3 {
		if _, err := mc.Enqueue(ctx, workflow.Job{Workflow: "export"}); err != nil {
			return err
		}
	}
	<-busy
	clk.Advance(20 * time.Minute)
	// snippet:sweep
	// Cancel every export that has waited more than 15 minutes for a worker.
	res, err := workflow.ExpireWaiting(ctx, mc, workflow.RunQuery{Workflow: "export"}, 15*time.Minute, clk.Now())
	if err != nil {
		return err
	}
	fmt.Printf("  checked %d waiting run(s), cancelled %d\n", res.Checked, len(res.Cancelled))
	// snippet:end
	return nil
}

func bounds(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mc := memory.New()
	if err := started(ctx, mc, workflow.Workflow{Name: "mail", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
		return err
	}
	// snippet:bounded
	depth, err := middleware.DepthFromRuns(mc) // waiting runs per queue, through RunQuerier
	if err != nil {
		return err
	}
	perTenant, err := middleware.TenantCount(mc) // unfinished runs per tenant
	if err != nil {
		return err
	}
	c := middleware.WrapClient(mc,
		middleware.Quota(perTenant, 2), // at most two unfinished runs per tenant
		middleware.Bounded(depth, middleware.QueueLimits(map[string]int{workflow.DefaultQueue: 3})),
	)
	// snippet:end
	later := time.Now().Add(time.Hour) // delayed, so the runs stay waiting
	for _, tenant := range []string{"acme", "acme", "acme", "globex", "globex"} {
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: "mail", Tenant: tenant, RunAt: later})
		switch {
		case errors.Is(err, middleware.ErrQuotaExceeded):
			fmt.Printf("  %s: refused, quota exceeded\n", tenant)
		case errors.Is(err, middleware.ErrQueueFull):
			fmt.Printf("  %s: refused, queue full\n", tenant)
		case err != nil:
			return err
		default:
			fmt.Printf("  %s: enqueued\n", tenant)
		}
	}
	return nil
}

func quiet(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := clock.NewFake(start) // 12:00 UTC: 21:00 in Tokyo, 13:00 in Berlin
	mc := memory.New(memory.WithClock(clk))
	if err := started(ctx, mc, workflow.Workflow{Name: "notify", Steps: []workflow.Step{{Name: "push", Run: noop}}}); err != nil {
		return err
	}
	// snippet:quiet
	night := workflow.Windows{Unless: []workflow.Window{{From: "21:00", To: "08:00"}}}
	zoneOf := func(job workflow.Job) *time.Location {
		loc, err := time.LoadLocation(job.Labels["tz"]) // the recipient's zone
		if err != nil {
			return nil // UTC
		}
		return loc
	}
	c := middleware.WrapClient(mc, middleware.QuietHours(night, zoneOf, clk))
	// snippet:end
	for _, tz := range []string{"Asia/Tokyo", "Europe/Berlin"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "notify", Labels: map[string]string{"tz": tz}})
		if err != nil {
			return err
		}
		page, err := mc.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
		if err != nil {
			return err
		}
		at := "now"
		if due := page.Runs[0].RunAt; !due.IsZero() {
			loc, _ := time.LoadLocation(tz)
			at = due.In(loc).Format("Mon 15:04 MST")
		}
		fmt.Printf("  %s: starts %s\n", tz, at)
	}
	return nil
}

// snippet:validate-type
// Order is the payload of the charge workflow. Its Validate method runs at
// enqueue, through ValidateJSON.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func (o *Order) Validate() error {
	if o.Amount <= 0 {
		return fmt.Errorf("order %s: amount %d is not positive", o.ID, o.Amount)
	}
	return nil
}

// snippet:end

func validation(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mc := memory.New()
	if err := started(ctx, mc, workflow.Workflow{Name: "charge", Steps: []workflow.Step{{Name: "charge", Run: noop}}}); err != nil {
		return err
	}
	// snippet:validate
	c := middleware.WrapClient(mc, middleware.ValidateJSON[Order]("charge"))
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: []byte(`{"id":"o-7","amount":0}`)})
	fmt.Println("  invalid:", errors.Is(err, workflow.ErrInvalid), err)
	// snippet:end
	payload, _ := json.Marshal(Order{ID: "o-8", Amount: 40})
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "charge", Payload: payload}); err != nil {
		return err
	}
	fmt.Println("  valid order enqueued")
	return nil
}

func ladder(context.Context) error {
	// snippet:ladder
	policy := workflow.LadderPolicy(5*time.Second, 5*time.Minute, 30*time.Minute, 2*time.Hour)
	wf := workflow.Workflow{Name: "deliver-webhook", Retry: policy, Steps: []workflow.Step{{Name: "post", Run: noop}}}
	// snippet:end
	var delays []string
	for retry := 1; retry <= wf.Retry.Max; retry++ {
		delays = append(delays, wf.Retry.NextDelay(errors.New("503"), retry, nil).String())
	}
	fmt.Printf("  %d retries after %s\n", wf.Retry.Max, strings.Join(delays, ", "))
	return nil
}

func conditions(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mc := memory.New()
	// snippet:check
	reserve := func(ctx context.Context, in []byte) ([]byte, error) {
		var qty int
		if err := json.Unmarshal(in, &qty); err != nil {
			return nil, workflow.Permanent(err)
		}
		stock := 10 - qty
		if err := workflow.Invariant(stock >= 0, "stock would drop to %d", stock); err != nil {
			return nil, err // permanent: retrying the same order cannot fix it
		}
		return json.Marshal(stock)
	}
	positive := func(_ context.Context, in []byte) error {
		if string(in) == "0" {
			return errors.New("quantity is zero")
		}
		return nil
	}
	step := workflow.Step{Name: "reserve", Run: middleware.Check(positive, nil)(reserve)}
	// snippet:end
	wf := workflow.Workflow{Name: "reserve", Retry: workflow.RetryPolicy{Max: 3}, Steps: []workflow.Step{step}}
	if err := started(ctx, mc, wf); err != nil {
		return err
	}
	for _, qty := range []string{"4", "0", "12"} {
		ref, err := mc.Enqueue(ctx, workflow.Job{Workflow: "reserve", Payload: []byte(qty)})
		if err != nil {
			return err
		}
		out, err := workflow.AwaitResult(ctx, mc, ref, await)
		if err == nil {
			fmt.Printf("  qty %s: stock left %s\n", qty, out)
			continue
		}
		page, qerr := mc.Runs(ctx, workflow.RunQuery{Ref: ref, Limit: 1})
		if qerr != nil {
			return qerr
		}
		info := page.Runs[0] // the run's error is stored as text
		fmt.Printf("  qty %s: %s after %d attempt(s), constraint violated: %v\n", qty, info.Status, info.Attempts,
			strings.Contains(info.Error, workflow.ErrConstraint.Error()))
	}
	return nil
}

func pool(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cur, peak atomic.Int32
	render := func(context.Context, []byte) ([]byte, error) {
		now := cur.Add(1)
		for p := peak.Load(); now > p && !peak.CompareAndSwap(p, now); p = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}
	// snippet:pool
	store := middleware.NewMemoryUnique(nil) // sqlstore.UniqueStore or the Redis one across processes
	gpus := middleware.Pool(store, "gpu", 2, 10*time.Minute, middleware.PoolSteps("render"), middleware.PoolRetry(5*time.Millisecond))
	mc := memory.New(memory.WithWorkers(6), memory.WithMiddleware(gpus))
	// snippet:end
	if err := started(ctx, mc, workflow.Workflow{Name: "thumbnail", Steps: []workflow.Step{{Name: "render", Run: render}}}); err != nil {
		return err
	}
	var refs []workflow.RunRef
	for range 6 {
		ref, err := mc.Enqueue(ctx, workflow.Job{Workflow: "thumbnail"})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	done := 0
	for _, ref := range refs {
		if st, err := workflow.Await(ctx, mc, ref, await); err != nil {
			return err
		} else if st == workflow.StatusSucceeded {
			done++
		}
	}
	fmt.Printf("  %d renders succeeded, at most %d at once\n", done, peak.Load())
	return nil
}

func section(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cur, peak atomic.Int32
	touch := func(context.Context, []byte) ([]byte, error) {
		now := cur.Add(1)
		for p := peak.Load(); now > p && !peak.CompareAndSwap(p, now); p = peak.Load() {
		}
		time.Sleep(10 * time.Millisecond)
		cur.Add(-1)
		return nil, nil
	}
	// snippet:section
	// debit and credit run in parallel, except that they both write the
	// account row: within one run, one at a time.
	ledger := middleware.CriticalSection(middleware.NewMemoryUnique(nil), "account-row", time.Minute, "debit", "credit")
	mc := memory.New(memory.WithWorkers(4), memory.WithMiddleware(ledger))
	// snippet:end
	wf := workflow.Workflow{Name: "transfer", Steps: []workflow.Step{{Name: "debit", Run: touch}, {Name: "credit", Run: touch}}}
	if err := started(ctx, mc, wf); err != nil {
		return err
	}
	ref, err := mc.Enqueue(ctx, workflow.Job{Workflow: "transfer"})
	if err != nil {
		return err
	}
	st, err := workflow.Await(ctx, mc, ref, await)
	if err != nil {
		return err
	}
	fmt.Printf("  transfer %s, at most %d step(s) in the section at once\n", st, peak.Load())
	return nil
}

func autoDisable(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var calls atomic.Int32
	deliver := func(_ context.Context, in []byte) ([]byte, error) {
		calls.Add(1)
		return nil, errors.New("endpoint returned 500") // the receiver is down
	}
	// snippet:autodisable
	switches := middleware.NewMemoryDisable()
	endpoint := func(_ workflow.StepInfo, in []byte) string { return string(in) } // the payload names the endpoint
	off := middleware.AutoDisable(switches, endpoint, 3, func(_ context.Context, key string) {
		fmt.Println("  disabled", key, "- tell its owner") // runs once, when the switch trips
	})
	mc := memory.New(memory.WithMiddleware(off))
	// snippet:end
	if err := started(ctx, mc, workflow.Workflow{Name: "webhook", Steps: []workflow.Step{{Name: "post", Run: deliver}}}); err != nil {
		return err
	}
	for i := range 5 {
		ref, err := mc.Enqueue(ctx, workflow.Job{Workflow: "webhook", Payload: []byte("https://hooks.example.com/a")})
		if err != nil {
			return err
		}
		st, err := workflow.Await(ctx, mc, ref, await)
		if err != nil {
			return err
		}
		fmt.Printf("  delivery %d: %s\n", i+1, st)
	}
	fmt.Printf("  endpoint called %d time(s)\n", calls.Load())
	// snippet:enable
	if err := switches.Enable(ctx, "https://hooks.example.com/a"); err != nil { // after the owner fixed it
		return err
	}
	// snippet:end
	on, _ := switches.Disabled(ctx, "https://hooks.example.com/a")
	fmt.Println("  disabled after Enable:", on)
	return nil
}
