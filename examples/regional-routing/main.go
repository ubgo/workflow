// Command regional-routing joins one client per region into one client
// that sends each job to the region its label names: refs that name
// their region, a status read from the right region, an unknown region
// refused, a signal reaching a run in another region, a schedule placed
// by the same router, one run listing across regions, and a router by
// tenant.
//
// Every region is the in-memory driver here; in production each is a
// sqlstore or Hatchet client in its own region.
//
// It is the program behind docs/operations/regional-routing.md: the
// guide copies its regions (marked "snippet:") and task docs:snippets
// fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/regional"
)

// statusPoll is how often waitStatus re-reads a run's status: short, so
// the demo finishes quickly; a real app waits on the run-finished hook.
const statusPoll = 5 * time.Millisecond

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "regional-routing:", err)
		os.Exit(1)
	}
}

// invoice is a one-step workflow that does nothing: the demo only cares
// which region its runs land in.
var invoice = workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{
	Name: "render",
	Run:  func(ctx context.Context, in []byte) ([]byte, error) { return nil, nil },
}}}

// approval waits for an "approved" event keyed by the order.
var approval = workflow.Workflow{Name: "approval", Steps: []workflow.Step{{
	Name: "wait",
	Wait: &workflow.WaitSpec{Event: "approved", KeyFrom: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["order"] })},
}}}

// region is the region a ref or a schedule ID names: the part before the
// first colon.
func region(id string) string {
	r, _, _ := strings.Cut(id, ":")
	return r
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Println("-- Route each job to its region")
	// snippet:new
	c, err := regional.New(map[string]workflow.Client{
		"eu": memory.New(), // in production: a client on the EU database or engine
		"us": memory.New(),
	},
		regional.ByLabel("region", "eu"),  // the job's "region" label; eu when it has none
		regional.WithScheduleRegion("eu"), // where declared Cron fires
	)
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{invoice, approval} {
		if err := c.Register(wf); err != nil { // registers in every region
			return err
		}
	}
	if err := c.Start(ctx); err != nil { // starts every region
		return err
	}
	// snippet:end

	// snippet:enqueue
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"region": "us"}})
	if err != nil {
		return err
	}
	fmt.Println("us customer's invoice ->", region(string(ref.RunID)))
	st, err := waitStatus(ctx, c, ref, workflow.StatusSucceeded) // the ref names its region, so Status asks us
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Println("status read from us:", st)
	other, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice"})
	if err != nil {
		return err
	}
	fmt.Println("invoice without a label ->", region(string(other.RunID)))

	// snippet:unknown
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"region": "ap"}})
	fmt.Println("unknown region refused:", errors.Is(err, regional.ErrUnknownRegion), errors.Is(err, workflow.ErrInvalid))
	// snippet:end

	fmt.Println("-- Signal a run in another region")
	waiting, err := c.Enqueue(ctx, workflow.Job{Workflow: "approval", Labels: map[string]string{"region": "us", "order": "42"}})
	if err != nil {
		return err
	}
	if _, err := waitStatus(ctx, c, waiting, workflow.StatusWaiting); err != nil {
		return err
	}
	// snippet:signal
	// Signal goes to every region, since a waiting run may sit in any.
	if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "42"}); err != nil {
		return err
	}
	// snippet:end
	if st, err = waitStatus(ctx, c, waiting, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Println("approval in us:", st)

	fmt.Println("-- Place a schedule with the same router")
	// snippet:schedule
	id, err := c.Schedule(ctx, workflow.Schedule{
		Name: "us-nightly", Workflow: "invoice", Cron: "0 2 * * *", Location: time.UTC,
		Labels: map[string]string{"region": "us"}, // read by the router, like a job's
	})
	// snippet:end
	if err != nil {
		return err
	}
	fmt.Println("schedule ->", region(string(id)))

	fmt.Println("-- List runs across regions")
	// snippet:runs
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "invoice"})
	if err != nil {
		return err
	}
	perRegion := map[string]int{}
	for _, r := range page.Runs { // every region's runs, newest first, refs prefixed
		perRegion[region(string(r.Ref.RunID))]++
	}
	// snippet:end
	fmt.Printf("invoices: eu %d, us %d\n", perRegion["eu"], perRegion["us"])

	fmt.Println("-- Route by tenant")
	if err := byTenant(ctx); err != nil {
		return err
	}
	fmt.Println("-- Keep an event in its region")
	if err := signalRouter(ctx); err != nil {
		return err
	}
	fmt.Println("-- Name the region that fires declared Cron")
	return scheduleRegion()
}

// signalRouter delivers each event only to the region the app's own order
// table names, and shows that a run waiting elsewhere never sees it.
func signalRouter(ctx context.Context) error {
	// snippet:signal-router
	orders := map[string]string{"42": "us"} // the app's own table: which region holds an order
	c, err := regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()},
		regional.ByLabel("region", "eu"),
		regional.WithSignalRouter(func(ctx context.Context, ev workflow.Event) ([]string, error) {
			r, ok := orders[ev.Key]
			if !ok {
				return nil, fmt.Errorf("%w: no region for order %q", workflow.ErrInvalid, ev.Key)
			}
			return []string{r}, nil // at least one region of the client
		}),
	)
	// snippet:end
	if err != nil {
		return err
	}
	if err := c.Register(approval); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// The same order key waits in both regions; only us gets the event.
	refs := map[string]workflow.RunRef{}
	for _, r := range []string{"eu", "us"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "approval", Labels: map[string]string{"region": r, "order": "42"}})
		if err != nil {
			return err
		}
		if _, err := waitStatus(ctx, c, ref, workflow.StatusWaiting); err != nil {
			return err
		}
		refs[r] = ref
	}
	if err := c.Signal(ctx, workflow.Event{Name: "approved", Key: "42"}); err != nil {
		return err
	}
	us, err := waitStatus(ctx, c, refs["us"], workflow.StatusSucceeded)
	if err != nil {
		return err
	}
	eu, err := c.Status(ctx, refs["eu"])
	if err != nil {
		return err
	}
	fmt.Printf("order 42: us %s, eu %s\n", us, eu)
	err = c.Signal(ctx, workflow.Event{Name: "approved", Key: "7"})
	fmt.Println("unknown order refused:", errors.Is(err, workflow.ErrInvalid))
	return nil
}

// scheduleRegion shows that a workflow with a declared Cron needs
// WithScheduleRegion once there is more than one region.
func scheduleRegion() error {
	nightly := workflow.Workflow{Name: "nightly", Cron: []string{"0 2 * * *"}, Steps: invoice.Steps}
	// snippet:schedule-region
	// regions builds a fresh pair of regions, one per client below.
	regions := func() map[string]workflow.Client {
		return map[string]workflow.Client{"eu": memory.New(), "us": memory.New()}
	}
	without, err := regional.New(regions(), regional.ByLabel("region", "eu"))
	if err != nil {
		return err
	}
	err = without.Register(nightly) // which region fires 02:00? Register refuses to guess
	fmt.Println("Cron without a schedule region refused:", errors.Is(err, workflow.ErrInvalid))

	with, err := regional.New(regions(), regional.ByLabel("region", "eu"),
		regional.WithScheduleRegion("eu"), // eu fires the tick; us gets the workflow without its Cron
	)
	if err != nil {
		return err
	}
	fmt.Println("with WithScheduleRegion(\"eu\"):", with.Register(nightly))
	// snippet:end
	return nil
}

// byTenant routes by the job's tenant through the app's own customer
// table.
func byTenant(ctx context.Context) error {
	// snippet:router
	customers := map[string]string{"acme": "eu", "globex": "us"} // the app's own table
	// router picks the region of the job's tenant from that table.
	router := regional.RouterFunc(func(ctx context.Context, job workflow.Job) (string, error) {
		r, ok := customers[job.Tenant]
		if !ok {
			return "", fmt.Errorf("%w: no region for tenant %q", workflow.ErrInvalid, job.Tenant)
		}
		return r, nil
	})
	// snippet:end
	c, err := regional.New(map[string]workflow.Client{"eu": memory.New(), "us": memory.New()}, router)
	if err != nil {
		return err
	}
	if err := c.Register(invoice); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	for _, tenant := range []string{"acme", "globex", "initech"} {
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Tenant: tenant})
		if err != nil {
			fmt.Printf("%s: refused (%v)\n", tenant, errors.Is(err, workflow.ErrInvalid))
			continue
		}
		fmt.Printf("%s -> %s\n", tenant, region(string(ref.RunID)))
	}
	return nil
}

// waitStatus polls a run's status until it is want.
func waitStatus(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) (workflow.Status, error) {
	for {
		st, err := c.Status(ctx, ref)
		if err != nil || st == want {
			return st, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(statusPoll):
		}
	}
}
