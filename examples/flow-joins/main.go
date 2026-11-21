// Command flow-joins shows joins over child runs: JoinChildren continuing
// on the first child to succeed (a discriminator) or on two of three (a
// partial join) and cancelling the rest, and MapRuns running one child per
// item of a paged source with a bounded number in flight and a failure
// budget, and a multi-merge that starts one downstream run per parallel
// branch. It runs on the in-memory driver, so nothing needs to be
// installed.
//
// It is the program behind docs/workflows/joins.md: the guide copies its
// regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

const (
	// awaitPoll is how often the program re-reads a run it waits for:
	// short, so the demo finishes quickly on the in-memory driver.
	awaitPoll = 2 * time.Millisecond
	// productCount and pageSize shape the paged source: ten products over
	// three pages, so MapRuns has to follow the cursor.
	productCount = 10
	pageSize     = 4
	// brokenProduct is the product whose sync fails, the one failure the
	// map's budget allows.
	brokenProduct = 4
	// raceLosers is how many mirrors the first race cancels: its two
	// "slow" mirrors, once eu has answered.
	raceLosers = 2
	// quotesDeadline bounds printQuotes' wait for the cancellations to
	// land; they land in milliseconds, so this only catches a hang.
	quotesDeadline = 5 * time.Second
)

// mirrorName names a price mirror; quote answers for any name except the
// two below, which behave badly on purpose.
type mirrorName string

const (
	// mirrorSlow answers only when its ctx ends.
	mirrorSlow mirrorName = "slow"
	// mirrorDown fails permanently.
	mirrorDown mirrorName = "down"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-joins:", err)
		os.Exit(1)
	}
}

// orderEvents are the parallel branches of the multi-merge: each starts
// its own notification as it arrives.
var orderEvents = []string{"paid", "packed", "shipped"}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// quote asks one mirror for a price. Mirror "slow" answers only when its
// ctx ends, so it is still running when the join decides.
func quote(ctx context.Context, in []byte) ([]byte, error) {
	var mirror mirrorName
	if err := json.Unmarshal(in, &mirror); err != nil {
		return nil, workflow.Permanent(err)
	}
	switch mirror {
	case mirrorSlow:
		<-ctx.Done()
		return nil, ctx.Err()
	case mirrorDown:
		return nil, workflow.Permanent(errors.New("mirror down"))
	}
	return json.Marshal(mirror + ": 42")
}

// syncProduct syncs one product; brokenProduct fails.
func syncProduct(_ context.Context, in []byte) ([]byte, error) {
	var id int
	if err := json.Unmarshal(in, &id); err != nil {
		return nil, workflow.Permanent(err)
	}
	if id == brokenProduct {
		return nil, workflow.Permanent(fmt.Errorf("product %d: bad data", id))
	}
	return nil, nil
}

// products is a paged source of productCount product IDs, pageSize per
// page.
func products(_ context.Context, cursor string) (workflow.MapPage[int], error) {
	start := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil {
			return workflow.MapPage[int]{}, err
		}
		start = n
	}
	end := min(start+pageSize, productCount)
	var ids []int
	for i := start; i < end; i++ {
		ids = append(ids, i)
	}
	return workflow.MapPage[int]{Items: ids, Next: strconv.Itoa(end), Done: end == productCount}, nil
}

// mirrors builds one quote job per mirror name, the children a join waits
// on.
func mirrors(names ...mirrorName) []workflow.Job {
	jobs := make([]workflow.Job, len(names))
	for i, n := range names {
		jobs[i] = workflow.Job{Workflow: "quote", Payload: []byte(`"` + string(n) + `"`)}
	}
	return jobs
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c, err := workflow.Use(memory.New(), workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		return err
	}
	// notified receives each notification the multi-merge starts.
	notified := make(chan string, len(orderEvents))
	// snippet:multi-merge
	// branch is one parallel step that starts its own downstream run.
	branch := func(name string) workflow.Step {
		return workflow.Step{Name: name, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			_, err := workflow.Spawn(ctx, c, "downstream", []workflow.Job{{Workflow: "notify", Payload: []byte(`"` + name + `"`)}})
			return nil, err
		}}
	}
	// snippet:end
	order := workflow.Workflow{Name: "order-events"}
	for _, ev := range orderEvents {
		order.Steps = append(order.Steps, branch(ev))
	}
	wfs := []workflow.Workflow{
		order,
		{Name: "notify", Steps: []workflow.Step{{Name: "send", Run: workflow.Define(func(_ context.Context, ev string) (struct{}, error) {
			notified <- ev
			return struct{}{}, nil
		})}}},
		{Name: "quote", Steps: []workflow.Step{{Name: "ask", Run: quote}}},
		{Name: "product", Steps: []workflow.Step{{Name: "sync", Run: syncProduct}}},
		{Name: "first-quote", Steps: []workflow.Step{{Name: "race", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:first
			res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("slow", "eu", "slow"), workflow.JoinSpec{
				Need: 1,
				Rest: workflow.JoinCancelRest,
				Poll: 5 * time.Millisecond, // how often the join re-reads its children
			})
			if err != nil {
				return nil, err // a Snooze while no mirror has answered yet
			}
			return res.Outputs[0], nil
			// snippet:end
		}}}},
		{Name: "quorum", Steps: []workflow.Step{{Name: "vote", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:quorum
			res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("eu", "down", "us", "slow"), workflow.JoinSpec{
				Need: 2, // two answers make a quorum
				Rest: workflow.JoinCancelRest,
				Poll: 5 * time.Millisecond, // how often the join re-reads its children
			})
			if err != nil {
				return nil, err
			}
			return json.Marshal(res.Winners)
			// snippet:end
		}}}},
		{Name: "catalogue", Steps: []workflow.Step{{Name: "map", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:map
			res, err := workflow.MapRuns(ctx, c, "products", products, func(_ int, id int) (workflow.Job, error) {
				return workflow.Job{Workflow: "product", Payload: []byte(strconv.Itoa(id))}, nil
			}, workflow.MapRunsOptions{InFlight: 3, MaxFailures: 1, Poll: 5 * time.Millisecond}) // 3 children at a time, 1 failure allowed
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
			// snippet:end
		}}}},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Println("discriminator:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "first-quote"})
	if err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Println("  first answer:", string(out))
	if err := printQuotes(ctx, c); err != nil {
		return err
	}

	fmt.Println("partial join:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "quorum"})
	if err != nil {
		return err
	}
	out, err = workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Println("  two of four answered, winners:", string(out))

	fmt.Println("distributed map:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "catalogue"})
	if err != nil {
		return err
	}
	out, err = workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	var res workflow.MapRunsResult
	if err := json.Unmarshal(out, &res); err != nil {
		return err
	}
	fmt.Printf("  started %d, succeeded %d, failed %d (item %d)\n", res.Started, res.Succeeded, res.Failed, res.FailedRefs[0].Index)

	fmt.Println("multi-merge:")
	ref, err = c.Enqueue(ctx, workflow.Job{Workflow: "order-events"})
	if err != nil {
		return err
	}
	if _, err := workflow.AwaitResult(ctx, c, ref, await); err != nil {
		return err
	}
	sent := make([]string, 0, len(orderEvents))
	for range orderEvents {
		sent = append(sent, <-notified)
	}
	slices.Sort(sent) // the branches run in parallel, so they arrive in any order
	fmt.Printf("  one notification per branch: %s\n", strings.Join(sent, ", "))
	return nil
}

// printQuotes counts the first race's mirror runs by status.
func printQuotes(ctx context.Context, c workflow.Client) error {
	deadline := time.Now().Add(quotesDeadline)
	for {
		page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "quote"})
		if err != nil {
			return err
		}
		count := map[workflow.Status]int{}
		for _, r := range page.Runs {
			count[r.Status]++
		}
		if count[workflow.StatusCancelled] == raceLosers || time.Now().After(deadline) {
			fmt.Printf("  mirrors: %d succeeded, %d cancelled\n", count[workflow.StatusSucceeded], count[workflow.StatusCancelled])
			return nil
		}
		time.Sleep(awaitPoll)
	}
}
