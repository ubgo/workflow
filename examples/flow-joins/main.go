// Command flow-joins shows joins over child runs: JoinChildren continuing
// on the first child to succeed (a discriminator) or on two of three (a
// partial join) and cancelling the rest, and MapRuns running one child per
// item of a paged source with a bounded number in flight and a failure
// budget. It runs on the in-memory driver, so nothing needs to be
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
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "flow-joins:", err)
		os.Exit(1)
	}
}

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: 2 * time.Millisecond}

// quote asks one mirror for a price. Mirror "slow" answers only when its
// ctx ends, so it is still running when the join decides.
func quote(ctx context.Context, in []byte) ([]byte, error) {
	var mirror string
	if err := json.Unmarshal(in, &mirror); err != nil {
		return nil, workflow.Permanent(err)
	}
	switch mirror {
	case "slow":
		<-ctx.Done()
		return nil, ctx.Err()
	case "down":
		return nil, workflow.Permanent(errors.New("mirror down"))
	}
	return json.Marshal(mirror + ": 42")
}

// syncProduct syncs one product; product 4 is broken.
func syncProduct(_ context.Context, in []byte) ([]byte, error) {
	var id int
	if err := json.Unmarshal(in, &id); err != nil {
		return nil, workflow.Permanent(err)
	}
	if id == 4 {
		return nil, workflow.Permanent(fmt.Errorf("product %d: bad data", id))
	}
	return nil, nil
}

// products is a paged source of 10 product IDs, 4 per page.
func products(_ context.Context, cursor string) (workflow.MapPage[int], error) {
	start := 0
	if cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil {
			return workflow.MapPage[int]{}, err
		}
		start = n
	}
	end := min(start+4, 10)
	var ids []int
	for i := start; i < end; i++ {
		ids = append(ids, i)
	}
	return workflow.MapPage[int]{Items: ids, Next: strconv.Itoa(end), Done: end == 10}, nil
}

func mirrors(names ...string) []workflow.Job {
	jobs := make([]workflow.Job, len(names))
	for i, n := range names {
		jobs[i] = workflow.Job{Workflow: "quote", Payload: []byte(`"` + n + `"`)}
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
	wfs := []workflow.Workflow{
		{Name: "quote", Steps: []workflow.Step{{Name: "ask", Run: quote}}},
		{Name: "product", Steps: []workflow.Step{{Name: "sync", Run: syncProduct}}},
		{Name: "first-quote", Steps: []workflow.Step{{Name: "race", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			// snippet:first
			res, err := workflow.JoinChildren(ctx, c, "mirrors", mirrors("slow", "eu", "slow"), workflow.JoinSpec{
				Need: 1,
				Rest: workflow.JoinCancelRest,
				Poll: 5 * time.Millisecond,
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
				Need: 2,
				Rest: workflow.JoinCancelRest,
				Poll: 5 * time.Millisecond,
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
			}, workflow.MapRunsOptions{InFlight: 3, MaxFailures: 1, Poll: 5 * time.Millisecond})
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
	return nil
}

// printQuotes counts the first race's mirror runs by status.
func printQuotes(ctx context.Context, c workflow.Client) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		page, err := c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "quote"})
		if err != nil {
			return err
		}
		count := map[workflow.Status]int{}
		for _, r := range page.Runs {
			count[r.Status]++
		}
		if count[workflow.StatusCancelled] == 2 || time.Now().After(deadline) {
			fmt.Printf("  mirrors: %d succeeded, %d cancelled\n", count[workflow.StatusSucceeded], count[workflow.StatusCancelled])
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}
