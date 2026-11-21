// Command backend-cutover moves an app from one backend to another while
// it keeps running, with package cutover: new jobs go to the new backend,
// the old one finishes the runs it holds, refs stored before the cutover
// still work, the schedules the app made on the old backend are moved, and
// Remaining says when the old backend can be retired. Both backends are
// in-memory drivers here; in production the old one is the database the
// app is leaving (say sqlstore on SQLite) and the new one where it goes
// (sqlstore on Postgres, Redis, Hatchet).
//
// It is the program behind docs/operations/cutover.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "backend-cutover:", err)
		os.Exit(1)
	}
}

// Polling tunables: in-memory runs end in microseconds, so short polls
// keep the program fast without spinning.
const (
	// awaitPoll is how often Await looks at a run's status.
	awaitPoll = 2 * time.Millisecond
	// startTries and startPoll bound waitRunning: a thousand looks a
	// millisecond apart, about a second for a worker to pick a run up.
	startTries = 1000
	startPoll  = time.Millisecond
)

// await is how the program waits for a run to end.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release := make(chan struct{})
	export := workflow.Workflow{Name: "export", Steps: []workflow.Step{{Name: "write",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			<-release // a long export, still running during the cutover
			return in, nil
		}}}}

	// snippet:client
	oldBackend, newBackend := memory.New(), memory.New()
	c := cutover.New(oldBackend, newBackend)   // schedules go to the new backend
	if err := c.Register(export); err != nil { // on both: the old one finishes what it holds
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:end

	// What the app did before the cutover, on the old backend: a run it
	// stored the ref of, and a schedule it made.
	stored, err := oldBackend.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"march"`)})
	if err != nil {
		return err
	}
	if _, err := oldBackend.Schedule(ctx, workflow.Schedule{Name: "nightly", Workflow: "export", Cron: "0 2 * * *",
		Location: time.UTC, Input: []byte(`"nightly"`)}); err != nil {
		return err
	}

	fmt.Println("-- New work goes to the new backend")
	if err := waitRunning(ctx, oldBackend, stored); err != nil {
		return err
	}
	// snippet:enqueue
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "export", Payload: []byte(`"april"`)})
	if err != nil {
		return err
	}
	fmt.Println("enqueued:", ref.RunID) // new:<the new backend's own ID>
	// snippet:end
	if err := waitRunning(ctx, c, ref); err != nil {
		return err
	}
	// snippet:status
	for _, r := range []workflow.RunRef{stored, ref} {
		st, err := c.Status(ctx, r) // an unprefixed ref stored before the cutover is looked up on the old backend first
		if err != nil {
			return err
		}
		fmt.Printf("status of %s: %s\n", r.RunID, st)
	}
	page, err := c.Runs(ctx, workflow.RunQuery{Workflow: "export"}) // both backends, one feed
	if err != nil {
		return err
	}
	// snippet:end
	var ids []string
	for _, r := range page.Runs {
		ids = append(ids, string(r.Ref.RunID))
	}
	slices.Sort(ids)
	fmt.Println("runs:", strings.Join(ids, ", "))

	fmt.Println("-- Move the schedules")
	if err := printSchedules(ctx, c); err != nil {
		return err
	}
	// snippet:move
	inputs := map[string][]byte{"nightly": []byte(`"nightly"`)} // a ScheduleInfo carries no Input: the app supplies it
	moved, err := c.MoveSchedules(ctx, func(info workflow.ScheduleInfo) ([]byte, error) {
		return inputs[info.Name], nil
	})
	if err != nil {
		return err
	}
	for _, m := range moved { // a cutover.Moved: Name, Workflow, From (old:...), To (new:...)
		fmt.Printf("moved %q from %s to %s\n", m.Name, m.From, m.To)
	}
	// snippet:end
	if err := printSchedules(ctx, c); err != nil {
		return err
	}

	fmt.Println("-- Retire the old backend")
	// snippet:remaining
	n, err := c.Remaining(ctx)
	if err != nil {
		return err
	}
	fmt.Println("unfinished runs on the old backend:", n)
	// snippet:end
	close(release)
	for _, r := range []workflow.RunRef{stored, ref} {
		if _, err := workflow.Await(ctx, c, r, await); err != nil {
			return err
		}
	}
	if n, err = c.Remaining(ctx); err != nil {
		return err
	}
	fmt.Println("after the export finished:", n, "- the app can drop the old backend")

	fmt.Println("-- Keep the schedules on the old backend for now")
	return keepOnOld(ctx)
}

// keepOnOld builds a cutover client that leaves schedules on the old
// backend, for an app not ready to move them yet.
func keepOnOld(ctx context.Context) error {
	// snippet:keep-old
	c := cutover.New(memory.New(), memory.New(),
		cutover.SchedulesOn(cutover.SideOld), // Workflow.Cron and Schedule stay on the old backend
	)
	// snippet:end
	if err := c.Register(workflow.Workflow{Name: "report", Steps: []workflow.Step{{Name: "build",
		Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	// snippet:sides
	id, err := c.Schedule(ctx, workflow.Schedule{Name: "weekly", Workflow: "report", Cron: "0 6 * * 1", Location: time.UTC})
	if err != nil {
		return err
	}
	side, _, _ := strings.Cut(string(id), ":") // every ID this client hands out starts with its Side
	fmt.Printf("schedule on the old side: %t (%s)\n", cutover.Side(side) == cutover.SideOld, id)
	_, err = c.MoveSchedules(ctx, func(workflow.ScheduleInfo) ([]byte, error) { return nil, nil })
	fmt.Println("MoveSchedules refused under SchedulesOn(SideOld):", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// waitRunning polls until the run's step has started, so the statuses the
// program prints do not depend on how fast a worker picks it up.
func waitRunning(ctx context.Context, c workflow.Client, ref workflow.RunRef) error {
	for range startTries {
		st, err := c.Status(ctx, ref)
		if err != nil || st == workflow.StatusRunning {
			return err
		}
		time.Sleep(startPoll)
	}
	return fmt.Errorf("run %s did not start", ref.RunID)
}

// printSchedules lists the cutover client's schedules with the side-prefixed
// ID each one has, to show where a schedule lives before and after a move.
func printSchedules(ctx context.Context, c *cutover.Client) error {
	list, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range list {
		fmt.Printf("schedule %q on %s\n", s.Name, s.ID)
	}
	return nil
}
