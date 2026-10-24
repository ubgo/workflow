package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func runInfo(t *testing.T, c *memory.Client, ref workflow.RunRef) workflow.RunInfo {
	t.Helper()
	page, err := c.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("Runs(%+v) = %+v, %v", ref, page, err)
	}
	return page.Runs[0]
}

func TestForceCompleteCancelsTheAttemptWithErrForced(t *testing.T) {
	cause := make(chan error, 1)
	started := make(chan struct{})
	wf := one("w", func(ctx context.Context, in []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return nil, ctx.Err()
	})
	c, _ := newClient(t, []workflow.Workflow{wf})
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	<-started
	if err := c.ForceComplete(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if got := <-cause; !errors.Is(got, workflow.ErrForced) {
		t.Errorf("the attempt's cause = %v, want ErrForced", got)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if info := runInfo(t, c, ref); info.Error != "" {
		t.Errorf("a force-completed run reports error %q", info.Error)
	}
}

func TestForceCompleteOfAFailedRunNotifiesAgain(t *testing.T) {
	wf := one("w", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("boom")) })
	c := memory.New()
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	events := make(chan workflow.RunEvent, 4)
	if err := c.OnRunFinished(func(_ context.Context, ev workflow.RunEvent) { events <- ev }); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	if ev := <-events; ev.Status != workflow.StatusFailed || ev.Step != "s" {
		t.Fatalf("first event = %+v, want failed in step s", ev)
	}
	if err := c.ForceComplete(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if ev := <-events; ev.Status != workflow.StatusSucceeded || ev.Step != "" {
		t.Fatalf("second event = %+v, want succeeded", ev)
	}
}

func TestErrorHistoryBoundAndRetry(t *testing.T) {
	wf := one("w", func(context.Context, []byte) ([]byte, error) { return nil, errors.New("boom") })
	wf.Retry = workflow.RetryPolicy{Max: 4, Backoff: workflow.Backoff{Initial: time.Second}}
	c, fc := newClient(t, []workflow.Workflow{wf}, memory.WithErrorHistory(2))
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	deadline := time.Now().Add(pollFor)
	for st, _ := c.Status(context.Background(), ref); st != workflow.StatusFailed; st, _ = c.Status(context.Background(), ref) {
		if time.Now().After(deadline) {
			t.Fatalf("status %q, want failed", st)
		}
		fc.Advance(time.Minute)
		time.Sleep(time.Millisecond)
	}
	errs := runInfo(t, c, ref).Errors
	if len(errs) != 2 || errs[0].Attempt != 4 || errs[1].Attempt != 5 {
		t.Fatalf("Errors = %+v, want attempts 4 and 5 (bound 2)", errs)
	}
	if _, err := c.Retry(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if got := runInfo(t, c, ref).Errors; len(got) < 2 {
		t.Errorf("Retry dropped the history: %+v", got)
	}
}

func TestErrorHistoryOff(t *testing.T) {
	wf := one("w", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("boom")) })
	c, _ := newClient(t, []workflow.Workflow{wf}, memory.WithErrorHistory(0))
	ref := enqueue(t, c, workflow.Job{Workflow: "w"})
	waitStatus(t, c, ref, workflow.StatusFailed)
	if errs := runInfo(t, c, ref).Errors; len(errs) != 0 {
		t.Errorf("Errors = %+v with history off", errs)
	}
}

func TestTriggerKeepsLastAcrossUpsert(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("w", ok)})
	ctx := context.Background()
	s := workflow.Schedule{Name: "n", Workflow: "w", Cron: "0 3 * * *", Location: time.UTC}
	id, err := c.Schedule(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := c.TriggerSchedule(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Schedule(ctx, s); err != nil {
		t.Fatal(err)
	}
	list, err := c.Schedules(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("Schedules = %+v, %v", list, err)
	}
	if list[0].Last.Run != ref {
		t.Errorf("Last.Run after an upsert = %+v, want the triggered run %+v", list[0].Last.Run, ref)
	}
	if err := c.Unschedule(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TriggerSchedule(ctx, id); !errors.Is(err, workflow.ErrNotFound) {
		t.Errorf("trigger of an unscheduled schedule: %v, want ErrNotFound", err)
	}
}
