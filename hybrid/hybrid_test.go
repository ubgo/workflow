package hybrid_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hybrid"
	"github.com/ubgo/workflow/memory"
)

func TestCronFiresOnTheSchedulerAndRunsOnJobs(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC))
	sched := memory.New(memory.WithClock(clk), memory.WithNodeID("sched"))
	jobs := memory.New(memory.WithClock(clk), memory.WithNodeID("jobs"))
	c := hybrid.New(sched, jobs)

	var mu sync.Mutex
	var infos []workflow.StepInfo
	wf := workflow.Workflow{Name: "report", Cron: []string{"* * * * *"}, Steps: []workflow.Step{{Name: "run",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			mu.Lock()
			infos = append(infos, info)
			mu.Unlock()
			return in, nil
		}}}}
	if err := c.Register(wf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // 10:01:00
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(infos)
		mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the cron tick never ran on the jobs client")
		}
		time.Sleep(2 * time.Millisecond)
	}
	got := infos[0]
	if got.Node != "jobs" {
		t.Errorf("the step ran on node %q, want the jobs client", got.Node)
	}
	if at, ok := got.TickAt(); !ok || !at.Equal(time.Date(2026, 1, 1, 10, 1, 0, 0, time.UTC)) {
		t.Errorf("TickAt = %s %v, want 10:01", at, ok)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "report"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobs.Status(ctx, ref); err != nil {
		t.Errorf("an Enqueue did not land on the jobs client: %v", err)
	}
}

func TestCapabilitiesAndRefusals(t *testing.T) {
	c := hybrid.New(memory.New(), memory.New())
	caps := c.Capabilities()
	if !caps.Has(workflow.CapCron | workflow.CapDelayedStart | workflow.CapTickTime) {
		t.Errorf("caps = %s, want cron from the scheduler and runs from jobs", caps)
	}
	if caps&(workflow.CapScheduleTrigger|workflow.CapOverlapSkip|workflow.CapScheduleKeep|workflow.CapTickEvents) != 0 {
		t.Errorf("caps = %s, claims what the split cannot honour", caps)
	}
	wf := workflow.Workflow{Name: "w", Cron: []string{"* * * * *"}, CronOverlap: workflow.OverlapSkip,
		Steps: []workflow.Step{{Name: "a", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}}
	if err := c.Register(wf); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("CronOverlap: %v, want ErrUnsupported", err)
	}
	if _, err := c.TriggerSchedule(context.Background(), "x"); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("TriggerSchedule: %v, want ErrUnsupported", err)
	}
}
