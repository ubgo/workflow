package hatchet_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// pauseHoldEnv turns on TestPauseHoldLive, which holds a run for six
// minutes (past the engine's five-minute default TTL plus its 30s expiry
// sweep), so it is not part of every live run.
const pauseHoldEnv = "WFTEST_PAUSE_TTL"

// liveAPI builds Config.API from the token the SDK reads.
func liveAPI(t *testing.T) *hatchet.API {
	t.Helper()
	api, err := hatchet.NewAPI(os.Getenv(envToken), os.Getenv("HATCHET_CLIENT_SERVER_URL"))
	if err != nil {
		t.Fatal(err)
	}
	return api
}

// TestPauseLive proves CapPauseWorkflow and CapSchedulePause on the
// engine: a paused workflow's new run stays queued and is listed by
// Paused, Resume lets it run, a cron trigger is paused and resumed and
// listed as paused meanwhile, an unknown trigger is ErrNotFound; and, as
// a control, a pause sent with a short TTL makes the engine cancel the
// held run, the trap the driver's long TTL avoids.
func TestPauseLive(t *testing.T) {
	skipWithoutEngine(t)
	prefix := "wfpause_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	control := workflow.Workflow{Name: prefix + "_ctl", Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix, API: liveAPI(t)}, wf, control)
	ctx := context.Background()

	if err := c.Pause(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Resume(context.Background(), workflow.PauseWorkflow(wf.Name)) })
	paused, err := c.Paused(ctx)
	if err != nil || !slices.Contains(paused, workflow.PauseWorkflow(wf.Name)) {
		t.Fatalf("Paused = %v, %v", paused, err)
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusQueued)
	holdStatus(t, c, ref, workflow.StatusQueued, 5*time.Second)
	if err := c.Resume(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
	if paused, _ := c.Paused(ctx); slices.Contains(paused, workflow.PauseWorkflow(wf.Name)) {
		t.Fatalf("still paused after Resume: %v", paused)
	}

	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: wf.Name, Name: "p", Cron: "0 4 1 1 *", Location: time.UTC})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Unschedule(context.Background(), id) })
	scheduleState := func() workflow.ScheduleInfo {
		t.Helper()
		list, err := c.Schedules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range list {
			if s.ID == id {
				return s
			}
		}
		t.Fatalf("schedule %s not listed", id)
		return workflow.ScheduleInfo{}
	}
	if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if s := scheduleState(); !s.Paused || !s.Next.IsZero() {
		t.Fatalf("after PauseSchedule: %+v", s)
	}
	if err := c.ResumeSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		t.Fatal(err)
	}
	if s := scheduleState(); s.Paused || s.Next.IsZero() {
		t.Fatalf("after ResumeSchedule: %+v", s)
	}
	if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: workflow.ScheduleID(uuid.NewString())}); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("unknown schedule: %v", err)
	}

	// Control: the engine cancels held runs once the pause TTL passes.
	if err := hatchet.PauseWithTTL(ctx, c, control.Name, "10s"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Resume(context.Background(), workflow.PauseWorkflow(control.Name)) })
	cref, err := c.Enqueue(ctx, workflow.Job{Workflow: control.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, cref, workflow.StatusCancelled)
}

// TestPauseHoldLive proves that Pause holds a run past the engine's
// five-minute default TTL: the run stays queued for six minutes (the
// default plus the engine's expiry sweep of up to 30s, with margin), then
// runs after Resume. It is on only with WFTEST_PAUSE_TTL=1.
func TestPauseHoldLive(t *testing.T) {
	skipWithoutEngine(t)
	if os.Getenv(pauseHoldEnv) == "" {
		t.Skipf("%s is not set: this test holds a run for six minutes", pauseHoldEnv)
	}
	prefix := "wfhold_" + uuid.NewString()[:8]
	wf := workflow.Workflow{Name: prefix, Steps: []workflow.Step{{Name: "a", Run: okStep}}}
	c, _ := startLive(t, hatchet.Config{WorkerName: prefix, API: liveAPI(t)}, wf)
	ctx := context.Background()
	if err := c.Pause(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Resume(context.Background(), workflow.PauseWorkflow(wf.Name)) })
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusQueued)
	holdStatus(t, c, ref, workflow.StatusQueued, 6*time.Minute)
	if err := c.Resume(ctx, workflow.PauseWorkflow(wf.Name)); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, c, ref, workflow.StatusSucceeded)
}
