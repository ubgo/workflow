package middleware_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

func TestSoftTimeoutAfter(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	var told atomic.Int32
	mw := middleware.SoftTimeout(middleware.SoftTimeoutConfig{After: time.Minute, Clock: clk,
		OnSoft: func(context.Context, workflow.StepInfo) { told.Add(1) }})
	started := make(chan struct{})
	h := mw(func(ctx context.Context, _ []byte) ([]byte, error) {
		if middleware.SoftExpired(ctx) {
			return nil, errors.New("expired too early")
		}
		close(started)
		<-middleware.SoftDone(ctx)
		if !middleware.SoftExpired(ctx) {
			return nil, errors.New("SoftExpired false after SoftDone closed")
		}
		return []byte("checkpointed"), nil
	})
	done := make(chan error, 1)
	go func() {
		out, err := h(context.Background(), nil)
		if err == nil && string(out) != "checkpointed" {
			err = errors.New("unexpected output " + string(out))
		}
		done <- err
	}()
	<-started
	if err := clk.BlockUntil(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(59 * time.Second)
	select {
	case err := <-done:
		t.Fatalf("returned before the soft deadline: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	clk.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if told.Load() != 1 {
		t.Fatalf("OnSoft told %d times", told.Load())
	}
}

func TestSoftTimeoutPerStep(t *testing.T) {
	mw := middleware.SoftTimeout(middleware.SoftTimeoutConfig{For: func(info workflow.StepInfo) time.Duration {
		if info.Step == "long" {
			return 5 * time.Millisecond
		}
		return 0
	}})
	h := mw(func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-middleware.SoftDone(ctx):
			return []byte("soft"), nil
		case <-time.After(time.Second):
			return []byte("no soft deadline"), nil
		}
	})
	out, err := h(workflow.WithStepInfo(context.Background(), workflow.StepInfo{Step: "long"}), nil)
	if err != nil || string(out) != "soft" {
		t.Fatalf("long step: %q %v", out, err)
	}
	// A step For gives no soft deadline: SoftDone is nil.
	out, err = mw(func(ctx context.Context, _ []byte) ([]byte, error) {
		if middleware.SoftDone(ctx) != nil {
			return nil, errors.New("soft deadline where For returned zero")
		}
		return []byte("ok"), nil
	})(workflow.WithStepInfo(context.Background(), workflow.StepInfo{Step: "short"}), nil)
	if err != nil || string(out) != "ok" {
		t.Fatalf("short step: %q %v", out, err)
	}
}

func TestSoftTimeoutRefusesBadConfig(t *testing.T) {
	for _, cfg := range []middleware.SoftTimeoutConfig{{}, {After: -1}} {
		_, err := middleware.SoftTimeout(cfg)(func(context.Context, []byte) ([]byte, error) { return nil, nil })(context.Background(), nil)
		if !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Fatalf("%+v: %v", cfg, err)
		}
	}
	if middleware.SoftExpired(context.Background()) || middleware.SoftDone(context.Background()) != nil {
		t.Fatal("outside SoftTimeout there is no soft deadline")
	}
}

type fakeLister []workflow.ScheduleInfo

func (f fakeLister) Schedules(context.Context) ([]workflow.ScheduleInfo, error) { return f, nil }

func TestLatestOnly(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 3, 1, 10, 30, 0, 0, time.UTC))
	lister := fakeLister{{ID: "s1", Cron: "0 * * * *", Location: time.UTC}}
	var skipped atomic.Int32
	mw := middleware.LatestOnly(middleware.LatestOnlyConfig{Schedules: lister, Clock: clk,
		OnSkip: func(context.Context, workflow.StepInfo, time.Time, time.Time) { skipped.Add(1) }})
	var ran atomic.Int32
	h := mw(func(context.Context, []byte) ([]byte, error) { ran.Add(1); return []byte("did work"), nil })
	run := func(labels map[string]string) []byte {
		t.Helper()
		ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: labels})
		out, err := h(ctx, []byte("input"))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	tick := func(at time.Time) map[string]string {
		return workflow.FiredLabelsAt(nil, "s1", "", at)
	}
	// 09:00's run at 10:30: the 10:00 tick is due, so it is stale.
	if out := run(tick(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))); string(out) != "input" {
		t.Fatalf("stale run did work: %q", out)
	}
	// 10:00's run at 10:30: the newest, it runs.
	if out := run(tick(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC))); string(out) != "did work" {
		t.Fatalf("latest run skipped: %q", out)
	}
	// Not fired by a schedule, or by a schedule that is gone: runs.
	run(nil)
	run(workflow.FiredLabelsAt(nil, "gone", "", time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)))
	if ran.Load() != 3 || skipped.Load() != 1 {
		t.Fatalf("ran %d skipped %d, want 3 and 1", ran.Load(), skipped.Load())
	}
	// Next overrides the lookup.
	next := middleware.LatestOnly(middleware.LatestOnlyConfig{Clock: clk,
		Next: func(context.Context, workflow.StepInfo, time.Time) (time.Time, bool, error) {
			return clk.Now().Add(-time.Second), true, nil
		}})(func(context.Context, []byte) ([]byte, error) { return []byte("did work"), nil })
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{Labels: tick(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC))})
	if out, _ := next(ctx, []byte("input")); string(out) != "input" {
		t.Fatalf("Next override not used: %q", out)
	}
	if _, err := middleware.LatestOnly(middleware.LatestOnlyConfig{})(nil)(ctx, nil); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("empty config: %v", err)
	}
}
