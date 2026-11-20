package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

func TestNewReplay_Refusals(t *testing.T) {
	fn := func(*workflow.ReplayCtx, []byte) ([]byte, error) { return nil, nil }
	if _, err := workflow.NewReplay(nil, workflow.ReplayOptions{}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil function: %v", err)
	}
	var unsup *workflow.UnsupportedError
	if _, err := workflow.NewReplay(fn, workflow.ReplayOptions{Client: noSnooze{memory.New()}}); !errors.As(err, &unsup) || !unsup.Missing.Has(workflow.CapSnooze) {
		t.Fatalf("a client without CapSnooze: %v", err)
	}
	h, err := workflow.NewReplay(fn, workflow.ReplayOptions{Client: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: "s"})
	if _, err := h(ctx, nil); !errors.Is(err, workflow.ErrNoJournal) || !workflow.IsPermanent(err) {
		t.Fatalf("no journal: %v", err)
	}
}

// TestReplay_SwallowedSuspend: code that ignores a wait's error and
// returns a result still waits, and every call after the ignored one
// returns the suspension again.
func TestReplay_SwallowedSuspend(t *testing.T) {
	var after atomic.Int32
	h, err := workflow.NewReplay(func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		_ = rc.Sleep("nap", time.Hour) // ignored: a bug in the caller
		if _, err := workflow.ReplayStep(rc, "later", func(context.Context) (int, error) { after.Add(1); return 1, nil }); err == nil {
			t.Error("a call after a suspension must return it")
		}
		return []byte(`"done"`), nil
	}, workflow.ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: "s"})
	run := workflow.WithJournal(workflow.NewMemoryJournal())(h)
	out, err := run(ctx, nil)
	if d, ok := workflow.SnoozeDelay(err); !ok || d <= 0 || out != nil {
		t.Fatalf("got %s %v, want a snooze", out, err)
	}
	if after.Load() != 0 {
		t.Fatal("the step after the suspension ran")
	}
}
