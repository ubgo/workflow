package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
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

// TestReplay_FirstRunSeesWhatReplaysSee: Replaying turns false where the
// code passes its history; the first run gets the JSON-decoded value and
// the recorded form of a Permanent error, exactly as replays do; and a
// transient error the code ignores still fails this run of the handler.
func TestReplay_FirstRunSeesWhatReplaysSee(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	errDeclined := errors.New("card declined")
	type seen struct {
		replayingBefore, replayingAfter bool
		zone                            string
		isSentinel                      bool
		text                            string
	}
	var runs []seen
	h, err := workflow.NewReplay(func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		var s seen
		s.replayingBefore = rc.Replaying()
		at, err := workflow.ReplayStep(rc, "when", func(context.Context) (time.Time, error) {
			return time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("X", 3600)), nil
		})
		if err != nil {
			return nil, err
		}
		s.zone, _ = at.Zone()
		_, err = workflow.ReplayStep(rc, "charge", func(context.Context) (int, error) { return 0, workflow.Permanent(errDeclined) })
		s.isSentinel, s.text = errors.Is(err, errDeclined), err.Error()
		if err := rc.Sleep("nap", time.Hour); err != nil {
			runs = append(runs, s)
			return nil, err
		}
		s.replayingAfter = rc.Replaying()
		runs = append(runs, s)
		return []byte(`"ok"`), nil
	}, workflow.ReplayOptions{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	run := workflow.WithJournal(workflow.NewMemoryJournal())(h)
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: "s"})
	if _, err := run(ctx, nil); !errors.Is(err, workflow.ErrSnoozed) {
		t.Fatalf("first run: %v", err)
	}
	clk.Advance(time.Hour)
	if out, err := run(ctx, nil); err != nil || string(out) != `"ok"` {
		t.Fatalf("second run: %s %v", out, err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs %+v", runs)
	}
	first, second := runs[0], runs[1]
	if first.replayingBefore || !second.replayingBefore || second.replayingAfter {
		t.Fatalf("Replaying: %+v %+v", first, second)
	}
	if first.zone != second.zone || first.isSentinel != second.isSentinel || first.text != second.text {
		t.Fatalf("the first run saw %+v, the replay %+v", first, second)
	}

	// A transient error the code ignores fails this run of the handler.
	var later atomic.Int32
	h2, _ := workflow.NewReplay(func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		_, _ = workflow.ReplayStep(rc, "flaky", func(context.Context) (int, error) { return 0, errors.New("timeout") })
		_, _ = workflow.ReplayStep(rc, "next", func(context.Context) (int, error) { later.Add(1); return 1, nil })
		return []byte(`"ignored"`), nil
	}, workflow.ReplayOptions{})
	run2 := workflow.WithJournal(workflow.NewMemoryJournal())(h2)
	if out, err := run2(ctx, nil); err == nil || err.Error() != "timeout" || workflow.IsPermanent(err) || out != nil || later.Load() != 0 {
		t.Fatalf("an ignored transient error: %s %v, later calls %d", out, err, later.Load())
	}
}

// TestReplayChild_RepeatedName: a loop that calls ReplayChild with one
// name on every pass spawns one child per pass and gets each one's
// result, as ReplayStep's names need not be unique either.
func TestReplayChild_RepeatedName(t *testing.T) {
	var c workflow.Client
	kid := workflow.Workflow{Name: "kid", Steps: []workflow.Step{{Name: "echo", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
	h, err := workflow.NewReplay(func(rc *workflow.ReplayCtx, _ []byte) ([]byte, error) {
		var got []byte
		for _, p := range []string{`"a"`, `"b"`, `"a"`} {
			out, err := workflow.ReplayChild(rc, "inv", c, workflow.Job{Workflow: "kid", Payload: []byte(p)}, workflow.ChildOptions{Poll: time.Millisecond})
			if err != nil {
				return nil, err
			}
			got = append(got, out...)
		}
		return got, nil
	}, workflow.ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	parent := workflow.Workflow{Name: "parent", Steps: []workflow.Step{{Name: "o", Run: h}}}
	c = flowClient(t, workflow.NewMemoryJournal(), kid, parent)
	ref := enqueue(t, c, workflow.Job{Workflow: "parent"})
	if st := waitRun(t, c, ref); st != workflow.StatusSucceeded {
		info, _ := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Ref: ref})
		t.Fatalf("parent is %s (%+v), want succeeded", st, info.Runs)
	}
	out, err := c.(workflow.ResultReader).Result(context.Background(), ref)
	if err != nil || string(out) != `"a""b""a"` {
		t.Fatalf("result %s, %v", out, err)
	}
	page, err := c.(workflow.RunQuerier).Runs(context.Background(), workflow.RunQuery{Workflow: "kid"})
	if err != nil || len(page.Runs) != 3 {
		t.Fatalf("%d children, %v; want one per call", len(page.Runs), err)
	}
}
