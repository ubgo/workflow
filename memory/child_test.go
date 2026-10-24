package memory_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestAwaitChildWaitsWithoutSpendingAttempts runs a parent whose step
// awaits a child: the parent snoozes while the child runs, gets the
// child's result, and spawns it once however many times it looked.
func TestAwaitChildWaitsWithoutSpendingAttempts(t *testing.T) {
	inner := memory.New()
	c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var spawned atomic.Int32
	child := one("child", func(ctx context.Context, in []byte) ([]byte, error) {
		spawned.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte(`"child:` + string(in) + `"`), nil
	})
	var looks atomic.Int32
	parent := one("parent", func(ctx context.Context, _ []byte) ([]byte, error) {
		looks.Add(1)
		return workflow.AwaitChild(ctx, c, "kid", workflow.Job{Workflow: "child", Payload: []byte("x")},
			workflow.ChildOptions{Poll: 5 * time.Millisecond})
	})
	parent.Retry = *workflow.RetriesNone() // each look must not spend an attempt
	for _, wf := range []workflow.Workflow{child, parent} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer inner.Stop()
	ref := enqueue(t, inner, workflow.Job{Workflow: "parent"})
	deadline := time.Now().Add(pollFor)
	for looks.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	waitStatus(t, inner, ref, workflow.StatusSucceeded)
	out, err := inner.Result(context.Background(), ref)
	if err != nil || string(out) != `"child:x"` {
		t.Fatalf("parent result = %s, %v; want the child's", out, err)
	}
	if n := spawned.Load(); n != 1 {
		t.Fatalf("the child ran %d times, want 1", n)
	}
}

func TestAwaitChildFailsWhenTheChildFails(t *testing.T) {
	inner := memory.New()
	c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	child := one("child", func(context.Context, []byte) ([]byte, error) { return nil, workflow.Permanent(errors.New("no")) })
	parent := one("parent", func(ctx context.Context, _ []byte) ([]byte, error) {
		return workflow.AwaitChild(ctx, c, "kid", workflow.Job{Workflow: "child"}, workflow.ChildOptions{Poll: time.Millisecond})
	})
	parent.Retry = workflow.RetryPolicy{Max: 5}
	for _, wf := range []workflow.Workflow{child, parent} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer inner.Stop()
	ref := enqueue(t, inner, workflow.Job{Workflow: "parent"})
	waitStatus(t, inner, ref, workflow.StatusFailed)
	info, err := inner.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(info.Runs) != 1 || info.Runs[0].Attempts != 1 {
		t.Fatalf("parent = %+v, %v; want one attempt (a failed child is Permanent)", info.Runs, err)
	}
}

// noSnooze is a memory client that does not claim CapSnooze.
type noSnooze struct{ *memory.Client }

func (n noSnooze) Capabilities() workflow.Caps {
	return n.Client.Capabilities() &^ workflow.CapSnooze
}

func TestAwaitChildRefusesAParentWithoutSnooze(t *testing.T) {
	inner := memory.New()
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: "s"})
	_, err := workflow.AwaitChild(ctx, noSnooze{inner}, "kid", workflow.Job{Workflow: "child"}, workflow.ChildOptions{})
	if !errors.Is(err, workflow.ErrUnsupported) || !workflow.IsPermanent(err) {
		t.Fatalf("AwaitChild on a parent without CapSnooze = %v, want a Permanent ErrUnsupported", err)
	}
	_, err = workflow.AwaitChild(ctx, inner, "kid", workflow.Job{Workflow: "child"}, workflow.ChildOptions{Parent: noSnooze{inner}})
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("ChildOptions.Parent without CapSnooze = %v, want ErrUnsupported", err)
	}
}

func TestPlainEnqueueCannotForgeAParentLabel(t *testing.T) {
	c, _ := newClient(t, []workflow.Workflow{one("child", ok)})
	_, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "child",
		Labels: map[string]string{workflow.LabelParentRun: "run-1", workflow.LabelParentStep: "s"}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a forged parent label = %v, want ErrInvalid", err)
	}
	_, err = c.Enqueue(context.Background(), workflow.Job{Workflow: "child", Chain: []workflow.Job{
		{Workflow: "child", Labels: map[string]string{workflow.LabelSchedule: "s"}}}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("a forged label in a chained job = %v, want ErrInvalid", err)
	}
}

// awaitHarness registers child and a parent that awaits n children on a
// journaled memory client.
func awaitHarness(t *testing.T, child workflow.Handler, n int) (*memory.Client, workflow.RunRef) {
	t.Helper()
	inner := memory.New()
	c, err := workflow.Use(inner, workflow.Journal(workflow.NewMemoryJournal()))
	if err != nil {
		t.Fatal(err)
	}
	parent := one("parent", func(ctx context.Context, _ []byte) ([]byte, error) {
		jobs := make([]workflow.Job, n)
		for i := range jobs {
			jobs[i] = workflow.Job{Workflow: "child", Payload: []byte{byte('a' + i)}}
		}
		outs, err := workflow.AwaitChildren(ctx, c, "kids", jobs, workflow.ChildOptions{Poll: time.Millisecond})
		if err != nil {
			return nil, err
		}
		var joined []byte
		for _, o := range outs {
			joined = append(joined, o...)
		}
		return joined, nil
	})
	parent.Retry = *workflow.RetriesNone()
	for _, wf := range []workflow.Workflow{one("child", child), parent} {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(inner.Stop)
	return inner, enqueue(t, inner, workflow.Job{Workflow: "parent"})
}

func TestAwaitChildrenJoinsResultsInJobOrder(t *testing.T) {
	inner, ref := awaitHarness(t, func(_ context.Context, in []byte) ([]byte, error) { return in, nil }, 3)
	waitStatus(t, inner, ref, workflow.StatusSucceeded)
	out, err := inner.Result(context.Background(), ref)
	if err != nil || string(out) != "abc" {
		t.Fatalf("joined = %q, %v; want abc", out, err)
	}
}

func TestAwaitChildFailsWhenTheChildIsCancelled(t *testing.T) {
	inner, ref := awaitHarness(t, func(context.Context, []byte) ([]byte, error) { return nil, workflow.Discard(errors.New("gone")) }, 1)
	waitStatus(t, inner, ref, workflow.StatusFailed)
	page, err := inner.Runs(context.Background(), workflow.RunQuery{Ref: ref})
	if err != nil || len(page.Runs) != 1 {
		t.Fatal(err)
	}
	if msg := page.Runs[0].Error; !strings.Contains(msg, "discarded") {
		t.Fatalf("parent error = %q, want it to name the child's end", msg)
	}
}
