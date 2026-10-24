package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
)

func TestUniqueUntilFinished_ReleasesOnRunEnd(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	n := &notifierStub{stubClient: &stubClient{caps: workflow.CapRunFinished}}
	by := func(j workflow.Job) string { return string(j.Payload) }
	mw, err := middleware.UniqueUntilFinished(n, by, store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(n, mw)
	ctx := context.Background()
	job := workflow.Job{Workflow: "w", Payload: []byte("a")}

	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate passed while the run is in flight: %v", err)
	}
	n.finish(0, workflow.StatusFailed)
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatalf("lock kept after the run ended: %v", err)
	}
	// A late duplicate event for the first run must not free the lock the
	// second job now holds (delivery is at least once).
	n.finish(0, workflow.StatusFailed)
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("a stale event freed a newer holder's lock: %v", err)
	}
	n.finish(1, workflow.StatusSucceeded)
	if _, err := c.Enqueue(ctx, job); err != nil {
		t.Fatalf("lock kept after the second run ended: %v", err)
	}
}

func TestUnique_OwnsItsLabels(t *testing.T) {
	store := middleware.NewMemoryUnique(clock.NewFake(time.Unix(0, 0)))
	n := &notifierStub{stubClient: &stubClient{}}
	by := func(j workflow.Job) string { return string(j.Payload) }
	mw, err := middleware.UniqueUntilFinished(n, by, store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(n, mw)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	stolen := n.jobs[0].Labels
	if stolen[middleware.UniqueKeyLabel] != "a" || stolen[middleware.UniqueOwnerLabel] == "" {
		t.Fatalf("labels not stamped: %v", stolen)
	}
	// A job that forges the labels, or any other internal label, is
	// refused, so its end cannot free the real holder's lock.
	forged := map[string]string{middleware.UniqueKeyLabel: "a", middleware.UniqueOwnerLabel: stolen[middleware.UniqueOwnerLabel], "tenant": "t1"}
	for _, labels := range []map[string]string{forged, {workflow.LabelPrefixInternal + "x": "y"}} {
		for _, payload := range []string{"", "b"} { // not unique, and unique
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte(payload), Labels: labels}); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("labels %v payload %q: err = %v, want ErrInvalid", labels, payload, err)
			}
		}
	}
	if len(n.jobs) != 1 {
		t.Fatalf("a refused job reached the driver: %d jobs", len(n.jobs))
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("a")}); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("lock lost: %v", err)
	}
	// Other app labels pass, unchanged and unmutated.
	app := map[string]string{"tenant": "t1"}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Labels: app}); err != nil {
		t.Fatal(err)
	}
	if got := n.jobs[1].Labels; got["tenant"] != "t1" || got[middleware.UniqueKeyLabel] != "" || len(app) != 1 {
		t.Fatalf("app labels = %v", got)
	}
}

func TestUniqueUntilFinished_RefusesWithoutNotifier(t *testing.T) {
	store := middleware.NewMemoryUnique(nil)
	_, err := middleware.UniqueUntilFinished(&stubClient{}, func(workflow.Job) string { return "k" }, store, time.Hour)
	var ue *workflow.UnsupportedError
	if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &ue) || ue.Missing != workflow.CapRunFinished {
		t.Fatalf("err = %v", err)
	}
	// Wrappers forward the hook when the inner client has it, and refuse
	// honestly when it does not.
	n := &notifierStub{stubClient: &stubClient{}}
	codec, _ := middleware.Encrypt(testKey(1))
	if _, err := middleware.UniqueUntilFinished(middleware.WithCodec(middleware.WrapClient(n), codec), func(workflow.Job) string { return "k" }, store, time.Hour); err != nil || len(n.observers) != 1 {
		t.Fatalf("wrapped notifier not forwarded: err=%v observers=%d", err, len(n.observers))
	}
	_, err = middleware.UniqueUntilFinished(middleware.WrapClient(&stubClient{}), func(workflow.Job) string { return "k" }, store, time.Hour)
	if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &ue) || ue.Missing != workflow.CapRunFinished {
		t.Fatalf("wrapped client without the hook: %v", err)
	}
}
