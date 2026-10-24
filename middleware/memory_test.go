package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// TestUniqueUntilFinished_OverWrappedMemoryClient stacks the middleware the
// way an app does (Route inside a WrapClient, then WithCodec) on the real
// memory driver, and proves the key is freed by the driver's own
// run-finished event, not by the TTL.
func TestUniqueUntilFinished_OverWrappedMemoryClient(t *testing.T) {
	mem := memory.New()
	codec, _ := middleware.Encrypt(testKey(1))
	wrapped := middleware.WithCodec(middleware.WrapClient(mem, middleware.Route(middleware.RouteRule{Labels: map[string]string{"queue": "default"}})), codec)

	by := func(j workflow.Job) string { return j.Labels["order"] }
	uniq, err := middleware.UniqueUntilFinished(wrapped, by, middleware.NewMemoryUnique(nil), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c := middleware.WrapClient(wrapped, uniq)

	release := make(chan struct{})
	step := middleware.DecodeStep(codec)(func(context.Context, []byte) ([]byte, error) {
		<-release
		return nil, nil
	})
	if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: step}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()

	job := workflow.Job{Workflow: "w", Payload: []byte("p"), Labels: map[string]string{"order": "o1"}}
	ref, err := c.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, job); !errors.Is(err, workflow.ErrDuplicate) {
		t.Fatalf("duplicate passed while the run is in flight: %v", err)
	}
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if st.IsTerminal() {
			if st != workflow.StatusSucceeded {
				t.Fatalf("run ended %q", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	// Status turns terminal before observers run, so poll for the release.
	for {
		_, err := c.Enqueue(ctx, job)
		if err == nil {
			return
		}
		if !errors.Is(err, workflow.ErrDuplicate) || time.Now().After(deadline) {
			t.Fatalf("key not released after the run ended: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}
