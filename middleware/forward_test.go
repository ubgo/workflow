package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

// TestWrappers_ForwardOptionalInterfaces: WrapClient and WithCodec pass
// the run query and pause families through to the driver, and refuse
// with an UnsupportedError over a client without them.
func TestWrappers_ForwardOptionalInterfaces(t *testing.T) {
	codec, _ := middleware.Encrypt(testKey(1))
	mem := memory.New()
	wrapped := middleware.WithCodec(middleware.WrapClient(mem), codec)
	if err := wrapped.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Wait: &workflow.WaitSpec{Event: "go"}}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := wrapped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer mem.Stop()
	ref, err := wrapped.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	page, err := wrapped.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: "w"})
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref != ref {
		t.Fatalf("Runs through the wrappers = %+v, %v", page, err)
	}
	if string(page.Runs[0].Payload) == "secret" {
		t.Fatal("the stored payload should be the sealed bytes")
	}
	if err := wrapped.(workflow.Pauser).Pause(ctx, workflow.PauseWorkflow("w")); err != nil {
		t.Fatal(err)
	}
	if got, _ := mem.Paused(ctx); len(got) != 1 {
		t.Fatalf("pause did not reach the driver: %v", got)
	}

	var ue *workflow.UnsupportedError
	_, err = middleware.WithCodec(&stubClient{}, codec).(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{})
	if !errors.As(err, &ue) || ue.Missing != workflow.CapRunQuery {
		t.Fatalf("over a client without Runs: %v", err)
	}
}
