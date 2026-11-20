package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/workflowtest"
)

// TestUniqueAcceptsLabelsTheLibraryAllowed: a library label a helper let
// through on the ctx (ExpireAfter's expiry here) passes Unique, while the
// same label set by the app without the allowance is still refused.
func TestUniqueAcceptsLabelsTheLibraryAllowed(t *testing.T) {
	fake := workflowtest.NewFake()
	noop := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	if err := fake.Register(workflow.Workflow{Name: "otp", Steps: []workflow.Step{{Name: "send", Run: noop}}}); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC))
	unique := middleware.Unique(func(j workflow.Job) string { return "otp:" + string(j.Payload) }, middleware.NewMemoryUnique(clk), time.Minute)
	c := middleware.WrapClient(fake, middleware.ExpireAfter(5*time.Minute, clk), unique)
	if _, err := c.Enqueue(context.Background(), workflow.Job{Workflow: "otp", Payload: []byte("u1")}); err != nil {
		t.Fatalf("ExpireAfter then Unique: %v", err)
	}
	bare := middleware.WrapClient(fake, unique)
	_, err := bare.Enqueue(context.Background(), workflow.Job{Workflow: "otp", Payload: []byte("u2"), Labels: map[string]string{middleware.LabelExpiresAt: "2026-11-20T09:05:00Z"}})
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("an app-set reserved label = %v, want ErrInvalid", err)
	}
	// The same Unique stacked twice is still refused: its own labels never
	// pass through the ctx allowance.
	twice := middleware.WrapClient(middleware.WrapClient(fake, unique), unique)
	if _, err := twice.Enqueue(context.Background(), workflow.Job{Workflow: "otp", Payload: []byte("u3")}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Unique stacked twice = %v, want ErrInvalid", err)
	}
}
