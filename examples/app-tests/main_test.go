package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

// TestRun runs the program and checks the lines its README shows.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `enqueued welcome-email key="welcome:ada@example.com" at now
enqueued reminder key="reminder:ada@example.com" at 2026-01-02T09:00:00Z
signalled email-confirmed/ada@example.com
cancelled: 1 run(s)
scheduled digest "0 9 * * MON" team=core
reminder status: cancelled
`
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// start is the 9am every test begins at.
var start = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

// newApp returns the app on a started Fake built with opts.
func newApp(t *testing.T, opts ...workflowtest.FakeOption) (*Accounts, *workflowtest.Fake) {
	t.Helper()
	fake := workflowtest.NewFake(opts...)
	if err := startOn(context.Background(), fake); err != nil {
		t.Fatal(err)
	}
	return &Accounts{Jobs: fake, Clock: clock.NewFake(start)}, fake
}

// snippet:assertions
func TestSignUpEnqueuesWelcomeAndReminder(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	if err := startOn(ctx, fake); err != nil { // Register every workflow, then Start
		t.Fatal(err)
	}
	app := &Accounts{Jobs: fake, Clock: clock.NewFake(start)}

	fake.AssertNothingEnqueued(t)
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	fake.AssertEnqueued(t, "welcome-email",
		workflowtest.HasPayloadJSON(map[string]string{"email": "ada@example.com"}),
		workflowtest.HasKey("welcome:ada@example.com"),
		workflowtest.HasLabel("source", "signup"),
		workflowtest.HasPriority(10))
	fake.AssertEnqueued(t, "reminder", workflowtest.HasRunAt(start.Add(24*time.Hour)))
	fake.AssertNotEnqueued(t, "nudge")
}

// snippet:end

// snippet:matchers
func TestMatchers(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	// HasPayload compares bytes; HasPayloadJSON compares decoded values,
	// so key order and spacing do not matter.
	fake.AssertEnqueued(t, "welcome-email", workflowtest.HasPayload([]byte(`{"email":"ada@example.com"}`)))
	fake.AssertEnqueued(t, "welcome-email", workflowtest.HasPayloadJSON(map[string]any{"email": "ada@example.com"}))
	// Where is the escape hatch; its description shows in failure messages.
	fake.AssertEnqueued(t, "reminder", workflowtest.Where("delayed by a day or more", func(j workflow.Job) bool {
		return !j.RunAt.Before(start.Add(24 * time.Hour))
	}))
}

// snippet:end

// snippet:inspect
func TestDoubleSubmitAndFinish(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	reminder, err := app.SignUp(ctx, "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil { // a double submit
		t.Fatal(err)
	}
	// The keys are held by unfinished runs, so the second call recorded nothing.
	fake.AssertEnqueuedTimes(t, "reminder", 1)
	if got := len(fake.Jobs()); got != 2 {
		t.Fatalf("Jobs() = %d, want 2", got)
	}

	// Finish models the run completing: its key is free again.
	if err := fake.Finish(reminder, workflow.StatusSucceeded); err != nil {
		t.Fatal(err)
	}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueuedTimes(t, "reminder", 2)
	if got := fake.Enqueued("reminder", workflowtest.HasKey("reminder:ada@example.com")); len(got) != 2 {
		t.Fatalf("Enqueued(reminder) = %d jobs, want 2", len(got))
	}
}

// snippet:end

// snippet:signals
func TestConfirmSignalsAndCancels(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	reminder, err := app.SignUp(ctx, "ada@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Confirm(ctx, "ada@example.com", reminder); err != nil {
		t.Fatal(err)
	}
	if _, err := app.StartDigest(ctx, "core"); err != nil {
		t.Fatal(err)
	}

	fake.AssertSignalled(t, "email-confirmed", "ada@example.com")
	fake.AssertCancelled(t, reminder)
	fake.AssertScheduled(t, "digest")
	if st, _ := fake.Status(ctx, reminder); st != workflow.StatusCancelled {
		t.Fatalf("reminder status = %s, want cancelled", st)
	}
}

// snippet:end

// snippet:like-production
func TestRefusedLikeProduction(t *testing.T) {
	ctx := context.Background()

	// FakeLike copies a real client's capabilities; it reads only
	// Capabilities, so an unstarted client built with test settings will do.
	app, _ := newApp(t, workflowtest.FakeLike(memory.New()))
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	// A production driver without delayed starts refuses the reminder, and
	// so does a Fake that claims the same capabilities.
	noDelay := memory.New().Capabilities() &^ workflow.CapDelayedStart
	app, _ = newApp(t, workflowtest.FakeCaps(noDelay))
	_, err := app.SignUp(ctx, "ada@example.com")
	if !errors.Is(err, workflow.ErrUnsupported) {
		t.Fatalf("SignUp on a driver without CapDelayedStart = %v, want ErrUnsupported", err)
	}

	// An expression key is refused at Start, as the memory driver refuses
	// it, unless FakeKeyExpr says the production engine accepts one.
	perTenant := workflow.Workflow{
		Name:        "sync",
		Steps:       []workflow.Step{{Name: "run", Run: noop}},
		Concurrency: &workflow.Concurrency{Key: workflow.KeyExpr("input.tenant"), MaxRuns: 1},
	}
	for _, tc := range []struct {
		opts   []workflowtest.FakeOption
		refuse bool
	}{{nil, true}, {[]workflowtest.FakeOption{workflowtest.FakeKeyExpr()}, false}} {
		fake := workflowtest.NewFake(tc.opts...)
		if err := fake.Register(perTenant); err != nil {
			t.Fatal(err)
		}
		if err := fake.Start(ctx); errors.Is(err, workflow.ErrUnsupported) != tc.refuse {
			t.Fatalf("Start = %v, want refused: %v", err, tc.refuse)
		}
	}
}

// snippet:end

// snippet:recorder
func TestSignUpOnARealDriver(t *testing.T) {
	ctx := t.Context()
	// The jobs really run on the memory driver; the Recorder notes each
	// enqueue the driver accepted.
	rec := workflowtest.NewRecorder(memory.New())
	if err := startOn(ctx, rec); err != nil {
		t.Fatal(err)
	}
	app := &Accounts{Jobs: rec, Clock: clock.Real{}}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil {
		t.Fatal(err)
	}

	rec.AssertEnqueued(t, "welcome-email", 1) // exactly once
	rec.AssertEnqueuedWith(t, "reminder", func(j workflow.Job) bool { return j.IdempotencyKey == "reminder:ada@example.com" })
	rec.AssertNotEnqueued(t, "nudge")
}

// snippet:end

// snippet:fake-clock
func TestNudgeAfterADay(t *testing.T) {
	ctx := context.Background()
	app, fake := newApp(t)
	clk := clock.NewFake(start)
	app.Clock = clk

	done := make(chan error, 1)
	go func() { done <- app.NudgeAfter(ctx, 24*time.Hour, "ada@example.com") }()

	// Wait until the goroutine has armed its timer, or Advance could run
	// first and the timer would be due a day after the advance.
	if err := clk.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(23 * time.Hour)
	if clk.Pending() != 1 {
		t.Fatal("the nudge fired early")
	}
	clk.Set(start.Add(24 * time.Hour)) // jump to an absolute time
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueued(t, "nudge", workflowtest.HasPayloadJSON(map[string]string{"email": "ada@example.com"}))
	if clk.Pending() != 0 {
		t.Fatal("a timer is still pending")
	}
}

// snippet:end
