// Command app-tests is app code written against workflow.Client and
// clock.Clock, so its tests (main_test.go) hand it a workflowtest.Fake, a
// workflowtest.Recorder around a real driver, and a fake clock. main runs
// the same code on a Fake and prints what it recorded.
//
// It is the program behind docs/testing/fake.md, docs/testing/recorder.md
// and docs/testing/fake-clock.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/workflowtest"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "app-tests:", err)
		os.Exit(1)
	}
}

// snippet:app
// Accounts is app code. It depends on the workflow.Client interface and a
// clock.Clock, never on a driver or on time.Now, so a test can swap both.
type Accounts struct {
	Jobs  workflow.Client
	Clock clock.Clock
}

// SignUp sends the welcome email now and a reminder in a day. The keys
// make a double submit enqueue one of each.
func (a *Accounts) SignUp(ctx context.Context, email string) (reminder workflow.RunRef, err error) {
	_, err = a.Jobs.Enqueue(ctx, workflow.Job{
		Workflow:       "welcome-email",
		Payload:        fmt.Appendf(nil, `{"email":%q}`, email),
		IdempotencyKey: "welcome:" + email,
		Labels:         map[string]string{"source": "signup"},
		Priority:       10,
	})
	if err != nil {
		return workflow.RunRef{}, err
	}
	return a.Jobs.Enqueue(ctx, workflow.Job{
		Workflow:       "reminder",
		Payload:        fmt.Appendf(nil, `{"email":%q}`, email),
		IdempotencyKey: "reminder:" + email,
		RunAt:          a.Clock.Now().Add(24 * time.Hour),
	})
}

// Confirm resumes the run waiting for this address's confirmation, and
// drops the reminder.
func (a *Accounts) Confirm(ctx context.Context, email string, reminder workflow.RunRef) error {
	if err := a.Jobs.Signal(ctx, workflow.Event{Name: "email-confirmed", Key: email}); err != nil {
		return err
	}
	return a.Jobs.Cancel(ctx, reminder)
}

// StartDigest schedules the weekly digest for one team.
func (a *Accounts) StartDigest(ctx context.Context, team string) (workflow.ScheduleID, error) {
	return a.Jobs.Schedule(ctx, workflow.Schedule{
		Workflow: "digest",
		Cron:     "0 9 * * MON",
		Location: time.UTC,
		Labels:   map[string]string{"team": team},
	})
}

// snippet:end

// snippet:nudge
// NudgeAfter waits d on the app's clock, then enqueues a nudge. A test on
// a fake clock moves d on without sleeping.
func (a *Accounts) NudgeAfter(ctx context.Context, d time.Duration, email string) error {
	t := a.Clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C():
	}
	_, err := a.Jobs.Enqueue(ctx, workflow.Job{Workflow: "nudge", Payload: fmt.Appendf(nil, `{"email":%q}`, email)})
	return err
}

// snippet:end

// noop is the handler of every workflow here: the Fake runs nothing, and
// the Recorder's real driver only needs something to run.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// Workflows is the app's workflow set, registered on whatever client it
// runs on: the production driver, a Fake, or a Recorder.
func Workflows() []workflow.Workflow {
	return []workflow.Workflow{
		{Name: "welcome-email", Steps: []workflow.Step{{Name: "send", Run: noop}}},
		{Name: "reminder", Steps: []workflow.Step{{Name: "send", Run: noop}}},
		{Name: "nudge", Steps: []workflow.Step{{Name: "send", Run: noop}}},
		{Name: "digest", Steps: []workflow.Step{{Name: "send", Run: noop}}},
		{Name: "confirm", Steps: []workflow.Step{{
			Name: "wait", Wait: &workflow.WaitSpec{Event: "email-confirmed", Timeout: 7 * 24 * time.Hour},
		}}},
	}
}

// startOn registers the app's workflows on c and starts it.
func startOn(ctx context.Context, c workflow.Client) error {
	for _, wf := range Workflows() {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	return c.Start(ctx)
}

// run is main without the process exit, so the test drives the same code:
// the app on a Fake, then what the Fake recorded.
func run(ctx context.Context, out io.Writer) error {
	fake := workflowtest.NewFake()
	if err := startOn(ctx, fake); err != nil {
		return err
	}
	app := &Accounts{Jobs: fake, Clock: clock.NewFake(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC))}

	reminder, err := app.SignUp(ctx, "ada@example.com")
	if err != nil {
		return err
	}
	if _, err := app.SignUp(ctx, "ada@example.com"); err != nil { // a double submit
		return err
	}
	if err := app.Confirm(ctx, "ada@example.com", reminder); err != nil {
		return err
	}
	if _, err := app.StartDigest(ctx, "core"); err != nil {
		return err
	}

	for _, j := range fake.Jobs() {
		when := "now"
		if !j.RunAt.IsZero() {
			when = j.RunAt.Format(time.RFC3339)
		}
		fmt.Fprintf(out, "enqueued %s key=%q at %s\n", j.Workflow, j.IdempotencyKey, when)
	}
	for _, ev := range fake.Signals() {
		fmt.Fprintf(out, "signalled %s/%s\n", ev.Name, ev.Key)
	}
	fmt.Fprintln(out, "cancelled:", len(fake.Cancels()), "run(s)")
	for _, s := range fake.Schedules() {
		fmt.Fprintf(out, "scheduled %s %q team=%s\n", s.Schedule.Workflow, s.Schedule.Cron, s.Schedule.Labels["team"])
	}
	st, err := fake.Status(ctx, reminder)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "reminder status:", st)
	return nil
}
