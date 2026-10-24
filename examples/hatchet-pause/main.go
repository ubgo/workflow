// Command hatchet-pause pauses a workflow and a schedule on the Hatchet
// driver. Both need the engine's REST API, which the driver reaches
// through Config.API (built by hatchet.NewAPI from the client token):
// without it the driver declares neither capability and refuses both
// calls before it connects.
//
// The refusals need no engine and always run. The live part runs only when
// HATCHET_CLIENT_TOKEN is set (the SDK also reads HATCHET_CLIENT_HOST_PORT
// and HATCHET_CLIENT_TLS_STRATEGY); see README.md.
//
// It is the program behind "Step 6: pause a workflow or a schedule" of
// docs/drivers/hatchet.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

const (
	// waitTimeout bounds each wait for a run to reach a status; a step on
	// a warm local engine takes well under a second.
	waitTimeout = time.Minute
	// statusPoll is how often a run's status is re-read; each read is an
	// engine API call.
	statusPoll = 200 * time.Millisecond
	// holdCheck is how long the program watches a run of a paused
	// workflow to see that it does not start.
	holdCheck = 2 * time.Second
)

// envToken turns the live part on; the SDK reads it itself.
const envToken = "HATCHET_CLIENT_TOKEN"

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hatchet-pause:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	if err := refusals(ctx, out); err != nil {
		return err
	}
	if os.Getenv(envToken) == "" {
		fmt.Fprintln(out, envToken, "is not set: the live run is skipped")
		return nil
	}
	return live(ctx, out)
}

// noop is a step that returns its input.
func noop(_ context.Context, in []byte) ([]byte, error) { return in, nil }

// refusals shows what the driver does without Config.API, before anything
// connects.
func refusals(ctx context.Context, out io.Writer) error {
	// snippet:refusals
	c := hatchet.New(hatchet.Config{}) // no API
	caps := c.Capabilities()
	fmt.Fprintf(out, "without API: PauseWorkflow=%v SchedulePause=%v\n", caps.Has(workflow.CapPauseWorkflow), caps.Has(workflow.CapSchedulePause))

	err := c.Pause(ctx, workflow.PauseWorkflow("reports"))
	fmt.Fprintf(out, "Pause: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))
	err = c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: "any"})
	fmt.Fprintf(out, "PauseSchedule: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

	// NewAPI reads the tenant from the token, so a value that is not one
	// is refused at once.
	_, err = hatchet.NewAPI("not-a-token", "")
	fmt.Fprintf(out, "NewAPI with a bad token: ErrInvalid=%v\n", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}

// waitFor polls ref until it reaches want.
func waitFor(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(waitTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run is %s after %s, want %s", st, waitTimeout, want)
		}
		time.Sleep(statusPoll)
	}
}

// live pauses a workflow and a schedule on the engine named by the
// HATCHET_CLIENT_* variables. The workflow name carries a random suffix so
// repeated runs on one engine do not see each other's runs.
func live(ctx context.Context, out io.Writer) error {
	report := "example-pause-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	// snippet:api
	// The tenant and, with no URL given, the address come from the token.
	api, err := hatchet.NewAPI(os.Getenv(envToken), os.Getenv("HATCHET_CLIENT_SERVER_URL"))
	if err != nil {
		return err
	}
	c := hatchet.New(hatchet.Config{
		WorkerName: "hatchet-pause-example",
		API:        api, // turns on CapPauseWorkflow and CapSchedulePause
	})
	// snippet:end
	caps := c.Capabilities()
	fmt.Fprintf(out, "with API: PauseWorkflow=%v SchedulePause=%v\n", caps.Has(workflow.CapPauseWorkflow), caps.Has(workflow.CapSchedulePause))
	if err := c.Register(workflow.Workflow{Name: report, Steps: []workflow.Step{{Name: "render", Run: noop}}}); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}

	// snippet:pause-workflow
	if err := c.Pause(ctx, workflow.PauseWorkflow(report)); err != nil {
		return err
	}
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: report}) // accepted, held by the engine
	if err != nil {
		return err
	}
	paused, err := c.Paused(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "workflow listed as paused:", slices.Contains(paused, workflow.PauseWorkflow(report)))
	// snippet:end
	time.Sleep(holdCheck)
	st, err := c.Status(ctx, held)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "held run finished while paused:", st.IsTerminal())
	// snippet:resume-workflow
	if err := c.Resume(ctx, workflow.PauseWorkflow(report)); err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, held, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(out, "after resume: succeeded")

	// The engine keeps one pause per workflow: there is no pause-everything.
	err = c.Pause(ctx, workflow.PauseAll())
	fmt.Fprintf(out, "PauseAll: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

	// snippet:pause-schedule
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	defer func() { _ = c.Unschedule(context.WithoutCancel(ctx), id) }()
	// Disables the cron trigger; a tick that falls due meanwhile is dropped.
	if err := c.PauseSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	// snippet:end
	if err := printSchedule(ctx, c, report, out); err != nil {
		return err
	}
	if err := c.ResumeSchedule(ctx, workflow.ScheduleTarget{ID: id}); err != nil {
		return err
	}
	return printSchedule(ctx, c, report, out)
}

// printSchedule prints whether the workflow's schedule is paused.
func printSchedule(ctx context.Context, c workflow.ScheduleLister, wf string, out io.Writer) error {
	list, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range list {
		if s.Workflow == wf {
			fmt.Fprintf(out, "schedule %s: paused=%v\n", s.Name, s.Paused)
		}
	}
	return nil
}
