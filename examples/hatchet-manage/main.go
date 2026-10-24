// Command hatchet-manage shows the queue and run management
// the Hatchet driver offers, and what it refuses: named queues served by
// their own workers, listing runs with filters, retrying a failed run
// from the step that failed, listing schedules, and draining the worker.
//
// The refusals need no engine and always run. The live part runs only when
// HATCHET_CLIENT_TOKEN is set (the SDK also reads HATCHET_CLIENT_HOST_PORT
// and HATCHET_CLIENT_TLS_STRATEGY); see README.md.
//
// It is the program behind the "Queues, run management and drain" section
// of docs/drivers/hatchet.md: the guide copies its regions (marked
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
	"sync/atomic"
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
	// drainTimeout bounds Drain's wait for the running steps.
	drainTimeout = 30 * time.Second
)

// envToken turns the live part on; the SDK reads it itself.
const envToken = "HATCHET_CLIENT_TOKEN"

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hatchet-manage:", err)
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

// refusals shows the parity features Hatchet cannot honour, refused
// before anything connects.
func refusals(ctx context.Context, out io.Writer) error {
	// snippet:refusals
	c := hatchet.New(hatchet.Config{})
	// Hatchet fixes a task's retries when it is declared: no deadline.
	err := c.Register(workflow.Workflow{Name: "until", Retry: workflow.RetryPolicy{Max: 3, Until: time.Hour},
		Steps: []workflow.Step{{Name: "only", Run: noop}}})
	fmt.Fprintf(out, "Retry.Until: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

	// ... and no per-dispatch policy either.
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "any", Retry: &workflow.RetryPolicy{Max: 5}})
	fmt.Fprintf(out, "Job.Retry: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

	// Each queue is its own Hatchet worker: queues cannot be ordered.
	err = hatchet.New(hatchet.Config{Queues: []workflow.QueueConfig{{Name: "high", Priority: 10}}}).Start(ctx)
	fmt.Fprintf(out, "QueueConfig.Priority: ErrUnsupported=%v\n", errors.Is(err, workflow.ErrUnsupported))

	// The driver says what it cannot do, so an app can check first.
	for _, cp := range []workflow.Caps{workflow.CapPauseWorkflow, workflow.CapDeleteRun, workflow.CapDiscard} {
		fmt.Fprintf(out, "%v: %v\n", cp, c.Capabilities().Has(cp))
	}
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

// live runs the management calls on the engine named by the
// HATCHET_CLIENT_* variables. Workflow names carry a random suffix so
// repeated runs on one engine do not see each other's runs.
func live(ctx context.Context, out io.Writer) error {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	report := "example-report-" + suffix
	var attempts atomic.Int32

	// snippet:api
	// The engine's REST API, for workflow and schedule pause: the tenant
	// and (with no URL given) the address come from the token.
	api, err := hatchet.NewAPI(os.Getenv(envToken), os.Getenv("HATCHET_CLIENT_SERVER_URL"))
	if err != nil {
		return err
	}
	// snippet:end

	// snippet:queues
	c := hatchet.New(hatchet.Config{
		WorkerName: "hatchet-manage-example",
		API:        api, // turns on CapPauseWorkflow and CapSchedulePause
		// One Hatchet worker per queue, each with its own slots. A workflow
		// whose queue is not listed gets no worker in this process.
		Queues: []workflow.QueueConfig{{Name: "reports", Workers: 2}},
	})
	err = c.Register(workflow.Workflow{
		Name:  report,
		Queue: "reports", // every run of this workflow goes to the "reports" worker
		Steps: []workflow.Step{
			{Name: "fetch", Run: noop},
			{Name: "render", After: []string{"fetch"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
				if attempts.Add(1) == 1 {
					return nil, errors.New("renderer offline") // the first run fails here
				}
				return in, nil
			}},
		},
	})
	// snippet:end
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: report, Labels: map[string]string{"tenant": "acme"}})
	if err != nil {
		return err
	}
	if err := waitFor(ctx, c, ref, workflow.StatusFailed); err != nil {
		return err
	}

	// snippet:runs
	page, err := c.Runs(ctx, workflow.RunQuery{
		Workflow: report,
		Labels:   map[string]string{"tenant": "acme"},
		Statuses: []workflow.Status{workflow.StatusFailed},
	})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(out, "failed run: queue=%s step=%s error=%q\n", r.Queue, r.FailedStep, r.Error)
	}
	// snippet:end

	// snippet:retry
	// Replays the failed step; "fetch" succeeded and does not run again.
	if _, err := c.Retry(ctx, ref); err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintf(out, "after retry: succeeded, render attempts=%d\n", attempts.Load())

	// snippet:schedules
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	again, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "same name, same definition, same ID:", again == id)
	list, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range list {
		if s.Workflow == report {
			fmt.Fprintf(out, "schedule %s: %s, next fire in UTC at %s\n", s.Name, s.Cron, s.Next.Format("15:04"))
		}
	}
	// snippet:end
	if err := c.Unschedule(ctx, id); err != nil {
		return err
	}

	// snippet:pause
	if err := c.Pause(ctx, workflow.PauseWorkflow(report)); err != nil {
		return err
	}
	held, err := c.Enqueue(ctx, workflow.Job{Workflow: report}) // accepted, but not started
	if err != nil {
		return err
	}
	paused, err := c.Paused(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "workflow listed as paused:", slices.Contains(paused, workflow.PauseWorkflow(report)))
	if err := c.Resume(ctx, workflow.PauseWorkflow(report)); err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, held, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(out, "after resume: succeeded")

	// snippet:drain
	dctx, stop := context.WithTimeout(ctx, drainTimeout)
	defer stop()
	// Stops taking new steps, waits for the running ones, then stops the
	// workers. Unfinished runs stay on the engine for another worker.
	if err := c.Drain(dctx); err != nil {
		return err
	}
	<-c.Done()
	// snippet:end
	fmt.Fprintln(out, "drained")
	return nil
}
