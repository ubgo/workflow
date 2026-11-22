// Command hatchet-schedules shows the Hatchet driver's schedule and web
// node features and its defaults: the default tunings, a client built from
// a Config, an enqueue-only client for web nodes, the overlap policy the
// engine cannot honour, and, against a real engine, a schedule run now,
// its last fire read from the engine's run list, a job enqueued from the
// web node, a delayed start with an idempotency key (held by the engine
// from the Enqueue through the delayed variant's start gate), and a
// delayed start cancelled with ErrCancelTooLate handled. The reserved
// names of the delayed variants are refused offline.
//
// The offline part needs no engine and always runs. The live part runs
// only when HATCHET_CLIENT_TOKEN is set (the SDK also reads
// HATCHET_CLIENT_HOST_PORT and HATCHET_CLIENT_TLS_STRATEGY), as in
// examples/hatchet-worker.
//
// It is the program behind parts of docs/drivers/hatchet.md and the
// Hatchet part of docs/operations/tuning.md: the guides copy its regions
// (marked "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
)

// envToken turns the live part on; the SDK reads it itself.
const envToken = "HATCHET_CLIENT_TOKEN"

const (
	// waitTimeout bounds the wait for one live run; a step on a warm local
	// engine takes seconds.
	waitTimeout = time.Minute
	// statusPoll is how often a live run's status is re-read; each read is
	// an engine API call, so not too often.
	statusPoll = 200 * time.Millisecond
	// historyWindow is how far back Schedules looks for a trigger's last
	// fire: a month, for crons that fire less than weekly.
	historyWindow = 30 * 24 * time.Hour
	// delayedBy is how far ahead the live delayed start is due: well past
	// the 15 seconds within which the engine may already have loaded it.
	delayedBy = 5 * time.Minute
	// slowEngineLag is the read-side lag and failed-attempt settle time of
	// an engine slower than hatchet-lite on a laptop.
	slowEngineLag = 10 * time.Second
	// uploadTimeout is near the upload step's real duration, so a crashed
	// worker's step is retried minutes later instead of a day later.
	uploadTimeout = 2 * time.Minute
)

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hatchet-schedules:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, w io.Writer) error {
	defaults(w)
	if err := offline(ctx, w); err != nil {
		return err
	}
	if os.Getenv(envToken) == "" {
		fmt.Fprintln(w, envToken, "is not set: the live run is skipped")
		return nil
	}
	return live(ctx, w)
}

// defaults prints what a zero Config field means.
func defaults(w io.Writer) {
	// snippet:defaults
	retry := hatchet.DefaultBatchCoordinatorRetry()
	fmt.Fprintln(w, "StepTimeout:", hatchet.DefaultStepTimeout)
	fmt.Fprintln(w, "ScheduleTimeout:", hatchet.DefaultScheduleTimeout)
	fmt.Fprintln(w, "RegisterTimeout:", hatchet.DefaultRegisterTimeout)
	fmt.Fprintln(w, "RunFinishedPoll:", hatchet.DefaultRunFinishedPoll)
	fmt.Fprintln(w, "ScheduleHistoryWindow:", hatchet.DefaultScheduleHistoryWindow)
	fmt.Fprintln(w, "PayloadLimit:", hatchet.DefaultPayloadLimit, "bytes")
	fmt.Fprintln(w, "GroupChunkRuns:", hatchet.DefaultGroupChunkRuns, "GroupChunkBytes:", hatchet.DefaultGroupChunkBytes)
	fmt.Fprintf(w, "BatchCoordinatorRetry: %d retries, %s doubling, capped at %s\n", retry.Max, retry.Backoff.Initial, retry.Backoff.Max)
	// snippet:end
}

// noop is a step that returns its input.
func noop(_ context.Context, in []byte) ([]byte, error) { return in, nil }

// offline shows what needs no engine: building clients, the enqueue-only
// client's refusal, and the overlap refusal.
func offline(ctx context.Context, w io.Writer) error {
	// snippet:new
	c := hatchet.New(hatchet.Config{
		// The zero Config builds the SDK client from the HATCHET_CLIENT_*
		// variables at Start; ClientOptions or Client replace that.
		Namespace:             os.Getenv("HATCHET_CLIENT_NAMESPACE"), // must equal the SDK's
		WorkerName:            "reports-worker",
		ScheduleHistoryWindow: historyWindow, // how far back ScheduleInfo.Last looks (default a week)
	})
	// snippet:end
	fmt.Fprintln(w, "trigger now:", c.Capabilities().Has(workflow.CapScheduleTrigger), "last fire:", c.Capabilities().Has(workflow.CapScheduleHistory))

	// snippet:enqueue-only
	web := hatchet.New(hatchet.Config{EnqueueOnly: true}) // connects on first use
	err := web.OnRunFinished(func(context.Context, workflow.RunEvent) {})
	fmt.Fprintln(w, "OnRunFinished on a web node: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
	fmt.Fprintln(w, "Start on a web node starts nothing:", web.Start(ctx) == nil)
	// snippet:end

	// snippet:overlap
	err = c.Register(workflow.Workflow{
		Name:        "digest",
		Cron:        []string{"*/5 * * * *"},
		CronOverlap: workflow.OverlapSkip, // the engine fires ticks without asking a worker
		Steps:       []workflow.Step{{Name: "send", Run: noop}},
	})
	fmt.Fprintln(w, "OverlapSkip: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))
	// snippet:end

	// snippet:quirks
	slow := hatchet.New(hatchet.Config{
		VisibilityLag: slowEngineLag, // Status retries the read side's 404 this long (default 5s)
		FailedSettle:  slowEngineLag, // a FAILED read with retries left is "between attempts" (default 5s)
	})
	err = slow.Register(workflow.Workflow{Name: "export", Steps: []workflow.Step{{
		Name:    "upload",
		Run:     noop,
		Timeout: uploadTimeout, // also how long a step on a dead worker waits before the engine reclaims it
	}}})
	// snippet:end
	fmt.Fprintf(w, "export registered with a %s step timeout: %v\n", uploadTimeout, err == nil)

	// snippet:delayed-names
	reserved := []struct {
		why string
		wf  workflow.Workflow
	}{
		{"a name ending in __wf_delayed", workflow.Workflow{Name: "invoice__wf_delayed", Steps: []workflow.Step{{Name: "send", Run: noop}}}},
		{"a step named wf__start", workflow.Workflow{Name: "invoice", Steps: []workflow.Step{{Name: "wf__start", Run: noop}}}},
	}
	for _, r := range reserved {
		err := c.Register(r.wf) // the driver's delayed variants and their start gate use these names
		fmt.Fprintf(w, "%s: ErrInvalid = %v\n", r.why, errors.Is(err, workflow.ErrInvalid))
	}
	// snippet:end
	return nil
}

// waitFor polls until the run reads want.
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

// live runs a schedule now, reads its last fire, enqueues from a web node
// and cancels a delayed start, on the engine named by the HATCHET_CLIENT_*
// variables. Workflow names carry a random suffix so repeated runs on one
// engine do not see each other's runs.
func live(ctx context.Context, w io.Writer) error {
	report := "example-nightly-" + strconv.FormatInt(time.Now().UnixNano(), 36) // base 36 keeps the unique suffix short
	c := hatchet.New(hatchet.Config{WorkerName: "hatchet-schedules-example", ScheduleHistoryWindow: historyWindow})
	if err := c.Register(workflow.Workflow{Name: report, Steps: []workflow.Step{{Name: "build", Run: noop}}}); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(runCtx); err != nil {
		return err
	}

	// snippet:trigger
	id, err := c.Schedule(ctx, workflow.Schedule{Workflow: report, Name: "nightly", Cron: "0 2 * * *", Location: time.UTC})
	if err != nil {
		return err
	}
	ref, err := c.TriggerSchedule(ctx, id) // runs it now; the cron is left alone
	if err != nil {
		return err
	}
	// snippet:end
	if err := waitFor(ctx, c, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	// snippet:last
	infos, err := c.Schedules(ctx) // Last comes from the engine's run list
	if err != nil {
		return err
	}
	for _, s := range infos {
		if s.ID == id {
			fmt.Fprintf(w, "schedule %s: last run %s, next fire %s\n", s.Name, s.Last.Status, s.Next.Format("15:04 MST"))
		}
	}
	// snippet:end
	if err := c.Unschedule(ctx, id); err != nil {
		return err
	}

	web := hatchet.New(hatchet.Config{EnqueueOnly: true})
	fromWeb, err := web.Enqueue(ctx, workflow.Job{Workflow: report}) // the worker above runs it
	if err != nil {
		return err
	}
	if err := waitFor(ctx, web, fromWeb, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(w, "enqueued from the web node:", workflow.StatusSucceeded)

	if err := keyedDelayedStart(ctx, w, c, report); err != nil {
		return err
	}

	delayed, err := c.Enqueue(ctx, workflow.Job{Workflow: report, RunAt: time.Now().Add(delayedBy)})
	if err != nil {
		return err
	}
	// snippet:cancel-too-late
	err = c.Cancel(ctx, delayed)
	switch {
	case errors.Is(err, hatchet.ErrCancelTooLate):
		// The engine had loaded the start and fired it; Cancel cancelled
		// the run unless it had already ended. Check what it did.
		fmt.Fprintln(w, "the delayed start fired before the cancel reached the engine")
	case err != nil:
		return err
	default:
		fmt.Fprintln(w, "delayed start cancelled before it fired")
	}
	// snippet:end
	return nil
}

// keyedDelayedStart enqueues a delayed start with an idempotency key on a
// started client: the engine holds the key from the Enqueue, a second
// Enqueue with it returns the same run, the waiting run cannot be
// rescheduled, a RunAt past Config.IdempotencyMaxHold is refused, and a
// Cancel frees the key before the step runs.
func keyedDelayedStart(ctx context.Context, w io.Writer, c *hatchet.Client, report string) error {
	// snippet:delayed-key
	// A key with RunAt: the run starts at once on the workflow's delayed
	// variant, whose start gate sleeps until RunAt, so the engine holds
	// the key from now on.
	job := workflow.Job{Workflow: report, RunAt: time.Now().Add(delayedBy), IdempotencyKey: "invoice-42"}
	keyed, err := c.Enqueue(ctx, job)
	if err != nil {
		return err
	}
	again, err := c.Enqueue(ctx, job) // the key is held: the same run comes back
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "keyed delayed start has a RunID:", !keyed.RunID.IsZero(), "same run for the key:", again == keyed)
	if err := waitFor(ctx, c, keyed, workflow.StatusScheduled); err != nil {
		return err
	}
	_, err = c.Reschedule(ctx, keyed, time.Now().Add(2*delayedBy)) // its gate is a durable sleep
	fmt.Fprintln(w, "Reschedule of a keyed start: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

	far := job
	far.IdempotencyKey = "invoice-43"
	far.RunAt = time.Now().Add(hatchet.DefaultIdempotencyMaxHold + delayedBy) // the key would lapse first
	_, err = c.Enqueue(ctx, far)
	fmt.Fprintln(w, "RunAt past IdempotencyMaxHold: ErrUnsupported =", errors.Is(err, workflow.ErrUnsupported))

	if err := c.Cancel(ctx, keyed); err != nil { // frees the key; the step never runs
		return err
	}
	// snippet:end
	return waitFor(ctx, c, keyed, workflow.StatusCancelled)
}
