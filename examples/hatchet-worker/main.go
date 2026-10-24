// Command hatchet-worker runs a workflow on a Hatchet engine through the
// Hatchet driver, and shows what the driver refuses before it ever
// connects: a workflow it cannot honour at Register, a tuning past the
// SDK's limits at Start.
//
// The refusals need no engine and always run. The live part runs only when
// HATCHET_CLIENT_TOKEN is set (the SDK also reads HATCHET_CLIENT_HOST_PORT
// and HATCHET_CLIENT_TLS_STRATEGY); see README.md for running it against a
// local engine.
//
// It is the program behind docs/drivers/hatchet.md and the Hatchet part of
// docs/operations/tuning.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/hatchet"
)

const (
	// liveRunTimeout bounds the wait for the live run to end; a step on a
	// warm local engine takes seconds.
	liveRunTimeout = time.Minute
	// statusPoll is how often the live run's status is re-read; each read
	// is an engine API call, so not too often.
	statusPoll = 200 * time.Millisecond
)

// envToken turns the live part on; the SDK reads it itself.
const envToken = "HATCHET_CLIENT_TOKEN"

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "hatchet-worker:", err)
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

// refusals shows what the driver refuses without an engine.
func refusals(ctx context.Context, out io.Writer) error {
	c := hatchet.New(hatchet.Config{})
	fmt.Fprintln(out, "capabilities:", c.Capabilities())

	// snippet:refusals
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		return err
	}
	refused := []workflow.Workflow{
		// Hatchet evaluates crons in UTC only.
		{Name: "berlin-report", Cron: []string{"0 9 * * *"}, CronLocation: berlin, Steps: []workflow.Step{{Name: "only", Run: noop}}},
		// Hatchet's concurrency keys are CEL expressions, not Go functions.
		{Name: "per-tenant", Concurrency: &workflow.Concurrency{
			Key:     workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }),
			MaxRuns: 1,
		}, Steps: []workflow.Step{{Name: "only", Run: noop}}},
		// Hatchet waits Factor^n seconds and never adds jitter.
		{Name: "jittered", Retry: workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{
			Initial: 2 * time.Second, Factor: 2, Jitter: 0.2,
		}}, Steps: []workflow.Step{{Name: "only", Run: noop}}},
	}
	for _, wf := range refused {
		err := c.Register(wf) // refused here, before anything connects
		fmt.Fprintf(out, "%s: ErrUnsupported=%v\n  %v\n", wf.Name, errors.Is(err, workflow.ErrUnsupported), err)
	}
	// snippet:end

	// snippet:refuse-tuning
	big := hatchet.New(hatchet.Config{GroupChunkRuns: 5000}) // the SDK's bulk limit is 1000
	err = big.Start(ctx)                                     // refused before it connects
	fmt.Fprintf(out, "GroupChunkRuns 5000: ErrInvalid=%v\n  %v\n", errors.Is(err, workflow.ErrInvalid), err)
	// snippet:end
	return nil
}

// tunings is a Config for big batches and a slow engine; live merges it
// into the worker's Config.
func tunings() hatchet.Config {
	// snippet:tunings
	return hatchet.Config{
		// Bulk triggers (batch.GroupRunner): at most this many runs and this
		// much JSON per RunMany call. The defaults (1000 runs, 2 MiB) are also
		// the maximum; lower them for an engine with a smaller gRPC limit.
		GroupChunkRuns:  500,
		GroupChunkBytes: 1 << 20,

		// One coordinator run per batch, which starts every job as a child.
		BatchCoordinator: true,
		// One coordinator attempt; also how long a coordinator on a dead worker
		// waits before the engine retries it (default 5 minutes).
		BatchCoordinatorTimeout: 10 * time.Minute,
		// Must fit Hatchet's Factor^n seconds: Initial equal to Factor seconds,
		// no Jitter, no DelayFor, no NoRetryOn (default: 10 retries, capped 2m).
		BatchCoordinatorRetry: &workflow.RetryPolicy{Max: 20, Backoff: workflow.Backoff{
			Initial: 2 * time.Second, Factor: 2, Max: 2 * time.Minute,
		}},
		// A batch whose input is bigger starts as bulk triggers instead
		// (default 2 MiB; raise it only with the engine's message limit).
		BatchCoordinatorMaxInput: 1 << 20,
	}
	// snippet:end
}

// live runs one job on the engine named by the HATCHET_CLIENT_* variables.
func live(ctx context.Context, out io.Writer) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	t := tunings()

	// snippet:new
	c := hatchet.New(hatchet.Config{
		// Client: an SDK client you built; nil builds one from ClientOptions
		// and the HATCHET_CLIENT_* environment variables at Start.
		Namespace:          os.Getenv("HATCHET_CLIENT_NAMESPACE"), // must equal the SDK's namespace
		WorkerName:         "hatchet-worker-example",              // its name in the dashboard
		Slots:              10,                                    // steps this worker runs at once
		StepTimeout:        10 * time.Minute,                      // for steps that set no Timeout (default 24h)
		IdempotencyMaxHold: 6 * time.Hour,                         // a key's longest hold (default 24h)
		RegisterTimeout:    time.Minute,                           // Start's wait for the worker (default 30s)
		VisibilityLag:      5 * time.Second,                       // how far the read side may trail a write
		Middleware:         nil,                                   // step middleware, outermost first
		Logger:             logger,                                // background errors (nil discards)
		Clock:              clock.Real{},                          // local tickers; nil means clock.Real

		GroupChunkRuns:           t.GroupChunkRuns, // tunings: see docs/operations/tuning.md
		GroupChunkBytes:          t.GroupChunkBytes,
		BatchCoordinator:         t.BatchCoordinator,
		BatchCoordinatorTimeout:  t.BatchCoordinatorTimeout,
		BatchCoordinatorRetry:    t.BatchCoordinatorRetry,
		BatchCoordinatorMaxInput: t.BatchCoordinatorMaxInput,
	})
	// snippet:end

	// snippet:start
	err := c.Register(workflow.Workflow{
		Name: "example-greet",
		Steps: []workflow.Step{{Name: "say", Run: workflow.Define(func(ctx context.Context, in struct{ Name string }) (string, error) {
			return "hello " + in.Name, nil
		})}},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()                       // cancelling Start's context stops the worker
	if err := c.Start(ctx); err != nil { // connects, declares, waits until the engine lists the worker
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "example-greet", Payload: []byte(`{"Name":"Ada"}`)})
	if err != nil {
		return err
	}
	// snippet:end

	deadline := time.Now().Add(liveRunTimeout)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			fmt.Fprintln(out, "live run:", st)
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("run is %s after %s", st, liveRunTimeout)
		}
		time.Sleep(statusPoll)
	}

	// snippet:native
	sdk := c.Native() // the Hatchet SDK client; nil before Start unless Config.Client was set
	workers, err := sdk.Workers().List(ctx)
	if err != nil {
		return err
	}
	// snippet:end
	listed := false
	if workers.Rows != nil {
		for _, w := range *workers.Rows {
			listed = listed || strings.HasPrefix(w.Name, "hatchet-worker-example-") // the driver adds a suffix per process
		}
	}
	fmt.Fprintln(out, "worker listed through Native:", listed)
	return nil
}
