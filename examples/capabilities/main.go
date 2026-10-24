// Command capabilities prints what each shipped driver declares, checks a
// workflow and a schedule against each before anything runs, and shows
// the errors the core contract returns: refusals, validation, lifecycle
// sentinels, the backend-outage marker and reserved labels. Nothing here
// needs a server: the SQL driver is built over an in-memory SQLite
// database, and the Hatchet driver opens no connection before Start.
//
// It is the program behind docs/concepts/capabilities.md,
// docs/concepts/errors.md, docs/concepts/labels.md and the validation
// section of docs/concepts/client.md: the guides copy its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
	_ "time/tzdata" // Asia/Kolkata on hosts without a zone database

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "capabilities:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	db, err := driversqlite.Memory()
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:declare
	sqlc, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	drivers := []struct {
		name string
		c    workflow.Client
	}{
		{"memory", memory.New()},
		{"sqlstore", sqlc},
		{"hatchet", hatchet.New(hatchet.Config{})}, // no connection before Start
	}
	for _, d := range drivers {
		caps := d.c.Capabilities()
		fmt.Fprintf(out, "%-8s durable=%-5v %s\n", d.name, caps.Has(workflow.CapDurable), caps)
	}
	// snippet:end

	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return err
	}

	// snippet:requires
	digest := workflow.Workflow{
		Name:         "digest",
		Cron:         []string{"0 9 * * *"},
		CronLocation: kolkata,
		Misfire:      workflow.Misfire{Policy: workflow.MisfireSkip},
		Retry:        workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2, Jitter: 0.2}},
		Steps: []workflow.Step{{Name: "send", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			return nil, nil
		}}},
	}
	need := workflow.Requires(digest)
	fmt.Fprintln(out, "digest needs:", need)
	for _, d := range drivers {
		if missing := d.c.Capabilities().Missing(need); missing != 0 {
			fmt.Fprintf(out, "  %s lacks %s\n", d.name, missing)
		}
	}
	// snippet:end

	// snippet:refusal
	err = workflow.CheckCaps(hatchet.New(hatchet.Config{}).Capabilities(), digest)
	if ue, ok := errors.AsType[*workflow.UnsupportedError](err); ok {
		fmt.Fprintf(out, "refused: field=%s missing=%s reason=%q\n", ue.Field, ue.Missing, ue.Reason)
	}
	fmt.Fprintln(out, "is ErrUnsupported:", errors.Is(err, workflow.ErrUnsupported))

	// The Hatchet driver also refuses at Register what its engine cannot
	// express, naming the field: here a Go key function, where Hatchet
	// takes CEL expressions (workflow.KeyExpr).
	perTenant := workflow.Workflow{
		Name:        "sync",
		Concurrency: &workflow.Concurrency{Key: workflow.KeyFunc(func(j workflow.Job) string { return j.Labels["tenant"] }), MaxRuns: 1},
		Steps:       digest.Steps,
	}
	fmt.Fprintln(out, "hatchet Register:", hatchet.New(hatchet.Config{}).Register(perTenant))
	// snippet:end

	// snippet:schedule
	nightly := workflow.Schedule{Name: "acme-nightly", Workflow: "digest", Cron: "0 2 * * *", Location: kolkata}
	fmt.Fprintln(out, "schedule needs:", workflow.RequiresSchedule(nightly))
	fmt.Fprintln(out, "on hatchet:", workflow.CheckScheduleCaps(hatchet.New(hatchet.Config{}).Capabilities(), nightly))
	fmt.Fprintln(out, "on memory: ", workflow.CheckScheduleCaps(memory.New().Capabilities(), nightly))
	// snippet:end

	// snippet:job
	job := workflow.Job{Workflow: "digest", RunAt: time.Date(2026, 12, 1, 9, 0, 0, 0, kolkata), IdempotencyKey: "digest-2026-12-01"}
	fmt.Fprintln(out, "job needs:", workflow.RequiresJob(job))
	// snippet:end

	if err := validation(out); err != nil {
		return err
	}
	if err := sentinels(ctx, out); err != nil {
		return err
	}
	labels(out)
	return nil
}

// validation shows that specs are checked before anything runs.
func validation(out io.Writer) error {
	// snippet:validate
	bad := workflow.Workflow{Name: "import", Steps: []workflow.Step{
		{Name: "load", After: []string{"parse"}, Run: func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }},
	}}
	err := workflow.Validate(bad)
	fmt.Fprintln(out, "Validate:", err)
	fmt.Fprintln(out, "is ErrInvalid:", errors.Is(err, workflow.ErrInvalid))

	err = memory.New().Register(bad) // every driver's Register validates
	fmt.Fprintln(out, "Register:", errors.Is(err, workflow.ErrInvalid))

	err = workflow.ValidateSchedule(workflow.Schedule{Workflow: "digest", Cron: "0 9 * * *"})
	fmt.Fprintln(out, "ValidateSchedule:", err)
	// snippet:end
	return nil
}

// sentinels shows the lifecycle and lookup errors every driver returns.
func sentinels(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wf := workflow.Workflow{Name: "ping", Steps: []workflow.Step{{Name: "pong", Run: func(ctx context.Context, in []byte) ([]byte, error) { return in, nil }}}}

	// snippet:sentinels
	c := memory.New()
	if err := c.Register(wf); err != nil {
		return err
	}
	_, err := c.Enqueue(ctx, workflow.Job{Workflow: "ping"})
	fmt.Fprintln(out, "before Start:", errors.Is(err, workflow.ErrNotStarted))

	if err := c.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintln(out, "second Start:", errors.Is(c.Start(ctx), workflow.ErrAlreadyStarted))
	fmt.Fprintln(out, "Register after Start:", errors.Is(c.Register(wf), workflow.ErrAlreadyStarted))

	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "pnig"})
	fmt.Fprintln(out, "unknown workflow:", errors.Is(err, workflow.ErrNotFound))
	_, err = c.Status(ctx, workflow.RunRef{RunID: "no-such-run"})
	fmt.Fprintln(out, "unknown run:", errors.Is(err, workflow.ErrNotFound))
	// snippet:end

	// snippet:unavailable
	// A driver marks "could not reach the backend" with Unavailable; the
	// original error stays reachable for errors.Is and errors.As.
	refused := errors.New("dial tcp 10.0.0.5:7077: connection refused")
	err = workflow.Unavailable(fmt.Errorf("enqueue: %w", refused))
	fmt.Fprintln(out, "outage:", workflow.IsUnavailable(err), errors.Is(err, refused))
	fmt.Fprintln(out, "refusal is no outage:", workflow.IsUnavailable(workflow.Unsupported("Job.RunAt", "")))
	// snippet:end
	return nil
}

// labels shows the label prefixes the library keeps for itself.
func labels(out io.Writer) {
	// snippet:labels
	fmt.Fprintln(out, "reserved:", workflow.ReservedLabelPrefixes())
	prefix, ok := workflow.ReservedLabel("otel.traceparent")
	fmt.Fprintln(out, "otel.traceparent reserved:", ok, prefix)

	err := workflow.ValidateLabels(map[string]string{"tenant": "acme", "workflow.batch.id": "b1"})
	fmt.Fprintln(out, "ValidateLabels:", err)
	// snippet:end
}
