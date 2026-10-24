package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
	"github.com/ubgo/workflow/hatchet"
)

// finallyTimeout bounds the wait for the live batch's Finally callback: the
// run-finished poll and a warm local engine need seconds, not minutes.
const finallyTimeout = 2 * time.Minute

// envToken turns the Hatchet part on. The Hatchet SDK reads it, and
// HATCHET_CLIENT_HOST_PORT and HATCHET_CLIENT_TLS_STRATEGY, itself.
const envToken = "HATCHET_CLIENT_TOKEN"

// runHatchet dispatches a batch on Hatchet through the batch coordinator:
// one trigger starts a coordinator run, whose task starts every job as a
// child run, and the Hatchet BatchSource learns the children's ends from
// the engine's run list by their batch label.
func runHatchet(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Workflow names unique to this run, so two runs on one shared engine
	// never take each other's jobs.
	prefix := fmt.Sprintf("batches-example-%d-", time.Now().UnixNano())

	// snippet:coordinator
	c := hatchet.New(hatchet.Config{
		WorkerName:       "batches-example",
		BatchCoordinator: true, // Start registers the coordinator workflow on this worker
	})
	store := batch.NewMemoryStore() // a sqlstore BatchStore when several processes share batches
	m := batch.New(c, store, batch.WithCompletionSource(
		batch.HookSource(),                     // fast, best effort on Hatchet
		c.BatchSource(store, time.Second, nil), // the LabelSource a coordinated batch needs
	))
	if err := m.Attach(ctx); err != nil {
		return err
	}
	// snippet:end

	done := make(chan batch.Progress, 1)
	for _, wf := range []workflow.Workflow{
		{Name: prefix + "resize", Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil }}}},
		{Name: prefix + "finally", Steps: []workflow.Step{{Name: "run", Run: workflow.Define(func(_ context.Context, p batch.Progress) (string, error) {
			done <- p
			return "", nil
		})}}},
	} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	jobs := make([]workflow.Job, 50)
	for i := range jobs {
		jobs[i] = workflow.Job{Workflow: prefix + "resize"}
	}
	id, err := m.Dispatch(ctx, batch.Batch{Name: "thumbnails", Jobs: jobs, Finally: prefix + "finally"})
	if err != nil {
		return err
	}
	st, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "hatchet: coordinated=%v, one parent run=%v\n", st.Coordinated, !st.Parent.IsZero())
	select {
	case p := <-done:
		fmt.Fprintf(out, "hatchet: finally succeeded=%d of %d\n", p.Succeeded, p.Total)
	case <-time.After(finallyTimeout):
		return fmt.Errorf("hatchet: no Finally within %s", finallyTimeout)
	}

	// snippet:needs-label-source
	plain := batch.New(c, batch.NewMemoryStore(), batch.WithCompletionSource(batch.PollSource(time.Second)))
	if err := plain.Attach(ctx); err != nil {
		return err
	}
	_, err = plain.Dispatch(ctx, batch.Batch{Jobs: jobs[:1]})
	fmt.Fprintln(out, "hatchet: coordinated batch without a LabelSource refused:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	return nil
}
