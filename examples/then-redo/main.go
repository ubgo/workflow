// Command then-redo shows how workflow.Then treats runs that redo work
// which already succeeded. An import that succeeded starts its index job;
// resetting that import (ResetOptions.Succeeded) and forking it give runs
// that carry workflow.LabelRedo, and by default Then starts nothing for
// them, since the first success already started the index. With
// workflow.ThenAfterRedo every redo starts the index again. A last part
// sends a rule's error to workflow.ThenErrors instead of the log.
//
// It runs on the in-memory driver, so nothing needs to be installed.
//
// It is the program behind the redo and error sections of
// docs/workflows/then.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// Workflow and step names. The reset and the fork both start the import
// again at its only step, so the name is shared by Register and by the
// ResetRunWith and ForkRun calls.
const (
	importWorkflow = "import"
	indexWorkflow  = "index"
	importStep     = "load"
)

// endTimeout bounds every wait for a run end: the in-memory driver ends
// these one-step runs within milliseconds, so a wait this long means a
// bug, and the program fails instead of hanging.
const endTimeout = 5 * time.Second

// pollInterval is how often an index run's status is re-read: these runs
// end within milliseconds, so a short poll keeps the program quick.
const pollInterval = 2 * time.Millisecond

// await is how the program waits for an index run.
var await = workflow.AwaitOptions{Poll: pollInterval}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "then-redo:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code
// and compares everything it writes to w.
func run(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "default: redo runs are skipped")
	skip, err := workflow.Then(indexAfterImport())
	if err != nil {
		return err
	}
	if err := redoRound(ctx, w, skip); err != nil {
		return err
	}

	fmt.Fprintln(w, "with ThenAfterRedo: every redo starts index")
	// snippet:after-redo
	follow, err := workflow.Then(indexAfterImport(), workflow.ThenAfterRedo()) // index must be idempotent
	if err != nil {
		return err
	}
	// snippet:end
	if err := redoRound(ctx, w, follow); err != nil {
		return err
	}
	return errorRound(ctx, w)
}

// indexAfterImport is the one rule of the redo rounds: when an import
// succeeds, start an index run.
func indexAfterImport() []workflow.ThenRule {
	return []workflow.ThenRule{{When: importWorkflow, Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
		return workflow.Job{Workflow: indexWorkflow}, true, nil
	}}}
}

// noop is the body of both workflows: the program counts runs, not work.
func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// rig is one client with the Then plugin and an observer of import ends.
type rig struct {
	c     workflow.Client
	ended chan workflow.RunEvent // every import end, after Then saw it
}

// newRig builds a client with then, registers both workflows and starts
// it. The observer is registered after Use, so it runs after Then's own
// observer for the same end (observers run in registration order): when
// it reports an import end, any index run Then enqueued for that end
// already exists.
func newRig(ctx context.Context, then workflow.Plugin) (*rig, error) {
	c, err := workflow.Use(memory.New(), then)
	if err != nil {
		return nil, err
	}
	r := &rig{c: c, ended: make(chan workflow.RunEvent, 1)}
	for _, name := range []string{importWorkflow, indexWorkflow} {
		if err := c.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: importStep, Run: noop}}}); err != nil {
			return nil, err
		}
	}
	if err := c.(workflow.RunNotifier).OnRunFinished(func(_ context.Context, ev workflow.RunEvent) {
		if ev.Workflow == importWorkflow {
			r.ended <- ev
		}
	}); err != nil {
		return nil, err
	}
	if err := c.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// waitEnd waits for the next import end.
func (r *rig) waitEnd(ctx context.Context) (workflow.RunEvent, error) {
	select {
	case ev := <-r.ended:
		return ev, nil
	case <-time.After(endTimeout):
		return workflow.RunEvent{}, errors.New("no import end within the timeout")
	case <-ctx.Done():
		return workflow.RunEvent{}, ctx.Err()
	}
}

// indexRuns counts the index runs Then has started so far and waits for
// each to end. The wait keeps the output fixed: a next job's idempotency
// key ("then/<rule>/<run ID>") holds only while that job is unfinished, and
// a reset run keeps its run ID, so a redo that ends while the first index
// still runs would be refused as a duplicate instead of starting a second.
func (r *rig) indexRuns(ctx context.Context) (int, error) {
	page, err := r.c.(workflow.RunQuerier).Runs(ctx, workflow.RunQuery{Workflow: indexWorkflow})
	if err != nil {
		return 0, err
	}
	for _, run := range page.Runs {
		if _, err := workflow.Await(ctx, r.c, run.Ref, await); err != nil {
			return 0, err
		}
	}
	return len(page.Runs), nil
}

// report waits for the next import end and prints what Then did for it.
func (r *rig) report(ctx context.Context, w io.Writer, what string) error {
	ev, err := r.waitEnd(ctx)
	if err != nil {
		return err
	}
	n, err := r.indexRuns(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  %s: %s, redo label %v, index runs so far %d\n",
		what, ev.Status, ev.Labels[workflow.LabelRedo] == "true", n)
	return nil
}

// redoRound runs one import on a client with then, resets it after it
// succeeded, forks it, and prints how many index runs Then started after
// each end.
func redoRound(ctx context.Context, w io.Writer, then workflow.Plugin) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, err := newRig(ctx, then)
	if err != nil {
		return err
	}
	c := r.c

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: importWorkflow})
	if err != nil {
		return err
	}
	if err := r.report(ctx, w, "first import"); err != nil {
		return err
	}

	// snippet:redo
	// Run the import again although it succeeded: the run keeps its ref
	// and gets LabelRedo.
	if _, err := c.(workflow.RunResetterWith).ResetRunWith(ctx, ref, importStep,
		workflow.ResetOptions{Succeeded: true}); err != nil {
		return err
	}
	// snippet:end
	if err := r.report(ctx, w, "reset import"); err != nil {
		return err
	}

	// snippet:fork
	// A fork of a succeeded run is a new run that also carries LabelRedo.
	if _, err := c.(workflow.RunForker).ForkRun(ctx, ref, importStep, workflow.ForkOptions{}); err != nil {
		return err
	}
	// snippet:end
	return r.report(ctx, w, "forked import")
}

// errorRound gives Then a rule whose Start fails and prints the
// *workflow.ThenError that ThenErrors receives.
func errorRound(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	fmt.Fprintln(w, "errors:")

	// snippet:errors
	thenErrs := make(chan error, 1)
	then, err := workflow.Then([]workflow.ThenRule{{
		Name: "audit", When: importWorkflow,
		Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
			return workflow.Job{}, false, errors.New("audit log is read-only")
		},
	}}, workflow.ThenErrors(func(err error) { thenErrs <- err })) // instead of slog.Default
	if err != nil {
		return err
	}
	// snippet:end

	r, err := newRig(ctx, then)
	if err != nil {
		return err
	}
	if _, err := r.c.Enqueue(ctx, workflow.Job{Workflow: importWorkflow}); err != nil {
		return err
	}
	if _, err := r.waitEnd(ctx); err != nil {
		return err
	}
	// Then's observer ran before ours, so its error is already queued.
	select {
	case err := <-thenErrs:
		var te *workflow.ThenError
		if !errors.As(err, &te) {
			return fmt.Errorf("ThenErrors got %T, want *workflow.ThenError", err)
		}
		fmt.Fprintf(w, "  rule %q, job started: %v, cause: %v\n", te.Rule, te.Job.Workflow != "", te.Err)
	default:
		return errors.New("ThenErrors received nothing")
	}
	return nil
}
