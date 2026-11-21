// Command flow-boundary shows three ways to act on a step around its
// work, all on the in-memory driver:
//
//   - a non-interrupting timer (workflow.Boundary): a reminder goes out
//     when a review step is still running after a while, and the review
//     carries on;
//   - an interrupting deadline: a tolerated step gives up on a slow
//     supplier and a trigger rule sends the timeout to a fallback step;
//   - setup and teardown: a setup step stores the resource it created as
//     run data (workflow.SetRunData), and a Finally step (RulesSpec.Finally)
//     reads it back (workflow.RunData) and releases it whether the work
//     succeeded or failed.
//
// It is the program behind the Boundary, interrupting deadline and setup
// and teardown sections of docs/workflows/trigger-rules.md: the guide
// copies its regions (marked "snippet:") and task docs:snippets fails when
// they drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// remindAfter is how long the review step may run before the boundary
// timer sends a reminder. It is short so the program finishes at once; a
// real app would use hours.
const remindAfter = 20 * time.Millisecond

// slowReview is how long the slow review takes: well past remindAfter, so
// the reminder always fires before the review returns, even on a busy
// machine running the race detector.
const slowReview = 10 * remindAfter

// supplierDeadline is how long the quote step waits for the supplier
// before it gives up and lets the fallback answer.
const supplierDeadline = 10 * time.Millisecond

// awaitPoll is how often the program looks at a run it waits for: a short
// poll keeps the program fast on the in-memory driver.
const awaitPoll = 2 * time.Millisecond

// await is how the program waits for a run.
var await = workflow.AwaitOptions{Poll: awaitPoll}

// claimSpeed is a claim's payload: how long its review takes.
type claimSpeed string

// The claims the program files: one reviewed before the boundary timer
// fires, one reviewed after it.
const (
	claimQuick claimSpeed = "quick"
	claimSlow  claimSpeed = "slow"
)

// suite is the e2e workflow's payload: which test suite runs.
type suite string

// The suites the program runs: one passes, one fails, and the teardown
// runs after both.
const (
	suiteGreen  suite = "green"
	suiteBroken suite = "broken"
)

// errTestsFailed is what the e2e work step returns for the "broken"
// payload, so the teardown is shown running after a failure too.
var errTestsFailed = errors.New("3 tests failed")

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "flow-boundary:", err)
		os.Exit(1)
	}
}

// notes collects what the steps did, in order, safely across goroutines:
// the boundary timer calls back from a goroutine of its own.
type notes struct {
	mu    sync.Mutex
	lines []string
}

// add records one line.
func (n *notes) add(s string) { n.mu.Lock(); n.lines = append(n.lines, s); n.mu.Unlock() }

// take returns the lines collected so far, joined, and forgets them.
func (n *notes) take() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := strings.Join(n.lines, ", ")
	n.lines = nil
	return s
}

// run is main without the process exit, writing to w, so the test drives
// the same code and checks every line.
func run(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var did notes

	// snippet:boundary
	// remind is the boundary timer's callback for a step still running.
	remind := func(ctx context.Context, info workflow.StepInfo) {
		// Runs once, in its own goroutine, while the step carries on.
		did.add(fmt.Sprintf("reminder: %s still in %q after %s", info.Labels["claim"], info.Step, remindAfter))
	}
	c, err := workflow.Use(
		memory.New(memory.WithMiddleware(workflow.Boundary(remindAfter, remind, "review"))),
		workflow.Journal(workflow.NewMemoryJournal()), // SetRunData and RunData keep run data here
	)
	if err != nil {
		return err
	}
	// snippet:end

	for _, wf := range []workflow.Workflow{claimWorkflow(&did)} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	quote, err := quoteWorkflow()
	if err != nil {
		return err
	}
	e2e, err := e2eWorkflow(&did)
	if err != nil {
		return err
	}
	for _, wf := range []workflow.Workflow{quote, e2e} {
		if err := c.Register(wf); err != nil {
			return err
		}
	}
	if err := c.Start(ctx); err != nil {
		return err
	}

	fmt.Fprintln(w, "boundary timer:")
	for _, claim := range []claimSpeed{claimQuick, claimSlow} {
		payload, err := json.Marshal(claim)
		if err != nil {
			return err
		}
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "claim", Payload: payload, Labels: map[string]string{"claim": "claim-" + string(claim)}})
		if err != nil {
			return err
		}
		out, err := workflow.AwaitResult(ctx, c, ref, await)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s review: %s\n    ran: %s\n", claim, out, did.take())
	}

	fmt.Fprintln(w, "interrupting deadline:")
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "quote"})
	if err != nil {
		return err
	}
	out, err := workflow.AwaitResult(ctx, c, ref, await)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  %s\n", out)

	fmt.Fprintln(w, "setup and teardown:")
	for _, s := range []suite{suiteGreen, suiteBroken} {
		payload, err := json.Marshal(s)
		if err != nil {
			return err
		}
		ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "e2e", Payload: payload})
		if err != nil {
			return err
		}
		st, err := workflow.Await(ctx, c, ref, await)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s suite: run %s\n    ran: %s\n", s, st, did.take())
	}
	return nil
}

// claimWorkflow files an insurance claim, then reviews it. The review of
// a "slow" claim runs past remindAfter, so the boundary timer on the
// "review" step sends a reminder while the review goes on.
func claimWorkflow(did *notes) workflow.Workflow {
	return workflow.Workflow{Name: "claim", Steps: []workflow.Step{
		{Name: "file", Run: func(_ context.Context, in []byte) ([]byte, error) {
			did.add("file")
			return in, nil
		}},
		{Name: "review", After: []string{"file"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			var speed claimSpeed
			if err := json.Unmarshal(in, &speed); err != nil {
				return nil, workflow.Permanent(err)
			}
			if speed == claimSlow {
				time.Sleep(slowReview) // a person takes their time
			}
			did.add("review")
			return json.Marshal("approved")
		}},
	}}
}

// quoteWorkflow asks a supplier for a price; the supplier never answers
// in time, so the quote step gives up at its own deadline and the
// fallback step answers with a cached price.
func quoteWorkflow() (workflow.Workflow, error) {
	// snippet:interrupt
	wf := workflow.Workflow{Name: "quote", Steps: []workflow.Step{
		{Name: "price", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, supplierDeadline) // the deadline lives in the step
			defer cancel()
			price, err := askSupplier(ctx)
			if errors.Is(err, context.DeadlineExceeded) {
				// An error matching ErrTimeout gets the kind FailureTimeout.
				return nil, workflow.TimeoutError(workflow.ErrStepTimeout, "supplier did not answer")
			}
			return price, err
		}},
		{Name: "fallback", After: []string{"price"}, Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			f := workflow.ParentFailures(ctx)[0]
			return json.Marshal(fmt.Sprintf("cached price 9.90 (%s failed: %s)", f.Step, f.Kind))
		}},
	}}
	return workflow.WithRules(wf, workflow.RulesSpec{
		Tolerate: []string{"price"},
		Rules:    map[string]workflow.TriggerRule{"fallback": workflow.RuleOneFailedOf(workflow.FailureTimeout)},
	})
	// snippet:end
}

// askSupplier stands for a remote call that is too slow: it answers only
// when ctx ends.
func askSupplier(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// e2eWorkflow creates a test environment, runs a suite in it and deletes
// it again, whatever the suite did.
func e2eWorkflow(did *notes) (workflow.Workflow, error) {
	// createEnv, runSuite and deleteEnv stand for the calls to the test
	// infrastructure; runSuite fails the broken suite.
	createEnv := func(context.Context) (string, error) { did.add("create env-42"); return "env-42", nil }
	runSuite := func(_ context.Context, in []byte) ([]byte, error) {
		did.add("test")
		var s suite
		if err := json.Unmarshal(in, &s); err != nil {
			return nil, workflow.Permanent(err)
		}
		if s == suiteBroken {
			return nil, workflow.Permanent(errTestsFailed)
		}
		return nil, nil
	}
	deleteEnv := func(_ context.Context, env string) error { did.add("delete " + env); return nil }
	// snippet:teardown
	return workflow.WithRules(workflow.Workflow{Name: "e2e", Steps: []workflow.Step{
		{Name: "setup", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			env, err := createEnv(ctx)
			if err != nil {
				return nil, err
			}
			if err := workflow.SetRunData(ctx, "env", env); err != nil {
				return nil, err
			}
			return in, nil // the suite to run
		}},
		{Name: "test", After: []string{"setup"}, Run: runSuite},
		{Name: "teardown", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			env, ok, err := workflow.RunData[string](ctx, "env")
			if err != nil || !ok {
				return nil, err // setup failed before it made one: nothing to release
			}
			return nil, deleteEnv(ctx, env)
		}},
	}}, workflow.RulesSpec{Finally: []string{"teardown"}}) // placed after every leaf
	// snippet:end
}
