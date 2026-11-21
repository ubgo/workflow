// Command release-errors shows how a step tells a release from a failure:
// workflow.RetryAfter marks an error that says "come back later" (an HTTP
// 429 with Retry-After), workflow.IsRelease detects the mark anywhere in an
// error chain, and RetryPolicy.MaxErrors counts only the errors that are
// not releases. It runs on the in-memory driver.
//
// It is the program behind the sentinel errors section of
// docs/concepts/errors.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// retryAfterWait is the Retry-After the fake partner API answers with. It
// is short so the demo finishes at once; a real API names seconds.
const retryAfterWait = 10 * time.Millisecond

// maxAttempts and maxErrors are the policy of the "sync" workflow: up to
// ten attempts in all, but only two that failed through the API's fault.
const (
	maxAttempts = 10
	maxErrors   = 2
)

// awaitPoll is how often the example polls for the run's end; any short
// interval works on the in-memory driver.
const awaitPoll = 2 * time.Millisecond

// errServer is the error a 5xx answer becomes: a real failure, counted
// against MaxErrors.
var errServer = errors.New("503 service unavailable")

// responses is what the fake partner API answers to each attempt, in order.
var responses = []int{
	http.StatusTooManyRequests,
	http.StatusTooManyRequests,
	http.StatusServiceUnavailable,
	http.StatusTooManyRequests,
	http.StatusServiceUnavailable,
	http.StatusOK, // too late: MaxErrors stopped the step at the fifth attempt
}

// main runs the example and exits non-zero on error.
func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "release-errors:", err)
		os.Exit(1)
	}
}

// snippet:mark
// callPartner turns an HTTP status into the error a step returns: a 429 is
// a release (RetryAfter with the wait the API asked for), a 5xx a plain
// error, a 4xx a Permanent one that retrying cannot fix.
func callPartner(status int) error {
	switch {
	case status == http.StatusTooManyRequests:
		return workflow.RetryAfter(fmt.Errorf("429 too many requests"), retryAfterWait)
	case status >= http.StatusInternalServerError:
		return errServer
	case status >= http.StatusBadRequest:
		return workflow.Permanent(fmt.Errorf("partner refused the request: %d", status))
	}
	return nil
}

// snippet:end

// run is main without the process exit, so the test drives the same code
// and reads what it wrote to w.
func run(ctx context.Context, w io.Writer) error {
	classify(w)
	return sync(ctx, w)
}

// classify shows what IsRelease and RetryAfterDelay see, without a client.
func classify(w io.Writer) {
	fmt.Fprintln(w, "classify:")
	// snippet:classify
	release := callPartner(http.StatusTooManyRequests)
	wrapped := fmt.Errorf("sync orders: %w", release) // the mark survives wrapping
	d, _ := workflow.RetryAfterDelay(wrapped, 1)
	fmt.Fprintln(w, "  429 is a release:", workflow.IsRelease(wrapped), "| wait:", d)
	fmt.Fprintln(w, "  503 is a release:", workflow.IsRelease(callPartner(http.StatusServiceUnavailable)))
	fmt.Fprintln(w, "  RetryAfter(nil, d) is nil:", workflow.RetryAfter(nil, time.Second) == nil)

	// Permanent still wins over a release: the policy does not retry it.
	policy := workflow.RetryPolicy{Max: maxAttempts, DelayFor: workflow.RetryAfterDelay}
	both := workflow.RetryAfter(workflow.Permanent(errors.New("account closed")), time.Second)
	fmt.Fprintln(w, "  RetryAfter(Permanent(err)) retried:", policy.Retryable(both, 1))
	// snippet:end
}

// sync runs one "sync" workflow against the fake partner API and prints
// every attempt with the count the driver keeps (workflow.Attempts).
func sync(ctx context.Context, w io.Writer) error {
	fmt.Fprintln(w, "sync:")
	c := memory.New()
	// snippet:policy
	policy := workflow.RetryPolicy{
		Max:       maxAttempts - 1, // retries after the first attempt
		MaxErrors: maxErrors,       // releases do not count here
		DelayFor:  workflow.RetryAfterDelay,
	}
	var count workflow.Attempts // the same counter every driver keeps
	// step calls the fake API with this attempt's answer and prints the count.
	step := func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		err := callPartner(responses[info.Attempt-1])
		if err != nil {
			count = count.Count(err) // N always grows, Errors only when !IsRelease(err)
			fmt.Fprintf(w, "  attempt %d: %v (release: %t) -> attempts %d, errors %d\n",
				info.Attempt, err, workflow.IsRelease(err), count.N, count.Errors)
		}
		return nil, err
	}
	// snippet:end
	if err := c.Register(workflow.Workflow{Name: "sync", Retry: policy, Steps: []workflow.Step{{Name: "call", Run: step}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer c.Stop()
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync"})
	if err != nil {
		return err
	}
	status, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: awaitPoll})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  run %s after %d errors and %d releases\n", status, count.Errors, count.N-count.Errors)
	return nil
}
