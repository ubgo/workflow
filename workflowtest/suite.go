package workflowtest

import (
	"testing"
)

// Run executes the whole conformance suite against h. Each assertion is a
// subtest on a fresh Instance; subtests whose capabilities the driver
// lacks are skipped with the missing capability named.
//
// The suite asserts: lifecycle errors; enqueue
// runs once and succeeds; DAG order and step inputs; Status reaches a
// terminal state through valid states; idempotency keys dedup while in
// flight and are released after; RunAt neither runs early nor late;
// retries follow step, workflow and RetriesNone rules and Permanent stops
// them; a RetryAfter delay under DelayFor and the Backoff.Jitter lower
// bound; step and workflow timeouts; OnFailure runs once; Cancel of a
// running and a scheduled run, idempotently; wait for event resumes on the
// matching key only (fixed Key and per-run KeyFrom) and times out;
// StepInfo on the handler's ctx; durable sleep; each concurrency
// strategy; rate limits; declared cron registered once and Schedule /
// Unschedule; each misfire policy (skip, fire once, fire all, Within and
// Max), a declared cron in its CronLocation, and the refusal of a zone or
// misfire policy the driver lacks; run-finished events (RunNotifier with CapRunFinished) for
// succeeded, failed and cancelled runs; capability refusals at Start and at Enqueue; and, for a
// durable driver with a Restart hook, runs that survive a restart and a
// step cut off by a restart that runs again on the new client; named
// queues and their refusals; Job.Retry, RetryPolicy.Until and MaxErrors;
// Discard; Runs filters and paging, Retry from the failed step, Delete
// and CancelAll; pausing a workflow and a queue; named-schedule upserts,
// time.Local refused, ScheduleWhen with tick events, windows, Schedules
// listing and schedule pause; graceful Drain; Retrying and Discarded run
// states; job chains and chain edits from a running step; worker scaling; QueueConfig.Limit across clients and a crash; retry precedence
// (job, step, workflow, default); DiscardOnExhaust per stopping rule and level;
// drain reports (finished and cut steps, grace edges, repeated drains);
// and closures, with the refusals of each on a driver that lacks it.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil {
		t.Fatal("workflowtest: Harness.New is nil")
	}
	nonce := newNonce()
	groups := []struct {
		name  string
		cases []testCase
	}{
		{"lifecycle", lifecycleCases},
		{"run", runCases},
		{"idempotency", idempotencyCases},
		{"delayed", delayedCases},
		{"retry", retryCases},
		{"retryseam", retrySeamCases},
		{"timeout", timeoutCases},
		{"onfailure", onFailureCases},
		{"cancel", cancelCases},
		{"wait", waitCases},
		{"sleep", sleepCases},
		{"concurrency", concurrencyCases},
		{"ratelimit", rateLimitCases},
		{"cron", cronCases},
		{"misfire", misfireCases},
		{"runfinished", hookCases},
		{"caps", capsCases},
		{"durable", durableCases},
		{"queue", queueCases},
		{"jobretry", jobRetryCases},
		{"discard", discardCases},
		{"runquery", runQueryCases},
		{"pause", pauseCases},
		{"schedule", scheduleCases},
		{"drain", drainCases},
		{"runstates", runStateCases},
		{"chain", chainCases},
		{"scale", scaleCases},
		{"closure", closureCases},
		{"func", funcCases},
		{"retryingerror", retryingErrorCases},
		{"labelprefix", labelPrefixCases},
		{"done", doneCases},
		{"overlap", overlapCases},
		{"trigger", triggerCases},
		{"schedhistory", scheduleHistoryCases},
		{"enqueueonly", enqueueOnlyCases},
		{"errorhistory", errorHistoryCases},
		{"shutdownrequeue", shutdownRequeueCases},
		{"force", forceCases},
		{"queuelimit", queueLimitCases},
		{"retryprecedence", retryPrecedenceCases},
		{"discardexhaust", discardExhaustCases},
		{"drainreport", drainReportCases},
	}
	for _, group := range append(groups, p5Groups...) {
		t.Run(group.name, func(t *testing.T) {
			for _, tc := range group.cases {
				t.Run(tc.name, func(t *testing.T) {
					tc.run(newEnv(t, &h, nonce))
				})
			}
		})
	}
}

// testCase is one named assertion.
type testCase struct {
	name string
	run  func(e *env)
}
