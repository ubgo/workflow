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
//
//ds:def id=workflowtest-run-2dxkjk9h owner=@khanakia stability=stable
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil {
		t.Fatal("workflowtest: Harness.New is nil")
	}
	nonce := newNonce()
	for _, group := range allGroups() {
		if h.Groups != nil && !h.Groups(group.name) {
			continue
		}
		t.Run(group.name, func(t *testing.T) {
			for _, tc := range group.cases {
				t.Run(tc.name, func(t *testing.T) {
					tc.run(newEnv(t, &h, nonce))
				})
			}
		})
	}
}

// GroupNames returns the name of every group Run executes, in Run order:
// the first element of each group's subtest path under the suite. A
// driver that shards the suite with Harness.Groups can check its shard
// table against it.
//
//ds:def id=workflowtest-groupnames-7jhfskgz owner=@khanakia stability=stable
func GroupNames() []string {
	var out []string
	for _, g := range allGroups() {
		out = append(out, g.name)
	}
	return out
}

// suiteGroup is one named group of cases.
type suiteGroup struct {
	name  string
	cases []testCase
}

// allGroups returns every group in Run order.
//
//ds:def id=workflowtest-allgroups-uk4d9m6p owner=@khanakia stability=stable
func allGroups() []suiteGroup {
	groups := []suiteGroup{
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
		{"concurrencylimits", concurrencyLimitCases},
		{"concurrencyhold", concurrencyHoldCases},
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
	return append(append(append(append(groups, p5Groups...), p3Groups...), attemptErrorGroups...), gapGroups...)
}

// testCase is one named assertion.
type testCase struct {
	name string
	run  func(e *env)
}
