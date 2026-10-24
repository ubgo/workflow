package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks every line the guides show.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

const want = `memory   durable=false DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|Closure|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|ErrorHistory|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport
sqlstore durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|RunFinished|RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone|Queue|QueueOverride|JobRetry|RetryUntil|RetryMaxErrors|Discard|RunQuery|RetryRun|DeleteRun|CancelAll|PauseWorkflow|PauseQueue|SchedulePause|ScheduleWhen|ScheduleWindow|TickEvents|ScheduleList|Drain|RunStates|Chain|WorkerScale|LabelPrefix|OverlapSkip|ScheduleTrigger|ScheduleHistory|EnqueueOnly|ErrorHistory|ShutdownRequeue|ForceRun|Snooze|Heartbeat|Tenant|RunTimeout|NodeIdentity|KeyFIFO|DueQuery|Result|CronSeconds|ScheduleActive|ScheduleKeep|TickTime|QueueLimitGlobal|DiscardExhausted|DrainReport
hatchet  durable=true  Durable|DelayedStart|WaitForEvent|DurableSleep|ConcurrencyKey|RateLimit|IdempotencyKey|Cron|Cancel|Queue|RunQuery|RetryRun|CancelAll|ScheduleList|Drain|ScheduleTrigger|ScheduleHistory|EnqueueOnly|Tenant|KeyFIFO|DueQuery|Result|CronSeconds|TickTime|DrainReport
digest needs: Cron|RetryJitter|MisfireSkip|CronZone
  hatchet lacks RetryJitter|MisfireSkip|CronZone
refused: field=Workflow missing=RetryJitter|MisfireSkip|CronZone reason="workflow digest"
is ErrUnsupported: true
hatchet Register: workflow: Workflow.Concurrency.Key.Func unsupported by this driver: workflow sync: Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression
schedule needs: Cron|CronZone
on hatchet: workflow: Schedule.Location unsupported by this driver (missing CronZone): schedule for workflow digest
on memory:  <nil>
job needs: DelayedStart|IdempotencyKey
Validate: workflow: invalid spec: workflow "import": step "load" is After unknown step "parse"
is ErrInvalid: true
Register: true
ValidateSchedule: workflow: invalid spec: schedule for "digest" has no Location; use time.UTC for UTC
before Start: true
second Start: true
Register after Start: true
unknown workflow: true
unknown run: true
outage: true true
refusal is no outage: false
reserved: [ubgo.workflow/ workflow.batch. otel.]
otel.traceparent reserved: true otel.
ValidateLabels: workflow: invalid spec: label "workflow.batch.id" uses the reserved prefix "workflow.batch."
`
