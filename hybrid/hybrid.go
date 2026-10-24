// Package hybrid splits one workflow.Client across two backends: every
// schedule fires on a scheduler client (usually the in-process memory
// driver) while every run lives on a jobs client (a durable driver), the
// common "cron in the process, jobs in the database" shape.
//
// A workflow registered here is registered on the jobs client without its
// schedule fields, and on the scheduler client as a one-step forwarder of
// the same name that enqueues the real job onto the jobs client with the
// fired run's labels (schedule ID, tick time) and an idempotency key made
// from the schedule and the tick, so a forwarder retried after a jobs
// outage does not start the job twice (on a jobs client with
// CapIdempotencyKey).
//
// Each node of a cluster runs its own scheduler, so a schedule fires once
// per node: use it where that is wanted (one node runs schedules) or the
// idempotency key absorbs it (every node fires the same tick, the jobs
// client keeps one run while the first is unfinished). For a schedule that
// must fire once per cluster, schedule on the durable driver itself.
package hybrid

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/ubgo/workflow"
)

// forwardRetry is the forwarder step's retry policy: a jobs backend that
// is briefly unreachable must not lose a tick.
var forwardRetry = workflow.RetryPolicy{Max: 5, Backoff: workflow.Backoff{Initial: time.Second, Max: 30 * time.Second, Factor: 2}}

// scheduleCaps are the capabilities the scheduler client answers for. Run
// history, triggers, tick events, overlap and KeepRuns are left out: on
// the scheduler they would describe the forwarder runs, not the jobs.
const scheduleCaps = workflow.CapCron | workflow.CapCronZone | workflow.CapMisfireSkip | workflow.CapMisfireFireOnce |
	workflow.CapMisfireFireAll | workflow.CapScheduleWhen | workflow.CapScheduleWindow | workflow.CapSchedulePause |
	workflow.CapScheduleList | workflow.CapCronSeconds | workflow.CapScheduleActive | workflow.CapTickTime

// notForwarded are the schedule capabilities neither backend answers for
// here, for the reason above.
const notForwarded = workflow.CapOverlapSkip | workflow.CapScheduleKeep | workflow.CapScheduleHistory |
	workflow.CapScheduleTrigger | workflow.CapTickEvents

// Client is a workflow.Client whose schedules fire on one backend and whose
// runs live on another. Build it with New.
type Client struct {
	workflow.Forward // run-side optional interfaces go to the jobs client
	sched, jobs      workflow.Client
}

var (
	_ workflow.Client         = (*Client)(nil)
	_ workflow.ScheduleLister = (*Client)(nil)
	_ workflow.SchedulePauser = (*Client)(nil)
	_ workflow.RunNotifier    = (*Client)(nil)
)

// New returns a client firing schedules on scheduler and running every
// job on jobs. Neither is started; Start starts both.
func New(scheduler, jobs workflow.Client) *Client {
	return &Client{Forward: workflow.Forward{Inner: jobs}, sched: scheduler, jobs: jobs}
}

// Capabilities is the jobs client's, with the schedule family taken from
// the scheduler client (see scheduleCaps and notForwarded).
func (c *Client) Capabilities() workflow.Caps {
	return (c.jobs.Capabilities()&^(scheduleCaps|notForwarded) | c.sched.Capabilities()&scheduleCaps)
}

// Register registers wf on the jobs client without its schedule fields and,
// as a forwarder, on the scheduler client. A workflow asking for what this
// split cannot honour (CronOverlap, CronKeepRuns) is refused.
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	if wf.CronOverlap == workflow.OverlapSkip {
		return workflow.Unsupported("Workflow.CronOverlap", "hybrid: the scheduler sees only its forwarder runs, not the jobs")
	}
	if wf.CronKeepRuns > 0 {
		return workflow.Unsupported("Workflow.CronKeepRuns", "hybrid: the jobs client keeps the runs; use its retention")
	}
	run := wf
	run.Cron, run.CronLocation, run.Misfire, run.CronWindows, run.ScheduleWhen = nil, nil, workflow.Misfire{}, workflow.Windows{}, nil
	run.CronOverlap, run.CronSeconds, run.CronActive = "", false, workflow.Active{}
	if err := c.jobs.Register(run); err != nil {
		return fmt.Errorf("hybrid: jobs: %w", err)
	}
	fwd := workflow.Workflow{
		Name:         wf.Name,
		Steps:        []workflow.Step{{Name: "forward", Run: c.forward(wf.Name), Retry: &forwardRetry}},
		Cron:         wf.Cron,
		CronLocation: wf.CronLocation,
		Misfire:      wf.Misfire,
		CronWindows:  wf.CronWindows,
		ScheduleWhen: wf.ScheduleWhen,
		CronSeconds:  wf.CronSeconds,
		CronActive:   wf.CronActive,
	}
	if err := c.sched.Register(fwd); err != nil {
		return fmt.Errorf("hybrid: scheduler: %w", err)
	}
	return nil
}

// forward enqueues the fired job onto the jobs client.
func (c *Client) forward(name string) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		job := workflow.Job{Workflow: name, Payload: in, Labels: maps.Clone(info.Labels), Tenant: info.Tenant}
		if c.jobs.Capabilities().Has(workflow.CapIdempotencyKey) {
			job.IdempotencyKey = forwardKey(info)
		}
		// The labels were stamped by the scheduler client (schedule ID,
		// tick time), so they pass on as the library's own.
		var reserved []string
		for k := range job.Labels {
			if _, ok := workflow.ReservedLabel(k); ok {
				reserved = append(reserved, k)
			}
		}
		if _, err := c.jobs.Enqueue(workflow.AllowReservedLabels(ctx, reserved...), job); err != nil {
			return nil, fmt.Errorf("hybrid: enqueue %s on the jobs client: %w", name, err)
		}
		return nil, nil
	}
}

// forwardKey is the idempotency key of a forwarded fire: the schedule and
// the tick it stands for, else the forwarder run.
func forwardKey(info workflow.StepInfo) string {
	if at, ok := workflow.TickTime(info.Labels); ok {
		return "hybrid/" + info.Labels[workflow.LabelSchedule] + "/" + at.Format(time.RFC3339Nano)
	}
	return "hybrid/" + info.RunID.String()
}

// Start starts the jobs client, then the scheduler client; when the
// scheduler fails to start, the error names it (cancel ctx to stop the
// jobs client).
func (c *Client) Start(ctx context.Context) error {
	if err := c.jobs.Start(ctx); err != nil {
		return fmt.Errorf("hybrid: jobs: %w", err)
	}
	if err := c.sched.Start(ctx); err != nil {
		return fmt.Errorf("hybrid: scheduler: %w", err)
	}
	return nil
}

// Enqueue runs a job on the jobs client.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	return c.jobs.Enqueue(ctx, job)
}

// Cancel cancels a run on the jobs client.
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	return c.jobs.Cancel(ctx, ref)
}

// Status reports a run on the jobs client.
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	return c.jobs.Status(ctx, ref)
}

// Signal signals the jobs client, where the runs wait.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error { return c.jobs.Signal(ctx, ev) }

// Schedule adds a schedule on the scheduler client; it refuses what the
// split cannot honour (Overlap, KeepRuns).
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	if s.Overlap == workflow.OverlapSkip {
		return "", workflow.Unsupported("Schedule.Overlap", "hybrid: the scheduler sees only its forwarder runs")
	}
	if s.KeepRuns > 0 {
		return "", workflow.Unsupported("Schedule.KeepRuns", "hybrid: the jobs client keeps the runs; use its retention")
	}
	return c.sched.Schedule(ctx, s)
}

// Unschedule removes a schedule from the scheduler client.
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	return c.sched.Unschedule(ctx, id)
}

// Schedules lists the scheduler client's schedules.
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	return workflow.Forward{Inner: c.sched}.Schedules(ctx)
}

// PauseSchedule pauses on the scheduler client.
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return workflow.Forward{Inner: c.sched}.PauseSchedule(ctx, t)
}

// ResumeSchedule resumes on the scheduler client.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	return workflow.Forward{Inner: c.sched}.ResumeSchedule(ctx, t)
}

// TriggerSchedule is refused: a trigger would return the forwarder's run,
// which the jobs client does not know.
func (c *Client) TriggerSchedule(context.Context, workflow.ScheduleID) (workflow.RunRef, error) {
	return workflow.RunRef{}, &workflow.UnsupportedError{Field: "TriggerSchedule", Missing: workflow.CapScheduleTrigger,
		Reason: "hybrid: a trigger would return the scheduler's forwarder run"}
}

// OnTick is refused: ticks would name forwarder runs.
func (c *Client) OnTick(workflow.TickObserver) error {
	return &workflow.UnsupportedError{Field: "OnTick", Missing: workflow.CapTickEvents, Reason: "hybrid: ticks would name the scheduler's forwarder runs"}
}

// OnRunFinished observes the jobs client's run ends.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	n, ok := c.jobs.(workflow.RunNotifier)
	if !ok {
		return &workflow.UnsupportedError{Field: "OnRunFinished", Missing: workflow.CapRunFinished, Reason: "hybrid: the jobs client does not implement workflow.RunNotifier"}
	}
	return n.OnRunFinished(fn)
}
