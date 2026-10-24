package workflow

import "context"

// ScheduleHooks are before, on-success, on-failure and after callbacks
// for one schedule, or for every schedule of a workflow, built
// only from TickNotifier and RunNotifier so they work on any driver that
// has both. Ping a heartbeat service from After or OnSuccess.
//
// Every hook is optional. They run on the driver's observer goroutines
// with that contract: at least once, so a hook must be idempotent, and
// best-effort unless the driver declares CapRunFinished and
// CapTickEvents.
type ScheduleHooks struct {
	// Workflow is the schedule's workflow; required.
	Workflow string

	// Name is the Schedule.Name (for a declared cron, its expression);
	// empty matches every schedule of Workflow.
	Name string

	// Fired is called when a tick enqueued a run, or failed to (t.Err).
	Fired func(ctx context.Context, t Tick)

	// Skipped is called when a tick was skipped or misfired.
	Skipped func(ctx context.Context, t Tick)

	// OnSuccess is called when a run the schedule fired succeeded.
	OnSuccess func(ctx context.Context, ev RunEvent)

	// OnFailure is called when a run the schedule fired failed.
	OnFailure func(ctx context.Context, ev RunEvent)

	// After is called when a run the schedule fired ended, in any status.
	After func(ctx context.Context, ev RunEvent)
}

// matchesTick reports whether t belongs to the hooked schedule.
func (h ScheduleHooks) matchesTick(t Tick) bool {
	return t.Workflow == h.Workflow && (h.Name == "" || t.Name == h.Name)
}

// matchesRun reports whether ev is a run the hooked schedule fired, read
// from the labels FiredLabels set.
func (h ScheduleHooks) matchesRun(ev RunEvent) bool {
	if ev.Workflow != h.Workflow || ev.Labels[LabelSchedule] == "" {
		return false
	}
	return h.Name == "" || ev.Labels[LabelScheduleName] == h.Name
}

func (h ScheduleHooks) wantsTicks() bool { return h.Fired != nil || h.Skipped != nil }
func (h ScheduleHooks) wantsRuns() bool {
	return h.OnSuccess != nil || h.OnFailure != nil || h.After != nil
}

// AttachScheduleHooks registers hooks on c before Start: one TickObserver
// when any hook needs ticks and one RunFinishedFunc when any needs run
// ends. It refuses, before registering anything, a hook without a
// Workflow (ErrInvalid) and a client lacking the notifier a hook needs
// (an UnsupportedError naming CapTickEvents or CapRunFinished). After
// Start the registrations return ErrAlreadyStarted.
func AttachScheduleHooks(c Client, hooks ...ScheduleHooks) error {
	var ticks, runs bool
	for i, h := range hooks {
		if h.Workflow == "" {
			return invalidf("schedule hooks %d have no Workflow", i)
		}
		ticks = ticks || h.wantsTicks()
		runs = runs || h.wantsRuns()
	}
	tn, hasTicks := c.(TickNotifier)
	rn, hasRuns := c.(RunNotifier)
	if ticks && !hasTicks {
		return &UnsupportedError{Field: "ScheduleHooks", Missing: CapTickEvents, Reason: "the client does not implement workflow.TickNotifier"}
	}
	if runs && !hasRuns {
		return &UnsupportedError{Field: "ScheduleHooks", Missing: CapRunFinished, Reason: "the client does not implement workflow.RunNotifier"}
	}
	hs := append([]ScheduleHooks(nil), hooks...)
	if ticks {
		if err := tn.OnTick(func(ctx context.Context, t Tick) { dispatchTick(ctx, hs, t) }); err != nil {
			return err
		}
	}
	if runs {
		if err := rn.OnRunFinished(func(ctx context.Context, ev RunEvent) { dispatchRun(ctx, hs, ev) }); err != nil {
			return err
		}
	}
	return nil
}

func dispatchTick(ctx context.Context, hs []ScheduleHooks, t Tick) {
	for _, h := range hs {
		if !h.matchesTick(t) {
			continue
		}
		switch {
		case t.Outcome == TickFired && h.Fired != nil:
			h.Fired(ctx, t)
		case t.Outcome != TickFired && h.Skipped != nil:
			h.Skipped(ctx, t)
		}
	}
}

func dispatchRun(ctx context.Context, hs []ScheduleHooks, ev RunEvent) {
	for _, h := range hs {
		if !h.matchesRun(ev) {
			continue
		}
		switch {
		case ev.Status == StatusSucceeded && h.OnSuccess != nil:
			h.OnSuccess(ctx, ev)
		case ev.Status == StatusFailed && h.OnFailure != nil:
			h.OnFailure(ctx, ev)
		}
		if h.After != nil {
			h.After(ctx, ev)
		}
	}
}
