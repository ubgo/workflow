package workflow

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Validate checks the shape of wf, independent of any driver: a name, at
// least one step, unique non-empty step names, exactly one kind per step,
// After naming existing steps and forming no cycle, and policies within
// their ranges. Every error wraps ErrInvalid and names the step or field.
// Drivers call it from Register; it does not parse cron expressions, which
// the driver's cron parser checks.
//
//ds:def id=validate-mnthpzkp owner=@khanakia stability=stable
func Validate(wf Workflow) error {
	if wf.Name == "" {
		return invalidf("workflow has no name")
	}
	if wf.Name == ClosureWorkflow {
		return invalidf("workflow name %q is reserved for closure runs", wf.Name)
	}
	if len(wf.Steps) == 0 {
		return invalidf("workflow %q has no steps", wf.Name)
	}
	if wf.Version < 0 || wf.MinVersion < 0 || wf.MinVersion > wf.Version {
		return invalidf("workflow %q: Version %d and MinVersion %d: both must be zero or more and MinVersion at most Version", wf.Name, wf.Version, wf.MinVersion)
	}
	index := make(map[string]int, len(wf.Steps))
	for i, s := range wf.Steps {
		if s.Name == "" {
			return invalidf("workflow %q: step %d has no name", wf.Name, i)
		}
		if _, dup := index[s.Name]; dup {
			return invalidf("workflow %q: step name %q is used twice", wf.Name, s.Name)
		}
		index[s.Name] = i
		if err := validateStep(s); err != nil {
			return invalidf("workflow %q: step %q: %v", wf.Name, s.Name, err)
		}
	}
	for _, s := range wf.Steps {
		for _, p := range s.After {
			if p == s.Name {
				return invalidf("workflow %q: step %q lists itself in After", wf.Name, s.Name)
			}
			if _, ok := index[p]; !ok {
				return invalidf("workflow %q: step %q is After unknown step %q", wf.Name, s.Name, p)
			}
		}
	}
	if name := findCycle(wf.Steps, index); name != "" {
		return invalidf("workflow %q: step %q is part of an After cycle", wf.Name, name)
	}
	return validatePolicies(wf)
}

//ds:def id=validatestep-x6aebswc owner=@khanakia stability=stable
func validateStep(s Step) error {
	if s.Kind() == "" {
		return errors.New("exactly one of Run, Wait and Sleep must be set")
	}
	if s.Sleep < 0 {
		return errors.New("negative Sleep")
	}
	if s.Timeout < 0 {
		return errors.New("negative Timeout")
	}
	if s.HeartbeatTimeout < 0 {
		return errors.New("HeartbeatTimeout is negative")
	}
	if s.HeartbeatTimeout > 0 && s.Kind() != StepRun {
		return errors.New("HeartbeatTimeout is only for Run steps")
	}
	if s.Slots < 0 {
		return errors.New("negative Slots")
	}
	if s.Slots > 1 && s.Kind() != StepRun {
		return errors.New("Slots is only for Run steps")
	}
	if w := s.Wait; w != nil {
		if w.Event == "" {
			return errors.New("Wait.Event is empty")
		}
		if !w.Deadline.IsZero() && w.Timeout != 0 {
			return errors.New("Wait sets both Deadline and Timeout")
		}
		if w.Timeout < 0 {
			return errors.New("Wait.Timeout is negative")
		}
		if w.Key != "" && !w.KeyFrom.IsZero() {
			return errors.New("Wait sets both Key and KeyFrom")
		}
		if w.KeyFrom.Func != nil && w.KeyFrom.Expr != "" {
			return errors.New("Wait.KeyFrom sets both Func and Expr")
		}
		if w.OwnRun && (w.Key != "" || !w.KeyFrom.IsZero()) {
			return errors.New("Wait sets OwnRun with Key or KeyFrom")
		}
	}
	if s.Retry != nil {
		if err := validateRetry(*s.Retry); err != nil {
			return err
		}
	}
	return nil
}

// validateRetry checks the ranges of a retry policy; the caller names
// where it sits.
//
//ds:def id=validateretry-jcn44n64 owner=@khanakia stability=stable
func validateRetry(p RetryPolicy) error {
	if p.Max < 0 {
		return errors.New("Retry.Max is negative")
	}
	if j := p.Backoff.Jitter; math.IsNaN(j) || j < 0 || j > 1 {
		return fmt.Errorf("Retry.Backoff.Jitter %v is outside [0, 1]", j)
	}
	if p.Until < 0 {
		return errors.New("Retry.Until is negative")
	}
	if p.MaxErrors < 0 {
		return errors.New("Retry.MaxErrors is negative")
	}
	return nil
}

// validateMisfire checks a misfire policy; the caller names where it sits.
//
//ds:def id=validatemisfire-mna4wuqq owner=@khanakia stability=stable
func validateMisfire(m Misfire) error {
	if m.Within < 0 {
		return errors.New("Misfire.Within is negative")
	}
	switch m.Policy {
	case "":
		if !m.IsZero() {
			return errors.New("Misfire sets Within or Max without a Policy")
		}
	case MisfireSkip:
		if m.Within != 0 {
			return errors.New("Misfire.Within is not used by MisfireSkip, which fires only within MisfireGrace")
		}
		if m.Max != 0 {
			return errors.New("Misfire.Max is only for MisfireFireAll")
		}
	case MisfireFireOnce:
		if m.Max != 0 {
			return errors.New("Misfire.Max is only for MisfireFireAll")
		}
	case MisfireFireAll:
		if m.Max < 1 {
			return errors.New("MisfireFireAll needs Misfire.Max of at least 1")
		}
	default:
		return fmt.Errorf("unknown Misfire.Policy %q", m.Policy)
	}
	return nil
}

//ds:def id=validatepolicies-x88m4hwu owner=@khanakia stability=stable
func validatePolicies(wf Workflow) error {
	if err := validateRetry(wf.Retry); err != nil {
		return invalidf("workflow %q: %v", wf.Name, err)
	}
	if wf.Timeout < 0 {
		return invalidf("workflow %q: Timeout is negative", wf.Name)
	}
	switch wf.Idempotency {
	case "", IdempotencyOptional, IdempotencyRequired, IdempotencyOff:
	default:
		return invalidf("workflow %q: unknown Idempotency %q", wf.Name, wf.Idempotency)
	}
	if c := wf.Concurrency; c != nil {
		if c.MaxRuns < 1 {
			return invalidf("workflow %q: Concurrency.MaxRuns must be at least 1", wf.Name)
		}
		if c.Key.Func != nil && c.Key.Expr != "" {
			return invalidf("workflow %q: Concurrency.Key sets both Func and Expr", wf.Name)
		}
		switch c.Strategy {
		case "", ConcurrencyQueueNewest, ConcurrencyDropNewest, ConcurrencyCancelNewest, ConcurrencyCancelInProgress, ConcurrencyRoundRobin:
		default:
			return invalidf("workflow %q: unknown Concurrency.Strategy %q", wf.Name, c.Strategy)
		}
		if c.FIFO && c.Strategy != "" && c.Strategy != ConcurrencyQueueNewest {
			return invalidf("workflow %q: Concurrency.FIFO needs ConcurrencyQueueNewest, not %q", wf.Name, c.Strategy)
		}
		if c.Weight != nil && c.Strategy != ConcurrencyRoundRobin {
			return invalidf("workflow %q: Concurrency.Weight needs ConcurrencyRoundRobin, not %q", wf.Name, c.Strategy)
		}
	}
	if err := validateConcurrencyLimits(wf); err != nil {
		return err
	}
	if err := validateHold(wf); err != nil {
		return err
	}
	for i, r := range wf.RateLimits {
		if r.Limit < 1 || r.Per <= 0 {
			return invalidf("workflow %q: RateLimits[%d] needs Limit >= 1 and a positive Per", wf.Name, i)
		}
		if r.Key.Func != nil && r.Key.Expr != "" {
			return invalidf("workflow %q: RateLimits[%d].Key sets both Func and Expr", wf.Name, i)
		}
	}
	for i, expr := range wf.Cron {
		if expr == "" {
			return invalidf("workflow %q: Cron[%d] is empty", wf.Name, i)
		}
	}
	if wf.CronLocation == time.Local {
		return invalidf("workflow %q: CronLocation is time.Local; name the zone, or leave it nil for UTC", wf.Name)
	}
	if len(wf.Cron) == 0 && !wf.Misfire.IsZero() {
		return invalidf("workflow %q: Misfire is set but the workflow declares no Cron", wf.Name)
	}
	if err := validateMisfire(wf.Misfire); err != nil {
		return invalidf("workflow %q: %v", wf.Name, err)
	}
	if wf.Queue != "" {
		if err := ValidateQueueName(wf.Queue); err != nil {
			return fmt.Errorf("workflow %q: %w", wf.Name, err)
		}
	}
	if !validOverlap(wf.CronOverlap) {
		return invalidf("workflow %q: unknown CronOverlap %q", wf.Name, wf.CronOverlap)
	}
	if wf.CronOverlap != "" && len(wf.Cron) == 0 {
		return invalidf("workflow %q: CronOverlap is set but the workflow declares no Cron", wf.Name)
	}
	if !wf.CronWindows.IsZero() && len(wf.Cron) == 0 {
		return invalidf("workflow %q: CronWindows is set but the workflow declares no Cron", wf.Name)
	}
	if err := validateWindows(wf.CronWindows); err != nil {
		return invalidf("workflow %q: CronWindows: %v", wf.Name, err)
	}
	if len(wf.Cron) == 0 && (wf.CronSeconds || !wf.CronActive.IsZero() || wf.CronKeepRuns != 0) {
		return invalidf("workflow %q: CronSeconds, CronActive or CronKeepRuns is set but the workflow declares no Cron", wf.Name)
	}
	if err := wf.CronActive.validate(); err != nil {
		return invalidf("workflow %q: CronActive: %v", wf.Name, err)
	}
	if wf.CronKeepRuns < 0 {
		return invalidf("workflow %q: CronKeepRuns is negative", wf.Name)
	}
	if wf.RunTimeout < 0 {
		return invalidf("workflow %q: RunTimeout is negative", wf.Name)
	}
	return nil
}

// findCycle returns the name of a step on an After cycle, or "" when the
// graph is acyclic. It is a depth-first search with three colours.
func findCycle(steps []Step, index map[string]int) string {
	const (
		unseen = iota
		onPath
		done
	)
	state := make([]int, len(steps))
	var visit func(i int) string
	visit = func(i int) string {
		state[i] = onPath
		for _, p := range steps[i].After {
			j := index[p]
			switch state[j] {
			case onPath:
				return steps[j].Name
			case unseen:
				if name := visit(j); name != "" {
					return name
				}
			}
		}
		state[i] = done
		return ""
	}
	for i := range steps {
		if state[i] == unseen {
			if name := visit(i); name != "" {
				return name
			}
		}
	}
	return ""
}

// ValidateSchedule checks the fields every driver needs: a workflow, an
// expression, an explicit time zone (time.Local is refused),
// well-formed windows, a well-formed misfire policy and no
// label under a reserved prefix (see ValidateLabels). Errors wrap
// ErrInvalid. The cron expression itself is parsed by the driver.
//
//ds:def id=validateschedule-knj437ed owner=@khanakia stability=stable
func ValidateSchedule(s Schedule) error {
	switch {
	case s.Workflow == "":
		return invalidf("schedule has no Workflow")
	case s.Cron == "":
		return invalidf("schedule for %q has no Cron", s.Workflow)
	case s.Location == nil:
		return invalidf("schedule for %q has no Location; use time.UTC for UTC", s.Workflow)
	case s.Location == time.Local:
		return invalidf("schedule for %q: Location is time.Local, which differs from machine to machine; name the zone", s.Workflow)
	}
	if err := validateWindows(s.Windows); err != nil {
		return invalidf("schedule for %q: Windows: %v", s.Workflow, err)
	}
	if err := validateMisfire(s.Misfire); err != nil {
		return invalidf("schedule for %q: %v", s.Workflow, err)
	}
	if !validOverlap(s.Overlap) {
		return invalidf("schedule for %q: unknown Overlap %q", s.Workflow, s.Overlap)
	}
	if err := s.Active.validate(); err != nil {
		return invalidf("schedule for %q: Active: %v", s.Workflow, err)
	}
	if s.KeepRuns < 0 {
		return invalidf("schedule for %q: KeepRuns is negative", s.Workflow)
	}
	if s.Tenant != "" {
		if err := ValidateTenant(s.Tenant); err != nil {
			return fmt.Errorf("schedule for %q: %w", s.Workflow, err)
		}
	}
	if err := ValidateLabels(s.Labels); err != nil {
		return fmt.Errorf("schedule for %q: %w", s.Workflow, err)
	}
	return nil
}

// ValidateJob checks the shape of a job, independent of any driver and of
// the workflow it names: a workflow name, a valid Queue when one is set,
// and a Retry override within range that carries no functions (a durable
// driver stores the job and cannot store a NoRetryOn or DelayFor). Errors
// wrap ErrInvalid. Drivers call it first in Enqueue. It does not check
// labels, which need the caller's ctx: the library's own packages set
// reserved labels on the jobs they pass down, so a driver's public
// Enqueue also calls CheckJobLabels(ctx, job).
//
//ds:def id=validatejob-a7nq3u2t owner=@khanakia stability=stable
func ValidateJob(job Job) error {
	if job.Workflow == "" {
		return invalidf("job has no Workflow")
	}
	if job.Queue != "" {
		if err := ValidateQueueName(job.Queue); err != nil {
			return fmt.Errorf("job for %q: %w", job.Workflow, err)
		}
	}
	if job.Tenant != "" {
		if err := ValidateTenant(job.Tenant); err != nil {
			return fmt.Errorf("job for %q: %w", job.Workflow, err)
		}
	}
	if p := job.Retry; p != nil {
		if p.NoRetryOn != nil || p.DelayFor != nil {
			return invalidf("job for %q: Job.Retry is data only; put NoRetryOn and DelayFor on the step", job.Workflow)
		}
		if err := validateRetry(*p); err != nil {
			return invalidf("job for %q: Job.%v", job.Workflow, err)
		}
	}
	return validateChain(job)
}

// validateConcurrencyLimits checks Workflow.ConcurrencyLimits. Each limit
// needs MaxRuns of at least 1 and one key form. Several keys are admitted
// all or nothing and a full one makes the run wait, so Concurrency beside
// them may only queue: dropping, cancelling or rotating runs on one key
// while another holds them back has no single meaning.
//
//ds:def id=validateconcurrencylimits-dyxqw4nf owner=@khanakia stability=stable
func validateConcurrencyLimits(wf Workflow) error {
	if len(wf.ConcurrencyLimits) == 0 {
		return nil
	}
	for i, l := range wf.ConcurrencyLimits {
		if l.MaxRuns < 1 {
			return invalidf("workflow %q: ConcurrencyLimits[%d].MaxRuns must be at least 1", wf.Name, i)
		}
		if l.Key.Func != nil && l.Key.Expr != "" {
			return invalidf("workflow %q: ConcurrencyLimits[%d].Key sets both Func and Expr", wf.Name, i)
		}
	}
	if c := wf.Concurrency; c != nil && c.Strategy != "" && c.Strategy != ConcurrencyQueueNewest {
		return invalidf("workflow %q: ConcurrencyLimits need Concurrency.Strategy ConcurrencyQueueNewest, not %q", wf.Name, c.Strategy)
	}
	return nil
}

// validateHold checks Concurrency.Hold and every ConcurrencyLimit.Hold.
// HoldWhileRunning on Concurrency is refused beside a strategy other than
// ConcurrencyQueueNewest, whose decisions (drop or cancel the newest,
// cancel the oldest holder, hand out round-robin turns) are taken when a
// run is admitted and have no meaning for a run that takes its slot back
// later, and beside FIFO, which promises one key's runs as a sequence: a
// later run would start while an earlier one is parked.
//
//ds:def id=validatehold-q4m7ze2s owner=@khanakia stability=stable
func validateHold(wf Workflow) error {
	if c := wf.Concurrency; c != nil {
		if !c.Hold.Valid() {
			return invalidf("workflow %q: unknown Concurrency.Hold %q", wf.Name, c.Hold)
		}
		if c.Hold.WhileRunning() {
			if c.Strategy != "" && c.Strategy != ConcurrencyQueueNewest {
				return invalidf("workflow %q: Concurrency.Hold %q needs Concurrency.Strategy ConcurrencyQueueNewest, not %q", wf.Name, c.Hold, c.Strategy)
			}
			if c.FIFO {
				return invalidf("workflow %q: Concurrency.Hold %q cannot go with Concurrency.FIFO: a later run of the key would start while an earlier one is parked", wf.Name, c.Hold)
			}
		}
	}
	for i, l := range wf.ConcurrencyLimits {
		if !l.Hold.Valid() {
			return invalidf("workflow %q: unknown ConcurrencyLimits[%d].Hold %q", wf.Name, i, l.Hold)
		}
	}
	return nil
}
