package workflow

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/ubgo/workflow/cron"
)

// LabelTriggerBackfill is the LabelTrigger value of a run Backfill
// started, so run listings and observers can tell a backfilled tick from
// a cron fire (no LabelTrigger) and a manual trigger (LabelTriggerManual).
const LabelTriggerBackfill = "backfill"

// LabelRunKey is the label Backfill and FireAtStartup (with
// StartupOncePer) set to the run's dedup key, the same value as its
// Job.IdempotencyKey. An idempotency key only dedups while its run is
// unfinished; on a client with CapRunQuery the helpers also look the key
// up among the runs the driver still keeps, so a repeated call does not
// start a run that already finished. It is under the reserved
// LabelPrefixInternal.
const LabelRunKey = LabelPrefixInternal + "run-key"

// DefaultBackfillMax is the most ticks one Backfill starts when
// BackfillSpec.Max is zero.
const DefaultBackfillMax = 1000

// maxBackfillScan bounds how many ticks BackfillPlan looks at (windows
// and the active period can skip most of them), so a per-second
// expression over years fails fast instead of scanning for minutes.
const maxBackfillScan = 1 << 20

// BackfillSpec names the ticks to replay and what each run gets. Set
// either Schedule (the schedule as the app declared it) or Workflow,
// Cron and Location (and optionally Seconds, Input, Labels), not both.
type BackfillSpec struct {
	// Schedule is the schedule to replay: its Workflow, Cron, Location,
	// Seconds, Input, Labels and Tenant are used, its Active period and
	// Windows limit the ticks (a tick they would skip is not replayed).
	Schedule *Schedule

	// ScheduleID is the live schedule's ID, when there is one: it goes
	// into LabelSchedule of each run and into the dedup key. Optional.
	ScheduleID ScheduleID

	// Workflow, Cron, Location, Seconds, Input and Labels describe the
	// ticks without a Schedule: a declared Workflow.Cron entry, or a
	// schedule that no longer exists. Location is required (time.Local
	// is refused); Labels under a reserved prefix are refused.
	Workflow string
	Cron     string
	Location *time.Location
	Seconds  bool
	Input    []byte
	Labels   map[string]string

	// Parser reads Cron when Seconds is false; nil means cron.Parse. Pass
	// the driver's custom dialect (cron.Extended) when the schedule uses
	// one.
	Parser cron.Parser

	// From and To bound the ticks to [From, To): a tick exactly at From
	// is replayed, one exactly at To is not, so consecutive ranges never
	// replay a tick twice.
	From, To time.Time

	// Max caps the ticks; zero means DefaultBackfillMax. A range holding
	// more is refused whole (ErrInvalid), never cut short silently.
	Max int
}

// backfillTarget is a BackfillSpec resolved to one shape.
type backfillTarget struct {
	workflow, cron, name string
	loc                  *time.Location
	seconds              bool
	input                []byte
	labels               map[string]string
	tenant               string
	active               Active
	windows              Windows
	scope                string
}

func (spec BackfillSpec) target() (backfillTarget, error) {
	var t backfillTarget
	if s := spec.Schedule; s != nil {
		if spec.Workflow != "" || spec.Cron != "" || spec.Location != nil || spec.Seconds || spec.Input != nil || spec.Labels != nil {
			return t, invalidf("backfill: set Schedule or Workflow/Cron/Location/Seconds/Input/Labels, not both")
		}
		if err := ValidateSchedule(*s); err != nil {
			return t, fmt.Errorf("backfill: %w", err)
		}
		t = backfillTarget{workflow: s.Workflow, cron: s.Cron, name: s.Name, loc: s.Location, seconds: s.Seconds,
			input: s.Input, labels: s.Labels, tenant: s.Tenant, active: s.Active, windows: s.Windows}
	} else {
		if err := ValidateSchedule(Schedule{Workflow: spec.Workflow, Cron: spec.Cron, Location: spec.Location, Labels: spec.Labels}); err != nil {
			return t, fmt.Errorf("backfill: %w", err)
		}
		t = backfillTarget{workflow: spec.Workflow, cron: spec.Cron, loc: spec.Location, seconds: spec.Seconds,
			input: spec.Input, labels: spec.Labels}
	}
	if t.name == "" {
		t.name = t.cron // as for a declared cron
	}
	switch {
	case !spec.ScheduleID.IsZero():
		t.scope = "id\x00" + string(spec.ScheduleID)
	case spec.Schedule != nil && spec.Schedule.Name != "":
		t.scope = "name\x00" + t.workflow + "\x00" + t.name
	default:
		t.scope = fmt.Sprintf("cron\x00%s\x00%s\x00%s\x00%t", t.workflow, t.cron, t.loc, t.seconds)
	}
	return t, nil
}

// BackfillPlan returns the ticks Backfill would start, ascending, without
// enqueuing anything: the expression's fire times in [From, To) in the
// schedule's zone (DST rules as for every schedule), minus those outside
// the Schedule's Active period or Windows. It refuses (ErrInvalid) a bad
// spec, an empty or inverted range, a negative Max, and a range holding
// more than Max ticks.
func BackfillPlan(spec BackfillSpec) ([]time.Time, error) {
	t, err := spec.target()
	if err != nil {
		return nil, err
	}
	return planTicks(spec, t)
}

func planTicks(spec BackfillSpec, t backfillTarget) ([]time.Time, error) {
	if spec.From.IsZero() || spec.To.IsZero() || !spec.To.After(spec.From) {
		return nil, invalidf("backfill for %q: want From before To, both set", t.workflow)
	}
	limit := spec.Max
	switch {
	case limit < 0:
		return nil, invalidf("backfill for %q: Max is negative", t.workflow)
	case limit == 0:
		limit = DefaultBackfillMax
	}
	sched, err := ParseCron(t.cron, t.seconds, spec.Parser)
	if err != nil {
		return nil, fmt.Errorf("%w: backfill for %q: %w", ErrInvalid, t.workflow, err)
	}
	var out []time.Time
	scanned := 0
	for at := sched.Next(spec.From.Add(-time.Nanosecond), t.loc); !at.IsZero() && at.Before(spec.To); at = sched.Next(at, t.loc) {
		if scanned++; scanned > maxBackfillScan {
			return nil, invalidf("backfill for %q: more than %d ticks to scan in [%s, %s); split the range", t.workflow, maxBackfillScan, spec.From.Format(time.RFC3339), spec.To.Format(time.RFC3339))
		}
		if !t.active.Contains(at) || !t.windows.Allows(at, t.loc) {
			continue
		}
		if len(out) == limit {
			return nil, invalidf("backfill for %q: more than %d ticks in [%s, %s); raise Max or split the range", t.workflow, limit, spec.From.Format(time.RFC3339), spec.To.Format(time.RFC3339))
		}
		out = append(out, at)
	}
	return out, nil
}

// BackfillOption tunes Backfill.
type BackfillOption func(*backfillOptions)

type backfillOptions struct{ allowDuplicates bool }

// BackfillAllowDuplicates lets Backfill run on a client without
// CapIdempotencyKey, where nothing stops a repeated or concurrent
// backfill from starting a tick twice (the run-key lookup still applies
// on a client with CapRunQuery). Use it only for idempotent jobs.
func BackfillAllowDuplicates() BackfillOption {
	return func(o *backfillOptions) { o.allowDuplicates = true }
}

// Backfill starts one run per tick of BackfillPlan, oldest first: the
// schedule's workflow with its Input, Tenant and labels, plus the fired
// labels of that tick (FiredLabelsAt: LabelSchedule when ScheduleID is
// set, LabelScheduleName, and LabelTickAt, so the step reads the interval
// it is for with StepInfo.TickAt), LabelTrigger set to
// LabelTriggerBackfill, and LabelRunKey. The runs start now; their tick
// is data, not a start time.
//
// Each run's Job.IdempotencyKey (and LabelRunKey) is derived from the
// schedule (its ScheduleID, else its Workflow and Name, else Workflow,
// Cron, Location and Seconds) and the tick, so a repeated or concurrent
// Backfill of an overlapping range starts nothing twice: the key folds an
// enqueue into an unfinished run, and on a client with CapRunQuery a run
// already finished is found by LabelRunKey among the runs the driver
// still keeps (its retention decides how long). Ticks the scheduler
// itself fired carry no run key and are not detected: pass a range the
// scheduler missed. It needs CapIdempotencyKey and refuses without it
// (ErrUnsupported) unless BackfillAllowDuplicates. It runs on every
// driver with those capabilities: memory, sqlstore, redis, Hatchet.
//
// It returns the RunRef of every tick in order, the existing run's for a
// tick already started. On an enqueue error it stops and returns the refs
// so far with the error, so a retry of the same call resumes where it
// stopped.
func Backfill(ctx context.Context, c Client, spec BackfillSpec, opts ...BackfillOption) ([]RunRef, error) {
	var o backfillOptions
	for _, opt := range opts {
		opt(&o)
	}
	t, err := spec.target()
	if err != nil {
		return nil, err
	}
	keyed := c.Capabilities().Has(CapIdempotencyKey)
	if !keyed && !o.allowDuplicates {
		return nil, &UnsupportedError{Field: "Backfill", Missing: CapIdempotencyKey, Reason: "without idempotency keys a repeated backfill would start ticks twice; pass BackfillAllowDuplicates to accept that"}
	}
	ticks, err := planTicks(spec, t)
	if err != nil {
		return nil, err
	}
	ctx = AllowReservedLabels(ctx, LabelSchedule, LabelScheduleName, LabelTickAt, LabelTrigger, LabelRunKey)
	refs := make([]RunRef, 0, len(ticks))
	for _, at := range ticks {
		key := scheduleRunKey("backfill", t.scope, at.UTC().Format(time.RFC3339Nano))
		labels := FiredLabelsAt(t.labels, spec.ScheduleID, t.name, at)
		if spec.ScheduleID.IsZero() {
			delete(labels, LabelSchedule)
		}
		labels[LabelTrigger] = LabelTriggerBackfill
		labels[LabelRunKey] = key
		job := Job{Workflow: t.workflow, Payload: t.input, Labels: labels, Tenant: t.tenant}
		if keyed {
			job.IdempotencyKey = key
		}
		ref, err := enqueueUnlessKeyed(ctx, c, job, key)
		if err != nil {
			return refs, fmt.Errorf("backfill for %q: tick %s: %w", t.workflow, at.Format(time.RFC3339), err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// scheduleRunKey is "<kind>/<16 hex digits of FNV-1a of scope>/<suffix>": short
// enough for every driver's key column whatever the workflow name and
// expression, stable across processes.
func scheduleRunKey(kind, scope, suffix string) string {
	h := fnv.New64a()
	h.Write([]byte(scope))
	return fmt.Sprintf("%s/%016x/%s", kind, h.Sum64(), suffix)
}

// enqueueUnlessKeyed enqueues job unless a run carrying LabelRunKey key is still
// known to the client (CapRunQuery), in which case it returns that run.
func enqueueUnlessKeyed(ctx context.Context, c Client, job Job, key string) (RunRef, error) {
	if q, ok := c.(RunQuerier); ok && c.Capabilities().Has(CapRunQuery) {
		page, err := q.Runs(ctx, RunQuery{Workflow: job.Workflow, Labels: map[string]string{LabelRunKey: key}, Limit: 1})
		if err != nil {
			return RunRef{}, err
		}
		if len(page.Runs) > 0 {
			return page.Runs[0].Ref, nil
		}
	}
	return c.Enqueue(ctx, job)
}
