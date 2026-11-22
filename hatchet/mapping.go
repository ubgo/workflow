package hatchet

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"github.com/hatchet-dev/hatchet/pkg/constants"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// mapping.go holds the pure translation between workflow specs and Hatchet
// SDK values: no connection, no goroutine, so every rule here is unit
// tested without a server. Every spec field either maps to an SDK value
// here or is refused with an UnsupportedError naming it; nothing is
// dropped: a field accepted and ignored is a bug nobody sees.

// Reserved names in the JSON documents the driver sends to Hatchet. Hatchet
// stores run inputs and task outputs as JSON objects, while a Handler deals
// in raw bytes, so the exact bytes travel base64-encoded under these keys.
const (
	// payloadField carries Job.Payload (and Schedule.Input) verbatim. When
	// the payload is a JSON object its top-level fields are copied beside
	// it, so a CEL concurrency key such as input.tenant sees them.
	payloadField = "_wf_payload"
	// outputField carries a step's output bytes.
	outputField = "_wf_out"
	// eventKeyField and eventPayloadField are the event document Signal
	// pushes; a wait step reads the payload (the key is informational,
	// matching uses eventKey).
	eventKeyField     = "key"
	eventPayloadField = "payload"
)

// Metadata keys the driver writes on every run. They share one prefix so a
// label can never shadow them.
const (
	//ds:def id=hatchet-metadataprefix-2ytt6uuf owner=@khanakia stability=stable
	metadataPrefix = "wf__"
	// metaIdempotencyKey is the per-run value the workflow's idempotency
	// expression reads (see idempotencyExpression).
	metaIdempotencyKey = metadataPrefix + "idempotency_key"
	// metaWorkflow names the workflow, so Status can tell which steps of
	// a run are waits and sleeps.
	metaWorkflow = metadataPrefix + "workflow"
	// metaScheduleToken marks the run a delayed start fires, so the run
	// can be found after the engine deletes the scheduled-run record.
	metaScheduleToken = metadataPrefix + "schedule_token"
	// metaEnqueuedAt is when the run was asked for (Unix milliseconds; for
	// a delayed start, its RunAt). Wait steps consider events from then.
	metaEnqueuedAt = metadataPrefix + "enqueued_at"
	// metaQueue is the run's queue (workflow.EffectiveQueue), so a run
	// query can filter and report it.
	metaQueue = metadataPrefix + "queue"
	// metaPriority is Job.Priority, which the engine's run listing does
	// not report, so a run query can.
	metaPriority = metadataPrefix + "priority"
	// metaTenant is Job.Tenant (workflow.CapTenant), filtered on the
	// engine by RunQuery.Tenant as an exact metadata match.
	metaTenant = metadataPrefix + "tenant"
	// metaRunAt is when a delayed start is due after Reschedule moved it
	// (Unix milliseconds). metaEnqueuedAt keeps the first RunAt, because
	// the ScheduledID is rebuilt from it and must not change; RunInfo.RunAt
	// reads metaRunAt first.
	metaRunAt = metadataPrefix + "run_at"
	// metaStartAt is when a delayed start with an idempotency key may run
	// its first steps (Unix milliseconds): such a run starts at once, so
	// the engine holds its key, and its start gate sleeps until then.
	metaStartAt = metadataPrefix + "start_at"
)

// startGateTask names the durable task a workflow's delayed variant runs
// before its first steps (declareDelayed). It sleeps on the engine until
// the run's metaStartAt. A step may not take the name (checkSupported).
//
//ds:def id=hatchet-startgatetask-bpsdadyd owner=@khanakia stability=stable
const startGateTask = "wf__start"

// delayedSuffix ends the engine name of a workflow's delayed variant
// (delayedName). A workflow may not take a name that ends with it
// (checkSupported).
//
//ds:def id=hatchet-delayedsuffix-s4kw5cfy owner=@khanakia stability=stable
const delayedSuffix = "__wf_delayed"

// delayedName is the name the delayed variant of workflow name is
// declared under: the workflow's own steps behind a start gate, with its
// idempotency expression, rate limits and on-failure step, and no cron.
func delayedName(name string) string { return name + delayedSuffix }

// hasStartGate reports whether wf gets a delayed variant, which is what
// lets a delayed start carry an idempotency key: the run starts at once as
// a run of the variant (Hatchet's scheduled runs take no key) and the
// variant's start gate sleeps until RunAt. The gate is not on the
// workflow itself because the engine holds a run's key only until the
// first of the tasks created with the run ends (ReleaseIdempotencyKeys in
// the engine runs when a task reaches a terminal state): a gate in every
// run would release every key a moment after the enqueue. A workflow with
// IdempotencyOff never takes a key, so it needs no variant; a workflow
// with a Concurrency gets none, because the engine holds a concurrency
// slot for a whole run and a variant would not share the workflow's slots
// (such a delayed key is refused instead).
//
//ds:def id=hatchet-hasstartgate-jam5t2ur owner=@khanakia stability=stable
func hasStartGate(wf workflow.Workflow) bool {
	return wf.Idempotency != workflow.IdempotencyOff && wf.Concurrency == nil
}

// startAtOf reads metaStartAt from a run's metadata.
func startAtOf(md map[string]string) (time.Time, bool) {
	ms, err := strconv.ParseInt(md[metaStartAt], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// runAtOf is RunInfo.RunAt of a run or scheduled run with metadata md:
// the time Reschedule last set, else the Job.RunAt Enqueue stamped (as
// the start gate's time on a delayed start with a key, or with the
// schedule token on any other), and zero for a run enqueued without
// RunAt.
func runAtOf(md map[string]string) time.Time {
	if ms, err := strconv.ParseInt(md[metaRunAt], 10, 64); err == nil {
		return time.UnixMilli(ms)
	}
	if at, ok := startAtOf(md); ok {
		return at
	}
	if md[metaScheduleToken] == "" {
		return time.Time{}
	}
	at, _ := enqueuedAt(md)
	return at
}

// enqueuedAt reads metaEnqueuedAt from a run's metadata.
func enqueuedAt(md map[string]string) (time.Time, bool) {
	ms, err := strconv.ParseInt(md[metaEnqueuedAt], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// scheduledRef is what a ScheduledID carries: the token stamped on the
// scheduled run and on the run it fires, and RunAt, which bounds the
// search for that run. It holds no engine ID, so the ScheduledID of a
// fired run can be rebuilt from the run's own metadata (RunEvent.Ref).
type scheduledRef struct {
	token string
	runAt time.Time
}

// scheduledIDSep joins the parts of a ScheduledID; it appears in neither
// (a UUID and a decimal number).
const scheduledIDSep = "~"

// id encodes r as an opaque ScheduledID.
func (r scheduledRef) id() workflow.ScheduledID {
	return workflow.ScheduledID(r.token + scheduledIDSep + strconv.FormatInt(r.runAt.UnixMilli(), 10))
}

// parseScheduledID decodes a ScheduledID made by scheduledRef.id.
func parseScheduledID(id workflow.ScheduledID) (scheduledRef, bool) {
	parts := strings.Split(string(id), scheduledIDSep)
	if len(parts) != 2 || parts[0] == "" {
		return scheduledRef{}, false
	}
	ms, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return scheduledRef{}, false
	}
	return scheduledRef{token: parts[0], runAt: time.UnixMilli(ms)}, true
}

// scheduledIDFromMetadata rebuilds the ScheduledID of a run a delayed
// start fired, from the token and RunAt stamped on it; "" for any other
// run.
//
//ds:def id=hatchet-scheduledidfrommetadata-qs7dg9ac owner=@khanakia stability=stable
func scheduledIDFromMetadata(md map[string]string) workflow.ScheduledID {
	token := md[metaScheduleToken]
	at, ok := enqueuedAt(md)
	if token == "" || !ok {
		return ""
	}
	return scheduledRef{token: token, runAt: at}.id()
}

// suspendedOn reports whether a live run of wf is parked on a wait or
// sleep step. active names the steps whose tasks are queued or running,
// unstarted the steps with no task yet. A wait step is a durable task
// that stays queued or running while it waits, and a sleep step is a task
// held back by a sleep condition, so the run is parked when every active
// task is a wait step and at least one wait is active or one wait or sleep
// step has not started.
func suspendedOn(wf workflow.Workflow, active []string, unstarted map[string]bool) bool {
	kinds := make(map[string]workflow.StepKind, len(wf.Steps))
	for _, s := range wf.Steps {
		kinds[s.Name] = s.Kind()
	}
	parked := false
	for _, name := range active {
		if kinds[name] != workflow.StepWait {
			return false
		}
		parked = true
	}
	for name := range unstarted {
		if k := kinds[name]; k == workflow.StepWait || k == workflow.StepSleep {
			parked = true
		}
	}
	return parked
}

// Metadata keys Hatchet itself writes: its own prefix, and the trace
// header its SDK injects on every trigger.
const (
	//ds:def id=hatchet-hatchetmetadataprefix-hsdu59q6 owner=@khanakia stability=stable
	hatchetMetadataPrefix = "hatchet__"
	//ds:def id=hatchet-traceparentkey-646qsgud owner=@khanakia stability=stable
	traceparentKey = "traceparent"
)

// Kinds of idempotency key. Hatchet stores keys per tenant, not per
// workflow, so every key also carries the engine-side workflow name.
const (
	idemKindCaller    = "key"       // Job.IdempotencyKey
	idemKindRun       = "run"       // an enqueue without a key: unique per call
	idemKindCron      = "cron"      // an engine cron fire: unique per cron and fire time
	idemKindEvent     = "event"     // an engine event trigger: unique per event
	idemKindUntracked = "untracked" // a run started outside this driver (dashboard button)
	idemKeySep        = "|"
)

// namespaceSeparator is what the SDK appends to a namespace before
// prefixing it onto every workflow name (pkg/client/loader:
// strings.ToLower(namespace + "_")).
const namespaceSeparator = "_"

// Hatchet's retry backoff is factor^n seconds for retry n, capped at a
// whole number of seconds (internal/services/controllers/task/controller.go
// pubRetryEvent), with factor in [1, 1000] and the cap in [1, 86400]
// (pkg/repository/workflow.go CreateWorkflowStepOpts).
const (
	hatchetMaxBackoffFactor  = 1000
	hatchetMaxBackoffSeconds = 86400
)

// Hatchet run priorities (features.RunPriority): 1 is the engine default
// and the lowest, 3 the highest.
const (
	priorityMedium = 2
	priorityHigh   = 3
)

// rateLimitWindows are the only windows a Hatchet rate limit accepts.
//
//ds:def id=hatchet-ratelimitwindows-5mdddvs7 owner=@khanakia stability=stable
var rateLimitWindows = map[time.Duration]hatchetsdk.RateLimitDuration{
	time.Second:        hatchetsdk.Second,
	time.Minute:        hatchetsdk.Minute,
	time.Hour:          hatchetsdk.Hour,
	24 * time.Hour:     hatchetsdk.Day,
	7 * 24 * time.Hour: hatchetsdk.Week,
}

// engineName is the name the engine knows a workflow by: the SDK's
// namespace prefix plus the workflow name.
//
//ds:def id=hatchet-enginename-q6x5kbtp owner=@khanakia stability=stable
func engineName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return strings.ToLower(namespace+namespaceSeparator) + name
}

// checkSupported returns an UnsupportedError for the first field of wf the
// Hatchet driver cannot honour as written, and nil when it can honour all
// of them. It runs in Start, after the capability check, so the error names
// the exact field rather than a capability.
//
//ds:def id=hatchet-checksupported-733uwgsp owner=@khanakia stability=stable
func checkSupported(wf workflow.Workflow) error {
	if strings.HasSuffix(wf.Name, delayedSuffix) {
		return fmt.Errorf("%w: workflow %q: a name ending in %q is reserved by the Hatchet driver (its delayed variants)", workflow.ErrInvalid, wf.Name, delayedSuffix)
	}
	if wf.ScheduleWhen != nil {
		return unsupported("Workflow.ScheduleWhen", workflow.CapScheduleWhen, "workflow "+wf.Name+": the Hatchet engine fires crons itself and asks no worker code first")
	}
	if !wf.CronWindows.IsZero() {
		return unsupported("Workflow.CronWindows", workflow.CapScheduleWindow, "workflow "+wf.Name+": the Hatchet engine fires every tick of a cron and has no time-of-day window")
	}
	if !workflow.IsUTC(wf.CronLocation) {
		return unsupported("Workflow.CronLocation", workflow.CapCronZone, "workflow "+wf.Name+": Hatchet evaluates crons in UTC only, not "+wf.CronLocation.String()+"; shifting the expression would break across DST")
	}
	if wf.CronOverlap == workflow.OverlapSkip {
		return unsupported("Workflow.CronOverlap", workflow.CapOverlapSkip, "workflow "+wf.Name+": "+overlapReason)
	}
	if !wf.CronActive.IsZero() {
		return unsupported("Workflow.CronActive", workflow.CapScheduleActive, "workflow "+wf.Name+": a Hatchet cron has no start or end, and the engine fires it without asking a worker")
	}
	if wf.CronKeepRuns > 0 {
		return unsupported("Workflow.CronKeepRuns", workflow.CapScheduleKeep, "workflow "+wf.Name+": "+keepRunsReason)
	}
	if wf.RunTimeout > 0 || wf.OnTimeout != nil {
		return unsupported("Workflow.RunTimeout", workflow.CapRunTimeout, "workflow "+wf.Name+": Hatchet times out each task, not a run, and reports a timeout with its own text, so neither a run deadline nor OnTimeout can be honoured")
	}
	if !wf.Misfire.IsZero() {
		return unsupported("Workflow.Misfire", caps.Missing(workflow.Requires(workflow.Workflow{Cron: wf.Cron, Misfire: wf.Misfire})), "workflow "+wf.Name+": "+misfireReason)
	}
	if c := wf.Concurrency; c != nil {
		if c.Key.Func != nil {
			return workflow.Unsupported("Workflow.Concurrency.Key.Func", "workflow "+wf.Name+": Hatchet evaluates concurrency keys on the engine; use workflow.KeyExpr with a CEL expression")
		}
		if _, err := concurrencyStrategy(c.Strategy); err != nil {
			return err
		}
		if c.Weight != nil {
			return unsupported("Workflow.Concurrency.Weight", workflow.CapKeyWeights, "workflow "+wf.Name+": Hatchet's GROUP_ROUND_ROBIN serves keys in equal turns and takes no weight per key")
		}
	}
	for i, r := range wf.RateLimits {
		if _, err := rateLimit(r, wf.Name, i); err != nil {
			return err
		}
	}
	if len(wf.RateLimits) > 0 && len(rootSteps(wf)) != 1 {
		return workflow.Unsupported("Workflow.RateLimits", "workflow "+wf.Name+": Hatchet limits tasks, not runs, so a rate-limited workflow needs exactly one first step")
	}
	for i, expr := range wf.Cron {
		if err := checkCron(expr, wf.CronSeconds); err != nil {
			return fmt.Errorf("workflow %q: Cron[%d]: %w", wf.Name, i, err)
		}
	}
	for _, s := range wf.Steps {
		if s.Name == startGateTask {
			return fmt.Errorf("%w: workflow %q: step name %q is reserved by the Hatchet driver (the start gate of its delayed variant)", workflow.ErrInvalid, wf.Name, startGateTask)
		}
		if s.Wait != nil && s.Wait.KeyFrom.Expr != "" {
			return workflow.Unsupported("WaitSpec.KeyFrom.Expr", "workflow "+wf.Name+" step "+s.Name+": Hatchet has no API that evaluates a CEL expression to a string for the driver; use workflow.KeyFunc")
		}
		if s.HeartbeatTimeout > 0 {
			return unsupported("Step.HeartbeatTimeout", workflow.CapHeartbeat, "workflow "+wf.Name+" step "+s.Name+": "+heartbeatReason)
		}
		if s.Kind() == workflow.StepRun {
			if err := checkRetry(workflow.EffectiveRetries(s, wf)); err != nil {
				return fmt.Errorf("workflow %q step %q: %w", wf.Name, s.Name, err)
			}
		}
	}
	return nil
}

// keepRunsReason is why a per-schedule run history limit is refused.
//
//ds:def id=hatchet-keeprunsreason-ethbbz58 owner=@khanakia stability=stable
const keepRunsReason = "Hatchet keeps runs for the engine's retention and has no run delete, so the driver cannot keep only a schedule's newest runs"

// heartbeatReason is why Step.HeartbeatTimeout is refused. The engine's
// RefreshTimeout adds to a task's single execution deadline in whole
// time units, so it cannot fail an attempt for a missed beat while also
// keeping Step.Timeout, and the engine reports the timeout with its own
// text, not workflow.ErrHeartbeatTimeout.
const heartbeatReason = "Hatchet has one execution deadline per task, extended by RefreshTimeout, so it cannot fail an attempt for a missed heartbeat while keeping Step.Timeout"

// misfireReason is why every explicit misfire policy is refused: the
// engine's own behaviour is the driver default, and no policy can be set.
//
//ds:def id=hatchet-misfirereason-eythccvb owner=@khanakia stability=stable
const misfireReason = "Hatchet never catches up a missed cron tick (no backfill) and has no setting for how late a tick may still fire, so only the driver default is honoured: a tick missed while the engine was down is dropped"

// checkRetry refuses the parts of a retry policy Hatchet cannot express:
// a backoff off its factor^n-seconds formula, jitter (the engine waits
// exactly that long), and DelayFor (the engine sets a retry's delay from
// the task's backoff only, never from the error).
//
//ds:def id=hatchet-checkretry-ccamdfbn owner=@khanakia stability=stable
func checkRetry(p workflow.RetryPolicy) error {
	if p.DelayFor != nil {
		return unsupported("Retry.DelayFor", workflow.CapRetryDelay, "Hatchet sets a retry's delay from the backoff only, never from the error (RetryAfter)")
	}
	if p.Until > 0 {
		return unsupported("Retry.Until", workflow.CapRetryUntil, "Hatchet fixes a task's retry count when the task is declared and has no retry deadline")
	}
	if p.MaxErrors > 0 {
		return unsupported("Retry.MaxErrors", workflow.CapRetryMaxErrors, "Hatchet counts every failed attempt alike and cannot tell a release from an error")
	}
	if p.DiscardOnExhaust {
		return unsupported("Retry.DiscardOnExhaust", workflow.CapDiscardExhausted, "Hatchet has no discarded status: a run out of retries is failed, so a policy that asks for it to be discarded is refused rather than failing the run silently")
	}
	_, _, err := retryBackoff(p.Backoff)
	return err
}

// checkCron accepts a plain five-field expression, or six fields with
// seconds first when seconds is set (Schedule.Seconds,
// Workflow.CronSeconds). The core parser checks the dialect; macros and
// @every are refused because Hatchet's own parser takes plain fields only
// and has no @every. The engine reads a six-field expression with seconds
// first (internal/services/ticker in hatchet v0.110.5), the meaning cron.ParseSeconds gives it, so the flag
// and the field count agree on the engine as in the core.
//
//ds:def id=hatchet-checkcron-s2s37tr5 owner=@khanakia stability=stable
func checkCron(expr string, seconds bool) error {
	if _, err := workflow.ParseCron(expr, seconds, nil); err != nil {
		return fmt.Errorf("%w: %v", workflow.ErrInvalid, err)
	}
	if strings.HasPrefix(strings.TrimSpace(expr), "@") {
		return workflow.Unsupported("Cron", "Hatchet takes plain five-field expressions, not "+strconv.Quote(expr))
	}
	return nil
}

// rootSteps returns the steps that start with the run.
func rootSteps(wf workflow.Workflow) []workflow.Step {
	var roots []workflow.Step
	for _, s := range wf.Steps {
		if len(s.After) == 0 {
			roots = append(roots, s)
		}
	}
	return roots
}

// topoOrder returns wf's steps with every step after all of its parents,
// the order Hatchet's WithParents needs (a parent's *Task must exist
// first). Validate has already rejected cycles and unknown parents.
func topoOrder(steps []workflow.Step) []workflow.Step {
	done := make(map[string]bool, len(steps))
	out := make([]workflow.Step, 0, len(steps))
	for len(out) < len(steps) {
		progressed := false
		for _, s := range steps {
			if done[s.Name] {
				continue
			}
			ready := true
			for _, p := range s.After {
				if !done[p] {
					ready = false
					break
				}
			}
			if ready {
				done[s.Name] = true
				out = append(out, s)
				progressed = true
			}
		}
		if !progressed {
			// Unreachable after Validate; return what is ordered rather
			// than loop forever.
			return out
		}
	}
	return out
}

// concurrencyStrategy maps a strategy onto Hatchet's wire value. Empty
// means ConcurrencyQueueNewest, the core default.
//
//ds:def id=hatchet-concurrencystrategy-hzvwerpw owner=@khanakia stability=stable
func concurrencyStrategy(s workflow.ConcurrencyStrategy) (hatchetsdk.ConcurrencyLimitStrategy, error) {
	switch s {
	case "", workflow.ConcurrencyQueueNewest, workflow.ConcurrencyRoundRobin:
		// GROUP_ROUND_ROBIN queues a run whose key is full and serves keys
		// in turn, which is what "the new run waits" means on Hatchet.
		// Within a key it starts queued runs by insert order, which is
		// Concurrency.FIFO (CapKeyFIFO, TestKeyFIFOLive).
		return hatchetsdk.GroupRoundRobin, nil
	case workflow.ConcurrencyDropNewest, workflow.ConcurrencyCancelNewest:
		// Hatchet records the refused run as cancelled; it never runs.
		return hatchetsdk.CancelNewest, nil
	case workflow.ConcurrencyCancelInProgress:
		return hatchetsdk.CancelInProgress, nil
	}
	return "", workflow.Unsupported("Concurrency.Strategy", "unknown strategy "+strconv.Quote(string(s)))
}

// concurrencyExpression is the CEL expression Hatchet groups runs by. A
// zero key puts every run of the workflow in one group.
func concurrencyExpression(k workflow.ConcurrencyKey) string {
	if k.Expr != "" {
		return k.Expr
	}
	return strconv.Quote("all")
}

// workflowConcurrency builds the workflow-level concurrency option value.
func workflowConcurrency(c workflow.Concurrency) (hatchetsdk.Concurrency, error) {
	strategy, err := concurrencyStrategy(c.Strategy)
	if err != nil {
		return hatchetsdk.Concurrency{}, err
	}
	maxRuns := int32(min(c.MaxRuns, math.MaxInt32)) // #nosec G115 -- clamped above
	return hatchetsdk.Concurrency{
		Expression:    concurrencyExpression(c.Key),
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}, nil
}

// rateLimit maps one RateLimit onto Hatchet. A key expression becomes a
// dynamic limit whose value is the declared Limit; a zero key becomes a
// static limit named after the workflow, which Start upserts.
//
// scope is the workflow's engine name. The engine keeps a dynamic
// limit's buckets per tenant by the key's value alone, so two workflows
// (or two limits of one) whose keys evaluate to the same value would
// share one bucket; the value is prefixed with the declaration's static
// key, so each (workflow, limit) has its own buckets, as the RateLimit
// contract and the other drivers have it.
//
//ds:def id=hatchet-ratelimit-mvwtadc2 owner=@khanakia stability=stable
func rateLimit(r workflow.RateLimit, scope string, i int) (*hatchetsdk.RateLimit, error) {
	field := "Workflow.RateLimits[" + strconv.Itoa(i) + "]"
	window, ok := rateLimitWindows[r.Per]
	if !ok {
		return nil, workflow.Unsupported(field+".Per", "Hatchet rate limits take a window of one second, minute, hour, day or week, not "+r.Per.String())
	}
	if r.Key.Func != nil {
		return nil, workflow.Unsupported(field+".Key.Func", "Hatchet evaluates rate-limit keys on the engine; use workflow.KeyExpr")
	}
	if r.Key.Expr == "" {
		// A keyless limit maps to a static Hatchet limit upserted by the
		// driver; its live test missed the long-run bound by 1.2s
		// (handler-observed starts), so it is refused until proven.
		// workflow.KeyExpr(`"all"`) gives one bucket through the proven
		// dynamic path.
		return nil, workflow.Unsupported(field+".Key", `a keyless Hatchet rate limit is not proven on the engine; use workflow.KeyExpr("\"all\"") for one bucket`)
	}
	units := 1
	// The engine requires Key on every limit; with KeyExpr it names the
	// expression set, and the buckets come from the expression's values.
	static := staticRateLimitKey(scope, i)
	expr, limit := scopedRateLimitExpr(static, r.Key.Expr), strconv.Itoa(r.Limit)
	rl := &hatchetsdk.RateLimit{Key: static, KeyExpr: &expr, LimitValueExpr: &limit, Units: &units, Duration: &window}
	return rl, nil
}

// staticRateLimitKey names a rate-limit declaration: the engine requires a
// Key on every limit, even one keyed by KeyExpr.
func staticRateLimitKey(scope string, i int) string {
	return "wf" + idemKeySep + scope + idemKeySep + strconv.Itoa(i)
}

// scopedRateLimitExpr is the CEL key expression the engine evaluates:
// static and a separator, then the caller's expression's value as a
// string (CEL string() accepts a string or a number), so its buckets are
// this declaration's alone.
//
//ds:def id=hatchet-scopedratelimitexpr-7864cad6 owner=@khanakia stability=stable
func scopedRateLimitExpr(static, expr string) string {
	return strconv.Quote(static+idemKeySep) + " + string(" + expr + ")"
}

// backoffSpec is a Hatchet retry backoff: factor^n seconds, capped.
type backoffSpec struct {
	factor     float32
	maxSeconds int
}

// retryBackoff maps a Backoff onto Hatchet's factor^n-seconds formula. It
// is exact only when Initial equals Factor seconds (Initial·Factor^(n-1) =
// Factor^n), so any other shape is refused rather than silently changed.
// A zero Backoff means "retry at once" and maps to no backoff (ok false).
//
//ds:def id=hatchet-retrybackoff-vrbwvu97 owner=@khanakia stability=stable
func retryBackoff(b workflow.Backoff) (spec backoffSpec, ok bool, err error) {
	if b.Initial <= 0 {
		return backoffSpec{}, false, nil
	}
	f := b.Factor
	if f < 1 {
		f = 1
	}
	const field = "Retry.Backoff"
	if b.Jitter > 0 {
		return backoffSpec{}, false, unsupported(field+".Jitter", workflow.CapRetryJitter, "Hatchet waits exactly Factor^n seconds and has no jitter")
	}
	if f > hatchetMaxBackoffFactor {
		return backoffSpec{}, false, workflow.Unsupported(field+".Factor", "Hatchet allows a factor of at most 1000")
	}
	if want := time.Duration(f * float64(time.Second)); b.Initial != want {
		return backoffSpec{}, false, workflow.Unsupported(field+".Initial", fmt.Sprintf("Hatchet waits Factor^n seconds, so Initial must equal Factor seconds (%s), not %s", want, b.Initial))
	}
	maxSeconds := hatchetMaxBackoffSeconds
	if b.Max > 0 {
		if b.Max%time.Second != 0 || b.Max > hatchetMaxBackoffSeconds*time.Second {
			return backoffSpec{}, false, workflow.Unsupported(field+".Max", "Hatchet caps backoff at a whole number of seconds up to 86400, not "+b.Max.String())
		}
		maxSeconds = int(b.Max / time.Second)
	}
	return backoffSpec{factor: float32(f), maxSeconds: maxSeconds}, true, nil
}

// stepTimeout resolves a step's execution timeout: its own, else the
// workflow default, else fallback. Hatchet always applies a timeout (its
// server default is short), so "none" becomes the driver's fallback.
//
//ds:def id=hatchet-steptimeout-jkmqcsw5 owner=@khanakia stability=stable
func stepTimeout(s workflow.Step, wf workflow.Workflow, fallback time.Duration) time.Duration {
	switch {
	case s.Timeout > 0:
		return s.Timeout
	case wf.Timeout > 0:
		return wf.Timeout
	}
	return fallback
}

// userTimeout is the timeout a run step's attempt is held to by the
// worker: the step's own, else the workflow default, else zero (none;
// the engine still applies the driver's fallback, see stepTimeout).
func userTimeout(s workflow.Step, wf workflow.Workflow) time.Duration {
	switch {
	case s.Timeout > 0:
		return s.Timeout
	case wf.Timeout > 0:
		return wf.Timeout
	}
	return 0
}

// engineTimeoutMargin is how much longer than a run step's own timeout
// the engine's execution timeout is: the worker enforces the step's
// timeout itself (Client.callTimed) so the attempt fails with
// workflow.ErrStepTimeout, and the engine's deadline, which fails the task
// with the engine's own text, only catches a handler that ignores its
// context, or a worker that died. It covers the engine starting its clock
// when it assigns the task, a moment before the handler starts.
//
//ds:def id=hatchet-enginetimeoutmargin-nwx47exf owner=@khanakia stability=stable
const engineTimeoutMargin = 5 * time.Second

// engineTimeout is the execution timeout a run step is declared with: the
// fallback when the step has no timeout of its own (userTimeout zero),
// else its timeout rounded up to whole seconds (the SDK sends whole
// seconds, truncating) plus engineTimeoutMargin.
//
//ds:def id=hatchet-enginetimeout-cuw8khf9 owner=@khanakia stability=stable
func engineTimeout(s workflow.Step, wf workflow.Workflow, fallback time.Duration) time.Duration {
	d := userTimeout(s, wf)
	if d <= 0 {
		return fallback
	}
	return wholeSeconds(d) + engineTimeoutMargin
}

// waitTimeout is the execution timeout a wait step is declared with. The
// engine counts a durable task's wait against that timeout and ends the
// task when it passes (seen live on hatchet-lite v0.110.5), so it is at
// least as long as the wait can last, in whole seconds, plus
// engineTimeoutMargin: the wait's Timeout, or the time left from now (the
// declaration) to its Deadline, which bounds every wait that begins
// later; a wait with neither (forever) gets the fallback,
// Config.StepTimeout. Step.Timeout, else Workflow.Timeout, raises it and
// never lowers it: on the other drivers a wait ends by its event, Timeout
// or Deadline only, and Workflow.Timeout is meant for the run steps. Now
// is the wall clock the engine shares, not Config.Clock: the engine
// compares the Deadline with its own.
//
//ds:def id=hatchet-waittimeout-cwq36qj9 owner=@khanakia stability=stable
func waitTimeout(s workflow.Step, wf workflow.Workflow, fallback time.Duration, now time.Time) time.Duration {
	w := s.Wait
	d := fallback
	switch {
	case w.Timeout > 0:
		d = wholeSeconds(w.Timeout) + engineTimeoutMargin
	case !w.Deadline.IsZero():
		d = wholeSeconds(max(w.Deadline.Sub(now), 0)) + engineTimeoutMargin
	}
	return max(d, wholeSeconds(userTimeout(s, wf)))
}

// wholeSeconds rounds d up to whole seconds: the SDK sends an execution
// timeout in seconds, truncating.
func wholeSeconds(d time.Duration) time.Duration {
	up := d.Truncate(time.Second)
	if up < d {
		up += time.Second
	}
	return up
}

// runPriority maps Job.Priority onto Hatchet's three levels. Zero keeps
// the engine default (its lowest level), 1 is medium, 2 and above high.
// Hatchet has nothing below its default, so a negative priority is
// refused.
//
//ds:def id=hatchet-runpriority-3dcz9qpp owner=@khanakia stability=stable
func runPriority(p int) (*hatchetsdk.RunPriority, error) {
	var v hatchetsdk.RunPriority
	switch {
	case p == 0:
		return nil, nil
	case p < 0:
		return nil, workflow.Unsupported("Job.Priority", "Hatchet has no priority below its default")
	case p == 1:
		v = priorityMedium
	default:
		v = priorityHigh
	}
	return &v, nil
}

// encodeInput builds the run input Hatchet stores for payload: the exact
// bytes under payloadField and, for a JSON object, its top-level fields
// beside them.
//
//ds:def id=hatchet-encodeinput-47cxpm7v owner=@khanakia stability=stable
func encodeInput(payload []byte) (map[string]any, error) {
	in := map[string]any{}
	if len(payload) == 0 {
		return in, nil
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(payload, &obj) == nil {
		if _, clash := obj[payloadField]; clash {
			return nil, workflow.Unsupported("Job.Payload", "top-level key "+payloadField+" is reserved by the Hatchet driver")
		}
		for k, v := range obj {
			in[k] = v
		}
	}
	in[payloadField] = base64.StdEncoding.EncodeToString(payload)
	return in, nil
}

// decodeEnvelope returns the bytes stored under field in a JSON document
// Hatchet handed back (a run input or a task output), or nil when the
// field is absent.
func decodeEnvelope(doc any, field string) ([]byte, error) {
	m, ok := doc.(map[string]any)
	if !ok || m == nil {
		return nil, nil
	}
	raw, ok := m[field]
	if !ok || raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("hatchet driver: %s is %T, not a base64 string", field, raw)
	}
	return base64.StdEncoding.DecodeString(s)
}

// encodeOutput is the document a task returns for out.
func encodeOutput(out []byte) map[string]any {
	if out == nil {
		return map[string]any{}
	}
	return map[string]any{outputField: base64.StdEncoding.EncodeToString(out)}
}

// eventKey is the Hatchet event key (and filter scope) for an event name
// and correlation key. Putting the correlation key in the event key, not
// in a CEL filter on the payload, lets a wait use event look-back, which
// Hatchet scopes by a string: a Signal sent between Status reporting
// "waiting" and the engine registering the durable wait is then still
// seen. The name's length prefix makes the encoding unambiguous whatever
// either part contains.
//
//ds:def id=hatchet-eventkey-p3rh4yc7 owner=@khanakia stability=stable
func eventKey(name, key string) string {
	return "wf:" + strconv.Itoa(len(name)) + ":" + name + ":" + key
}

// sleepTriggerPrefix starts the trigger key of a sleep condition
// (pkg/worker/condition: "sleep:" + duration).
const sleepTriggerPrefix = "sleep:"

// eventTrigger picks the trigger key of the event that resumed a wait
// step from the keys Hatchet reports; ok is false when only the timeout's
// sleep condition fired. The event's key is not assumed to be the bare
// event name, since the SDK may namespace it.
func eventTrigger(keys []string) (string, bool) {
	for _, k := range keys {
		if !strings.HasPrefix(k, sleepTriggerPrefix) {
			return k, true
		}
	}
	return "", false
}

// eventDocument is what Signal pushes for ev.
func eventDocument(ev workflow.Event) map[string]any {
	doc := map[string]any{eventKeyField: ev.Key}
	if ev.Payload != nil {
		doc[eventPayloadField] = base64.StdEncoding.EncodeToString(ev.Payload)
	}
	return doc
}

// runIdempotencyKey is the value Enqueue stamps under metaIdempotencyKey:
// the caller's key scoped to the engine-side workflow name, or a fresh
// value (unique per call) when there is none or the policy is off.
func runIdempotencyKey(scope, key string, policy workflow.IdempotencyPolicy, fresh func() string) string {
	if key != "" && policy != workflow.IdempotencyOff {
		return scope + idemKeySep + idemKindCaller + idemKeySep + key
	}
	return scope + idemKeySep + idemKindRun + idemKeySep + fresh()
}

// idempotencyExpression is the CEL expression Hatchet evaluates on every
// trigger of the workflow known to the engine as scope. It must always
// yield a string: Hatchet drops a run whose expression fails. Precedence:
// the key this driver stamped; an engine cron fire (workflow + cron name +
// fire time); an engine event (workflow + event id); anything else
// (workflow + "untracked", so dashboard-triggered runs cannot overlap).
func idempotencyExpression(scope string) string {
	md := func(k string) string { return "additional_metadata[" + strconv.Quote(k) + "]" }
	has := func(k string) string { return strconv.Quote(k) + " in additional_metadata" }
	prefix := func(kind string) string { return strconv.Quote(scope + idemKeySep + kind + idemKeySep) }
	cronAt := constants.CronScheduledAtKey.String()
	cronName := constants.CronNameKey.String()
	eventID := constants.EventIDKey.String()
	return fmt.Sprintf(
		"%s ? string(%s) : (%s ? %s + string(%s) + %q + (%s ? string(%s) : %q) : (%s ? %s + string(%s) : %s))",
		has(metaIdempotencyKey), md(metaIdempotencyKey),
		has(cronAt), prefix(idemKindCron), md(cronAt), idemKeySep, has(cronName), md(cronName), "",
		has(eventID), prefix(idemKindEvent), md(eventID),
		strconv.Quote(scope+idemKeySep+idemKindUntracked),
	)
}

// runMetadata merges the job's labels with the driver's own keys, refusing
// a label that would shadow one of them.
//
//ds:def id=hatchet-runmetadata-hrnkbdru owner=@khanakia stability=stable
func runMetadata(labels map[string]string, idemKey string) (map[string]string, error) {
	md := make(map[string]string, len(labels)+1)
	for k, v := range labels {
		if isReservedLabel(k) {
			return nil, workflow.Unsupported("Job.Labels", "label "+strconv.Quote(k)+" uses a prefix reserved by the driver or Hatchet")
		}
		md[k] = v
	}
	md[metaIdempotencyKey] = idemKey
	return md, nil
}

// jobLabels recovers Job.Labels from a run's metadata by dropping the
// keys the driver and Hatchet add.
//
// A run the engine's cron fired also gets workflow.LabelTickAt, from the
// tick time the engine stamps on it (hatchet__cron_scheduled_at, the fire
// time its scheduler computed, not when the step started), so every
// fired run carries its logical time (workflow.CapTickTime).
func jobLabels(md map[string]string) map[string]string {
	out := make(map[string]string, len(md))
	for k, v := range md {
		if isReservedLabel(k) {
			continue
		}
		out[k] = v
	}
	if _, set := out[workflow.LabelTickAt]; !set {
		if at, err := time.Parse(time.RFC3339, md[metaCronScheduledAt]); err == nil {
			out[workflow.LabelTickAt] = at.UTC().Format(time.RFC3339Nano)
		}
	}
	return out
}

// isReservedLabel reports whether k belongs to the driver or Hatchet.
// traceparent is the W3C trace header the SDK injects on every trigger.
//
//ds:def id=hatchet-isreservedlabel-s394qze6 owner=@khanakia stability=stable
func isReservedLabel(k string) bool {
	return strings.HasPrefix(k, metadataPrefix) || strings.HasPrefix(k, hatchetMetadataPrefix) || k == traceparentKey
}

// labelsAny converts labels to the map type Hatchet's REST triggers take.
func labelsAny(labels map[string]string) map[string]any {
	out := make(map[string]any, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// runStatus maps a Hatchet run status. An unknown value is an error, never
// a guess: a fallback that reported every run as "running" would blind
// any reconciler reading it.
func runStatus(s rest.V1TaskStatus) (workflow.Status, error) {
	switch s {
	case rest.V1TaskStatusQUEUED:
		return workflow.StatusQueued, nil
	case rest.V1TaskStatusRUNNING:
		return workflow.StatusRunning, nil
	case rest.V1TaskStatusCOMPLETED:
		return workflow.StatusSucceeded, nil
	case rest.V1TaskStatusFAILED:
		return workflow.StatusFailed, nil
	case rest.V1TaskStatusCANCELLED:
		return workflow.StatusCancelled, nil
	}
	return "", fmt.Errorf("hatchet driver: unknown run status %q", s)
}

// checkDelayedKey checks a delayed start that carries an idempotency key
// (key is the key after the workflow's policy, now the wall clock). Such a
// run starts at once, so the engine holds its key, and the workflow's
// start gate sleeps until RunAt (hasStartGate). It is refused, naming
// Job.IdempotencyKey, when the workflow has no gate (it has a
// Concurrency, whose slots its delayed variant would not share), when this client
// did not register the workflow (an enqueue-only client cannot tell
// whether the workers declared a gate), and when RunAt is further ahead
// than maxHold (Config.IdempotencyMaxHold), the longest the engine holds
// a key: the key would lapse before the run starts.
//
//ds:def id=hatchet-checkdelayedkey-8s6en9y6 owner=@khanakia stability=stable
func checkDelayedKey(wf workflow.Workflow, registered bool, key string, runAt, now time.Time, maxHold time.Duration) error {
	if key == "" || runAt.IsZero() {
		return nil
	}
	switch {
	case !registered:
		return workflow.Unsupported("Job.IdempotencyKey", "with Job.RunAt on workflow "+wf.Name+", which this client did not register: a keyed delayed start runs on the workflow's delayed variant, and an enqueue-only client cannot tell whether the workers declared one; register the workflow on this client")
	case wf.Concurrency != nil:
		return workflow.Unsupported("Job.IdempotencyKey", "with Job.RunAt on workflow "+wf.Name+", which has a Concurrency: a keyed delayed start runs at once on a copy of the workflow that sleeps until RunAt, and Hatchet keeps concurrency slots per workflow and holds one for a whole run, so the copy could neither share the workflow's slots nor sleep without holding one")
	case runAt.Sub(now) > maxHold:
		return workflow.Unsupported("Job.IdempotencyKey", fmt.Sprintf("with Job.RunAt more than Config.IdempotencyMaxHold (%s) ahead: the engine holds a key at most that long from the enqueue, so it would lapse before the run starts", maxHold))
	}
	return nil
}

// checkScheduleMisfire refuses any explicit misfire policy (see
// misfireReason).
//
//ds:def id=hatchet-checkschedulemisfire-ucbwxfy2 owner=@khanakia stability=stable
func checkScheduleMisfire(m workflow.Misfire) error {
	if !m.IsZero() {
		return unsupported("Schedule.Misfire", caps.Missing(workflow.RequiresSchedule(workflow.Schedule{Misfire: m, Location: time.UTC})), misfireReason)
	}
	return nil
}

// unsupported is workflow.Unsupported that also names the capability the
// driver lacks, so a caller (and the conformance suite) can match on it.
//
//ds:def id=hatchet-unsupported-z4dbbpq3 owner=@khanakia stability=stable
func unsupported(field string, missing workflow.Caps, reason string) error {
	return &workflow.UnsupportedError{Field: field, Missing: missing, Reason: reason}
}

// checkScheduleZone refuses any zone but UTC: Hatchet evaluates crons in
// UTC only, and shifting the expression would break across DST.
//
//ds:def id=hatchet-checkschedulezone-9z4wzpcs owner=@khanakia stability=stable
func checkScheduleZone(loc *time.Location) error {
	if !workflow.IsUTC(loc) {
		return unsupported("Schedule.Location", workflow.CapCronZone, "Hatchet evaluates crons in UTC only, not "+loc.String())
	}
	return nil
}

// errNotFoundStatus is the SDK's text for a 404: its feature clients turn
// every non-200 into an untyped error naming the status code
// (sdks/go/features/utils.go validateJSON200Response), so the status is
// the only signal available.
const errNotFoundStatus = "got status 404"

// errNoContentStatus is the SDK's text for a 204. The delete endpoints
// answer 204 on success while the SDK accepts only 200, so a 204 is a
// successful delete reported as an error.
const errNoContentStatus = "got status 204"

// isNoContent reports whether err is the SDK's 204 "error".
//
//ds:def id=hatchet-isnocontent-trj95gub owner=@khanakia stability=stable
func isNoContent(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNoContentStatus)
}

// isNotFound reports whether err is the SDK's 404.
func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNotFoundStatus)
}

// minIdempotencyEngineVersion is the first engine with workflow
// idempotency keys (Hatchet CHANGELOG v0.98.9). An older engine accepts the
// config and ignores it, so keyed enqueues would start duplicates while
// looking protected.
const minIdempotencyEngineVersion = "v0.98.9"

// engineVersionAtLeast reports whether version (vX.Y.Z, any suffix after
// the patch ignored) is at least min. An unreadable version is an error,
// never trusted.
func engineVersionAtLeast(version, minVersion string) (bool, error) {
	have, err := parseVersion(version)
	if err != nil {
		return false, err
	}
	want, err := parseVersion(minVersion)
	if err != nil {
		return false, err
	}
	for i := range have {
		if have[i] != want[i] {
			return have[i] > want[i], nil
		}
	}
	return true, nil
}

func parseVersion(v string) ([3]int, error) {
	var out [3]int
	s := strings.TrimPrefix(strings.TrimSpace(v), "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return out, fmt.Errorf("hatchet driver: cannot read engine version %q", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return out, fmt.Errorf("hatchet driver: cannot read engine version %q", v)
		}
		out[i] = n
	}
	return out, nil
}

// nonRetryable reports whether err must skip the remaining retries: it is
// Permanent, a cancellation, or excluded by the policy's NoRetryOn.
func nonRetryable(err error, p workflow.RetryPolicy) bool {
	// Discard ends a run as Permanent does here: without CapDiscard the
	// run fails rather than ending cancelled (workflow.Discard).
	if workflow.IsPermanent(err) || workflow.IsDiscarded(err) || errors.Is(err, workflow.ErrCancelled) {
		return true
	}
	return p.NoRetryOn != nil && p.NoRetryOn(err)
}
