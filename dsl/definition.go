package dsl

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// Definition is a workflow written as data: the JSON form Parse reads,
// Marshal writes and Build turns into a workflow.Workflow. Every field maps
// to the Workflow field of the same meaning; what is code in Go (a step's
// Run, OnFailure, a concurrency key function) is a name here, resolved
// against a Registry by Build. Durations are Go duration strings such as
// "30s" or "1h30m"; an empty string means the field is not set (zero), as
// in Go.
//
// Fields that only Go can express (ScheduleWhen, RetryPolicy.NoRetryOn and
// DelayFor, KeyExpr, OnTimeout, rate limits) have no form here: a workflow
// that needs them is written in Go, or built from a Definition and then
// edited in Go before Register.
//
//ds:def id=dsl-definition-m3et9n3c owner=@khanakia stability=stable
type Definition struct {
	// Name is Workflow.Name: the key Job.Workflow refers to.
	Name string `json:"name"`

	// Version is Workflow.Version, the version of this definition for
	// rolling deploys. Zero is unversioned. Needs CapWorkflowVersion when
	// set.
	Version int `json:"version,omitempty"`

	// MinVersion is Workflow.MinVersion, the oldest run version this
	// definition still runs.
	MinVersion int `json:"min_version,omitempty"`

	// Queue is Workflow.Queue; empty means workflow.DefaultQueue.
	Queue string `json:"queue,omitempty"`

	// Timeout is Workflow.Timeout, the default per-attempt timeout of
	// every step, as a duration string.
	Timeout string `json:"timeout,omitempty"`

	// RunTimeout is Workflow.RunTimeout, the cap on a run's whole life, as
	// a duration string. Needs CapRunTimeout when set.
	RunTimeout string `json:"run_timeout,omitempty"`

	// Retry is Workflow.Retry, the default policy of steps without their
	// own. Absent means no retries.
	Retry *RetryDef `json:"retry,omitempty"`

	// Concurrency is Workflow.Concurrency. Absent means no cap.
	Concurrency *ConcurrencyDef `json:"concurrency,omitempty"`

	// Steps are the workflow's steps, a DAG through After, in the order
	// Workflow.Steps keeps them.
	Steps []StepDef `json:"steps"`

	// OnFailure names the registered handler that runs once when a run
	// fails for good (Workflow.OnFailure). Empty means none.
	OnFailure string `json:"on_failure,omitempty"`

	// Cron is Workflow.Cron: expressions declared with the workflow. The
	// driver's cron parser checks them at Register, not Parse, so a driver
	// with a custom dialect reads its own syntax.
	Cron []string `json:"cron,omitempty"`

	// CronLocation is the IANA zone Cron is evaluated in
	// (Workflow.CronLocation), loaded with cron.LoadZone. Empty means
	// UTC; "Local" is refused. Needs CapCronZone for any zone but UTC.
	CronLocation string `json:"cron_location,omitempty"`
}

// StepDef is one step of a Definition. Exactly one of Handler, Wait and
// Sleep is set, as exactly one of Step.Run, Step.Wait and Step.Sleep is.
//
//ds:def id=dsl-stepdef-uta5kgvn owner=@khanakia stability=stable
type StepDef struct {
	// Name is Step.Name, unique within the definition.
	Name string `json:"name"`

	// After names the parent steps (Step.After).
	After []string `json:"after,omitempty"`

	// Handler names the registered workflow.Handler the step runs
	// (Step.Run).
	Handler string `json:"handler,omitempty"`

	// Retry is Step.Retry. Absent means the definition's Retry; {"max": 0}
	// means never retry, as workflow.RetriesNone does.
	Retry *RetryDef `json:"retry,omitempty"`

	// Timeout is Step.Timeout, as a duration string.
	Timeout string `json:"timeout,omitempty"`

	// Sleep makes this a durable sleep step (Step.Sleep), as a positive
	// duration string. Needs CapDurableSleep.
	Sleep string `json:"sleep,omitempty"`

	// Wait makes this a wait step (Step.Wait). Needs CapWaitForEvent.
	Wait *WaitDef `json:"wait,omitempty"`
}

// WaitDef is a StepDef's wait (workflow.WaitSpec). At most one of Key and
// KeyLabel is set.
//
//ds:def id=dsl-waitdef-4pcz226d owner=@khanakia stability=stable
type WaitDef struct {
	// Event is the event name to wait for.
	Event string `json:"event"`

	// Key is a fixed key, the same for every run (WaitSpec.Key).
	Key string `json:"key,omitempty"`

	// KeyLabel names the job label whose value is the run's key: Build
	// turns it into WaitSpec.KeyFrom with a workflow.KeyFunc reading
	// Job.Labels[KeyLabel], so every run waits for its own event ("payment
	// for THIS order"). A run without the label waits on the empty key.
	KeyLabel string `json:"key_label,omitempty"`

	// Timeout is WaitSpec.Timeout, how long the wait lasts from when it
	// begins, as a duration string. Empty means wait forever.
	Timeout string `json:"timeout,omitempty"`
}

// RetryDef is a workflow.RetryPolicy with its durations as strings. The
// core's RetryPolicy encodes durations as integer nanoseconds, which no
// person writes by hand, so a definition has its own shape.
//
//ds:def id=dsl-retrydef-v8rg24uk owner=@khanakia stability=stable
type RetryDef struct {
	// Max is RetryPolicy.Max: retries after the first attempt.
	Max int `json:"max"`

	// Backoff is RetryPolicy.Backoff. Absent means retry at once.
	Backoff *BackoffDef `json:"backoff,omitempty"`

	// Until is RetryPolicy.Until, as a duration string. Needs
	// CapRetryUntil when set.
	Until string `json:"until,omitempty"`

	// MaxErrors is RetryPolicy.MaxErrors. Needs CapRetryMaxErrors when
	// set.
	MaxErrors int `json:"max_errors,omitempty"`

	// DiscardOnExhaust is RetryPolicy.DiscardOnExhaust. Needs
	// CapDiscardExhausted when set.
	DiscardOnExhaust bool `json:"discard_on_exhaust,omitempty"`
}

// BackoffDef is a workflow.Backoff with its durations as strings.
//
//ds:def id=dsl-backoffdef-v5y23xnr owner=@khanakia stability=stable
type BackoffDef struct {
	// Initial is the first retry's wait, as a duration string.
	Initial string `json:"initial,omitempty"`

	// Factor multiplies the wait per retry; below 1 counts as 1.
	Factor float64 `json:"factor,omitempty"`

	// Max caps the wait, as a duration string.
	Max string `json:"max,omitempty"`

	// Jitter is Backoff.Jitter, a fraction in [0, 1]. Needs
	// CapRetryJitter when set.
	Jitter float64 `json:"jitter,omitempty"`
}

// ConcurrencyDef is a workflow.Concurrency whose key is read from a job
// label.
//
//ds:def id=dsl-concurrencydef-c824mmdq owner=@khanakia stability=stable
type ConcurrencyDef struct {
	// KeyLabel names the job label whose value is the run's concurrency
	// key: Build turns it into a workflow.KeyFunc reading
	// Job.Labels[KeyLabel]. Empty means one key for every run, so MaxRuns
	// caps the whole workflow.
	KeyLabel string `json:"key_label,omitempty"`

	// MaxRuns is Concurrency.MaxRuns, at least 1.
	MaxRuns int `json:"max_runs"`

	// Strategy is Concurrency.Strategy ("queue_newest", "drop_newest",
	// "cancel_newest", "cancel_in_progress", "round_robin"); empty means
	// queue_newest.
	Strategy string `json:"strategy,omitempty"`

	// FIFO is Concurrency.FIFO. Needs CapKeyFIFO when set.
	FIFO bool `json:"fifo,omitempty"`
}

// FieldError is a definition refused at one place in its JSON: Path names
// it the way the document reads ("steps[2].handler", "retry.backoff.max",
// "" for the whole document) and Reason says what is wrong. It matches
// workflow.ErrInvalid under errors.Is, so a caller that treats every
// refused spec alike needs no type switch, and one that shows the error
// next to a form field reads Path with errors.As.
//
//ds:def id=dsl-fielderror-zst5dca3 owner=@khanakia stability=stable
type FieldError struct {
	// Path is the JSON path of the refused value; empty for the document.
	Path string

	// Reason says what is wrong, without the path.
	Reason string
}

// Error reads "workflow: invalid spec: dsl: <path>: <reason>".
func (e *FieldError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("%v: dsl: %s", workflow.ErrInvalid, e.Reason)
	}
	return fmt.Sprintf("%v: dsl: %s: %s", workflow.ErrInvalid, e.Path, e.Reason)
}

// Unwrap returns workflow.ErrInvalid, so errors.Is matches it.
func (e *FieldError) Unwrap() error { return workflow.ErrInvalid }

// fieldErr builds a FieldError at path.
func fieldErr(path, format string, args ...any) error {
	return &FieldError{Path: path, Reason: fmt.Sprintf(format, args...)}
}

// at joins a parent path and a child name: at("", "name") is "name",
// at("retry", "max") is "retry.max".
func at(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// index is the path of element i of the array at path.
func index(path string, i int) string { return path + "[" + strconv.Itoa(i) + "]" }

// Validate checks what only the data form can get wrong, each refusal a
// *FieldError naming its JSON path: a name, duration strings that parse
// (and a positive sleep), exactly one of handler, wait and sleep per step,
// a wait with an event and at most one of key and key_label, and a
// loadable cron_location. It needs no Registry. Build calls it, then
// resolves handler names, then runs workflow.Validate for everything a Go
// workflow is checked for (step names, After naming existing steps, cycles,
// ranges, strategies), so a definition is refused where the same workflow
// written in Go would be.
//
//ds:def id=dsl-definition-validate-3tczvf3z owner=@khanakia stability=stable
func (d Definition) Validate() error {
	if d.Name == "" {
		return fieldErr("name", "is empty")
	}
	for _, f := range []struct{ path, value string }{{"timeout", d.Timeout}, {"run_timeout", d.RunTimeout}} {
		if _, err := duration(f.path, f.value); err != nil {
			return err
		}
	}
	if d.Retry != nil {
		if _, err := d.Retry.policy("retry"); err != nil {
			return err
		}
	}
	if len(d.Steps) == 0 {
		return fieldErr("steps", "a definition needs at least one step")
	}
	for i, s := range d.Steps {
		if err := s.validate(index("steps", i)); err != nil {
			return err
		}
	}
	if _, err := location(d.CronLocation); err != nil {
		return err
	}
	return nil
}

// validate checks one step at path.
//
//ds:def id=dsl-stepdef-validate-28zs3f25 owner=@khanakia stability=stable
func (s StepDef) validate(path string) error {
	if s.Name == "" {
		return fieldErr(at(path, "name"), "is empty")
	}
	kinds := 0
	if s.Handler != "" {
		kinds++
	}
	if s.Wait != nil {
		kinds++
	}
	if s.Sleep != "" {
		kinds++
	}
	if kinds != 1 {
		return fieldErr(path, "step %q sets %d of handler, wait and sleep; exactly one is needed", s.Name, kinds)
	}
	if _, err := duration(at(path, "timeout"), s.Timeout); err != nil {
		return err
	}
	if s.Sleep != "" {
		d, err := duration(at(path, "sleep"), s.Sleep)
		if err != nil {
			return err
		}
		if d <= 0 {
			return fieldErr(at(path, "sleep"), "%q is not a positive duration", s.Sleep)
		}
	}
	if w := s.Wait; w != nil {
		wp := at(path, "wait")
		if w.Event == "" {
			return fieldErr(at(wp, "event"), "is empty")
		}
		if w.Key != "" && w.KeyLabel != "" {
			return fieldErr(wp, "sets both key and key_label; a wait has a fixed key or one read from a label")
		}
		if _, err := duration(at(wp, "timeout"), w.Timeout); err != nil {
			return err
		}
	}
	if s.Retry != nil {
		if _, err := s.Retry.policy(at(path, "retry")); err != nil {
			return err
		}
	}
	return nil
}

// duration parses a duration string; empty is zero.
//
//ds:def id=dsl-duration-qep8j5u5 owner=@khanakia stability=stable
func duration(path, s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fieldErr(path, "%q is not a Go duration such as \"30s\" or \"1h30m\"", s)
	}
	return d, nil
}

// location loads a cron zone; empty is nil (UTC).
//
//ds:def id=dsl-location-bhcn5hdh owner=@khanakia stability=stable
func location(name string) (*time.Location, error) {
	if name == "" {
		return nil, nil
	}
	loc, err := cron.LoadZone(name)
	if err != nil {
		return nil, fieldErr("cron_location", "%q is not a loadable IANA zone: %v", name, err)
	}
	return loc, nil
}

// policy converts r into a workflow.RetryPolicy, naming path on a bad
// duration. Ranges (a negative Max, Jitter outside [0, 1]) are left to
// workflow.Validate.
func (r RetryDef) policy(path string) (workflow.RetryPolicy, error) {
	p := workflow.RetryPolicy{Max: r.Max, MaxErrors: r.MaxErrors, DiscardOnExhaust: r.DiscardOnExhaust}
	var err error
	if p.Until, err = duration(at(path, "until"), r.Until); err != nil {
		return p, err
	}
	if b := r.Backoff; b != nil {
		bp := at(path, "backoff")
		p.Backoff.Factor, p.Backoff.Jitter = b.Factor, b.Jitter
		if p.Backoff.Initial, err = duration(at(bp, "initial"), b.Initial); err != nil {
			return p, err
		}
		if p.Backoff.Max, err = duration(at(bp, "max"), b.Max); err != nil {
			return p, err
		}
	}
	return p, nil
}

// Marshal writes def as indented JSON, the form Parse reads back to an
// equal Definition, for storing definitions in a file, a DataStore or a
// database row. It does not validate; Validate and Build do.
//
//ds:def id=dsl-marshal-h2h9vs2c owner=@khanakia stability=stable
func Marshal(def Definition) ([]byte, error) {
	b, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: dsl: %w", workflow.ErrInvalid, err) // a NaN factor or jitter
	}
	return b, nil
}
