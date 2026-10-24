package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// AttemptOutput is what one step attempt wrote, as an admin screen shows
// it: the text middleware.CaptureOutput captured, how the attempt ended
// and when.
type AttemptOutput struct {
	Step    string
	Attempt int
	// Started and Finished bound the attempt, on the capturing clock.
	Started, Finished time.Time
	// Text is the captured output (middleware.FormatOutput's format).
	Text string
	// Truncated is true when Text was cut to the source's size bound.
	Truncated bool
	// Error is the attempt's error message; empty when it succeeded.
	Error string
}

// OutputSource returns the captured output of one run's attempts, oldest
// first. The library keeps no output itself: CaptureOutput hands each
// attempt's output to a sink the app chooses, so where it is kept (a
// table, an object store, a log service) is the app's decision, and an
// OutputSource is how the admin reads it back. A run with no captured
// output returns an empty slice, not an error.
type OutputSource interface {
	RunOutput(ctx context.Context, run workflow.RunID) ([]AttemptOutput, error)
}

// OutputSourceFunc adapts a function to OutputSource: the glue that
// maps another module's output store (one that does not import admin) to
// AttemptOutput.
type OutputSourceFunc func(ctx context.Context, run workflow.RunID) ([]AttemptOutput, error)

// RunOutput calls f.
func (f OutputSourceFunc) RunOutput(ctx context.Context, run workflow.RunID) ([]AttemptOutput, error) {
	return f(ctx, run)
}

// WithOutputs lets Admin.RunOutput, the run output route and the UI's
// run page read captured output from src. Without it they report that
// no output store is wired. Output can hold whatever a step logged, so
// wiring a source is the opt-in that shows it to admin users.
func WithOutputs(src OutputSource) Option { return func(a *Admin) { a.outputs = src } }

// HasOutputs reports whether an OutputSource is wired (WithOutputs).
func (a *Admin) HasOutputs() bool { return a.outputs != nil }

// errNoOutputs is the refusal of RunOutput without WithOutputs.
var errNoOutputs = &workflow.UnsupportedError{Field: "admin.output", Reason: "no output source is wired (admin.WithOutputs)"}

// RunOutput returns the captured output of the run ref's attempts, oldest
// first. Without WithOutputs it returns an error matching
// workflow.ErrUnsupported. A ref with only a ScheduledID has not run yet
// and has no output.
func (a *Admin) RunOutput(ctx context.Context, ref workflow.RunRef) ([]AttemptOutput, error) {
	if a.outputs == nil {
		return nil, errNoOutputs
	}
	if ref.IsZero() {
		return nil, fmt.Errorf("%w: admin: run ref is empty", workflow.ErrInvalid)
	}
	if ref.RunID.IsZero() {
		return []AttemptOutput{}, nil
	}
	return a.outputs.RunOutput(ctx, ref.RunID)
}

// MemoryOutputLimits bounds a MemoryOutputs. Zero or less in a field
// keeps its default.
type MemoryOutputLimits struct {
	// Runs is how many runs' output is kept; past it the run whose output
	// arrived first is forgotten. Default DefaultOutputRuns.
	Runs int
	// PerRun is how many attempts are kept per run, the newest. Default
	// DefaultOutputPerRun.
	PerRun int
	// Bytes bounds one attempt's Text; past it the text is cut and marked
	// Truncated. Default DefaultOutputBytes.
	Bytes int
}

// The MemoryOutputs defaults.
const (
	DefaultOutputRuns   = 1000
	DefaultOutputPerRun = 20
	DefaultOutputBytes  = 64 << 10
)

// MemoryOutputs keeps captured output in process memory, bounded by
// MemoryOutputLimits. It is both ends: a middleware.OutputSink for
// CaptureOutput and an OutputSource for WithOutputs. It sees only the
// attempts that ran in this process, so it suits one process or a
// development setup; a fleet stores output somewhere shared and writes
// its own OutputSource over it.
type MemoryOutputs struct {
	lim   MemoryOutputLimits
	mu    sync.Mutex
	runs  map[workflow.RunID][]AttemptOutput
	order []workflow.RunID // arrival order of each run's first output, for eviction
}

// NewMemoryOutputs returns an empty MemoryOutputs.
func NewMemoryOutputs(lim MemoryOutputLimits) *MemoryOutputs {
	if lim.Runs <= 0 {
		lim.Runs = DefaultOutputRuns
	}
	if lim.PerRun <= 0 {
		lim.PerRun = DefaultOutputPerRun
	}
	if lim.Bytes <= 0 {
		lim.Bytes = DefaultOutputBytes
	}
	return &MemoryOutputs{lim: lim, runs: map[workflow.RunID][]AttemptOutput{}}
}

// errNoStepInfo is Deliver's error for output captured outside a driver,
// which names no run to file it under.
var errNoStepInfo = errors.New("admin: output has no StepInfo (captured outside a driver)")

// Deliver stores one attempt's output under its run. Output without
// StepInfo names no run and is refused.
func (m *MemoryOutputs) Deliver(_ context.Context, out middleware.Output) error {
	if !out.HasInfo || out.Info.RunID.IsZero() {
		return errNoStepInfo
	}
	var b strings.Builder
	if err := middleware.FormatOutput(&b, out); err != nil {
		return err
	}
	rec := AttemptOutput{Step: out.Info.Step, Attempt: out.Info.Attempt, Started: out.Started, Finished: out.Finished, Text: b.String()}
	if len(rec.Text) > m.lim.Bytes {
		rec.Text, rec.Truncated = rec.Text[:m.lim.Bytes], true
	}
	if out.Err != nil {
		rec.Error = out.Err.Error()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := out.Info.RunID
	hist, seen := m.runs[id]
	if !seen {
		m.order = append(m.order, id)
		for len(m.order) > m.lim.Runs {
			delete(m.runs, m.order[0])
			m.order = m.order[1:]
		}
	}
	hist = append(hist, rec)
	if len(hist) > m.lim.PerRun {
		hist = hist[len(hist)-m.lim.PerRun:]
	}
	m.runs[id] = hist
	return nil
}

// RunOutput returns a copy of the run's stored attempts, oldest first.
func (m *MemoryOutputs) RunOutput(_ context.Context, run workflow.RunID) ([]AttemptOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AttemptOutput{}, m.runs[run]...), nil
}
