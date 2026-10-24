package workflow

import (
	"context"
	"math"
	"time"
)

// MaxProgressData bounds Progress.Data, so a step reporting progress in a
// loop cannot grow a run's stored record without limit.
const MaxProgressData = 64 << 10

// Progress is how far a run has got, as a step reported it from inside
// (ReportProgress): a percentage, a message for a person, and optional
// app data (rows exported so far, a cursor). The latest report wins.
type Progress struct {
	// Percent is from 0 to 100.
	Percent float64

	// Message is a short human-readable note ("exported 4,000 of 10,000
	// orders").
	Message string

	// Data is optional app data, at most MaxProgressData bytes.
	Data []byte

	// Step is the step that reported it; the driver fills it.
	Step string

	// At is when it was reported, on the driver's clock; the driver
	// fills it.
	At time.Time
}

// Clone returns a copy whose Data is its own.
func (p Progress) Clone() Progress {
	if p.Data != nil {
		p.Data = append([]byte(nil), p.Data...)
	}
	return p
}

// ValidateProgress checks p: Percent within [0, 100] and Data within
// MaxProgressData. Errors wrap ErrInvalid.
func ValidateProgress(p Progress) error {
	if math.IsNaN(p.Percent) || p.Percent < 0 || p.Percent > 100 {
		return invalidf("progress: Percent %v is outside [0, 100]", p.Percent)
	}
	if len(p.Data) > MaxProgressData {
		return invalidf("progress: Data is %d bytes, more than %d", len(p.Data), MaxProgressData)
	}
	return nil
}

// ProgressReporter stores a running step's progress with its run. A driver
// that keeps progress sets one on the step's ctx (WithProgressReporter);
// it fills Step and At, keeps the latest report as RunInfo.Progress, and
// (with a LifecycleNotifier) emits LifecycleProgress.
type ProgressReporter interface {
	ReportProgress(ctx context.Context, p Progress) error
}

// ProgressFunc adapts a function to ProgressReporter.
type ProgressFunc func(ctx context.Context, p Progress) error

// ReportProgress calls f.
func (f ProgressFunc) ReportProgress(ctx context.Context, p Progress) error { return f(ctx, p) }

type progressKey struct{}

// WithProgressReporter returns a ctx carrying r. Drivers call it.
func WithProgressReporter(ctx context.Context, r ProgressReporter) context.Context {
	return context.WithValue(ctx, progressKey{}, r)
}

// ReportProgress records p as the run's progress from inside a step, so
// an app (an export page) can show how far a long job has got: read it
// back with RunInfo.Progress (RunQuerier) or the LifecycleProgress event.
// It validates p first (ErrInvalid). Outside a step, or on a driver that
// keeps no progress, it returns an UnsupportedError: unlike Heartbeat, the
// caller asked for data to be kept, so silently dropping it would be a
// lie. A handler that does not care can ignore the error.
func ReportProgress(ctx context.Context, p Progress) error {
	if err := ValidateProgress(p); err != nil {
		return err
	}
	r, ok := ctx.Value(progressKey{}).(ProgressReporter)
	if !ok {
		return Unsupported("ReportProgress", "the driver keeps no progress, or ctx is not a step's")
	}
	return r.ReportProgress(ctx, p)
}
