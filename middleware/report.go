package middleware

import (
	"context"
	"errors"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// ReportConfig configures Report. The zero value reports final failures
// only.
type ReportConfig struct {
	// Attempts also reports every failed step attempt (Failure.Final
	// false), from step middleware on the worker that ran it. The last
	// attempt of a failing run is then reported twice: once as an
	// attempt, once as the run's end.
	Attempts bool

	// Discarded also reports runs a step discarded.
	Discarded bool

	// Clock stamps Failure.At. Nil means clock.Real.
	Clock clock.Clock
}

// ReportFailures returns a run-finished observer that hands every run that
// ends failed (and discarded, when cfg.Discarded) to r.
func ReportFailures(r workflow.Reporter, cfg ReportConfig) workflow.RunFinishedFunc {
	clk := reportClock(cfg)
	return func(ctx context.Context, ev workflow.RunEvent) {
		if f, ok := workflow.FailureOf(ev, cfg.Discarded, clk.Now()); ok {
			r.Report(ctx, f)
		}
	}
}

// ReportAttempts returns step middleware that hands every failed attempt
// to r as a Failure with Final false and Status StatusRetrying, then
// returns the step's result unchanged. A released step (ErrReleased) is
// not a failure and is not reported.
func ReportAttempts(r workflow.Reporter, cfg ReportConfig) workflow.Middleware {
	clk := reportClock(cfg)
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			out, err := next(ctx, in)
			if err == nil || errors.Is(err, ErrReleased) {
				return out, err
			}
			info, _ := workflow.StepInfoFrom(ctx)
			r.Report(ctx, workflow.Failure{
				Ref:      workflow.RunRef{RunID: info.RunID},
				Workflow: info.Workflow,
				Status:   workflow.StatusRetrying,
				Step:     info.Step,
				Attempt:  info.Attempt,
				Err:      err,
				Labels:   info.Labels,
				At:       clk.Now(),
			})
			return out, err
		}
	}
}

// Report bundles ReportFailures (and ReportAttempts when cfg.Attempts) as
// a plugin named "report" for workflow.Use.
func Report(r workflow.Reporter, cfg ReportConfig) workflow.Plugin {
	p := observerPlugin{name: "report", fn: ReportFailures(r, cfg)}
	if cfg.Attempts {
		p.mw = []workflow.Middleware{ReportAttempts(r, cfg)}
		return stepObserverPlugin{p}
	}
	return p
}

// stepObserverPlugin is an observerPlugin that also contributes step
// middleware; a separate type so a plugin without middleware does not
// implement StepMiddlewareProvider with an empty list.
type stepObserverPlugin struct{ observerPlugin }

func (p stepObserverPlugin) StepMiddleware() []workflow.Middleware { return p.mw }

func reportClock(cfg ReportConfig) clock.Clock {
	if cfg.Clock == nil {
		return clock.Real{}
	}
	return cfg.Clock
}
