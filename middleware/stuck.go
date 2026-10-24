package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// StuckEvent reports a step attempt that has run longer than expected.
type StuckEvent struct {
	// Info is the attempt (workflow, step, run, attempt, node).
	Info workflow.StepInfo
	// Started is when the attempt started, on the middleware's clock.
	Started time.Time
	// Running is how long it had run when reported.
	Running time.Duration
}

// StuckConfig configures Stuck.
type StuckConfig struct {
	// After is how long an attempt of a step may run before it counts as
	// stuck; zero or less means never. Nil means Default for every step.
	After func(workflow.StepInfo) time.Duration

	// Default is the threshold when After is nil.
	Default time.Duration

	// OnStuck is told once per attempt, on its own goroutine, while the
	// attempt keeps running. Required.
	OnStuck func(ctx context.Context, ev StuckEvent)

	// Clock times the attempts; nil means clock.Real.
	Clock clock.Clock
}

// Stuck reports step attempts that run far longer than expected (a hung
// HTTP call, a lock never released), the way a dashboard flags a job
// stuck for an hour: detection only, the attempt is not touched (a
// Timeout or HeartbeatTimeout ends attempts). It works on every driver,
// since it wraps the step in the process running it. A config without
// OnStuck, or with neither After nor a positive Default, fails every
// step with ErrInvalid rather than watching nothing.
func Stuck(cfg StuckConfig) workflow.Middleware {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if cfg.OnStuck == nil || (cfg.After == nil && cfg.Default <= 0) {
				return nil, workflow.Permanent(fmt.Errorf("%w: Stuck needs OnStuck and After or a positive Default", workflow.ErrInvalid))
			}
			info, _ := workflow.StepInfoFrom(ctx)
			limit := cfg.Default
			if cfg.After != nil {
				limit = cfg.After(info)
			}
			if limit <= 0 {
				return next(ctx, in)
			}
			started := clk.Now()
			tm := clk.NewTimer(limit)
			done := make(chan struct{})
			watched := make(chan struct{})
			go func() {
				defer close(watched)
				select {
				case <-tm.C():
					cfg.OnStuck(context.WithoutCancel(ctx), StuckEvent{Info: info, Started: started, Running: clk.Now().Sub(started)})
				case <-done:
					tm.Stop()
				}
			}()
			defer func() {
				close(done)
				<-watched // OnStuck, if it fired, has returned before the attempt ends
			}()
			return next(ctx, in)
		}
	}
}
