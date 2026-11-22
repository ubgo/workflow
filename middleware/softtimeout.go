package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// SoftTimeoutConfig configures SoftTimeout.
//
//ds:def id=mw-softtimeoutconfig-dyuryky7 owner=@khanakia stability=stable
type SoftTimeoutConfig struct {
	// For returns how long an attempt of a step runs before its soft
	// deadline; zero or less means none for that step. Nil means After
	// for every step. Set it below the step's Timeout: drivers time the
	// hard timeout on their own clock, not as a ctx deadline, so the
	// middleware cannot read it and a soft deadline past it never fires.
	For func(workflow.StepInfo) time.Duration

	// After is the soft deadline when For is nil.
	After time.Duration

	// OnSoft, when set, is told once per attempt that passes its soft
	// deadline, on the watching goroutine, while the attempt keeps
	// running (to log or count it).
	OnSoft func(ctx context.Context, info workflow.StepInfo)

	// Clock times the attempt; nil means clock.Real.
	Clock clock.Clock
}

// softKey is the ctx key of the channel SoftDone returns.
type softKey struct{}

// SoftTimeout gives each step attempt a soft deadline before its hard one
// (RL-10): when it passes, the channel SoftDone(ctx) returns is closed, so
// a long handler can notice, save a checkpoint (Memo, SetRunData) and
// return RetryAfter or Snooze before the hard timeout kills its ctx and
// throws its partial work away. The attempt is not touched: a handler that
// ignores the soft deadline runs on until its hard deadline, as without
// the middleware. Stuck reports long attempts from outside; SoftTimeout
// tells the attempt itself.
//
// It wraps the step in the process running it, so it works on every
// driver. A config with neither For nor a positive After fails every step
// with a Permanent error wrapping ErrInvalid rather than watching nothing.
//
//ds:def id=mw-softtimeout-637qt26k owner=@khanakia stability=stable
func SoftTimeout(cfg SoftTimeoutConfig) workflow.Middleware {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if cfg.For == nil && cfg.After <= 0 {
				return nil, workflow.Permanent(fmt.Errorf("%w: SoftTimeout needs For or a positive After", workflow.ErrInvalid))
			}
			wait := cfg.After
			if cfg.For != nil {
				info, _ := workflow.StepInfoFrom(ctx)
				wait = cfg.For(info)
			}
			if wait <= 0 {
				return next(ctx, in)
			}
			soft := make(chan struct{})
			ctx = context.WithValue(ctx, softKey{}, (<-chan struct{})(soft))
			tm := clk.NewTimer(wait)
			done := make(chan struct{})
			watched := make(chan struct{})
			go func() {
				defer close(watched)
				select {
				case <-tm.C():
					close(soft)
					if cfg.OnSoft != nil {
						info, _ := workflow.StepInfoFrom(ctx)
						cfg.OnSoft(context.WithoutCancel(ctx), info)
					}
				case <-done:
					tm.Stop()
				}
			}()
			defer func() {
				close(done)
				<-watched
			}()
			return next(ctx, in)
		}
	}
}

// SoftDone returns the channel SoftTimeout closes when the attempt passes
// its soft deadline. Outside SoftTimeout (or for an attempt that got no
// soft deadline) it returns nil, which blocks for ever in a select, so a
// handler can select on it unconditionally.
//
//ds:def id=mw-softdone-2jx34jfy owner=@khanakia stability=stable
func SoftDone(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(softKey{}).(<-chan struct{})
	return ch
}

// SoftExpired reports whether the attempt has passed its soft deadline,
// for a handler that checks between units of work rather than selecting.
//
//ds:def id=mw-softexpired-aew5dkmh owner=@khanakia stability=stable
func SoftExpired(ctx context.Context) bool {
	ch := SoftDone(ctx)
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
