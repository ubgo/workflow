package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// ErrPanic is wrapped by the error Recover returns for a panicking step.
var ErrPanic = errors.New("workflow/middleware: step panicked")

// StepPhase names the line Logging is about to write.
type StepPhase string

// The phases of one step attempt, as Logging reports them to a LogLevel
// function.
const (
	StepStarted  StepPhase = "started"
	StepFinished StepPhase = "finished"
	StepFailed   StepPhase = "failed"
)

// StepLogEvent is what a LogLevel function sees for each line Logging
// writes. Err is set only for StepFailed. Info is the driver's StepInfo
// and HasInfo is false when ctx carried none.
type StepLogEvent struct {
	Phase   StepPhase
	Err     error
	Info    workflow.StepInfo
	HasInfo bool
}

// LoggingOption configures Logging.
type LoggingOption func(*loggingConfig)

type loggingConfig struct {
	level func(context.Context, StepLogEvent) slog.Level
}

// defaultLogLevel is Logging's level when no WithLogLevel is given: Debug
// for start and finish, Error for every failed attempt.
func defaultLogLevel(_ context.Context, ev StepLogEvent) slog.Level {
	if ev.Phase == StepFailed {
		return slog.LevelError
	}
	return slog.LevelDebug
}

// WithLogLevel picks the level of each line Logging writes. The driver's
// retry policy is not visible to middleware, so a function that logs
// retried attempts at Warn and only the last one at Error compares
// ev.Info.Attempt with the attempts the app configured:
//
//	middleware.Logging(l, nil, middleware.WithLogLevel(func(_ context.Context, ev middleware.StepLogEvent) slog.Level {
//		switch {
//		case ev.Phase != middleware.StepFailed:
//			return slog.LevelDebug
//		case workflow.IsPermanent(ev.Err) || ev.Info.Attempt >= maxAttempts:
//			return slog.LevelError
//		default:
//			return slog.LevelWarn
//		}
//	}))
//
// A nil fn keeps the default.
func WithLogLevel(fn func(ctx context.Context, ev StepLogEvent) slog.Level) LoggingOption {
	return func(c *loggingConfig) {
		if fn != nil {
			c.level = fn
		}
	}
}

// Logging logs each step's start and end at Debug and a failure at Error
// (WithLogLevel changes those levels), with the input and output sizes and
// the duration read from clk (nil means clock.Real). When the driver set a
// workflow.StepInfo on ctx, each line also carries workflow, step, run_id
// and attempt. It never logs payload bytes or labels: either can hold
// personal data or secrets, and a codec may not have run yet. A nil
// logger makes the middleware a no-op.
func Logging(l *slog.Logger, clk clock.Clock, opts ...LoggingOption) workflow.Middleware {
	if clk == nil {
		clk = clock.Real{}
	}
	cfg := loggingConfig{level: defaultLogLevel}
	for _, o := range opts {
		o(&cfg)
	}
	return func(next workflow.Handler) workflow.Handler {
		if l == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			l := l
			info, hasInfo := workflow.StepInfoFrom(ctx)
			if hasInfo {
				l = l.With(slog.String("workflow", info.Workflow), slog.String("step", info.Step),
					slog.String("run_id", info.RunID.String()), slog.Int("attempt", info.Attempt))
			}
			ev := StepLogEvent{Phase: StepStarted, Info: info, HasInfo: hasInfo}
			start := clk.Now()
			l.Log(ctx, cfg.level(ctx, ev), "workflow step started", slog.Int("in_bytes", len(in)))
			out, err := next(ctx, in)
			dur := clk.Now().Sub(start)
			if err != nil {
				ev.Phase, ev.Err = StepFailed, err
				l.Log(ctx, cfg.level(ctx, ev), "workflow step failed",
					slog.Duration("duration", dur),
					slog.Bool("permanent", workflow.IsPermanent(err)),
					slog.String("error", err.Error()))
				return out, err
			}
			ev.Phase = StepFinished
			l.Log(ctx, cfg.level(ctx, ev), "workflow step finished",
				slog.Duration("duration", dur), slog.Int("out_bytes", len(out)))
			return out, nil
		}
	}
}

// Recover turns a panic in a step into an error wrapping ErrPanic, so one
// bad step fails its attempt instead of killing the worker. The stack goes
// to l at Error (nil l drops it), never into the error string, which ends
// up in run status where it would leak file paths and be unreadable. The
// error is retryable: whether a panic is permanent is the retry policy's
// call (NoRetryOn: errors.Is(err, ErrPanic)), not this middleware's.
// The error implements workflow.StackTracer, so a driver with
// CapErrorHistory stores the stack in AttemptError.Stack, apart from the
// message.
func Recover(l *slog.Logger) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) (out []byte, err error) {
			defer func() {
				if r := recover(); r != nil {
					stack := string(debug.Stack())
					if l != nil {
						l.ErrorContext(ctx, "workflow step panicked",
							slog.Any("panic", r), slog.String("stack", stack))
					}
					out, err = nil, &panicError{msg: fmt.Sprintf("%v: %v", ErrPanic, r), stack: stack}
				}
			}()
			return next(ctx, in)
		}
	}
}

// Timeout gives each step attempt a context that ends after d. It is
// cooperative: the step must watch ctx, because abandoning a goroutine
// that keeps running would leak it and let two attempts of one step
// overlap. A d of zero or less is a no-op. Drivers already apply
// Step.Timeout; this is for a cap set by the app across every workflow.
func Timeout(d time.Duration) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		if d <= 0 {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, in)
		}
	}
}

// FailOn marks an error permanent when match reports true, so the step
// fails at once instead of spending its retries. An error already permanent stays so. A nil match is a
// no-op.
func FailOn(match func(error) bool) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		if match == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			out, err := next(ctx, in)
			if err != nil && !workflow.IsPermanent(err) && match(err) {
				return out, workflow.Permanent(err)
			}
			return out, err
		}
	}
}

// Skip does not run the step when skip reports true, and returns a nil
// output and a nil error: the step counts as done. Child steps then receive empty input, so a step whose
// children need its output should not be skipped. A nil skip is a no-op.
func Skip(skip func(ctx context.Context, in []byte) bool) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		if skip == nil {
			return next
		}
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if skip(ctx, in) {
				return nil, nil
			}
			return next(ctx, in)
		}
	}
}

// panicError is Recover's error: its message names the panic value, its
// stack stays out of the message (StackTrace), and it matches ErrPanic.
type panicError struct {
	msg   string
	stack string
}

func (e *panicError) Error() string      { return e.msg }
func (e *panicError) Unwrap() error      { return ErrPanic }
func (e *panicError) StackTrace() string { return e.stack }
