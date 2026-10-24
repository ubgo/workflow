package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// snippet:plugin
// tenancy is a plugin: one value an app adds with workflow.Use, which
// requires a tenant label on every job, keeps one queued job per tenant
// and workflow at a time, logs every step attempt and every run end.
// Each contribution is an optional interface it implements; Use finds
// them by type assertion.
type tenancy struct {
	log    *lineLog
	unique middleware.UniqueStore
}

// Name names the plugin in every refusal Use returns.
func (p *tenancy) Name() string { return "tenancy" }

// snippet:end

// snippet:enqueue
// EnqueueMiddleware makes workflow.Use run these on every Enqueue through
// the client it returns. The first entry sees the job first.
func (p *tenancy) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	byTenant := func(j workflow.Job) string { return j.Labels["tenant"] + "/" + j.Workflow }
	return []workflow.EnqueueMiddleware{
		requireLabel("tenant"),
		middleware.Unique(byTenant, p.unique, time.Hour),
	}
}

// requireLabel refuses a job without the label, before it reaches the
// driver.
func requireLabel(key string) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if job.Labels[key] == "" {
				return workflow.RunRef{}, fmt.Errorf("%w: job for %q has no %q label", workflow.ErrInvalid, job.Workflow, key)
			}
			return next(ctx, job)
		}
	}
}

// snippet:end

// snippet:step
// StepMiddleware wraps every step of every workflow registered through
// the client Use returns.
func (p *tenancy) StepMiddleware() []workflow.Middleware {
	return []workflow.Middleware{logAttempts(p.log)}
}

// logAttempts is step middleware: it reads what the driver put on the
// ctx (workflow.StepInfo), calls the next handler and logs the outcome.
// It never logs the payload, which may hold personal data.
func logAttempts(log *lineLog) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			out, err := next(ctx, in)
			outcome := "ok"
			if err != nil {
				outcome = "error: " + err.Error()
			}
			log.Printf("step %s/%s attempt %d tenant=%s: %s", info.Workflow, info.Step, info.Attempt, info.Labels["tenant"], outcome)
			return out, err
		}
	}
}

// snippet:end

// snippet:observer
// RunFinishedObservers are registered on the driver's RunNotifier by Use,
// so the client must implement it and Use must run before Start.
func (p *tenancy) RunFinishedObservers() []workflow.RunFinishedFunc {
	return []workflow.RunFinishedFunc{func(ctx context.Context, ev workflow.RunEvent) {
		p.log.Printf("run %s of %s for tenant=%s: %s", ev.Ref.RunID, ev.Workflow, ev.Labels["tenant"], ev.Status)
	}}
}

// snippet:end

// lineLog collects lines from any goroutine and writes them in order.
type lineLog struct {
	mu  sync.Mutex
	out io.Writer
}

func (l *lineLog) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, format+"\n", args...)
}
