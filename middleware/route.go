package middleware

import (
	"context"

	"github.com/ubgo/workflow"
)

// RouteRule maps the jobs Match accepts to a workflow name and labels.
type RouteRule struct {
	// Match selects the jobs this rule routes. Nil matches every job.
	Match func(workflow.Job) bool

	// Workflow, when set, replaces Job.Workflow.
	Workflow string

	// Labels are added to Job.Labels. A label the job already carries
	// wins: a choice made at the enqueue site beats the routing table.
	Labels map[string]string
}

// Route applies the first rule whose Match accepts the job and passes
// every job on, matched or not: routing is an
// app-side table that keeps queue names and tenant labels out of the code
// that enqueues. The job's Labels map is copied before it is changed, so
// the caller's map is never mutated.
func Route(rules ...RouteRule) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			for _, r := range rules {
				if r.Match != nil && !r.Match(job) {
					continue
				}
				if r.Workflow != "" {
					job.Workflow = r.Workflow
				}
				if len(r.Labels) > 0 {
					labels := make(map[string]string, len(job.Labels)+len(r.Labels))
					for k, v := range r.Labels {
						labels[k] = v
					}
					for k, v := range job.Labels {
						labels[k] = v
					}
					job.Labels = labels
				}
				break
			}
			return next(ctx, job)
		}
	}
}
