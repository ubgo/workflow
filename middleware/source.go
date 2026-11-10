package middleware

import (
	"context"
	"fmt"
	"slices"

	"github.com/ubgo/workflow"
)

// RequireSource refuses, with an error wrapping workflow.ErrInvalid, a job
// whose trigger source cannot be told (workflow.SourceOf returns ""), so a
// run never lands in a runs view without saying who started it. Put it in
// the client's enqueue middleware; a job made with workflow.WithSource, or
// carrying a label the library stamps (a schedule fire, a spawned child, a
// batch member, an event), passes.
//
// With allowed sources given, the job's source must also be one of them,
// so a typo ("webook") is refused at the enqueue site instead of creating
// a filter value nobody looks for. Each allowed source must pass
// workflow.ValidateSource, or every enqueue is refused with that error, so
// a bad allow-list fails on the first call rather than never matching.
//
// It checks the job it is handed only: a chained job's source is its own
// (the driver enqueues it later, past the middleware).
//
//ds:def id=mw-requiresource-3tsntxp2 owner=@khanakia stability=stable
func RequireSource(allowed ...workflow.Source) workflow.EnqueueMiddleware {
	var badAllowed error
	for _, s := range allowed {
		if err := workflow.ValidateSource(s); err != nil {
			badAllowed = fmt.Errorf("middleware.RequireSource: allowed %w", err)
			break
		}
	}
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if badAllowed != nil {
				return workflow.RunRef{}, badAllowed
			}
			src, _ := workflow.SourceOf(job.Labels)
			if src == "" {
				return workflow.RunRef{}, fmt.Errorf("%w: job for %q has no source; set one with workflow.WithSource", workflow.ErrInvalid, job.Workflow)
			}
			if len(allowed) > 0 && !slices.Contains(allowed, src) {
				return workflow.RunRef{}, fmt.Errorf("%w: job for %q has source %q, not one of %v", workflow.ErrInvalid, job.Workflow, src, allowed)
			}
			return next(ctx, job)
		}
	}
}
