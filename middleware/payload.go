package middleware

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	"github.com/ubgo/workflow"
)

// MaxPayload refuses a job whose payload, or a chained job's, is over
// limit bytes, with a *workflow.PayloadTooLargeError (matches
// ErrPayloadTooLarge and ErrInvalid), before it reaches the driver: an app
// picks its own cap below the engine's hard one, so an oversized job fails
// at the caller rather than deep in a broker. Put it after a claim check,
// which shrinks big payloads first, so only what would really travel is
// measured. A limit of zero or less refuses every enqueue with ErrInvalid
// rather than meaning "no limit" by accident.
func MaxPayload(limit int) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if limit <= 0 {
				return workflow.RunRef{}, fmt.Errorf("%w: MaxPayload needs a positive limit", workflow.ErrInvalid)
			}
			if err := workflow.CheckPayload(job, limit, "middleware.MaxPayload"); err != nil {
				return workflow.RunRef{}, err
			}
			return next(ctx, job)
		}
	}
}

// PayloadVersion stamps every job with the payload version this code
// writes (workflow.LabelPayloadVersion), so a step built with
// workflow.DefineVersioned can upcast a payload an older version of the
// app enqueued. A job that already carries a version (a retry, a job
// forwarded from elsewhere) keeps it; one carrying a version newer than
// current is refused with ErrInvalid, since this code could not read it.
// The labels map is copied, never changed in place.
func PayloadVersion(current int) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			if current < 0 {
				return workflow.RunRef{}, fmt.Errorf("%w: PayloadVersion needs a version >= 0", workflow.ErrInvalid)
			}
			if _, set := job.Labels[workflow.LabelPayloadVersion]; set {
				v, err := workflow.PayloadVersionOf(job.Labels)
				if err != nil {
					return workflow.RunRef{}, err
				}
				if v > current {
					return workflow.RunRef{}, fmt.Errorf("%w: job payload version %d is newer than this code's %d", workflow.ErrInvalid, v, current)
				}
				return next(workflow.AllowReservedLabels(ctx, workflow.LabelPayloadVersion), job)
			}
			labels := maps.Clone(job.Labels)
			if labels == nil {
				labels = map[string]string{}
			}
			labels[workflow.LabelPayloadVersion] = strconv.Itoa(current)
			job.Labels = labels
			return next(workflow.AllowReservedLabels(ctx, workflow.LabelPayloadVersion), job)
		}
	}
}
