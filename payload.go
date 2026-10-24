package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// JobOf builds a Job for workflow whose Payload is v encoded as JSON, the
// typed counterpart of filling Job.Payload by hand. Labels, RunAt and the
// rest are set on the returned Job as usual.
func JobOf[T any](workflow string, v T) (Job, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Job{}, invalidf("job for %q: encode payload: %v", workflow, err)
	}
	return Job{Workflow: workflow, Payload: b}, nil
}

// DecodePayload decodes a JSON payload (a step input, RunInfo.Payload, a
// result) into a T. Empty input decodes to the zero T. A decode failure
// is Permanent: retrying the same bytes cannot succeed.
func DecodePayload[T any](raw []byte) (T, error) {
	var v T
	if len(raw) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, Permanent(fmt.Errorf("workflow: decode payload: %w", err))
	}
	return v, nil
}

// ErrPayloadTooLarge matches PayloadTooLargeError.
var ErrPayloadTooLarge = errors.New("workflow: payload too large")

// PayloadTooLargeError says a payload is over a limit: the driver's hard
// cap (PayloadLimiter) or one the app set (middleware.MaxPayload). It
// matches ErrPayloadTooLarge and ErrInvalid, since the same bytes will
// never fit.
type PayloadTooLargeError struct {
	Workflow string
	Size     int
	Limit    int
	// Source says whose limit it is ("driver", "middleware", or a driver
	// name).
	Source string
}

// Error names the sizes, so the message alone says by how much it is over.
func (e *PayloadTooLargeError) Error() string {
	return fmt.Sprintf("workflow: payload of job for %q is %d bytes, over the %s limit of %d (offload it with middleware.NewClaimCheck)",
		e.Workflow, e.Size, e.Source, e.Limit)
}

// Is matches ErrPayloadTooLarge and ErrInvalid.
func (e *PayloadTooLargeError) Is(target error) bool {
	return target == ErrPayloadTooLarge || target == ErrInvalid
}

// PayloadLimiter is an optional interface a driver's client implements to
// declare its hard payload cap in bytes (an engine's gRPC message limit,
// a broker's message size), so code above it (the claim check, a batch
// splitter) can size against the real number. Zero means no cap.
type PayloadLimiter interface {
	PayloadLimit() int
}

// CheckPayload returns a *PayloadTooLargeError when job.Payload, or the
// payload of any job in its Chain, is over limit bytes, and nil
// otherwise or when limit <= 0. Drivers with a hard cap call it from
// Enqueue, before anything is stored.
func CheckPayload(job Job, limit int, source string) error {
	if limit <= 0 {
		return nil
	}
	if len(job.Payload) > limit {
		return &PayloadTooLargeError{Workflow: job.Workflow, Size: len(job.Payload), Limit: limit, Source: source}
	}
	for _, next := range job.Chain {
		if err := CheckPayload(next, limit, source); err != nil {
			return err
		}
	}
	return nil
}

// LabelPayloadVersion carries the version of a job's payload shape, set
// at enqueue by middleware.PayloadVersion, so a step can upcast a payload
// written by older code (Upcast).
const LabelPayloadVersion = LabelPrefixInternal + "payload-version"

// Upcaster turns a payload of one version into the next version's shape
// (version n to n+1): rename a field, fill a default, split a value.
type Upcaster func(raw []byte) ([]byte, error)

// PayloadVersionOf returns the payload version a run's labels carry, and
// 0 when none is set (a job enqueued before versioning, which counts as
// the oldest version).
func PayloadVersionOf(labels map[string]string) (int, error) {
	v, ok := labels[LabelPayloadVersion]
	if !ok {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, invalidf("label %s is %q, not a version number", LabelPayloadVersion, v)
	}
	return n, nil
}

// Upcast brings raw, written at version from, up to version to by
// applying ups[from], ups[from+1], ... ups[to-1] in turn; ups[n] turns
// version n into n+1. A missing step in the chain, a from above to, or an
// upcaster's error is Permanent, since retrying the same bytes cannot
// help.
func Upcast(raw []byte, from, to int, ups map[int]Upcaster) ([]byte, error) {
	if from > to {
		return nil, Permanent(invalidf("payload version %d is newer than this code's %d", from, to))
	}
	for v := from; v < to; v++ {
		up, ok := ups[v]
		if !ok {
			return nil, Permanent(invalidf("no upcaster from payload version %d to %d", v, v+1))
		}
		out, err := up(raw)
		if err != nil {
			return nil, Permanent(fmt.Errorf("workflow: upcast payload version %d to %d: %w", v, v+1, err))
		}
		raw = out
	}
	return raw, nil
}

// DefineVersioned is Define for a first step whose payload shape has
// versions: it reads the run's payload version (PayloadVersionOf on the
// StepInfo labels), upcasts the input to current with ups, then decodes
// it into In. Pair it with middleware.PayloadVersion(current), which
// stamps new jobs. Use it on steps that receive the run's payload (no
// After); a later step's input is a parent's output, which has no
// version.
func DefineVersioned[In, Out any](current int, ups map[int]Upcaster, fn func(ctx context.Context, in In) (Out, error)) Handler {
	typed := Define(fn)
	return func(ctx context.Context, raw []byte) ([]byte, error) {
		info, _ := StepInfoFrom(ctx)
		from, err := PayloadVersionOf(info.Labels)
		if err != nil {
			return nil, Permanent(err)
		}
		up, err := Upcast(raw, from, current, ups)
		if err != nil {
			return nil, err
		}
		return typed(ctx, up)
	}
}
