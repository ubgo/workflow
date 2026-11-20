package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// LabelShape is the fingerprint of a workflow's steps that ShapeGuard
// writes on a run at enqueue.
const LabelShape = LabelPrefixInternal + "shape"

// ErrShapeChanged matches the failure ShapeGuard gives a step of a run
// enqueued under a different set of steps than the one now registered.
var ErrShapeChanged = errors.New("workflow: the workflow's steps changed under a running run")

// Shape returns the fingerprint of wf's step graph: a hash of every
// step's name, kind and After, independent of their order in Steps. Two
// definitions with the same fingerprint run a stored run the same way;
// renaming, adding or removing a step, or changing an edge changes it.
// Handler code, retries and timeouts are not part of it.
func Shape(wf Workflow) string {
	lines := make([]string, len(wf.Steps))
	for i, s := range wf.Steps {
		after := slices.Clone(s.After)
		slices.Sort(after)
		lines[i] = s.Name + "|" + string(s.Kind()) + "|" + strings.Join(after, ",")
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:8])
}

// shapePlugin is ShapeGuard.
type shapePlugin struct{ shapes map[string]string }

// ShapeGuard returns a plugin that detects renamed, added, removed or
// rewired steps under runs already in flight (DE-12): at enqueue it
// stamps each job of one of wfs with LabelShape (Shape of that workflow
// now), and at each step it compares the run's stamp with the current
// definition; a mismatch fails the step with ErrShapeChanged, Permanent,
// before the handler runs, instead of letting a stored run resume against
// steps it was not written for. Runs without the label (enqueued before
// the guard, or fired by a driver's own scheduler, which does not run
// enqueue middleware) pass unchecked.
//
// Keep a changed definition apart from old runs by giving it a new
// workflow name (or a version, where the driver supports one) and
// draining the old; the guard is what tells you a deploy forgot to. It
// runs on every driver.
func ShapeGuard(wfs ...Workflow) Plugin {
	p := shapePlugin{shapes: map[string]string{}}
	for _, wf := range wfs {
		p.shapes[wf.Name] = Shape(wf)
	}
	return p
}

func (p shapePlugin) Name() string { return "shape-guard" }

func (p shapePlugin) EnqueueMiddleware() []EnqueueMiddleware {
	return []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, job Job) (RunRef, error) {
			shape, ok := p.shapes[job.Workflow]
			if !ok {
				return next(ctx, job)
			}
			job.Labels = maps.Clone(job.Labels)
			if job.Labels == nil {
				job.Labels = map[string]string{}
			}
			job.Labels[LabelShape] = shape
			return next(AllowReservedLabels(ctx, LabelShape), job)
		}
	}}
}

func (p shapePlugin) StepMiddleware() []Middleware {
	return []Middleware{func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := StepInfoFrom(ctx)
			if !ok {
				return next(ctx, in)
			}
			stamped, has := info.Labels[LabelShape]
			want, known := p.shapes[info.Workflow]
			if has && known && stamped != want {
				return nil, Permanent(fmt.Errorf("%w: run %s of %q was enqueued with steps %s, now %s (step %q)", ErrShapeChanged, info.RunID, info.Workflow, stamped, want, info.Step))
			}
			return next(ctx, in)
		}
	}}
}
