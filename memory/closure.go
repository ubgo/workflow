package memory

import (
	"context"
	"fmt"
	"maps"

	"github.com/ubgo/workflow"
)

// EnqueueClosure runs c.Fn as a run of workflow.ClosureWorkflow in this
// process; see workflow.ClosureEnqueuer. The run goes through the same
// queues, priorities, pauses, retries, middleware and run-finished
// observers as any other, and Status, Cancel, Runs, Retry and Delete work
// on its RunRef. It lives only in this process's memory, like every run
// of this driver.
func (c *Client) EnqueueClosure(ctx context.Context, cl workflow.ClosureJob) (workflow.RunRef, error) {
	if err := ctx.Err(); err != nil {
		return workflow.RunRef{}, err
	}
	if err := workflow.ValidateClosure(cl); err != nil {
		return workflow.RunRef{}, err
	}
	fn := cl.Fn
	step := workflow.Step{
		Name:    workflow.ClosureStep,
		Retry:   cl.Retry,
		Timeout: cl.Timeout,
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			return nil, fn(ctx)
		},
	}
	wf := workflow.Workflow{Name: workflow.ClosureWorkflow, Steps: []workflow.Step{step}, Queue: cl.Queue}
	reg := &registered{
		wf:       wf,
		handlers: map[string]workflow.Handler{step.Name: c.wrap(step.Run)},
		children: map[string][]string{},
		steps:    map[string]workflow.Step{step.Name: step},
		closure:  true,
	}
	labels := maps.Clone(cl.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[workflow.LabelClosure] = cl.Name
	job := workflow.Job{Workflow: wf.Name, Labels: labels, Priority: cl.Priority, RunAt: cl.RunAt}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usableLocked(); err != nil {
		return workflow.RunRef{}, err
	}
	if q := workflow.EffectiveQueue(wf, job); !c.servesLocked(q) {
		return workflow.RunRef{}, fmt.Errorf("%w: queue %q is not served by this client (WithQueues); the closure would never run", workflow.ErrInvalid, q)
	}
	c.pruneLocked()
	return c.startRunLocked(c.newRunLocked(reg, job)), nil
}
