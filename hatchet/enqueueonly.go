package hatchet

import (
	"context"
	"fmt"

	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// startEnqueueOnly is Start for Config.EnqueueOnly: no worker, no
// poller, only the stop when ctx ends, so Done and ErrNotStarted mean
// what they mean on a worker client. Called with c.mu held.
func (c *Client) startEnqueueOnly(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	c.runCtx, c.stop, c.started = runCtx, cancel, true
	go func() {
		<-runCtx.Done()
		c.mu.Lock()
		c.drained = true
		c.mu.Unlock()
		close(c.done)
	}()
}

// enqueueOnlySDK returns the SDK client of an enqueue-only client,
// building it on first use (Config.ClientOptions, as Start does). Called
// with c.mu held.
func (c *Client) enqueueOnlySDK() (*hatchetsdk.Client, error) {
	if c.drained {
		return nil, workflow.ErrNotStarted
	}
	if c.sdk == nil {
		sdk, err := newSDK(c.cfg.ClientOptions)
		if err != nil {
			return nil, err
		}
		c.sdk = sdk
	}
	return c.sdk, nil
}

// newSDK builds the SDK client. SDK v0.107.1 panics when its config does
// not load (pkg/client/client.go defaultClientOpts, a missing token for
// one), which on a lazy connect would crash the request that enqueues:
// the panic becomes an error.
func newSDK(opts []hatchetsdk.ClientOpt) (sdk *hatchetsdk.Client, err error) {
	defer func() {
		if p := recover(); p != nil {
			sdk, err = nil, fmt.Errorf("hatchet driver: connect: %v", p)
		}
	}()
	sdk, err = hatchetsdk.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("hatchet driver: connect: %w", err)
	}
	return sdk, nil
}

// enqueueOnlyLookup is lookup for an enqueue-only client: a handle that
// triggers the workflow by name (the worker process declared its tasks
// and policies on the engine) and the registered spec, or a spec with
// only the name for a workflow this client never registered. Called with
// c.mu held.
func (c *Client) enqueueOnlyLookup(name string) (*hatchetsdk.Workflow, workflow.Workflow, error) {
	sdk, err := c.enqueueOnlySDK()
	if err != nil {
		return nil, workflow.Workflow{}, err
	}
	hw, ok := c.workflows[name]
	if !ok {
		hw = sdk.NewWorkflow(name)
		c.workflows[name] = hw
	}
	spec, ok := c.specs[name]
	if !ok {
		spec = workflow.Workflow{Name: name}
	}
	return hw, spec, nil
}

// isRegistered reports whether name was registered on this client.
func (c *Client) isRegistered(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.specs[name]
	return ok
}
