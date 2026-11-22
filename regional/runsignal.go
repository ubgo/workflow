package regional

import (
	"context"
	"errors"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunSignaller    = (*Client)(nil)
	_ workflow.RunSignalReader = (*Client)(nil)
)

// SignalRun signals the run ref names in its region with the region
// prefix stripped, so the run's own address matches what its driver
// resolved (workflow.RunSignaller). A ref without a region prefix of this
// client goes, addressed as given, to the regions Signal would reach.
//
//ds:def id=regional-client-signalrun-xj8vwpgd owner=@khanakia stability=stable
func (c *Client) SignalRun(ctx context.Context, ref workflow.RunRef, ev workflow.Event) error {
	if name, inner, ok := decodeRef(ref); ok {
		if rc, known := c.regions[name]; known {
			return workflow.SignalRun(ctx, rc, inner, ev.Name, ev.Payload)
		}
	}
	ev.Key = workflow.RunKey(ref.RunID)
	return c.Signal(ctx, ev)
}

// PendingSignals asks the region the run's prefix names
// (workflow.RunSignalReader); steps see prefixed RunIDs, so the run
// OnSignals reads is always one of this client's. A run of no region
// here has none. The region must implement workflow.RunSignalReader.
//
//ds:def id=regional-client-pendingsignals-4e47r89a owner=@khanakia stability=stable
func (c *Client) PendingSignals(ctx context.Context, run workflow.RunID, names []string, limit int) ([]workflow.RunSignal, error) {
	var out []workflow.RunSignal
	err := onRegion(c, "PendingSignals", workflow.Caps{}, workflow.RunRef{RunID: run}, func(_ string, r workflow.RunSignalReader, inner workflow.RunRef) error {
		var err error
		out, err = r.PendingSignals(ctx, inner.RunID, names, limit)
		return err
	})
	if errors.Is(err, workflow.ErrNotFound) {
		return nil, nil
	}
	return out, err
}

// TakeSignal takes the signal in the region the run's prefix names
// (workflow.RunSignalReader).
func (c *Client) TakeSignal(ctx context.Context, run workflow.RunID, id string) (bool, error) {
	var took bool
	err := onRegion(c, "TakeSignal", workflow.Caps{}, workflow.RunRef{RunID: run}, func(_ string, r workflow.RunSignalReader, inner workflow.RunRef) error {
		var err error
		took, err = r.TakeSignal(ctx, inner.RunID, id)
		return err
	})
	return took, err
}
