package failover

import (
	"context"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunSignaller    = (*Client)(nil)
	_ workflow.RunSignalReader = (*Client)(nil)
)

// SignalRun signals the run ref names on its backend with the backend
// prefix stripped, so the run's own address matches what its driver
// resolved (workflow.RunSignaller). A ref without a prefix goes, addressed
// as given, to both backends, as Signal does.
func (f *Client) SignalRun(ctx context.Context, ref workflow.RunRef, ev workflow.Event) error {
	if s, inner, ok := decodeRef(ref); ok && s != unknownSide {
		return workflow.SignalRun(ctx, f.backend(s), inner, ev.Name, ev.Payload)
	}
	ev.Key = workflow.RunKey(ref.RunID)
	return f.Signal(ctx, ev)
}

// PendingSignals asks the backend the run's prefix names
// (workflow.RunSignalReader); steps see prefixed RunIDs. The backend must
// implement workflow.RunSignalReader (Supports answers for both).
func (f *Client) PendingSignals(ctx context.Context, run workflow.RunID, names []string, limit int) ([]workflow.RunSignal, error) {
	var out []workflow.RunSignal
	err := f.onRef(workflow.RunRef{RunID: run}, func(c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunSignalReader)
		if !ok {
			return workflow.Unsupported("PendingSignals", "the backend does not implement workflow.RunSignalReader")
		}
		var err error
		out, err = r.PendingSignals(ctx, inner.RunID, names, limit)
		return err
	})
	return out, err
}

// TakeSignal takes the signal on the backend the run's prefix names
// (workflow.RunSignalReader).
func (f *Client) TakeSignal(ctx context.Context, run workflow.RunID, id string) (bool, error) {
	var took bool
	err := f.onRef(workflow.RunRef{RunID: run}, func(c workflow.Client, inner workflow.RunRef) error {
		r, ok := c.(workflow.RunSignalReader)
		if !ok {
			return workflow.Unsupported("TakeSignal", "the backend does not implement workflow.RunSignalReader")
		}
		var err error
		took, err = r.TakeSignal(ctx, inner.RunID, id)
		return err
	})
	return took, err
}
