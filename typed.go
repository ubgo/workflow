package workflow

import (
	"context"
	"encoding/json"
	"fmt"
)

// Define wraps a typed function into a Handler that decodes its input from
// JSON and encodes its output to JSON, so app code works with structs, not
// bytes. Empty input decodes to the zero In. Input that does not decode is
// a Permanent error: retrying the same bytes can never succeed.
func Define[In, Out any](fn func(ctx context.Context, in In) (Out, error)) Handler {
	return func(ctx context.Context, raw []byte) ([]byte, error) {
		var in In
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, Permanent(fmt.Errorf("workflow: decode input: %w", err))
			}
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(out)
		if err != nil {
			return nil, Permanent(fmt.Errorf("workflow: encode output: %w", err))
		}
		return b, nil
	}
}
