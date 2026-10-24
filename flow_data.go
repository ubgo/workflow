package workflow

import (
	"context"
	"encoding/json"
	"fmt"
)

// runDataPrefix starts the journal key of a run-scoped value. Memo keys
// start with a step name's length and a colon, so the two never meet.
const runDataPrefix = "data/"

// SetRunData stores v under name for the whole run (WX-01): every later
// step of the run, and every later attempt, reads it with RunData. It is
// the run scope between a step's own input and output (block scope) and
// the app's database (global scope). It writes through the journal, so it
// needs the Journal plugin and is as durable as the JournalStore; values
// are forgotten with the run's journal when the run finishes.
//
// A later SetRunData of the same name replaces the value. Two steps
// running in parallel that set one name leave whichever wrote last, so
// give parallel steps their own names. v is stored as JSON; keep it small.
func SetRunData(ctx context.Context, name string, v any) error {
	j, err := journalFrom(ctx)
	if err != nil {
		return err
	}
	if name == "" {
		return Permanent(invalidf("run data name is empty"))
	}
	b, err := encodeJSON("run data "+name, v)
	if err != nil {
		return err
	}
	if err := j.store.Save(ctx, j.run, runDataPrefix+name, b); err != nil {
		return fmt.Errorf("workflow: run data %q: save: %w", name, err)
	}
	return nil
}

// RunData returns the value SetRunData stored under name in this run, and
// false when none was stored. A stored value that does not decode into T
// is a Permanent error.
func RunData[T any](ctx context.Context, name string) (T, bool, error) {
	var zero T
	j, err := journalFrom(ctx)
	if err != nil {
		return zero, false, err
	}
	raw, ok, err := j.store.Load(ctx, j.run, runDataPrefix+name)
	if err != nil {
		return zero, false, fmt.Errorf("workflow: run data %q: load: %w", name, err)
	}
	if !ok {
		return zero, false, nil
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, false, Permanent(fmt.Errorf("workflow: run data %q: stored value does not decode: %w", name, err))
	}
	return v, true, nil
}
