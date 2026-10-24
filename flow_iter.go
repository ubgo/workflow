package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// ErrLoopLimit means a Loop reached its iteration limit without its body
// saying it was done. It is returned marked Permanent: running the same
// loop again would reach the same limit.
var ErrLoopLimit = errors.New("workflow: loop reached its iteration limit")

// MapOption configures MapEach.
type MapOption func(*mapConfig)

type mapConfig struct{ concurrency int }

// MapConcurrency runs at most n items at once. The default is 1 (one
// after another); n below 1 counts as 1.
func MapConcurrency(n int) MapOption { return func(c *mapConfig) { c.concurrency = max(n, 1) } }

// MapEach runs fn once per item and returns the results in the order of
// items: multiple instances whose number is known only when the step runs
// (WC-14), joined back into the step's result. Each item is memoised
// under name and its index, as Memo does, so a retried step runs again
// only the items that did not succeed; items must therefore keep their
// order across attempts.
//
// Every item runs even when others fail, so successes are kept for the
// next attempt; the error is then the join of each failed item's error
// (labelled with its index), and the results are nil. A Permanent error
// from any item makes the joined error Permanent. When ctx ends, items
// not yet begun are skipped with ctx's error.
//
// To run each instance as its own run instead (its own retries, its own
// worker, visible in run listings), enqueue them with Spawn. MapEach needs
// the Journal plugin, as Memo does.
func MapEach[In, Out any](ctx context.Context, name string, items []In, fn func(ctx context.Context, i int, item In) (Out, error), opts ...MapOption) ([]Out, error) {
	cfg := mapConfig{concurrency: 1}
	for _, o := range opts {
		o(&cfg)
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return nil, err
	}
	key, err := j.claim(name)
	if err != nil {
		return nil, err
	}
	out := make([]Out, len(items))
	errs := make([]error, len(items))
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup
	for i, item := range items {
		if err := ctx.Err(); err != nil {
			errs[i] = fmt.Errorf("item %d: %w", i, err)
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			v, err := memoKey(ctx, j, key+"#"+strconv.Itoa(i), name+"#"+strconv.Itoa(i), func(ctx context.Context) (Out, error) {
				return fn(ctx, i, item)
			})
			if err != nil {
				errs[i] = fmt.Errorf("item %d: %w", i, err)
				return
			}
			out[i] = v
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("workflow: map %q: %w", name, err)
	}
	return out, nil
}

// loopEntry is one stored Loop iteration: the state the body returned and
// whether it said it was done.
type loopEntry[S any] struct {
	State S    `json:"s"`
	Done  bool `json:"d"`
}

// Loop is a structured loop (WC-21): it calls body with the iteration
// number and the current state until body reports done, and returns the
// final state. Each iteration is memoised under name and its number, so a
// retried step replays the finished iterations from the journal (without
// calling body) and resumes at the first one that did not finish.
//
// limit bounds the iterations and is required (at least 1), so a loop is
// never unbounded by accident; reaching it without done returns a
// Permanent ErrLoopLimit. A body error is returned as it is and the
// iteration runs again on the next attempt. The state is stored as JSON
// after every iteration, so keep it small (a cursor, a counter), not the
// data itself. Loop needs the Journal plugin, as Memo does.
//
// For a loop over steps of the graph rather than code (repeat a block of
// steps), enqueue a new run of the workflow from its last step: a run's
// graph is fixed when it starts.
func Loop[S any](ctx context.Context, name string, init S, limit int, body func(ctx context.Context, i int, state S) (next S, done bool, err error)) (S, error) {
	if limit < 1 {
		return init, Permanent(invalidf("loop %q: limit %d is below 1", name, limit))
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return init, err
	}
	key, err := j.claim(name)
	if err != nil {
		return init, err
	}
	state := init
	for i := range limit {
		in := state
		e, err := memoKey(ctx, j, key+"#"+strconv.Itoa(i), name+"#"+strconv.Itoa(i), func(ctx context.Context) (loopEntry[S], error) {
			next, done, err := body(ctx, i, in)
			return loopEntry[S]{State: next, Done: done}, err
		})
		if err != nil {
			return state, err
		}
		state = e.State
		if e.Done {
			return state, nil
		}
	}
	return state, Permanent(fmt.Errorf("%w: loop %q ran %d iterations", ErrLoopLimit, name, limit))
}

// encodeJSON marshals v, for helpers that store values in the journal or
// in step outputs; a value that does not encode is a Permanent error.
func encodeJSON(what string, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, Permanent(fmt.Errorf("workflow: %s: encode: %w", what, err))
	}
	return b, nil
}
