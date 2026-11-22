package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// MultiChoiceSpec is a multi-choice (WC-06): one or more of several
// branches run, picked at run time by Decide, and a join waits only for
// the branches taken (WC-07, structured synchronizing merge; WC-05, simple
// merge, is the same join with one branch taken).
//
//ds:def id=multichoicespec-tjvnntqy owner=@khanakia stability=stable
type MultiChoiceSpec struct {
	// Name is the name of the join step; later steps put it in After.
	Name string

	// After lists the steps the choice waits for, as Step.After.
	After []string

	// Decide returns the names of the branches to run: at least one, each
	// a branch, none twice. Anything else fails the run (Permanent,
	// ErrInvalid); an error retries the decide step.
	Decide func(ctx context.Context, in []byte) ([]string, error)

	// Branches are the paths; at least one.
	Branches []Branch
}

// multiRoute is the decide step's output.
type multiRoute struct {
	Branches []string `json:"branches"`
	Input    []byte   `json:"input"`
}

// MultiChoice expands c into ordinary steps, like Choice: a decide step
// ("<Name>.decide"), one step per branch ("<Name>.<branch>") and a join
// named Name. Every branch step starts once the decide step is done; the
// ones not chosen end at once without calling their Run, so the join
// waits for exactly the branches taken. The join's output is a JSON
// object keyed by branch name holding the outputs of the branches taken
// (valid JSON embedded, anything else as a string), in branch order.
// No capability is needed.
//
//ds:def id=multichoice-5d9vshey owner=@khanakia stability=stable
func MultiChoice(c MultiChoiceSpec) ([]Step, error) {
	if c.Name == "" || c.Decide == nil || len(c.Branches) == 0 {
		return nil, invalidf("multi-choice %q needs a Name, a Decide and branches", c.Name)
	}
	names := make([]string, 0, len(c.Branches))
	for _, b := range c.Branches {
		if b.Name == "" || b.Run == nil {
			return nil, invalidf("multi-choice %q: every branch needs a Name and a Run", c.Name)
		}
		if slices.Contains(names, b.Name) {
			return nil, invalidf("multi-choice %q: branch %q is listed twice", c.Name, b.Name)
		}
		names = append(names, b.Name)
	}
	decideName := c.Name + ChoiceDecideSuffix
	steps := []Step{{Name: decideName, After: slices.Clone(c.After), Run: multiDecide(c.Name, c.Decide, names)}}
	branchSteps := make([]string, 0, len(c.Branches))
	for _, b := range c.Branches {
		name := c.Name + "." + b.Name
		branchSteps = append(branchSteps, name)
		steps = append(steps, Step{
			Name: name, After: []string{decideName}, Retry: b.Retry, Timeout: b.Timeout,
			Run: multiBranch(c.Name, b.Name, b.Run),
		})
	}
	return append(steps, Step{Name: c.Name, After: branchSteps, Run: multiJoin(c.Name, names, branchSteps)}), nil
}

//ds:def id=multidecide-mmex4jpe owner=@khanakia stability=stable
func multiDecide(choice string, decide func(context.Context, []byte) ([]string, error), branches []string) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		picked, err := decide(ctx, in)
		if err != nil {
			return nil, err
		}
		if len(picked) == 0 {
			return nil, Permanent(invalidf("multi-choice %q: Decide picked no branch", choice))
		}
		for i, p := range picked {
			if !slices.Contains(branches, p) {
				return nil, Permanent(invalidf("multi-choice %q: Decide returned %q, which is not a branch", choice, p))
			}
			if slices.Contains(picked[:i], p) {
				return nil, Permanent(invalidf("multi-choice %q: Decide returned %q twice", choice, p))
			}
		}
		return encodeJSON("multi-choice "+choice, multiRoute{Branches: picked, Input: in})
	}
}

//ds:def id=multibranch-x5xhz9st owner=@khanakia stability=stable
func multiBranch(choice, branch string, run Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		var r multiRoute
		if err := json.Unmarshal(in, &r); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: multi-choice %q: branch %q: decide output does not decode: %w", choice, branch, err))
		}
		if !slices.Contains(r.Branches, branch) {
			return encodeJSON("multi-choice "+choice, choiceOut{})
		}
		out, err := run(ctx, r.Input)
		if err != nil {
			return nil, err
		}
		return encodeJSON("multi-choice "+choice, choiceOut{Taken: true, Output: out})
	}
}

//ds:def id=multijoin-3etse6jd owner=@khanakia stability=stable
func multiJoin(choice string, names, branchSteps []string) Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		outs := map[string]json.RawMessage{}
		if len(branchSteps) == 1 {
			outs[branchSteps[0]] = in
		} else if err := json.Unmarshal(in, &outs); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: multi-choice %q: join input does not decode: %w", choice, err))
		}
		var taken []string
		res := map[string][]byte{}
		for i, s := range branchSteps {
			var o choiceOut
			if raw := outs[s]; len(raw) > 0 && string(raw) != "null" {
				if err := json.Unmarshal(raw, &o); err != nil {
					return nil, Permanent(fmt.Errorf("workflow: multi-choice %q: branch %q output does not decode: %w", choice, s, err))
				}
			}
			if o.Taken {
				taken = append(taken, names[i])
				res[names[i]] = o.Output
			}
		}
		if len(taken) == 0 {
			return nil, Permanent(fmt.Errorf("workflow: multi-choice %q: no branch was taken", choice))
		}
		return objectOf(taken, res), nil
	}
}

// objectOf writes a JSON object of keys in order, each value embedded as
// raw JSON when valid, as a JSON string otherwise, null when empty: the
// rule of StepInput, also for a single key.
func objectOf(keys []string, vals map[string][]byte) []byte {
	if len(keys) == 1 {
		// StepInput returns a single parent's output bare; pad with a
		// second key and cut it off again to keep one rule for the values.
		full := StepInput(nil, []string{keys[0], ""}, vals)
		cut := len(`,"":null}`)
		return append(full[:len(full)-cut], '}')
	}
	return StepInput(nil, keys, vals)
}
