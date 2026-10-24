package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// ChoiceDecideSuffix is appended to a Choice's name to name its decide
// step; each branch step is named "<choice>.<branch>", and the join step
// carries the Choice's own name, so later steps list the Choice's name in
// After as they would any step.
const ChoiceDecideSuffix = ".decide"

// Branch is one path of a Choice: an ordinary step body with its own
// retry policy and timeout.
type Branch struct {
	// Name identifies the branch; Decide returns it. Unique within the
	// Choice.
	Name string

	// Run is the branch's work. It receives the input the decide step
	// received, and its output becomes the Choice's output.
	Run Handler

	// Retry overrides the workflow default for this branch, as Step.Retry.
	Retry *RetryPolicy

	// Timeout caps one attempt of this branch, as Step.Timeout.
	Timeout time.Duration
}

// ChoiceSpec is an exclusive choice (WC-04): exactly one of several
// branches runs, picked at run time by Decide.
type ChoiceSpec struct {
	// Name is the name of the step that carries the chosen branch's
	// output; later steps put it in After.
	Name string

	// After lists the steps the choice waits for, as Step.After. Decide
	// and the chosen branch receive their input (StepInput).
	After []string

	// Decide returns the Name of the branch to run. A name that is not a
	// branch fails the run (Permanent, ErrInvalid); an error retries the
	// decide step under the workflow's policy.
	Decide func(ctx context.Context, in []byte) (string, error)

	// Branches are the paths; at least one.
	Branches []Branch
}

// choiceRoute is the decide step's output: the branch picked and the
// input it is to receive. Input is []byte so any bytes, JSON or not,
// survive the trip (encoding/json writes it as base64).
type choiceRoute struct {
	Branch string `json:"branch"`
	Input  []byte `json:"input"`
}

// choiceOut is a branch step's output: whether the branch was the one
// taken, and its handler's output when it was.
type choiceOut struct {
	Taken  bool   `json:"taken"`
	Output []byte `json:"output,omitempty"`
}

// Choice expands c into ordinary steps, so it runs on every driver with
// no capability beyond what the steps need: a decide step
// ("<Name>.decide"), one step per branch ("<Name>.<branch>") and a join
// step named Name. Every branch step starts once the decide step is done;
// the branches not chosen return at once without calling their Run, and
// the join step returns the chosen branch's output unchanged. A run
// listing therefore shows every branch step as succeeded; the one that did
// work is the one whose Run was called.
//
// Append the result to Workflow.Steps. It returns an error wrapping
// ErrInvalid when the spec is incomplete or a branch name repeats.
func Choice(c ChoiceSpec) ([]Step, error) {
	if c.Name == "" {
		return nil, invalidf("choice has no Name")
	}
	if c.Decide == nil {
		return nil, invalidf("choice %q has no Decide", c.Name)
	}
	if len(c.Branches) == 0 {
		return nil, invalidf("choice %q has no branches", c.Name)
	}
	names := make([]string, 0, len(c.Branches))
	for _, b := range c.Branches {
		if b.Name == "" || b.Run == nil {
			return nil, invalidf("choice %q: every branch needs a Name and a Run", c.Name)
		}
		if slices.Contains(names, b.Name) {
			return nil, invalidf("choice %q: branch %q is listed twice", c.Name, b.Name)
		}
		names = append(names, b.Name)
	}
	decideName := c.Name + ChoiceDecideSuffix
	steps := []Step{{Name: decideName, After: slices.Clone(c.After), Run: choiceDecide(c.Name, c.Decide, names)}}
	branchSteps := make([]string, 0, len(c.Branches))
	for _, b := range c.Branches {
		name := c.Name + "." + b.Name
		branchSteps = append(branchSteps, name)
		steps = append(steps, Step{
			Name: name, After: []string{decideName}, Retry: b.Retry, Timeout: b.Timeout,
			Run: choiceBranch(c.Name, b.Name, b.Run),
		})
	}
	return append(steps, Step{Name: c.Name, After: branchSteps, Run: choiceJoin(c.Name, branchSteps)}), nil
}

// choiceDecide calls decide and records the branch and the input.
func choiceDecide(choice string, decide func(context.Context, []byte) (string, error), branches []string) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		branch, err := decide(ctx, in)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(branches, branch) {
			return nil, Permanent(invalidf("choice %q: Decide returned %q, which is not a branch", choice, branch))
		}
		return encodeJSON("choice "+choice, choiceRoute{Branch: branch, Input: in})
	}
}

// choiceBranch runs run only when the decide step picked this branch.
func choiceBranch(choice, branch string, run Handler) Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		var r choiceRoute
		if err := json.Unmarshal(in, &r); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: choice %q: branch %q: decide output does not decode: %w", choice, branch, err))
		}
		if r.Branch != branch {
			return encodeJSON("choice "+choice, choiceOut{})
		}
		out, err := run(ctx, r.Input)
		if err != nil {
			return nil, err
		}
		return encodeJSON("choice "+choice, choiceOut{Taken: true, Output: out})
	}
}

// choiceJoin returns the output of the one branch taken. Its input is the
// single branch's output, or an object keyed by branch step (StepInput).
func choiceJoin(choice string, branchSteps []string) Handler {
	return func(_ context.Context, in []byte) ([]byte, error) {
		outs := map[string]json.RawMessage{}
		if len(branchSteps) == 1 {
			outs[branchSteps[0]] = in
		} else if err := json.Unmarshal(in, &outs); err != nil {
			return nil, Permanent(fmt.Errorf("workflow: choice %q: join input does not decode: %w", choice, err))
		}
		for _, s := range branchSteps {
			var o choiceOut
			if raw := outs[s]; len(raw) > 0 && string(raw) != "null" {
				if err := json.Unmarshal(raw, &o); err != nil {
					return nil, Permanent(fmt.Errorf("workflow: choice %q: branch %q output does not decode: %w", choice, s, err))
				}
			}
			if o.Taken {
				return o.Output, nil
			}
		}
		return nil, Permanent(fmt.Errorf("workflow: choice %q: no branch was taken", choice))
	}
}
