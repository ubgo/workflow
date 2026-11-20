package workflow

import "slices"

// Stages lays out steps as a list of stages (WP-17): every step of a
// stage runs in parallel, and a stage starts only once every step of the
// stage before it has ended. It is sugar over Step.After, so it runs on
// every driver: each step of stage k gets every step name of stage k-1
// added to its After (a step's own After entries are kept, so a stage may
// also wait on a step outside the staged list).
//
// A step's input follows StepInput as for any step: a step of the second
// or a later stage with one step in the stage before receives that step's
// output, and with several it receives a JSON object keyed by their
// names. Append the result to Workflow.Steps. It returns an error wrapping
// ErrInvalid for an empty stage or a step without a name; Validate checks
// the rest (duplicate names, unknown After entries) at Register, as for
// steps written by hand.
func Stages(stages ...[]Step) ([]Step, error) {
	var out []Step
	var prev []string
	for i, stage := range stages {
		if len(stage) == 0 {
			return nil, invalidf("stage %d has no steps", i)
		}
		names := make([]string, 0, len(stage))
		for _, s := range stage {
			if s.Name == "" {
				return nil, invalidf("stage %d has a step without a name", i)
			}
			after := slices.Clone(s.After)
			for _, p := range prev {
				if !slices.Contains(after, p) {
					after = append(after, p)
				}
			}
			s.After = after
			out = append(out, s)
			names = append(names, s.Name)
		}
		prev = names
	}
	return out, nil
}
