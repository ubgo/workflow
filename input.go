package workflow

import (
	"encoding/json"
)

// StepInput builds the input bytes a step receives, so every driver
// follows one rule:
//
//   - no parents: the run's payload;
//   - one parent: that parent's output, unchanged;
//   - several parents: a JSON object keyed by parent name, in the order of
//     parents, each output embedded as raw JSON when it is valid JSON and
//     as a JSON string otherwise. A parent missing from outs is null.
//
// A handler joining several parents decodes the object (Define does this
// for a struct whose fields are named after the parents).
//
// The rule is part of the contract, not an implementation detail:
// middleware.DecodeStep relies on valid JSON being embedded unchanged (a
// sealed value is a JSON string) and on empty or missing outputs being
// null, so changing it means changing the codec with it.
func StepInput(payload []byte, parents []string, outs map[string][]byte) []byte {
	switch len(parents) {
	case 0:
		return payload
	case 1:
		return outs[parents[0]]
	}
	buf := []byte{'{'}
	for i, p := range parents {
		if i > 0 {
			buf = append(buf, ',')
		}
		name, _ := json.Marshal(p) // marshalling a string cannot fail
		buf = append(buf, name...)
		buf = append(buf, ':')
		out, ok := outs[p]
		switch {
		case !ok || len(out) == 0:
			buf = append(buf, "null"...)
		case json.Valid(out):
			buf = append(buf, out...)
		default:
			s, _ := json.Marshal(string(out)) // marshalling a string cannot fail
			buf = append(buf, s...)
		}
	}
	return append(buf, '}')
}
