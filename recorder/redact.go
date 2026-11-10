package recorder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

// Redacted is the value a JSONPaths redactor writes in place of a sensitive
// value (as a JSON string in a body, as is in an event attribute).
//
//ds:def id=rec-redacted-uvn3fy6r owner=@khanakia stability=stable
const Redacted = "[REDACTED]"

// PayloadKind is which body or field a Redactor is asked about.
//
//ds:def id=rec-payloadkind-2vp35d73 owner=@khanakia stability=stable
type PayloadKind string

// The kinds of data a Recorder stores and so redacts.
const (
	// PayloadInput is the run's input (Job.Payload) at enqueue.
	PayloadInput PayloadKind = "input"
	// PayloadStepInput is a step attempt's input.
	PayloadStepInput PayloadKind = "step-input"
	// PayloadStepOutput is a step attempt's output.
	PayloadStepOutput PayloadKind = "step-output"
	// PayloadResult is the run's result (WithResultReader).
	PayloadResult PayloadKind = "result"
	// PayloadEvent is a step event's attribute.
	PayloadEvent PayloadKind = "event"
)

// PayloadInfo says what is about to be stored, so one Redactor can treat
// workflows, steps and kinds differently.
//
//ds:def id=rec-payloadinfo-9axyr3hj owner=@khanakia stability=stable
type PayloadInfo struct {
	Kind     PayloadKind
	Workflow string
	// Step is empty for PayloadInput and PayloadResult.
	Step string
}

// Redactor scrubs data before a Recorder stores it. It runs on the step's
// goroutine for every body and event, so it must be quick and safe for
// concurrent use, and must not modify its input slice (return a new one).
//
// Event messages are not redacted: a message is the step author's own
// text. Put values in attributes, which are.
//
//ds:def id=rec-redactor-x3e2p5bt owner=@khanakia stability=stable
type Redactor interface {
	// RedactPayload returns body as it may be stored.
	RedactPayload(info PayloadInfo, body []byte) []byte

	// RedactAttr returns a as it may be stored.
	RedactAttr(info PayloadInfo, a workflow.EventAttr) workflow.EventAttr
}

// jsonPaths is the Redactor JSONPaths returns.
type jsonPaths struct {
	paths [][]string
}

// JSONPaths returns a Redactor that replaces the values at the given dot
// paths of a JSON body with Redacted, and the value of an event attribute
// whose key matches a path. A segment "*" matches any key of an object or
// any element of an array; a number segment matches that array index.
//
//	recorder.JSONPaths("password", "card.number", "users.*.token")
//
// A body that is not valid JSON is stored unchanged (the redactor cannot
// find a path in it); an app with non-JSON payloads writes its own
// Redactor, or sets WithMaxPayload(0) to keep no bodies at all. A body with
// nothing to redact is stored byte for byte; one with a match is
// re-encoded, so its object keys come out sorted. An empty path or segment
// is an error wrapping workflow.ErrInvalid.
//
//ds:def id=rec-jsonpaths-66ph5w5h owner=@khanakia stability=stable
func JSONPaths(paths ...string) (Redactor, error) {
	jp := &jsonPaths{}
	for _, p := range paths {
		segs := strings.Split(p, ".")
		for _, s := range segs {
			if s == "" {
				return nil, fmt.Errorf("%w: recorder.JSONPaths: empty segment in path %q", workflow.ErrInvalid, p)
			}
		}
		jp.paths = append(jp.paths, segs)
	}
	return jp, nil
}

// RedactPayload implements Redactor.
func (jp *jsonPaths) RedactPayload(_ PayloadInfo, body []byte) []byte {
	if len(jp.paths) == 0 || !json.Valid(body) {
		return body
	}
	out := json.RawMessage(body)
	changed := false
	for _, p := range jp.paths {
		v, ok := redactAt(out, p)
		if ok {
			out, changed = v, true
		}
	}
	if !changed {
		return body
	}
	return out
}

// RedactAttr implements Redactor: an attribute whose dot-split key matches
// a path is replaced.
func (jp *jsonPaths) RedactAttr(_ PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	keySegs := strings.Split(a.Key, ".")
	for _, p := range jp.paths {
		if segsMatch(p, keySegs) {
			a.Value = Redacted
			return a
		}
	}
	return a
}

// segsMatch reports whether key matches path segment by segment, "*"
// matching any one segment.
func segsMatch(path, key []string) bool {
	if len(path) != len(key) {
		return false
	}
	for i := range path {
		if path[i] != "*" && path[i] != key[i] {
			return false
		}
	}
	return true
}

// redactedJSON is Redacted as a JSON string.
var redactedJSON = json.RawMessage(strconv.Quote(Redacted))

// redactAt returns raw with the values at path replaced, and whether
// anything was. raw is valid JSON. Objects and arrays are decoded one level
// at a time into json.RawMessage, so values off the path are kept as they
// were written.
func redactAt(raw json.RawMessage, path []string) (json.RawMessage, bool) {
	if len(path) == 0 {
		return redactedJSON, true
	}
	seg, rest := path[0], path[1:]
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 {
		return raw, false
	}
	switch trimmed[0] {
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return raw, false
		}
		changed := false
		for k, v := range obj {
			if seg != "*" && seg != k {
				continue
			}
			if nv, ok := redactAt(v, rest); ok {
				obj[k], changed = nv, true
			}
		}
		if !changed {
			return raw, false
		}
		out, err := json.Marshal(obj)
		if err != nil {
			return raw, false
		}
		return out, true
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return raw, false
		}
		idx := -1
		if seg != "*" {
			n, err := strconv.Atoi(seg)
			if err != nil || n < 0 || n >= len(arr) {
				return raw, false
			}
			idx = n
		}
		changed := false
		for i, v := range arr {
			if idx >= 0 && i != idx {
				continue
			}
			if nv, ok := redactAt(v, rest); ok {
				arr[i], changed = nv, true
			}
		}
		if !changed {
			return raw, false
		}
		out, err := json.Marshal(arr)
		if err != nil {
			return raw, false
		}
		return out, true
	}
	return raw, false
}

// perWorkflow is the Redactor PerWorkflow returns.
type perWorkflow struct {
	byName   map[string]Redactor
	fallback Redactor
}

// PerWorkflow returns a Redactor that uses byName[info.Workflow] when the
// workflow has an entry and fallback otherwise; a nil fallback stores the
// data of other workflows unchanged. The map is copied.
//
//ds:def id=rec-perworkflow-vx8puukg owner=@khanakia stability=stable
func PerWorkflow(byName map[string]Redactor, fallback Redactor) Redactor {
	m := make(map[string]Redactor, len(byName))
	for k, v := range byName {
		if v != nil {
			m[k] = v
		}
	}
	return &perWorkflow{byName: m, fallback: fallback}
}

func (p *perWorkflow) pick(info PayloadInfo) Redactor {
	if r, ok := p.byName[info.Workflow]; ok {
		return r
	}
	return p.fallback
}

// RedactPayload implements Redactor.
func (p *perWorkflow) RedactPayload(info PayloadInfo, body []byte) []byte {
	if r := p.pick(info); r != nil {
		return r.RedactPayload(info, body)
	}
	return body
}

// RedactAttr implements Redactor.
func (p *perWorkflow) RedactAttr(info PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	if r := p.pick(info); r != nil {
		return r.RedactAttr(info, a)
	}
	return a
}

// chain is the Redactor Chain returns.
type chain []Redactor

// Chain returns a Redactor that applies rs in order, each to what the one
// before returned; nil entries are skipped.
//
//ds:def id=rec-chain-ryg3zfvp owner=@khanakia stability=stable
func Chain(rs ...Redactor) Redactor {
	var c chain
	for _, r := range rs {
		if r != nil {
			c = append(c, r)
		}
	}
	return c
}

// RedactPayload implements Redactor.
func (c chain) RedactPayload(info PayloadInfo, body []byte) []byte {
	for _, r := range c {
		body = r.RedactPayload(info, body)
	}
	return body
}

// RedactAttr implements Redactor.
func (c chain) RedactAttr(info PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	for _, r := range c {
		a = r.RedactAttr(info, a)
	}
	return a
}
