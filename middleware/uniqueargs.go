package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/ubgo/workflow"
)

// DefaultArgsKeyPrefix starts every key UniqueByArgs builds, so its locks
// cannot collide with keys an app's own key function builds in the same
// UniqueStore.
const DefaultArgsKeyPrefix = "args:"

// ArgsOption tunes UniqueByArgs.
type ArgsOption func(*argsKey)

// argsKey is UniqueByArgs's configuration.
type argsKey struct {
	prefix string
	fields []string // nil: the whole payload counts
	queue  bool
	labels []string
}

// ArgsFields makes only these top-level fields of a JSON object payload
// count: two jobs whose payloads differ only in other fields share a key.
// A field missing from the payload counts as absent, which differs from
// one set to null. A payload that is not a JSON object cannot be cut to
// fields, so its whole bytes count instead: the key gets narrower, never
// wider, so no job is ever dropped as a duplicate of a different one.
func ArgsFields(names ...string) ArgsOption {
	return func(k *argsKey) { k.fields = append([]string(nil), names...) }
}

// ArgsQueue makes Job.Queue count, so the same arguments on two queues
// are two unique jobs.
func ArgsQueue() ArgsOption { return func(k *argsKey) { k.queue = true } }

// ArgsLabels makes the values of these Job.Labels keys count (a missing
// label counts as absent).
func ArgsLabels(keys ...string) ArgsOption {
	return func(k *argsKey) { k.labels = append([]string(nil), keys...) }
}

// ArgsKeyPrefix replaces DefaultArgsKeyPrefix.
func ArgsKeyPrefix(p string) ArgsOption { return func(k *argsKey) { k.prefix = p } }

// UniqueByArgs returns a key function for Unique, UniqueUntilFinished and
// UniqueUntilStarted that makes a job unique by its workflow and its
// arguments: prefix + workflow + ":" + the hex SHA-256 of the canonical
// payload (plus the queue and labels the options add).
//
// A JSON payload is canonical after decoding and encoding again, so key
// order and whitespace do not matter; numbers keep their literal text
// (1 and 1.0 differ). A payload that is not JSON is hashed as its bytes.
// It hashes the payload the middleware sees, so place it before a codec
// or claim check whose output changes from call to call (encryption).
func UniqueByArgs(opts ...ArgsOption) func(workflow.Job) string {
	k := argsKey{prefix: DefaultArgsKeyPrefix}
	for _, o := range opts {
		o(&k)
	}
	return func(job workflow.Job) string {
		h := sha256.New()
		h.Write(k.canonical(job.Payload))
		if k.queue {
			h.Write([]byte("\x00queue\x00" + job.Queue))
		}
		for _, l := range k.labels {
			v, ok := job.Labels[l]
			h.Write([]byte("\x00label\x00" + l + "\x00"))
			if ok {
				h.Write([]byte("=" + v))
			}
		}
		return k.prefix + job.Workflow + ":" + hex.EncodeToString(h.Sum(nil))
	}
}

// canonical returns the bytes of payload that count, in canonical form.
// It never fails: what cannot be decoded counts as its raw bytes. The two
// cannot collide: a canonical form is one valid JSON value, and raw bytes
// are used only when the payload is not one (or, with ArgsFields, is not
// an object, which no picked object encodes to).
func (k argsKey) canonical(payload []byte) []byte {
	var v any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil || dec.More() {
		return payload
	}
	if k.fields != nil {
		obj, ok := v.(map[string]any)
		if !ok {
			return payload
		}
		picked := make(map[string]any, len(k.fields))
		for _, f := range k.fields {
			if fv, ok := obj[f]; ok {
				picked[f] = fv
			}
		}
		v = picked
	}
	out, err := json.Marshal(v)
	if err != nil {
		return payload
	}
	return out
}

// UniqueTag is the struct tag ArgsTagged reads: a field tagged
// `unique:"true"` counts toward the key.
const UniqueTag = "unique"

// ArgsTagged makes the fields of T tagged `unique:"true"` the ones that
// count (ArgsFields with their JSON names), so the uniqueness rule lives
// on the payload type next to the fields it names:
//
//	type SyncArgs struct {
//		ShopID string `json:"shop_id" unique:"true"`
//		Cursor string `json:"cursor"`
//	}
//	by := middleware.UniqueByArgs(middleware.ArgsTagged[SyncArgs]())
//
// The JSON name comes from the json tag (its first part), else the Go
// field name, as encoding/json does. A T that is not a struct, or has no
// tagged field, makes the whole payload count, the same as no option.
func ArgsTagged[T any]() ArgsOption {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return func(*argsKey) {}
	}
	var names []string
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() || f.Tag.Get(UniqueTag) != "true" {
			continue
		}
		name := f.Name
		if tag, _, _ := strings.Cut(f.Tag.Get("json"), ","); tag != "" && tag != "-" {
			name = tag
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return func(*argsKey) {}
	}
	return ArgsFields(names...)
}
