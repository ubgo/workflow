package dsl

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"

	"github.com/ubgo/workflow"
)

// Parse reads a Definition from its JSON form and validates it
// (Definition.Validate). It is strict, so a stored definition is never
// loaded with part of its meaning dropped: a field it does not know (a
// typo such as "handlr", or a field of a newer release), a value of the
// wrong JSON type, and data after the document are refused. Every error
// wraps workflow.ErrInvalid, and every refusal of the document's content is
// a *FieldError naming the JSON path ("steps[2].handler").
//
// Parse needs no Registry: handler names are resolved by Build, so a
// deploy pipeline can check a definition's shape before any program
// registers handlers. YAML is a superset of JSON; convert YAML to JSON with
// the app's YAML library first (see the package doc).
func Parse(data []byte) (Definition, error) {
	def, err := parseDoc[Definition](data)
	if err != nil {
		return Definition{}, err
	}
	if err := def.Validate(); err != nil {
		return Definition{}, err
	}
	return def, nil
}

// parseDoc decodes one JSON object into a T strictly: the shape is checked
// first against T's JSON fields (so an error names the JSON path), then
// the document is decoded with unknown fields refused.
func parseDoc[T any](data []byte) (T, error) {
	var zero T
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // keep 1.5 apart from 1, so an integer field can refuse it
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return zero, fieldErr("", "not a JSON document: %v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return zero, fieldErr("", "data after the JSON document")
	}
	if _, ok := doc.(map[string]any); !ok {
		return zero, fieldErr("", "want a JSON object, got %s", jsonKind(doc))
	}
	if err := checkShape(doc, reflect.TypeFor[T](), ""); err != nil {
		return zero, err
	}
	strict := json.NewDecoder(bytes.NewReader(data))
	strict.DisallowUnknownFields()
	var v T
	if err := strict.Decode(&v); err != nil {
		return zero, fieldErr("", "%v", err)
	}
	return v, nil
}

// checkShape walks the decoded document v against the Go type t, refusing
// an unknown field or a value of the wrong JSON type with the path where
// it sits. encoding/json names neither the path of an unknown field nor
// the array index of a type mismatch, and a person fixing a definition by
// hand needs both. A JSON null is accepted anywhere and means "not set",
// as encoding/json treats it.
func checkShape(v any, t reflect.Type, path string) error {
	if v == nil {
		return nil
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		obj, ok := v.(map[string]any)
		if !ok {
			return fieldErr(path, "want an object, got %s", jsonKind(v))
		}
		fields := jsonFields(t)
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		slices.Sort(keys) // report the same field first on every run
		for _, k := range keys {
			ft, ok := fields[k]
			if !ok {
				return fieldErr(at(path, k), "unknown field (known: %s)", knownFields(fields))
			}
			if err := checkShape(obj[k], ft, at(path, k)); err != nil {
				return err
			}
		}
	case reflect.Slice:
		arr, ok := v.([]any)
		if !ok {
			return fieldErr(path, "want an array, got %s", jsonKind(v))
		}
		for i, e := range arr {
			if err := checkShape(e, t.Elem(), index(path, i)); err != nil {
				return err
			}
		}
	case reflect.String:
		if _, ok := v.(string); !ok {
			return fieldErr(path, "want a string, got %s", jsonKind(v))
		}
	case reflect.Int:
		n, ok := v.(json.Number)
		if !ok {
			return fieldErr(path, "want an integer, got %s", jsonKind(v))
		}
		if _, err := n.Int64(); err != nil {
			return fieldErr(path, "want an integer, got %s", n)
		}
	case reflect.Float64:
		n, ok := v.(json.Number)
		if !ok {
			return fieldErr(path, "want a number, got %s", jsonKind(v))
		}
		if _, err := n.Float64(); err != nil {
			return fieldErr(path, "want a number, got %s", n)
		}
	case reflect.Bool:
		if _, ok := v.(bool); !ok {
			return fieldErr(path, "want true or false, got %s", jsonKind(v))
		}
	}
	return nil
}

// jsonFields maps the JSON names of t's fields to their types, read from
// the json tags, so the shape check can never disagree with the encoder.
func jsonFields(t reflect.Type) map[string]reflect.Type {
	out := make(map[string]reflect.Type, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		out[name] = f.Type
	}
	return out
}

// knownFields lists the field names for an unknown-field refusal.
func knownFields(fields map[string]reflect.Type) string {
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// jsonKind names the JSON type of a decoded value for an error.
func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case json.Number:
		return "a number"
	case bool:
		return "a boolean"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return "an unknown value"
}

// Load is Parse then Build: the one call that turns a stored definition
// into a workflow.Workflow ready for Client.Register. Every error wraps
// workflow.ErrInvalid.
func Load(data []byte, reg *Registry) (workflow.Workflow, error) {
	def, err := Parse(data)
	if err != nil {
		return workflow.Workflow{}, err
	}
	return Build(def, reg)
}
