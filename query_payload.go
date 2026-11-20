package workflow

import (
	"bytes"
	"encoding/json"
	"strings"
)

// validPayloadPath reports whether path is a dot path of plain names.
func validPayloadPath(path string) bool {
	if path == "" {
		return false
	}
	for _, seg := range strings.Split(path, ".") {
		if seg == "" || (seg[0] >= '0' && seg[0] <= '9') {
			return false
		}
		for _, r := range seg {
			if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
				return false
			}
		}
	}
	return true
}

// payloadMatches reports whether payload is a JSON object holding every
// field of want with its value (RunQuery.Payload).
func payloadMatches(payload []byte, want map[string]string) bool {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || payload[0] != '{' {
		return false
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(payload, &root); err != nil {
		return false
	}
	for path, value := range want {
		got, ok := payloadField(root, path)
		if !ok || got != value {
			return false
		}
	}
	return true
}

// payloadField returns the text of the field at path: a string unquoted,
// a number as written, true, false or null; ok is false for a missing
// field or one holding an object or array.
func payloadField(root map[string]json.RawMessage, path string) (string, bool) {
	segs := strings.Split(path, ".")
	obj := root
	for i, seg := range segs {
		raw, ok := obj[seg]
		if !ok {
			return "", false
		}
		raw = bytes.TrimSpace(raw)
		if i < len(segs)-1 {
			if len(raw) == 0 || raw[0] != '{' || json.Unmarshal(raw, &obj) != nil {
				return "", false
			}
			continue
		}
		switch {
		case len(raw) == 0 || raw[0] == '{' || raw[0] == '[':
			return "", false
		case raw[0] == '"':
			var s string
			if json.Unmarshal(raw, &s) != nil {
				return "", false
			}
			return s, true
		default:
			return string(raw), true
		}
	}
	return "", false
}
