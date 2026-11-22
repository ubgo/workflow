package events

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ubgo/workflow"
)

// CloudEvents 1.0 (https://github.com/cloudevents/spec), the vendor-neutral
// envelope for events: the JSON event format in structured mode and the
// HTTP protocol binding in binary and structured mode. Only the parts the
// bus needs are here, with the standard library alone; batch mode and the
// other event formats (Avro, Protobuf) are not.
const (
	// CloudEventsSpecVersion is the only specversion this package reads
	// and writes.
	//ds:def id=events-cloudeventsspecversion-he42kjdr owner=@khanakia stability=stable
	CloudEventsSpecVersion = "1.0"

	// ContentTypeCloudEventsJSON is the media type of a structured-mode
	// event in the JSON format.
	//ds:def id=events-contenttypecloudeventsjson-rq8pngja owner=@khanakia stability=stable
	ContentTypeCloudEventsJSON = "application/cloudevents+json"

	// contentTypeCloudEventsBatchJSON is the batch media type, refused.
	contentTypeCloudEventsBatchJSON = "application/cloudevents-batch+json"

	// ceHeaderPrefix starts every binary-mode attribute header.
	ceHeaderPrefix = "ce-"
)

// coreAttributes are the context attribute names the spec defines, plus
// the two data members of the JSON format: an extension may not reuse one.
//
//ds:def id=events-coreattributes-etsexf8s owner=@khanakia stability=stable
var coreAttributes = map[string]bool{
	"id": true, "source": true, "specversion": true, "type": true,
	"datacontenttype": true, "dataschema": true, "subject": true, "time": true,
	"data": true, "data_base64": true,
}

// CloudEvent is one event in the CloudEvents 1.0 envelope. ID, Source,
// SpecVersion and Type are required (Validate); the rest are optional.
//
// Data holds the event's payload as bytes, whatever the format: JSON
// text when DataContentType is JSON (or empty, which the JSON format
// reads as JSON), the decoded text for a text type, and the raw bytes
// otherwise. MarshalJSON picks the member: "data" with the JSON value for
// JSON data, "data" with a string for text, "data_base64" for anything
// else.
//
//ds:def id=events-cloudevent-banpczfp owner=@khanakia stability=stable
type CloudEvent struct {
	// ID identifies the event; ID and Source together are unique per
	// distinct event, so a consumer deduplicates by them.
	ID string

	// Source is a URI-reference naming the context the event happened
	// in ("https://shop.example/orders", "/billing").
	Source string

	// SpecVersion is CloudEventsSpecVersion; any other value is refused.
	SpecVersion string

	// Type names the kind of event ("com.example.order.paid").
	Type string

	// DataContentType is Data's media type (RFC 2046); empty means
	// JSON in the JSON format.
	DataContentType string

	// DataSchema is a URI naming the schema Data adheres to; a common
	// place for a payload version.
	DataSchema string

	// Subject names the event's subject within Source (an order ID).
	Subject string

	// Time is when the event happened; zero means unset.
	Time time.Time

	// Data is the payload; nil means the event has none.
	Data []byte

	// Extensions are the extension attributes by name. Names are
	// lower-case ASCII letters and digits and never a core attribute's
	// name. Values are strings here: a JSON number or boolean is read as
	// its literal text and written back as a string, which the spec
	// allows for every attribute type.
	Extensions map[string]string
}

// Validate reports, with an error wrapping workflow.ErrInvalid, the
// first rule e breaks: a missing ID, Source or Type, a SpecVersion other
// than "1.0", a Source or DataSchema that is not a URI, a
// DataContentType that is not a media type, or an extension name that
// is not lower-case letters and digits or reuses a core attribute's name.
//
//ds:def id=events-cloudevent-validate-neytqwkd owner=@khanakia stability=stable
func (e CloudEvent) Validate() error {
	switch {
	case e.SpecVersion != CloudEventsSpecVersion:
		return fmt.Errorf("%w: cloudevent: specversion %q is not %q", workflow.ErrInvalid, e.SpecVersion, CloudEventsSpecVersion)
	case e.ID == "":
		return fmt.Errorf("%w: cloudevent: id is required", workflow.ErrInvalid)
	case e.Source == "":
		return fmt.Errorf("%w: cloudevent: source is required", workflow.ErrInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: cloudevent: type is required", workflow.ErrInvalid)
	}
	if _, err := url.Parse(e.Source); err != nil {
		return fmt.Errorf("%w: cloudevent: source %q is not a URI-reference", workflow.ErrInvalid, e.Source)
	}
	if e.DataSchema != "" {
		if u, err := url.Parse(e.DataSchema); err != nil || !u.IsAbs() {
			return fmt.Errorf("%w: cloudevent: dataschema %q is not an absolute URI", workflow.ErrInvalid, e.DataSchema)
		}
	}
	if e.DataContentType != "" {
		if _, _, err := mime.ParseMediaType(e.DataContentType); err != nil {
			return fmt.Errorf("%w: cloudevent: datacontenttype %q: %v", workflow.ErrInvalid, e.DataContentType, err)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(e.Extensions)) {
		if !ValidExtensionName(name) {
			return fmt.Errorf("%w: cloudevent: extension name %q is not lower-case letters and digits, or names a core attribute", workflow.ErrInvalid, name)
		}
	}
	return nil
}

// ValidExtensionName reports whether name may name a CloudEvents
// extension attribute: one or more lower-case ASCII letters or digits,
// and not the name of a core attribute or a data member.
//
//ds:def id=events-validextensionname-m2rx2xn7 owner=@khanakia stability=stable
func ValidExtensionName(name string) bool {
	if name == "" || coreAttributes[name] {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// isJSONType reports whether a datacontenttype says JSON: empty (the
// JSON format's default), application/json, text/json, or any +json
// suffix.
func isJSONType(ct string) bool {
	if ct == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return mt == "application/json" || mt == "text/json" || strings.HasSuffix(mt, "+json")
}

// isTextType reports whether a datacontenttype is text the JSON format
// carries as a string: text/*, or an XML type.
func isTextType(ct string) bool {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return strings.HasPrefix(mt, "text/") || mt == "application/xml" || strings.HasSuffix(mt, "+xml")
}

// MarshalJSON writes e in the CloudEvents JSON format (structured mode),
// attributes in a fixed order and extensions sorted by name, so the same
// event always encodes to the same bytes. It refuses an event that fails
// Validate, and Data that is not valid JSON under an explicit JSON
// DataContentType; under an empty one, non-JSON data goes out as
// data_base64.
//
//ds:def id=events-cloudevent-marshaljson-zbwekqxp owner=@khanakia stability=stable
func (e CloudEvent) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	member := func(name string, value []byte) {
		if !first {
			buf.WriteByte(',')
		}
		first = false
		k, _ := json.Marshal(name)
		buf.Write(k)
		buf.WriteByte(':')
		buf.Write(value)
	}
	str := func(name, v string) {
		if v == "" {
			return
		}
		b, _ := json.Marshal(v)
		member(name, b)
	}
	str("specversion", e.SpecVersion)
	str("id", e.ID)
	str("source", e.Source)
	str("type", e.Type)
	str("subject", e.Subject)
	if !e.Time.IsZero() {
		str("time", e.Time.Format(time.RFC3339Nano))
	}
	str("datacontenttype", e.DataContentType)
	str("dataschema", e.DataSchema)
	for _, name := range slices.Sorted(maps.Keys(e.Extensions)) {
		b, _ := json.Marshal(e.Extensions[name])
		member(name, b)
	}
	if e.Data != nil {
		switch {
		case isJSONType(e.DataContentType) && json.Valid(e.Data):
			var c bytes.Buffer
			if err := json.Compact(&c, e.Data); err != nil {
				return nil, fmt.Errorf("%w: cloudevent: data: %v", workflow.ErrInvalid, err)
			}
			member("data", c.Bytes())
		case e.DataContentType != "" && isJSONType(e.DataContentType):
			return nil, fmt.Errorf("%w: cloudevent: datacontenttype %q says JSON but data is not valid JSON", workflow.ErrInvalid, e.DataContentType)
		case isTextType(e.DataContentType) && utf8.Valid(e.Data):
			b, _ := json.Marshal(string(e.Data))
			member("data", b)
		default:
			b, _ := json.Marshal(base64.StdEncoding.EncodeToString(e.Data))
			member("data_base64", b)
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON reads e from the CloudEvents JSON format and validates
// it. Core attributes must be JSON strings (null counts as absent); an
// extension may be a string, number or boolean, kept as its literal
// text. "data" and "data_base64" together are refused. Data is the JSON
// value as written when DataContentType is JSON, the string's text when
// "data" is a JSON string under another type, and the decoded bytes of
// data_base64. Every error wraps workflow.ErrInvalid.
//
//ds:def id=events-cloudevent-unmarshaljson-k846eshc owner=@khanakia stability=stable
func (e *CloudEvent) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("%w: cloudevent: %v", workflow.ErrInvalid, err)
	}
	var out CloudEvent
	strAttr := func(name string) (string, error) {
		v, ok := raw[name]
		if !ok || string(v) == "null" {
			return "", nil
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return "", fmt.Errorf("%w: cloudevent: attribute %q is not a string", workflow.ErrInvalid, name)
		}
		return s, nil
	}
	targets := []struct {
		name string
		dst  *string
	}{
		{"specversion", &out.SpecVersion}, {"id", &out.ID}, {"source", &out.Source}, {"type", &out.Type},
		{"subject", &out.Subject}, {"datacontenttype", &out.DataContentType}, {"dataschema", &out.DataSchema},
	}
	for _, t := range targets {
		s, err := strAttr(t.name)
		if err != nil {
			return err
		}
		*t.dst = s
	}
	ts, err := strAttr("time")
	if err != nil {
		return err
	}
	if ts != "" {
		if out.Time, err = time.Parse(time.RFC3339Nano, ts); err != nil {
			return fmt.Errorf("%w: cloudevent: time %q is not RFC 3339", workflow.ErrInvalid, ts)
		}
	}
	data, hasData := raw["data"]
	if hasData && string(data) == "null" {
		hasData = false
	}
	data64, hasData64 := raw["data_base64"]
	if hasData64 && string(data64) == "null" {
		hasData64 = false
	}
	switch {
	case hasData && hasData64:
		return fmt.Errorf("%w: cloudevent: both data and data_base64 are set", workflow.ErrInvalid)
	case hasData64:
		var s string
		if err := json.Unmarshal(data64, &s); err != nil {
			return fmt.Errorf("%w: cloudevent: data_base64 is not a string", workflow.ErrInvalid)
		}
		if out.Data, err = base64.StdEncoding.DecodeString(s); err != nil {
			return fmt.Errorf("%w: cloudevent: data_base64: %v", workflow.ErrInvalid, err)
		}
	case hasData:
		var s string
		if !isJSONType(out.DataContentType) && json.Unmarshal(data, &s) == nil {
			out.Data = []byte(s)
		} else {
			out.Data = slices.Clone([]byte(data))
		}
	}
	for name, v := range raw {
		if coreAttributes[name] || string(v) == "null" {
			continue
		}
		val, err := extensionValue(v)
		if err != nil {
			return fmt.Errorf("%w: cloudevent: extension %q: %v", workflow.ErrInvalid, name, err)
		}
		if out.Extensions == nil {
			out.Extensions = map[string]string{}
		}
		out.Extensions[name] = val
	}
	if err := out.Validate(); err != nil {
		return err
	}
	*e = out
	return nil
}

// extensionValue reads a JSON extension value: a string as itself, a
// number or boolean as its literal text. Objects and arrays are refused:
// the spec's attribute types are all scalars.
//
//ds:def id=events-extensionvalue-bg4xbcng owner=@khanakia stability=stable
func extensionValue(v json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s, nil
	}
	var b bool
	if json.Unmarshal(v, &b) == nil {
		return strconv.FormatBool(b), nil
	}
	var n json.Number
	if json.Unmarshal(v, &n) == nil {
		return n.String(), nil
	}
	return "", fmt.Errorf("value %s is not a string, number or boolean", v)
}

// WriteHTTP writes e in HTTP binary mode: every attribute but
// datacontenttype as a "ce-<name>" header in h (values percent-encoded
// as the HTTP binding requires), DataContentType as Content-Type, and
// returns the body, which is Data as is. It refuses an event that fails
// Validate.
//
//ds:def id=events-cloudevent-writehttp-6f97kqts owner=@khanakia stability=stable
func (e CloudEvent) WriteHTTP(h http.Header) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	set := func(name, v string) {
		if v != "" {
			h.Set(ceHeaderPrefix+name, percentEncode(v))
		}
	}
	set("specversion", e.SpecVersion)
	set("id", e.ID)
	set("source", e.Source)
	set("type", e.Type)
	set("subject", e.Subject)
	set("dataschema", e.DataSchema)
	if !e.Time.IsZero() {
		set("time", e.Time.Format(time.RFC3339Nano))
	}
	for name, v := range e.Extensions {
		set(name, v)
	}
	if e.DataContentType != "" {
		h.Set("Content-Type", e.DataContentType)
	}
	return e.Data, nil
}

// WriteHTTPStructured writes e in HTTP structured mode: Content-Type
// application/cloudevents+json in h, and returns the body, e in the JSON
// format (MarshalJSON).
//
//ds:def id=events-cloudevent-writehttpstructured-zar4anuu owner=@khanakia stability=stable
func (e CloudEvent) WriteHTTPStructured(h http.Header) ([]byte, error) {
	body, err := e.MarshalJSON()
	if err != nil {
		return nil, err
	}
	h.Set("Content-Type", ContentTypeCloudEventsJSON+"; charset=utf-8")
	return body, nil
}

// ReadHTTP reads a CloudEvent from an HTTP request in either mode:
// structured when Content-Type is application/cloudevents+json, binary
// when a ce-specversion header is present. It reads at most
// DefaultWebhookMaxBody bytes; wrap r.Body in http.MaxBytesReader for a
// smaller cap. Every refusal wraps workflow.ErrInvalid: a request in
// neither mode, batch mode or another event format, a body over the cap,
// a header that does not percent-decode to UTF-8, and an event that
// fails Validate.
//
//ds:def id=events-readhttp-9tkdmpb7 owner=@khanakia stability=stable
func ReadHTTP(r *http.Request) (CloudEvent, error) {
	ct := r.Header.Get("Content-Type")
	mt := ""
	if ct != "" {
		var err error
		if mt, _, err = mime.ParseMediaType(ct); err != nil {
			return CloudEvent{}, fmt.Errorf("%w: cloudevent: Content-Type %q: %v", workflow.ErrInvalid, ct, err)
		}
	}
	switch {
	case mt == ContentTypeCloudEventsJSON:
		body, err := readBody(r)
		if err != nil {
			return CloudEvent{}, err
		}
		var e CloudEvent
		if err := e.UnmarshalJSON(body); err != nil {
			return CloudEvent{}, err
		}
		return e, nil
	case mt == contentTypeCloudEventsBatchJSON:
		return CloudEvent{}, fmt.Errorf("%w: cloudevent: batch mode is not supported", workflow.ErrInvalid)
	case strings.HasPrefix(mt, "application/cloudevents"):
		return CloudEvent{}, fmt.Errorf("%w: cloudevent: event format %q is not supported (only %s)", workflow.ErrInvalid, mt, ContentTypeCloudEventsJSON)
	case r.Header.Get(ceHeaderPrefix+"specversion") == "":
		return CloudEvent{}, fmt.Errorf("%w: cloudevent: request is in neither binary mode (no ce-specversion header) nor structured mode", workflow.ErrInvalid)
	}
	e := CloudEvent{DataContentType: ct}
	for key, vals := range r.Header {
		lk := strings.ToLower(key)
		if !strings.HasPrefix(lk, ceHeaderPrefix) || len(vals) == 0 {
			continue
		}
		name := strings.TrimPrefix(lk, ceHeaderPrefix)
		v, err := percentDecode(vals[0])
		if err != nil {
			return CloudEvent{}, fmt.Errorf("%w: cloudevent: header %s: %v", workflow.ErrInvalid, key, err)
		}
		switch name {
		case "specversion":
			e.SpecVersion = v
		case "id":
			e.ID = v
		case "source":
			e.Source = v
		case "type":
			e.Type = v
		case "subject":
			e.Subject = v
		case "dataschema":
			e.DataSchema = v
		case "time":
			if e.Time, err = time.Parse(time.RFC3339Nano, v); err != nil {
				return CloudEvent{}, fmt.Errorf("%w: cloudevent: time %q is not RFC 3339", workflow.ErrInvalid, v)
			}
		case "datacontenttype":
			// Content-Type carries it in binary mode; a header of this
			// name is not an extension either.
		default:
			if e.Extensions == nil {
				e.Extensions = map[string]string{}
			}
			e.Extensions[name] = v
		}
	}
	body, err := readBody(r)
	if err != nil {
		return CloudEvent{}, err
	}
	if len(body) > 0 {
		e.Data = body
	}
	if err := e.Validate(); err != nil {
		return CloudEvent{}, err
	}
	return e, nil
}

// readBody reads r's body up to DefaultWebhookMaxBody bytes.
//
//ds:def id=events-readbody-f5zjvq7w owner=@khanakia stability=stable
func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, DefaultWebhookMaxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: cloudevent: read body: %v", workflow.ErrInvalid, err)
	}
	if len(body) > DefaultWebhookMaxBody {
		return nil, fmt.Errorf("%w: cloudevent: body is over %d bytes", workflow.ErrInvalid, DefaultWebhookMaxBody)
	}
	return body, nil
}

// percentEncode encodes a header value as the HTTP binding requires:
// space, double quote, percent and every byte outside printable ASCII
// become %XX.
func percentEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= 0x20 || c >= 0x7F || c == '"' || c == '%' {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// percentDecode reverses percentEncode and requires UTF-8.
func percentDecode(s string) (string, error) {
	v, err := url.PathUnescape(s)
	if err != nil {
		return "", err
	}
	if !utf8.ValidString(v) {
		return "", fmt.Errorf("value is not UTF-8 after percent-decoding")
	}
	return v, nil
}

// ToMessage turns a CloudEvent into a bus Message: ID to ID, Type to
// Name, Subject to Key, Data to Payload, and each extension to the label
// of the same name. Source, Time, DataContentType and DataSchema have no
// Message field and are dropped; a caller that needs one (a version in
// DataSchema) reads it from e before publishing. Extension names are
// lower-case letters and digits, so no label lands under a reserved
// prefix. Validate e first: ToMessage does not.
//
//ds:def id=events-tomessage-yu5mytv4 owner=@khanakia stability=stable
func ToMessage(e CloudEvent) Message {
	m := Message{ID: e.ID, Name: e.Type, Key: e.Subject, Payload: e.Data}
	if len(e.Extensions) > 0 {
		m.Labels = maps.Clone(e.Extensions)
	}
	return m
}

// FromMessage turns a bus Message into a CloudEvent from source: ID to
// ID, Name to Type, Key to Subject, Payload to Data, SpecVersion "1.0",
// and each label whose key is a valid extension name (ValidExtensionName:
// lower-case letters and digits, not a core attribute's name) to the
// extension of that name. Other labels ("tenant-id", "order.channel")
// have no CloudEvents spelling and are dropped, so a label meant to cross
// over is named like an extension. Version, Time and DataContentType are
// not set; with an empty DataContentType the JSON format reads JSON data
// as JSON.
//
//ds:def id=events-frommessage-nnam4pwr owner=@khanakia stability=stable
func FromMessage(m Message, source string) CloudEvent {
	e := CloudEvent{ID: m.ID, Source: source, SpecVersion: CloudEventsSpecVersion, Type: m.Name, Subject: m.Key, Data: m.Payload}
	for k, v := range m.Labels {
		if !ValidExtensionName(k) {
			continue
		}
		if e.Extensions == nil {
			e.Extensions = map[string]string{}
		}
		e.Extensions[k] = v
	}
	return e
}
