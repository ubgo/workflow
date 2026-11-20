package workflow

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Message attributes (MQ-16): a job's metadata (its workflow, tenant,
// queue, idempotency key, priority, start time and labels) and its trace
// context crossing a hop that is not this library's own store: a broker's
// message attributes (SQS, SNS, Pub/Sub), Kafka or NATS headers, HTTP
// headers. AttributeCodec turns a Job into a flat string map and back;
// Attributes is the default, held to an AttributeLimits that says what
// the transport accepts. A job that does not fit is refused with
// ErrInvalid, never silently cut: a dropped traceparent or tenant is a
// bug no one sees.

// AttributeCodec maps a job's metadata to a transport's attributes and
// back. Attributes is the default; an app whose consumers expect other
// names implements its own.
type AttributeCodec interface {
	// Encode returns the attributes that carry job's metadata. The
	// payload is not encoded: it travels as the message body.
	Encode(job Job) (map[string]string, error)

	// Decode reads attributes Encode wrote (or a sender following the
	// same names) back into job metadata.
	Decode(attrs map[string]string) (DecodedAttributes, error)
}

// DecodedAttributes is what Decode read.
type DecodedAttributes struct {
	// Job carries the metadata: Workflow, Tenant, Queue, IdempotencyKey,
	// Priority, RunAt and Labels (trace labels excluded). Payload is nil.
	Job Job
	// Trace holds the trace context attributes under their transport
	// names (traceparent, tracestate, baggage), ready for a propagator's
	// map carrier on the consumer side. Empty when the message had none.
	Trace map[string]string
	// Other holds the attributes that are not this codec's, as received.
	Other map[string]string
}

// AttributeLimits is what a transport accepts as attribute keys and
// values. Zero numbers mean no limit; a nil ValidKey accepts any key.
type AttributeLimits struct {
	// Name names the transport in refusals.
	Name string
	// MaxCount is the most attributes one message may carry.
	MaxCount int
	// MaxKeyBytes and MaxValueBytes bound each key and value.
	MaxKeyBytes   int
	MaxValueBytes int
	// MaxTotalBytes bounds every key and value together.
	MaxTotalBytes int
	// ValidKey returns an error for a key the transport refuses (its
	// characters, a reserved prefix).
	ValidKey func(key string) error
	// ValidValue returns an error for a value the transport refuses
	// (control characters in a header). Values must always be valid
	// UTF-8.
	ValidValue func(value string) error
	// CaseInsensitive is true when the transport folds key case (HTTP
	// headers), so label keys that differ only in case cannot be told
	// apart and labels are always packed into one attribute.
	CaseInsensitive bool
}

// keyChars returns a ValidKey that accepts ASCII letters, digits and the
// characters in extra.
func keyChars(extra string) func(string) error {
	return func(k string) error {
		if k == "" {
			return fmt.Errorf("empty key")
		}
		for _, r := range k {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(extra, r)) {
				return fmt.Errorf("key %q has the character %q", k, r)
			}
		}
		return nil
	}
}

// Transport limits, as each documents them (checked 2026-11-20). Use one
// as Attributes.Limits, or write your own for another transport.
var (
	// SQSAttributes: at most 10 message attributes; names up to 256
	// characters of letters, digits, '_', '-' and '.', not starting or
	// ending with '.', without "..", and not starting with "AWS." or
	// "Amazon." in any case. The same rules hold for SNS.
	SQSAttributes = AttributeLimits{Name: "SQS", MaxCount: 10, MaxKeyBytes: 256, ValidKey: func(k string) error {
		if err := keyChars("_-.")(k); err != nil {
			return err
		}
		lk := strings.ToLower(k)
		switch {
		case strings.HasPrefix(k, ".") || strings.HasSuffix(k, ".") || strings.Contains(k, ".."):
			return fmt.Errorf("key %q starts or ends with '.' or holds \"..\"", k)
		case strings.HasPrefix(lk, "aws.") || strings.HasPrefix(lk, "amazon."):
			return fmt.Errorf("key %q uses a prefix AWS reserves", k)
		}
		return nil
	}}

	// PubSubAttributes: at most 100 attributes; keys up to 256 bytes,
	// values up to 1024 bytes; keys must not start with "goog".
	PubSubAttributes = AttributeLimits{Name: "Pub/Sub", MaxCount: 100, MaxKeyBytes: 256, MaxValueBytes: 1024, ValidKey: func(k string) error {
		if k == "" {
			return fmt.Errorf("empty key")
		}
		if strings.HasPrefix(strings.ToLower(k), "goog") {
			return fmt.Errorf("key %q uses the prefix Pub/Sub reserves", k)
		}
		return nil
	}}

	// KafkaHeaders: header keys are strings and values bytes, with no
	// count or size limit of their own (the record's size limit holds).
	KafkaHeaders = AttributeLimits{Name: "Kafka", ValidKey: func(k string) error {
		if k == "" {
			return fmt.Errorf("empty key")
		}
		return nil
	}}

	// NATSHeaders: header names of printable ASCII without ':' or
	// spaces, as in MIME headers; names are case-sensitive.
	NATSHeaders = AttributeLimits{Name: "NATS", ValidValue: noControl, ValidKey: func(k string) error {
		if k == "" {
			return fmt.Errorf("empty key")
		}
		for _, r := range k {
			if r <= ' ' || r > '~' || r == ':' {
				return fmt.Errorf("key %q has the character %q", k, r)
			}
		}
		return nil
	}}

	// HTTPHeaders: header names are tokens (letters, digits and
	// !#$%&'*+-.^_`|~) and case-insensitive; values hold no control
	// characters but tab, so no value can split a header.
	HTTPHeaders = AttributeLimits{Name: "HTTP", ValidKey: keyChars("!#$%&'*+-.^_`|~"), ValidValue: noControl, CaseInsensitive: true}
)

// noControl refuses control characters other than tab: a CR or LF in a
// header value would end the header.
func noControl(v string) error {
	for _, r := range v {
		if r < ' ' && r != '\t' || r == 0x7f {
			return fmt.Errorf("control character %q", r)
		}
	}
	return nil
}

// DefaultAttributePrefix starts the name of every attribute Attributes
// writes, except the trace context's standard names.
const DefaultAttributePrefix = "ubgo-"

// DefaultTraceAttributes maps the trace labels the otel module writes
// (LabelPrefixOTel plus the W3C propagator's keys) to the standard names
// other tracers read from message attributes and headers.
func DefaultTraceAttributes() map[string]string {
	return map[string]string{
		LabelPrefixOTel + "traceparent": "traceparent",
		LabelPrefixOTel + "tracestate":  "tracestate",
		LabelPrefixOTel + "baggage":     "baggage",
	}
}

// Attributes is the default AttributeCodec. It writes, under Prefix:
//
//	<prefix>workflow, <prefix>tenant, <prefix>queue,
//	<prefix>idempotency-key, <prefix>priority (decimal),
//	<prefix>run-at (RFC 3339, UTC, nanoseconds)
//
// each only when set; the trace labels under their standard names
// (Trace); and the other labels either one attribute each,
// <prefix>l-<label key>, or, when any key would not be a valid attribute
// name, the count would pass Limits.MaxCount, the transport folds case or
// PackLabels is set, all together as one JSON object in <prefix>labels.
// Decode reads both forms.
type Attributes struct {
	// Limits is what the transport accepts. The zero value accepts
	// anything.
	Limits AttributeLimits
	// Prefix starts every name but the trace ones; empty means
	// DefaultAttributePrefix.
	Prefix string
	// Trace maps label keys to attribute names; nil means
	// DefaultTraceAttributes.
	Trace map[string]string
	// PackLabels always packs labels into one attribute.
	PackLabels bool
}

var _ AttributeCodec = Attributes{}

// Names of the metadata attributes after the prefix.
const (
	attrWorkflow    = "workflow"
	attrTenant      = "tenant"
	attrQueue       = "queue"
	attrIdempotency = "idempotency-key"
	attrPriority    = "priority"
	attrRunAt       = "run-at"
	attrLabel       = "l-"
	attrLabels      = "labels"
)

func (a Attributes) prefix() string {
	if a.Prefix == "" {
		return DefaultAttributePrefix
	}
	return a.Prefix
}

func (a Attributes) trace() map[string]string {
	if a.Trace == nil {
		return DefaultTraceAttributes()
	}
	return a.Trace
}

// Encode implements AttributeCodec.
func (a Attributes) Encode(job Job) (map[string]string, error) {
	p := a.prefix()
	out := map[string]string{}
	set := func(k, v string) {
		if v != "" {
			out[k] = v
		}
	}
	set(p+attrWorkflow, job.Workflow)
	set(p+attrTenant, job.Tenant)
	set(p+attrQueue, job.Queue)
	set(p+attrIdempotency, job.IdempotencyKey)
	if job.Priority != 0 {
		out[p+attrPriority] = strconv.Itoa(job.Priority)
	}
	if !job.RunAt.IsZero() {
		out[p+attrRunAt] = job.RunAt.UTC().Format(time.RFC3339Nano)
	}
	trace := a.trace()
	labels := map[string]string{}
	for k, v := range job.Labels {
		if name, ok := trace[k]; ok {
			out[name] = v
			continue
		}
		labels[k] = v
	}
	if len(labels) > 0 {
		perLabel := !a.PackLabels && !a.Limits.CaseInsensitive &&
			(a.Limits.MaxCount == 0 || len(out)+len(labels) <= a.Limits.MaxCount)
		for k := range labels {
			if !perLabel {
				break
			}
			perLabel = a.checkKey(p+attrLabel+k) == nil
		}
		if perLabel {
			for k, v := range labels {
				out[p+attrLabel+k] = v
			}
		} else {
			b, err := json.Marshal(labels)
			if err != nil {
				return nil, invalidf("attributes: labels: %v", err)
			}
			out[p+attrLabels] = string(b)
		}
	}
	if err := a.check(out); err != nil {
		return nil, err
	}
	return out, nil
}

// checkKey applies the limits to one key.
func (a Attributes) checkKey(k string) error {
	if a.Limits.MaxKeyBytes > 0 && len(k) > a.Limits.MaxKeyBytes {
		return fmt.Errorf("key %q is %d bytes, over %d", k, len(k), a.Limits.MaxKeyBytes)
	}
	if a.Limits.ValidKey != nil {
		return a.Limits.ValidKey(k)
	}
	return nil
}

// check applies the limits to a whole encoding.
func (a Attributes) check(attrs map[string]string) error {
	name := a.Limits.Name
	if name == "" {
		name = "transport"
	}
	if a.Limits.MaxCount > 0 && len(attrs) > a.Limits.MaxCount {
		return invalidf("attributes: %d attributes, %s takes at most %d", len(attrs), name, a.Limits.MaxCount)
	}
	total := 0
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		v := attrs[k]
		if err := a.checkKey(k); err != nil {
			return invalidf("attributes: %s: %v", name, err)
		}
		if !utf8.ValidString(v) {
			return invalidf("attributes: %s: the value of %q is not valid UTF-8", name, k)
		}
		if a.Limits.ValidValue != nil {
			if err := a.Limits.ValidValue(v); err != nil {
				return invalidf("attributes: %s: the value of %q: %v", name, k, err)
			}
		}
		if a.Limits.MaxValueBytes > 0 && len(v) > a.Limits.MaxValueBytes {
			return invalidf("attributes: %s: the value of %q is %d bytes, over %d", name, k, len(v), a.Limits.MaxValueBytes)
		}
		total += len(k) + len(v)
	}
	if a.Limits.MaxTotalBytes > 0 && total > a.Limits.MaxTotalBytes {
		return invalidf("attributes: %s: %d bytes of attributes, over %d", name, total, a.Limits.MaxTotalBytes)
	}
	return nil
}

// Decode implements AttributeCodec. With Limits.CaseInsensitive the names
// match in any case.
func (a Attributes) Decode(attrs map[string]string) (DecodedAttributes, error) {
	p := a.prefix()
	norm := func(s string) string { return s }
	if a.Limits.CaseInsensitive {
		norm = strings.ToLower
		p = strings.ToLower(p)
	}
	traceNames := map[string]string{} // normalised attribute name -> label key
	for label, name := range a.trace() {
		traceNames[norm(name)] = label
	}
	var d DecodedAttributes
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		v, nk := attrs[k], norm(k)
		if _, ok := traceNames[nk]; ok {
			if d.Trace == nil {
				d.Trace = map[string]string{}
			}
			d.Trace[nk] = v
			continue
		}
		rest, ours := strings.CutPrefix(nk, p)
		if !ours {
			if d.Other == nil {
				d.Other = map[string]string{}
			}
			d.Other[k] = v
			continue
		}
		switch rest {
		case attrWorkflow:
			d.Job.Workflow = v
		case attrTenant:
			d.Job.Tenant = v
		case attrQueue:
			d.Job.Queue = v
		case attrIdempotency:
			d.Job.IdempotencyKey = v
		case attrPriority:
			n, err := strconv.Atoi(v)
			if err != nil {
				return DecodedAttributes{}, invalidf("attributes: %s is %q, not a number", k, v)
			}
			d.Job.Priority = n
		case attrRunAt:
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return DecodedAttributes{}, invalidf("attributes: %s is %q, not an RFC 3339 time", k, v)
			}
			d.Job.RunAt = t
		case attrLabels:
			var m map[string]string
			if err := json.Unmarshal([]byte(v), &m); err != nil {
				return DecodedAttributes{}, invalidf("attributes: %s is not a JSON object of strings: %v", k, err)
			}
			if d.Job.Labels == nil {
				d.Job.Labels = map[string]string{}
			}
			maps.Copy(d.Job.Labels, m)
		default:
			if !strings.HasPrefix(rest, attrLabel) {
				if d.Other == nil {
					d.Other = map[string]string{}
				}
				d.Other[k] = v
				continue
			}
			if d.Job.Labels == nil {
				d.Job.Labels = map[string]string{}
			}
			// The label key as sent: the remainder of the original key
			// (case folding keeps an ASCII prefix's length).
			d.Job.Labels[k[len(k)-len(rest)+len(attrLabel):]] = v
		}
	}
	return d, nil
}

// TraceLabels returns the decoded trace context as the labels the otel
// module reads (DefaultTraceAttributes reversed, or the codec's Trace),
// for a consumer that enqueues the message as a job and wants its steps
// to continue the sender's trace. Such labels are reserved: enqueue them
// under AllowReservedPrefix(ctx, LabelPrefixOTel), and not through the
// otel module's enqueue middleware, which writes its own.
func (a Attributes) TraceLabels(d DecodedAttributes) map[string]string {
	if len(d.Trace) == 0 {
		return nil
	}
	out := map[string]string{}
	for label, name := range a.trace() {
		key := name
		if a.Limits.CaseInsensitive {
			key = strings.ToLower(name)
		}
		if v, ok := d.Trace[key]; ok {
			out[label] = v
		}
	}
	return out
}

// SetHeader writes attrs into h, one header per attribute, replacing
// headers of the same names. Encode with HTTPHeaders limits first.
func SetHeader(h http.Header, attrs map[string]string) {
	for k, v := range attrs {
		h.Set(k, v)
	}
}

// HeaderAttributes reads every header of h as an attribute (the first
// value of each, under its canonical name), for Decode with HTTPHeaders
// limits, which match names in any case.
func HeaderAttributes(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) > 0 {
			out[k] = vs[0]
		}
	}
	return out
}
