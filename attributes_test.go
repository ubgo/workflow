package workflow_test

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func sampleJob() workflow.Job {
	return workflow.Job{
		Workflow: "invoice", Tenant: "acme", Queue: "billing", IdempotencyKey: "inv/42", Priority: -3,
		RunAt: time.Date(2026, 11, 20, 9, 30, 0, 123456789, time.FixedZone("CET", 3600)),
		Labels: map[string]string{
			"source":                                 "shop",
			"Order-ID":                               "42",
			workflow.LabelPrefixOTel + "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			workflow.LabelPrefixOTel + "tracestate":  "vendor=1",
		},
	}
}

// TestAttributesRoundTrip: every transport profile carries the whole
// metadata and the trace context across, Decode(Encode(job)) giving the
// job back and the trace under its standard names.
func TestAttributesRoundTrip(t *testing.T) {
	job := sampleJob()
	for _, lim := range []workflow.AttributeLimits{{}, workflow.SQSAttributes, workflow.PubSubAttributes, workflow.KafkaHeaders, workflow.NATSHeaders, workflow.HTTPHeaders} {
		t.Run(lim.Name, func(t *testing.T) {
			codec := workflow.Attributes{Limits: lim}
			attrs, err := codec.Encode(job)
			if err != nil {
				t.Fatal(err)
			}
			if attrs["traceparent"] != job.Labels["otel.traceparent"] || attrs["tracestate"] != "vendor=1" {
				t.Fatalf("trace not under its standard names: %v", attrs)
			}
			if lim.CaseInsensitive { // through real HTTP headers
				h := http.Header{}
				workflow.SetHeader(h, attrs)
				attrs = workflow.HeaderAttributes(h)
			}
			d, err := codec.Decode(attrs)
			if err != nil {
				t.Fatal(err)
			}
			want := job
			want.RunAt = job.RunAt.UTC()
			want.Labels = map[string]string{"source": "shop", "Order-ID": "42"}
			if d.Job.Workflow != want.Workflow || d.Job.Tenant != want.Tenant || d.Job.Queue != want.Queue ||
				d.Job.IdempotencyKey != want.IdempotencyKey || d.Job.Priority != want.Priority || !d.Job.RunAt.Equal(want.RunAt) ||
				!maps.Equal(d.Job.Labels, want.Labels) {
				t.Fatalf("decoded %+v\nwant %+v", d.Job, want)
			}
			if got := codec.TraceLabels(d); !maps.Equal(got, map[string]string{"otel.traceparent": job.Labels["otel.traceparent"], "otel.tracestate": "vendor=1"}) {
				t.Fatalf("TraceLabels = %v", got)
			}
		})
	}
}

// TestAttributesPacking: labels go one attribute each when they fit and
// into one JSON attribute when a key is not a valid name, the count would
// pass the limit, or the transport folds case.
func TestAttributesPacking(t *testing.T) {
	job := workflow.Job{Workflow: "w", Labels: map[string]string{"a": "1", "b": "2"}}
	attrs, err := workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(job)
	if err != nil {
		t.Fatal(err)
	}
	if attrs["ubgo-l-a"] != "1" || attrs["ubgo-labels"] != "" {
		t.Fatalf("small labels on SQS = %v; want one attribute each", attrs)
	}
	job.Labels["ubgo.workflow/batch"] = "b1" // '/' is not valid in an SQS name
	if attrs, _ = (workflow.Attributes{Limits: workflow.SQSAttributes}).Encode(job); attrs["ubgo-labels"] == "" || attrs["ubgo-l-a"] != "" {
		t.Fatalf("a label key SQS cannot name = %v; want packed", attrs)
	}
	many := workflow.Job{Workflow: "w", Labels: map[string]string{}}
	for i := range 12 {
		many.Labels[fmt.Sprintf("k%d", i)] = "v"
	}
	attrs, err = workflow.Attributes{Limits: workflow.SQSAttributes}.Encode(many)
	if err != nil || len(attrs) != 2 {
		t.Fatalf("12 labels on SQS = %d attributes, %v; want workflow plus one packed attribute", len(attrs), err)
	}
	d, _ := workflow.Attributes{Limits: workflow.SQSAttributes}.Decode(attrs)
	if !maps.Equal(d.Job.Labels, many.Labels) {
		t.Fatalf("packed labels decoded to %v", d.Job.Labels)
	}
	if attrs, _ = (workflow.Attributes{PackLabels: true}).Encode(job); attrs["ubgo-labels"] == "" {
		t.Fatal("PackLabels did not pack")
	}
	custom := workflow.Attributes{Prefix: "x-job-", Trace: map[string]string{"tp": "traceparent"}}
	attrs, _ = custom.Encode(workflow.Job{Workflow: "w", Labels: map[string]string{"tp": "00-1", "k": "v"}})
	if attrs["x-job-workflow"] != "w" || attrs["traceparent"] != "00-1" || attrs["x-job-l-k"] != "v" {
		t.Fatalf("custom prefix and trace map = %v", attrs)
	}
}

// TestAttributesRefuse: what does not fit is refused with ErrInvalid,
// never cut.
func TestAttributesRefuse(t *testing.T) {
	cases := map[string]struct {
		lim workflow.AttributeLimits
		job workflow.Job
	}{
		"Pub/Sub value over 1024 bytes": {workflow.PubSubAttributes, workflow.Job{Workflow: "w", IdempotencyKey: strings.Repeat("k", 1025)}},
		"SQS over 10 attributes": {workflow.SQSAttributes, workflow.Job{Workflow: "w", Tenant: "t", Queue: "q", IdempotencyKey: "i", Priority: 1, RunAt: time.Unix(1, 0),
			Labels: map[string]string{"x": "1", "otel.traceparent": "a", "otel.tracestate": "b", "otel.baggage": "c", "otel.extra": "d", "y": "2"}}},
		"HTTP value with a newline": {workflow.HTTPHeaders, workflow.Job{Workflow: "w", Tenant: "a\r\nX-Evil: 1"}},
		"invalid UTF-8 value":       {workflow.KafkaHeaders, workflow.Job{Workflow: "w", Tenant: "\xff"}},
		"total over the limit":      {workflow.AttributeLimits{MaxTotalBytes: 20}, workflow.Job{Workflow: "a-long-workflow-name"}},
		"SQS reserved prefix":       {workflow.SQSAttributes, workflow.Job{Workflow: "w", Labels: map[string]string{"otel.traceparent": "x"}}},
		"Pub/Sub reserved prefix":   {workflow.PubSubAttributes, workflow.Job{Workflow: "w"}},
		"key over the limit (SQS)":  {workflow.SQSAttributes, workflow.Job{Workflow: "w"}},
		"NATS value with a newline": {workflow.NATSHeaders, workflow.Job{Workflow: "w\n"}},
	}
	codecs := map[string]workflow.Attributes{
		"SQS reserved prefix":     {Limits: workflow.SQSAttributes, Trace: map[string]string{"otel.traceparent": "AWS.trace"}},
		"Pub/Sub reserved prefix": {Limits: workflow.PubSubAttributes, Prefix: "goog-"},
		"SQS over 10 attributes": {Limits: workflow.SQSAttributes, Trace: map[string]string{
			"otel.traceparent": "traceparent", "otel.tracestate": "tracestate", "otel.baggage": "baggage", "otel.extra": "x-extra"}},
		"key over the limit (SQS)": {Limits: workflow.SQSAttributes, Prefix: strings.Repeat("p", 250)},
	}
	for name, c := range cases {
		codec, ok := codecs[name]
		if !ok {
			codec = workflow.Attributes{Limits: c.lim}
		}
		if attrs, err := codec.Encode(c.job); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: Encode = %v, %v; want ErrInvalid", name, attrs, err)
		}
	}
	for name, attrs := range map[string]map[string]string{
		"priority": {"ubgo-priority": "high"},
		"run-at":   {"ubgo-run-at": "tomorrow"},
		"labels":   {"ubgo-labels": "[1]"},
	} {
		if _, err := (workflow.Attributes{}).Decode(attrs); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Decode of a bad %s = %v; want ErrInvalid", name, err)
		}
	}
}

// TestAttributesDecodeOther: attributes that are not the codec's are
// returned as Other, so a consumer still sees the sender's own.
func TestAttributesDecodeOther(t *testing.T) {
	d, err := workflow.Attributes{}.Decode(map[string]string{"ubgo-workflow": "w", "x-sender": "erp", "ubgo-unknown": "?"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Job.Workflow != "w" || d.Other["x-sender"] != "erp" || d.Other["ubgo-unknown"] != "?" || d.Trace != nil {
		t.Fatalf("decoded %+v", d)
	}
}
