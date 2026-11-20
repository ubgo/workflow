package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// TestCloudEventSpecJSONExamples reads the JSON examples of the CloudEvents
// 1.0 JSON format spec (text data, JSON data, binary data in data_base64)
// and checks every attribute, then that each encodes and decodes back to
// the same event.
func TestCloudEventSpecJSONExamples(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want events.CloudEvent
	}{
		{"xml text with extensions", `{
			"specversion" : "1.0",
			"type" : "com.github.pull_request.opened",
			"source" : "https://github.com/cloudevents/spec/pull",
			"subject" : "123",
			"id" : "A234-1234-1234",
			"time" : "2018-04-05T17:31:00Z",
			"comexampleextension1" : "value",
			"comexampleothervalue" : 5,
			"datacontenttype" : "text/xml",
			"data" : "<much wow=\"xml\"/>"
		}`, events.CloudEvent{
			SpecVersion: "1.0", Type: "com.github.pull_request.opened", Source: "https://github.com/cloudevents/spec/pull",
			Subject: "123", ID: "A234-1234-1234", Time: time.Date(2018, 4, 5, 17, 31, 0, 0, time.UTC),
			Extensions:      map[string]string{"comexampleextension1": "value", "comexampleothervalue": "5"},
			DataContentType: "text/xml", Data: []byte(`<much wow="xml"/>`),
		}},
		{"json data", `{
			"specversion" : "1.0",
			"type" : "com.example.someevent",
			"source" : "/mycontext",
			"id" : "C234-1234-1234",
			"time" : "2018-04-05T17:31:00Z",
			"comexampleextension1" : "value",
			"datacontenttype" : "application/json",
			"data" : { "appinfoA" : "abc", "appinfoB" : 123, "appinfoC" : true }
		}`, events.CloudEvent{
			SpecVersion: "1.0", Type: "com.example.someevent", Source: "/mycontext", ID: "C234-1234-1234",
			Time: time.Date(2018, 4, 5, 17, 31, 0, 0, time.UTC), Extensions: map[string]string{"comexampleextension1": "value"},
			DataContentType: "application/json", Data: []byte(`{ "appinfoA" : "abc", "appinfoB" : 123, "appinfoC" : true }`),
		}},
		{"binary data_base64", `{
			"specversion" : "1.0",
			"type" : "com.example.someevent",
			"source" : "/mycontext",
			"id" : "B234-1234-1234",
			"datacontenttype" : "application/vnd.apache.thrift.binary",
			"data_base64" : "AAEC/w=="
		}`, events.CloudEvent{
			SpecVersion: "1.0", Type: "com.example.someevent", Source: "/mycontext", ID: "B234-1234-1234",
			DataContentType: "application/vnd.apache.thrift.binary", Data: []byte{0, 1, 2, 0xff},
		}},
		{"no data, null optional attribute", `{"specversion":"1.0","type":"t","source":"/s","id":"1","subject":null}`,
			events.CloudEvent{SpecVersion: "1.0", Type: "t", Source: "/s", ID: "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got events.CloudEvent
			if err := json.Unmarshal([]byte(tc.in), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("decoded\n%+v\nwant\n%+v", got, tc.want)
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var back events.CloudEvent
			if err := json.Unmarshal(b, &back); err != nil {
				t.Fatalf("re-decode %s: %v", b, err)
			}
			// JSON data comes back compacted; compare it as JSON.
			if json.Valid(tc.want.Data) && strings.HasPrefix(tc.want.DataContentType, "application/json") {
				var c bytes.Buffer
				_ = json.Compact(&c, tc.want.Data)
				tc.want.Data = c.Bytes()
			}
			if !reflect.DeepEqual(back, tc.want) {
				t.Fatalf("round trip through %s\n%+v\nwant\n%+v", b, back, tc.want)
			}
		})
	}
}

// TestCloudEventMarshalIsStable pins the exact encoding: attribute order,
// sorted extensions, and the data member chosen by content type.
func TestCloudEventMarshalIsStable(t *testing.T) {
	base := events.CloudEvent{SpecVersion: "1.0", ID: "1", Source: "/shop", Type: "order.paid", Subject: "42",
		Time: time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC), Extensions: map[string]string{"zeta": "z", "alpha": "a"}}
	cases := []struct {
		name, ct string
		data     []byte
		want     string
	}{
		{"json, no content type", "", []byte(`{ "order": 42 }`), `"data":{"order":42}`},
		{"json content type", "application/json", []byte(`[1, 2]`), `"datacontenttype":"application/json","alpha":"a","zeta":"z","data":[1,2]`},
		{"text", "text/plain; charset=utf-8", []byte("hi \"you\""), `"data":"hi \"you\""`},
		{"non-JSON under no content type goes base64", "", []byte("not json"), `"data_base64":"bm90IGpzb24="`},
		{"binary", "application/octet-stream", []byte{0, 255}, `"data_base64":"AP8="`},
		{"invalid UTF-8 text goes base64", "text/plain", []byte{0xff}, `"data_base64":"/w=="`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := base
			e.DataContentType, e.Data = tc.ct, tc.data
			b, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(b), `{"specversion":"1.0","id":"1","source":"/shop","type":"order.paid","subject":"42","time":"2026-11-20T09:00:00Z"`) {
				t.Fatalf("attribute order: %s", b)
			}
			if !strings.Contains(string(b), tc.want) {
				t.Fatalf("%s\ndoes not contain %s", b, tc.want)
			}
		})
	}
}

// TestCloudEventRefusals: every rule Validate and the JSON reader keep.
func TestCloudEventRefusals(t *testing.T) {
	ok := events.CloudEvent{SpecVersion: "1.0", ID: "1", Source: "/s", Type: "t"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid event refused: %v", err)
	}
	mod := func(f func(*events.CloudEvent)) events.CloudEvent { e := ok; f(&e); return e }
	invalid := map[string]events.CloudEvent{
		"no id":                     mod(func(e *events.CloudEvent) { e.ID = "" }),
		"no source":                 mod(func(e *events.CloudEvent) { e.Source = "" }),
		"no type":                   mod(func(e *events.CloudEvent) { e.Type = "" }),
		"old specversion":           mod(func(e *events.CloudEvent) { e.SpecVersion = "0.3" }),
		"no specversion":            mod(func(e *events.CloudEvent) { e.SpecVersion = "" }),
		"upper-case extension":      mod(func(e *events.CloudEvent) { e.Extensions = map[string]string{"Tenant": "a"} }),
		"dashed extension":          mod(func(e *events.CloudEvent) { e.Extensions = map[string]string{"tenant-id": "a"} }),
		"extension named data":      mod(func(e *events.CloudEvent) { e.Extensions = map[string]string{"data": "a"} }),
		"bad media type":            mod(func(e *events.CloudEvent) { e.DataContentType = "not a type/" }),
		"relative dataschema":       mod(func(e *events.CloudEvent) { e.DataSchema = "/schemas/v2" }),
		"source that is not a URI":  mod(func(e *events.CloudEvent) { e.Source = "%zz" }),
		"invalid JSON in JSON type": mod(func(e *events.CloudEvent) { e.DataContentType = "application/json"; e.Data = []byte("{") }),
	}
	for name, e := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(e); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("Marshal err = %v, want ErrInvalid", err)
			}
		})
	}
	bad := map[string]string{
		"data and data_base64": `{"specversion":"1.0","id":"1","source":"/s","type":"t","data":1,"data_base64":"AA=="}`,
		"id not a string":      `{"specversion":"1.0","id":1,"source":"/s","type":"t"}`,
		"bad time":             `{"specversion":"1.0","id":"1","source":"/s","type":"t","time":"yesterday"}`,
		"object extension":     `{"specversion":"1.0","id":"1","source":"/s","type":"t","ext":{"a":1}}`,
		"missing type":         `{"specversion":"1.0","id":"1","source":"/s"}`,
		"bad base64":           `{"specversion":"1.0","id":"1","source":"/s","type":"t","data_base64":"!!"}`,
		"not an object":        `[1]`,
	}
	for name, in := range bad {
		t.Run(name, func(t *testing.T) {
			var e events.CloudEvent
			if err := json.Unmarshal([]byte(in), &e); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("Unmarshal err = %v, want ErrInvalid", err)
			}
		})
	}
}

// TestCloudEventHTTPBinaryMode writes an event in binary mode, checks the
// headers and percent-encoding the HTTP binding requires, and reads it
// back from a real request.
func TestCloudEventHTTPBinaryMode(t *testing.T) {
	e := events.CloudEvent{SpecVersion: "1.0", ID: "1234-1234-1234", Source: "/mycontext/subcontext", Type: "com.example.someevent",
		Time: time.Date(2018, 4, 5, 3, 56, 24, 0, time.UTC), Subject: "Hello World ü %", DataContentType: "application/json; charset=utf-8",
		Data: []byte(`{"order":42}`), Extensions: map[string]string{"tenant": "acme"}}
	h := http.Header{}
	body, err := e.WriteHTTP(h)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Ce-Specversion": "1.0", "Ce-Id": "1234-1234-1234", "Ce-Source": "/mycontext/subcontext", "Ce-Type": "com.example.someevent",
		"Ce-Time": "2018-04-05T03:56:24Z", "Ce-Subject": "Hello%20World%20%C3%BC%20%25", "Ce-Tenant": "acme",
		"Content-Type": "application/json; charset=utf-8",
	}
	for k, v := range want {
		if got := h.Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
	if string(body) != `{"order":42}` {
		t.Fatalf("body = %s", body)
	}
	got := roundTrip(t, h, body)
	if !reflect.DeepEqual(got, e) {
		t.Fatalf("read back\n%+v\nwant\n%+v", got, e)
	}
}

// TestCloudEventHTTPSpecBinaryExample reads the HTTP binding spec's
// binary-mode example request.
func TestCloudEventHTTPSpecBinaryExample(t *testing.T) {
	h := http.Header{}
	h.Set("ce-specversion", "1.0")
	h.Set("ce-type", "com.example.someevent")
	h.Set("ce-time", "2018-04-05T03:56:24Z")
	h.Set("ce-id", "1234-1234-1234")
	h.Set("ce-source", "/mycontext/subcontext")
	h.Set("Content-Type", "application/json; charset=utf-8")
	got := roundTrip(t, h, []byte(`{"much":"wow"}`))
	want := events.CloudEvent{SpecVersion: "1.0", Type: "com.example.someevent", Time: time.Date(2018, 4, 5, 3, 56, 24, 0, time.UTC),
		ID: "1234-1234-1234", Source: "/mycontext/subcontext", DataContentType: "application/json; charset=utf-8", Data: []byte(`{"much":"wow"}`)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got\n%+v\nwant\n%+v", got, want)
	}
}

// TestCloudEventHTTPStructuredMode writes and reads structured mode.
func TestCloudEventHTTPStructuredMode(t *testing.T) {
	e := events.CloudEvent{SpecVersion: "1.0", ID: "1", Source: "/s", Type: "t", Data: []byte(`{"a":1}`)}
	h := http.Header{}
	body, err := e.WriteHTTPStructured(h)
	if err != nil {
		t.Fatal(err)
	}
	if ct := h.Get("Content-Type"); !strings.HasPrefix(ct, events.ContentTypeCloudEventsJSON) {
		t.Fatalf("Content-Type = %q", ct)
	}
	if h.Get("Ce-Id") != "" {
		t.Fatal("structured mode wrote ce- headers")
	}
	if got := roundTrip(t, h, body); !reflect.DeepEqual(got, e) {
		t.Fatalf("got %+v, want %+v", got, e)
	}
}

// TestReadHTTPRefusals: requests that are not a CloudEvent this package
// reads are refused with ErrInvalid.
func TestReadHTTPRefusals(t *testing.T) {
	cases := map[string]struct {
		h    map[string]string
		body string
	}{
		"plain JSON, no ce headers": {map[string]string{"Content-Type": "application/json"}, `{}`},
		"batch mode":                {map[string]string{"Content-Type": "application/cloudevents-batch+json"}, `[]`},
		"other event format":        {map[string]string{"Content-Type": "application/cloudevents+avro"}, ``},
		"binary without id":         {map[string]string{"ce-specversion": "1.0", "ce-source": "/s", "ce-type": "t"}, ``},
		"bad percent-encoding":      {map[string]string{"ce-specversion": "1.0", "ce-id": "%zz", "ce-source": "/s", "ce-type": "t"}, ``},
		"structured but invalid":    {map[string]string{"Content-Type": events.ContentTypeCloudEventsJSON}, `{"id":"1"}`},
		"body over the cap": {map[string]string{"ce-specversion": "1.0", "ce-id": "1", "ce-source": "/s", "ce-type": "t"},
			strings.Repeat("x", events.DefaultWebhookMaxBody+1)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			for k, v := range tc.h {
				r.Header.Set(k, v)
			}
			if _, err := events.ReadHTTP(r); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

// roundTrip sends h and body to a server that reads a CloudEvent.
func roundTrip(t *testing.T, h http.Header, body []byte) events.CloudEvent {
	t.Helper()
	var got events.CloudEvent
	var readErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, readErr = events.ReadHTTP(r)
	}))
	defer srv.Close()
	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = h.Clone()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	return got
}

// TestCloudEventMessageMapping: ToMessage and FromMessage map ID, type,
// subject, data and extensions as documented, drop labels with no
// extension spelling, and a CloudEvent published on the bus starts a run
// carrying its extensions as labels.
func TestCloudEventMessageMapping(t *testing.T) {
	e := events.CloudEvent{SpecVersion: "1.0", ID: "evt_1", Source: "/shop", Type: "order.paid", Subject: "42",
		DataSchema: "https://shop.example/schemas/order-paid/v2", Data: []byte(`{"order":42}`), Extensions: map[string]string{"tenant": "acme"}}
	m := events.ToMessage(e)
	want := events.Message{ID: "evt_1", Name: "order.paid", Key: "42", Payload: []byte(`{"order":42}`), Labels: map[string]string{"tenant": "acme"}}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("ToMessage = %+v, want %+v", m, want)
	}
	m.Labels["tenant-id"] = "dropped" // no extension spelling
	m.Labels["data"] = "dropped"      // a core attribute's name
	back := events.FromMessage(m, "/shop")
	if back.Source != "/shop" || back.ID != "evt_1" || back.Type != "order.paid" || back.Subject != "42" || back.SpecVersion != "1.0" ||
		!reflect.DeepEqual(back.Extensions, map[string]string{"tenant": "acme"}) || string(back.Data) != `{"order":42}` {
		t.Fatalf("FromMessage = %+v", back)
	}
	if err := back.Validate(); err != nil {
		t.Fatalf("FromMessage gave an invalid event: %v", err)
	}

	var rec recorder
	c := started(t, rec.workflow("receipt"))
	b, err := events.New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe(events.Subscription{Name: "receipt", Event: "order.paid", Workflow: "receipt"}); err != nil {
		t.Fatal(err)
	}
	p, err := b.Publish(context.Background(), events.ToMessage(e))
	if err != nil {
		t.Fatal(err)
	}
	waitAll(t, c, p)
	if rec.count() != 1 || rec.runs[0].Labels["tenant"] != "acme" || rec.ins[0] != `{"order":42}` {
		t.Fatalf("run = %+v %v", rec.runs, rec.ins)
	}
}
