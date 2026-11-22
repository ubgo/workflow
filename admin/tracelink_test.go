package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

func httptestRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer admin")
	return req
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestTraceLink: the run detail carries the link fn builds; only an
// absolute http or https URL is shown, a panic means no link, lists never
// carry it, and without the option there is none.
//
//ds:def id=admin-testtracelink-nqdpbxrk owner=@khanakia stability=stable
func TestTraceLink(t *testing.T) {
	cases := []struct {
		name string
		link string
		want string
	}{
		{"https", "https://tracing.example.com/trace/0af7651916cd43dd8448eb211c80319c", "https://tracing.example.com/trace/0af7651916cd43dd8448eb211c80319c"},
		{"http upper-case scheme", "HTTP://jaeger.local:16686/trace/abc", "HTTP://jaeger.local:16686/trace/abc"},
		{"empty means none", "", ""},
		{"javascript refused", "javascript:alert(1)", ""},
		{"data refused", "data:text/html,<script>alert(1)</script>", ""},
		{"relative refused", "/trace/abc", ""},
		{"scheme-relative refused", "//evil.example.com/x", ""},
		{"no host refused", "https:///trace", ""},
		{"control character refused", "https://a.example.com/\nx", ""},
		{"panic", "panic", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var saw workflow.RunInfo
			h := NewHandler(New(seeded()), bearerAuth(), WithTraceLink(func(r workflow.RunInfo) string {
				saw = r
				if tc.link == "panic" {
					panic("bad template")
				}
				return tc.link
			}))
			rec := do(t, h, "GET", "/runs/r2", "")
			var out struct {
				TraceURL *string `json:"trace_url"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			got := ""
			if out.TraceURL != nil {
				got = *out.TraceURL
			}
			if got != tc.want {
				t.Errorf("trace_url %q, want %q", got, tc.want)
			}
			if saw.Ref.RunID != "r2" || saw.Labels["tenant"] != "a" {
				t.Errorf("fn saw %+v", saw)
			}
		})
	}
	h := NewHandler(New(seeded()), bearerAuth(), WithTraceLink(func(workflow.RunInfo) string { return "https://t.example.com/x" }))
	if body := do(t, h, "GET", "/runs", "").Body.String(); strings.Contains(body, "trace_url") {
		t.Errorf("a list carries trace_url: %s", body)
	}
	if body := do(t, NewHandler(New(seeded()), bearerAuth()), "GET", "/runs/r2", "").Body.String(); strings.Contains(body, "trace_url") {
		t.Errorf("trace_url without WithTraceLink: %s", body)
	}
}

// TestTraceLink_UIRendersIt: the run page builds the link with the DOM
// (no innerHTML), checks the scheme again, and opens it without a
// referrer; the CSP is unchanged.
func TestTraceLink_UIRendersIt(t *testing.T) {
	js := string(mustRead(t, "ui/app.js"))
	for _, want := range []string{"traceHref(r.trace_url)", "rel: 'noopener noreferrer'", "p.protocol === 'http:' || p.protocol === 'https:'"} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js lacks %q", want)
		}
	}
	if strings.Contains(js, "innerHTML") {
		t.Error("app.js uses innerHTML")
	}
	if !strings.Contains(uiCSP, "script-src 'self'") || strings.Contains(uiCSP, "unsafe-inline") {
		t.Errorf("CSP changed: %s", uiCSP)
	}
}

func mustRead(t *testing.T, name string) []byte {
	t.Helper()
	b, err := uiFiles.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
