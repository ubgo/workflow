package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
)

// TestUI_FilesEmbedded: every UI file is served with its type and the
// page's security headers, and a name outside the set is a 404.
func TestUI_FilesEmbedded(t *testing.T) {
	h := NewHandler(New(seeded()), bearerAuth())
	for name, ctype := range uiTypes {
		rec := do(t, h, "GET", "/ui/"+name, "")
		if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != ctype || rec.Body.Len() == 0 {
			t.Errorf("%s = %d %q (%d bytes)", name, rec.Code, rec.Header().Get("Content-Type"), rec.Body.Len())
		}
		for hd, want := range map[string]string{
			"Content-Security-Policy": uiCSP, "X-Content-Type-Options": "nosniff",
			"X-Frame-Options": "DENY", "Cache-Control": "no-store",
		} {
			if got := rec.Header().Get(hd); got != want {
				t.Errorf("%s: %s = %q, want %q", name, hd, got, want)
			}
		}
	}
	if rec := do(t, h, "GET", "/ui/", ""); !strings.Contains(rec.Body.String(), `<script src="app.js"`) {
		t.Errorf("/ui/ is not the page: %s", rec.Body)
	}
	for _, p := range []string{"/ui/nope.js", "/ui/ui/app.js", "/ui/doc.go"} {
		if rec := do(t, h, "GET", p, ""); rec.Code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", p, rec.Code)
		}
	}
	// ServeMux cleans a dotted path with a redirect before routing; the
	// cleaned path is not a UI file either.
	if rec := do(t, h, "GET", "/ui/../http.go", ""); rec.Code == http.StatusOK {
		t.Errorf("dotted path served: %s", rec.Body)
	}
}

// TestUI_RedirectKeepsPrefix: GET / and GET /ui answer a relative
// Location, so under a mount prefix the browser stays under it.
func TestUI_RedirectKeepsPrefix(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/ops/admin/", http.StripPrefix("/ops/admin", NewHandler(New(seeded()), bearerAuth())))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cl := srv.Client()
	for _, p := range []string{"/ops/admin/", "/ops/admin/ui"} {
		req, _ := http.NewRequest("GET", srv.URL+p, nil)
		req.Header.Set("Authorization", "Bearer admin")
		// Follow by hand: the client would drop the header on redirect.
		cl.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := cl.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		loc, err := resp.Location()
		if err != nil || resp.StatusCode != http.StatusFound || loc.Path != "/ops/admin/ui/" {
			t.Errorf("%s = %d to %v (%v), want 302 to /ops/admin/ui/", p, resp.StatusCode, loc, err)
		}
	}
}

// TestUI_NoThirdPartyOrInlineCode is a lint over the embedded files: no
// URL to another host (the UI must work offline and leak nothing), no
// inline script or style the CSP would refuse, and no HTML built from
// strings in the script, since run errors and labels are app-written
// text that must only ever reach the page as text.
func TestUI_NoThirdPartyOrInlineCode(t *testing.T) {
	remote := regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`)
	for name := range uiTypes {
		b, err := fs.ReadFile(uiFiles, "ui/"+name)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		// The favicon is an inline SVG whose xmlns is a name, not a fetch.
		src = strings.ReplaceAll(src, "http://www.w3.org/2000/svg", "")
		if m := remote.FindString(src); m != "" {
			t.Errorf("%s references %q; the UI loads nothing from another host", name, m)
		}
		switch name {
		case "index.html":
			for _, bad := range []string{"<script>", "<style", " style=", " onclick=", " onload=", "javascript:"} {
				if strings.Contains(src, bad) {
					t.Errorf("index.html has %q, which the CSP refuses", bad)
				}
			}
		case "app.js":
			for _, bad := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function", ".style.", "'style'"} {
				if strings.Contains(src, bad) {
					t.Errorf("app.js uses %q", bad)
				}
			}
		}
	}
}

func TestHandler_RunCount(t *testing.T) {
	f := seeded()
	h := NewHandler(New(f), bearerAuth())
	var out struct {
		Runs      []runJSON `json:"runs"`
		Total     *int      `json:"total"`
		Truncated bool      `json:"truncated"`
		Cursors   []string  `json:"cursors"`
	}
	rec := do(t, h, "GET", "/runs?limit=1&count=1", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s %v", rec.Code, rec.Body, err)
	}
	if out.Total == nil || *out.Total != 3 || out.Truncated || !slices.Equal(out.Cursors, []string{"", "1", "2"}) || len(out.Runs) != 1 {
		t.Errorf("count = %+v", out)
	}
	// The count starts at the first page whatever cursor opened this one.
	out.Total, out.Cursors = nil, nil
	_ = json.Unmarshal(do(t, h, "GET", "/runs?limit=1&count=1&cursor=2", "").Body.Bytes(), &out)
	if out.Total == nil || *out.Total != 3 || len(out.Cursors) != 3 || out.Runs[0].Ref.RunID != "r1" {
		t.Errorf("count from page 3 = %+v", out)
	}
	// Without count=1 the fields are absent.
	if rec := do(t, h, "GET", "/runs", ""); strings.Contains(rec.Body.String(), `"total"`) || strings.Contains(rec.Body.String(), `"cursors"`) {
		t.Errorf("count fields without count=1: %s", rec.Body)
	}
	// /failures counts its own status only.
	out.Total, out.Cursors = nil, nil
	_ = json.Unmarshal(do(t, h, "GET", "/failures?count=1", "").Body.Bytes(), &out)
	if out.Total == nil || *out.Total != 1 || !slices.Equal(out.Cursors, []string{""}) {
		t.Errorf("failures count = %+v", out)
	}
	// A label filter is counted too: label search drives the same pages.
	out.Total = nil
	_ = json.Unmarshal(do(t, h, "GET", "/runs?label=tenant=a&count=1", "").Body.Bytes(), &out)
	if out.Total == nil || *out.Total != 1 || out.Runs[0].Ref.RunID != "r2" {
		t.Errorf("label count = %+v", out)
	}
}

func TestAdmin_CountRunsBounded(t *testing.T) {
	a := New(seeded(), WithMaxScan(2))
	c, err := a.CountRuns(t.Context(), workflow.RunQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c.Total != 2 || !c.Truncated || !slices.Equal(c.Cursors, []string{"", "1"}) {
		t.Errorf("bounded count = %+v", c)
	}
	if _, err := a.CountRuns(t.Context(), workflow.RunQuery{Cursor: "1"}); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("count with a cursor: %v", err)
	}
	if _, err := New(&fakeClient{}).CountRuns(t.Context(), workflow.RunQuery{}); !errors.Is(err, workflow.ErrUnsupported) {
		t.Errorf("count without RunQuery: %v", err)
	}
}

func TestHandler_SchedulePause(t *testing.T) {
	f := seeded()
	var res []Resource
	auth := bearerAuth()
	auth.Authorize = func(_ context.Context, req Request[user]) error { res = append(res, req.Resource); return nil }
	h := NewHandler(New(f), auth)
	for _, p := range []string{"/schedules/s1/pause", "/schedules/s1/resume"} {
		if rec := do(t, h, "POST", p, ""); rec.Code != 200 {
			t.Errorf("%s = %d %s", p, rec.Code, rec.Body)
		}
	}
	if want := []string{"PauseSchedule s1", "ResumeSchedule s1"}; !slices.Equal(f.Calls(), want) {
		t.Errorf("calls = %q, want %q", f.Calls(), want)
	}
	if len(res) != 2 || res[0].Schedule != "s1" {
		t.Errorf("resources = %+v", res)
	}
	// A client that lists schedules but cannot pause them is refused.
	hn := NewHandler(New(&fakeClient{caps: workflow.CapScheduleList}), bearerAuth())
	if rec := do(t, hn, "POST", "/schedules/s1/pause", ""); rec.Code != http.StatusNotImplemented {
		t.Errorf("pause without CapSchedulePause = %d", rec.Code)
	}
	if New(&fakeClient{caps: workflow.CapScheduleList}).Supports(OpSchedulePause) {
		t.Error("OpSchedulePause supported without CapSchedulePause")
	}
}

// TestHandler_LabelPrefix: label=key=prefix* searches by prefix on a
// client with CapLabelPrefix and is refused (501, no driver call) on one
// without it, on lists, counts and bulk cancel alike.
func TestHandler_LabelPrefix(t *testing.T) {
	f := seeded()
	f.runs[0].Labels = map[string]string{"tenant": "acme"}
	f.runs[1].Labels = map[string]string{"tenant": "abc"}
	h := NewHandler(New(f), bearerAuth())
	rec := do(t, h, "GET", "/runs?label=tenant=ac*&count=1", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"run_id":"r3"`) || strings.Contains(rec.Body.String(), `"r2"`) || !strings.Contains(rec.Body.String(), `"total":1`) {
		t.Errorf("prefix ac* = %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "GET", "/runs?label=tenant=*", "")
	if !strings.Contains(rec.Body.String(), `"r3"`) || !strings.Contains(rec.Body.String(), `"r2"`) || strings.Contains(rec.Body.String(), `"r1"`) {
		t.Errorf("key present = %s", rec.Body)
	}
	if q := f.queries[0]; q.LabelPrefixes["tenant"] != "ac" || len(q.Labels) != 0 {
		t.Errorf("query = %+v", q)
	}
	if rec := do(t, h, "POST", "/runs/cancel", `{"label_prefixes":{"tenant":"ac"}}`); rec.Code != 200 {
		t.Errorf("cancel by prefix = %d %s", rec.Code, rec.Body)
	}

	n := seeded()
	n.caps = allCaps &^ workflow.CapLabelPrefix
	hn := NewHandler(New(n), bearerAuth())
	for _, r := range []route{
		{"GET", "/runs?label=tenant=ac*", "", ""},
		{"GET", "/failures?label=tenant=ac*", "", ""},
		{"GET", "/runs?label=tenant=ac*&count=1", "", ""},
		{"POST", "/runs/cancel", `{"label_prefixes":{"tenant":"ac"}}`, ""},
	} {
		if rec := do(t, hn, r.method, r.path, r.body); rec.Code != http.StatusNotImplemented || !strings.Contains(rec.Body.String(), "LabelPrefix") {
			t.Errorf("without CapLabelPrefix %s %s = %d %s", r.method, r.path, rec.Code, rec.Body)
		}
	}
	if c := n.Calls(); len(c) != 0 {
		t.Errorf("driver reached without CapLabelPrefix: %v", c)
	}
	// An exact label still works there.
	if rec := do(t, hn, "GET", "/runs?label=tenant=a", ""); rec.Code != 200 {
		t.Errorf("exact label = %d", rec.Code)
	}
}
