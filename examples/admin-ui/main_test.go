package main

import (
	"context"
	"strings"
	"testing"
)

// TestRun runs the program and checks the lines the guide shows: the UI
// is closed to an anonymous caller, opens for a cookie session, carries
// the CSP, and a viewer is refused a write.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out, ""); err != nil {
		t.Fatal(err)
	}
	want := `anonymous GET /admin/ui/: 401
viewer GET /admin/: 302 to ui/
viewer GET /admin/ui/: 200 text/html; charset=utf-8, page true
CSP: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'
X-Frame-Options: DENY
viewer runs tenant=acme*: 2 runs, total 2
viewer pauses the schedule: 403
admin pauses the schedule: 200
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
