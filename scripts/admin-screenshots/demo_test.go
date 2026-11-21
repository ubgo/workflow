package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDemo checks the demo data without a browser: the succeeded and
// waiting runs exist, and the UI would show the reset, fork and
// signal-waiting operations (the capabilities route says the client does
// them). The screenshots themselves need Chrome and run by hand.
func TestDemo(t *testing.T) {
	mux, runs, stop, err := demo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if runs.Succeeded.RunID == "" || runs.Waiting.RunID == "" {
		t.Fatalf("runs = %+v", runs)
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/capabilities", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: "demo"})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("capabilities: %d %s", rec.Code, rec.Body)
	}
	var caps struct {
		Operations []struct {
			Operation string `json:"operation"`
			Supported bool   `json:"supported"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	ops := map[string]bool{}
	for _, o := range caps.Operations {
		ops[o.Operation] = o.Supported
	}
	for _, op := range []string{"reset_succeeded", "fork_run", "signal_waiting"} {
		if !ops[op] {
			t.Errorf("operation %s not supported", op)
		}
	}
}
