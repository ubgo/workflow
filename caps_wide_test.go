package workflow_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/failover"
	"github.com/ubgo/workflow/workflowtest"
)

// TestCapsAbove64 proves that a capability numbered past the first 64
// works end to end: a driver declares it, a wrapper keeps or drops it, a
// check finds it missing, and the refusal names it, in every text and
// JSON form. Numbers 191 and the last one (255) sit in the third and
// fourth words, far from the numbers real capabilities take next.
func TestCapsAbove64(t *testing.T) {
	high, undoHigh := workflow.DefineCapForTest(191, "Wide191")
	t.Cleanup(undoHigh)
	top, undoTop := workflow.DefineCapForTest(workflow.CapBitsForTest-1, "Wide255")
	t.Cleanup(undoTop)

	// Declare: two drivers, one with both wide capabilities.
	both := workflowtest.NewFake(workflowtest.FakeCaps(workflow.CapsOf(workflow.CapCron, high, top)))
	onlyHigh := workflowtest.NewFake(workflowtest.FakeCaps(workflow.CapsOf(workflow.CapCron, high)))
	if got := both.Capabilities(); !got.Has(high) || !got.Has(top) || !got.Has(workflow.CapCron) {
		t.Fatalf("declared set = %s, want Cron|Wide191|Wide255", got)
	}
	if onlyHigh.Capabilities().Has(top) || !onlyHigh.Capabilities().HasAny(workflow.CapsOf(high, top)) {
		t.Fatalf("onlyHigh = %s", onlyHigh.Capabilities())
	}

	// Check through a wrapper: failover declares the intersection.
	fo := failover.New(both, onlyHigh).Capabilities()
	if !fo.Has(high) || fo.Has(top) {
		t.Fatalf("failover over both and onlyHigh = %s, want Wide191 without Wide255", fo)
	}

	// Refuse: the missing set is exactly the wide capability, and the
	// UnsupportedError names it.
	need := workflow.CapsOf(workflow.CapCron, high, top)
	missing := onlyHigh.Capabilities().Missing(need)
	if missing != top {
		t.Fatalf("Missing = %s, want Wide255", missing)
	}
	err := error(&workflow.UnsupportedError{Field: "Wide", Missing: missing, Reason: "test"})
	if !errors.Is(err, workflow.ErrUnsupported) || !strings.Contains(err.Error(), "(missing Wide255)") {
		t.Fatalf("refusal = %v", err)
	}
	if !both.Capabilities().Missing(need).IsZero() {
		t.Fatalf("both lacks %s", both.Capabilities().Missing(need))
	}

	// Text and JSON forms round-trip.
	set := workflow.CapsOf(workflow.CapDurable, high, top)
	if got := set.String(); got != "Durable|Wide191|Wide255" {
		t.Fatalf("String = %q", got)
	}
	parsed, err := workflow.ParseCaps("Durable | Wide191|Wide255")
	if err != nil || parsed != set {
		t.Fatalf("ParseCaps = %s, %v", parsed, err)
	}
	b, err := json.Marshal(struct{ Caps workflow.Caps }{set})
	if err != nil || string(b) != `{"Caps":"Durable|Wide191|Wide255"}` {
		t.Fatalf("json = %s, %v", b, err)
	}
	var back struct{ Caps workflow.Caps }
	if err := json.Unmarshal(b, &back); err != nil || back.Caps != set {
		t.Fatalf("json back = %s, %v", back.Caps, err)
	}
	if got := set.Without(high, workflow.CapDurable); got != top {
		t.Fatalf("Without = %s", got)
	}
	if got := set.Intersect(workflow.CapsOf(top, workflow.CapCron)); got != top {
		t.Fatalf("Intersect = %s", got)
	}
	if !workflow.AllCaps().Has(set) {
		t.Fatalf("AllCaps lacks the declared wide capabilities")
	}

	// A number this release does not declare (a newer release's value)
	// prints by number and still round-trips.
	undoHigh()
	if got := set.String(); got != "Durable|#191|Wide255" {
		t.Fatalf("String with an undeclared number = %q", got)
	}
	if again, err := workflow.ParseCaps(set.String()); err != nil || again != set {
		t.Fatalf("ParseCaps(%q) = %s, %v", set.String(), again, err)
	}
}

func TestCapsTextAndJSON(t *testing.T) {
	if got := (workflow.Caps{}).String(); got != "none" {
		t.Fatalf("empty String = %q", got)
	}
	for _, s := range []string{"", "none", " none "} {
		if c, err := workflow.ParseCaps(s); err != nil || !c.IsZero() {
			t.Fatalf("ParseCaps(%q) = %s, %v", s, c, err)
		}
	}
	for _, s := range []string{"Durable|Nope", "#-1", "#256", "#x", "Durable||Cron"} {
		if _, err := workflow.ParseCaps(s); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("ParseCaps(%q) = %v, want ErrInvalid", s, err)
		}
	}
	// The v0.1 numeric form: bit n is capability n.
	var c workflow.Caps
	if err := json.Unmarshal([]byte("129"), &c); err != nil || c != workflow.CapsOf(workflow.CapDurable, workflow.CapCron) {
		t.Fatalf("129 reads as %s, %v; want Durable|Cron", c, err)
	}
	if err := json.Unmarshal([]byte(`"Cron|DrainReport"`), &c); err != nil || c != workflow.CapsOf(workflow.CapCron, workflow.CapDrainReport) {
		t.Fatalf("string reads as %s, %v", c, err)
	}
	before := c
	if err := json.Unmarshal([]byte("null"), &c); err != nil || c != before {
		t.Fatalf("null changed %s to %s, %v", before, c, err)
	}
	for _, bad := range []string{"-1", "1.5", `"Nope"`, "true", "18446744073709551616"} {
		if err := json.Unmarshal([]byte(bad), &c); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("json %s = %v, want ErrInvalid", bad, err)
		}
	}
	// String lists in number order whatever the build order.
	if got := workflow.CapsOf(workflow.CapDrainReport, workflow.CapDurable).String(); got != "Durable|DrainReport" {
		t.Fatalf("String order = %q", got)
	}
	if got := workflow.AllCaps().String(); !strings.HasPrefix(got, "Durable|DelayedStart|") || strings.Contains(got, "#") {
		t.Fatalf("AllCaps = %q", got)
	}
}
