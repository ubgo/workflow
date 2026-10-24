package workflow_test

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

func TestBackoffJitteredDelay(t *testing.T) {
	b := workflow.Backoff{Initial: 10 * time.Second, Factor: 2, Jitter: 0.5}
	if got := b.JitteredDelay(1, 0); got != 10*time.Second {
		t.Errorf("u=0: %v, want the full 10s", got)
	}
	if got := b.JitteredDelay(1, 0.5); got != 7500*time.Millisecond {
		t.Errorf("u=0.5: %v, want 7.5s", got)
	}
	if got := b.JitteredDelay(2, 0.999999); got < 10*time.Second || got > 20*time.Second {
		t.Errorf("retry 2, u~1: %v, want within [10s, 20s]", got)
	}
	if got := b.JitteredDelay(1, 7); got != 5*time.Second {
		t.Errorf("u clamped to 1: %v, want 5s", got)
	}
	none := workflow.Backoff{Initial: time.Second}
	if got := none.JitteredDelay(1, 0.9); got != time.Second {
		t.Errorf("no Jitter: %v, want the exact delay", got)
	}
}

func TestRetryPolicyNextDelay(t *testing.T) {
	half := func() float64 { return 0.5 }
	plain := workflow.RetryPolicy{Max: 3, Backoff: workflow.Backoff{Initial: time.Second, Factor: 2}}
	if got := plain.NextDelay(errors.New("x"), 2, half); got != 2*time.Second {
		t.Errorf("default policy: %v, want the plain backoff 2s (unchanged)", got)
	}
	jit := plain
	jit.Backoff.Jitter = 1
	if got := jit.NextDelay(errors.New("x"), 2, half); got != time.Second {
		t.Errorf("jitter 1, u 0.5: %v, want 1s", got)
	}
	for range 100 { // the default source stays in range
		if got := jit.NextDelay(errors.New("x"), 1, nil); got < 0 || got > time.Second {
			t.Fatalf("default jitter source gave %v, want within [0, 1s]", got)
		}
	}

	after := plain
	after.DelayFor = workflow.RetryAfterDelay
	if got := after.NextDelay(workflow.RetryAfter(errors.New("429"), time.Hour), 1, half); got != time.Hour {
		t.Errorf("RetryAfter under DelayFor: %v, want 1h", got)
	}
	wrapped := fmt.Errorf("call: %w", workflow.RetryAfter(errors.New("429"), time.Minute))
	if got := after.NextDelay(wrapped, 1, half); got != time.Minute {
		t.Errorf("RetryAfter deeper in the chain: %v, want 1m", got)
	}
	if got := after.NextDelay(errors.New("plain"), 1, half); got != time.Second {
		t.Errorf("no RetryAfter: %v, want the backoff 1s", got)
	}
	if got := plain.NextDelay(workflow.RetryAfter(errors.New("429"), time.Hour), 1, half); got != time.Second {
		t.Errorf("RetryAfter without DelayFor: %v, want it ignored (backoff 1s)", got)
	}
	custom := plain
	custom.DelayFor = func(err error, retry int) (time.Duration, bool) {
		if strings.Contains(err.Error(), "slow") {
			return time.Duration(retry) * time.Hour, true
		}
		return 0, false
	}
	if got := custom.NextDelay(errors.New("slow down"), 3, half); got != 3*time.Hour {
		t.Errorf("custom DelayFor: %v, want 3h", got)
	}
	neg := plain
	neg.DelayFor = func(error, int) (time.Duration, bool) { return -time.Hour, true }
	if got := neg.NextDelay(errors.New("x"), 1, half); got != 0 {
		t.Errorf("negative DelayFor: %v, want 0", got)
	}
}

func TestRetryAfterError(t *testing.T) {
	if workflow.RetryAfter(nil, time.Second) != nil {
		t.Fatal("RetryAfter(nil) is not nil")
	}
	base := errors.New("rate limited")
	err := workflow.RetryAfter(base, -time.Second)
	if !errors.Is(err, base) || err.Error() != base.Error() {
		t.Fatalf("RetryAfter hides the original: %v", err)
	}
	if d, ok := workflow.RetryAfterDelay(err, 1); !ok || d != 0 {
		t.Fatalf("negative delay: %v %v, want 0 true", d, ok)
	}
	if _, ok := workflow.RetryAfterDelay(base, 1); ok {
		t.Fatal("RetryAfterDelay found a delay on a plain error")
	}
	p := workflow.RetryPolicy{Max: 3, DelayFor: workflow.RetryAfterDelay}
	if p.Retryable(workflow.RetryAfter(workflow.Permanent(base), time.Second), 1) {
		t.Fatal("RetryAfter(Permanent(err)) is retryable; Permanent must win")
	}
	if !p.Retryable(workflow.RetryAfter(base, time.Second), 1) {
		t.Fatal("RetryAfter(err) is not retryable")
	}
}

func TestUnavailable(t *testing.T) {
	if workflow.Unavailable(nil) != nil {
		t.Fatal("Unavailable(nil) is not nil")
	}
	base := errors.New("dial tcp: connection refused")
	err := fmt.Errorf("enqueue: %w", workflow.Unavailable(base))
	if !errors.Is(err, workflow.ErrUnavailable) || !workflow.IsUnavailable(err) {
		t.Fatal("a marked error does not match ErrUnavailable")
	}
	if !errors.Is(err, base) {
		t.Fatal("a marked error hides the original")
	}
	if workflow.IsUnavailable(base) || workflow.IsUnavailable(workflow.ErrInvalid) {
		t.Fatal("an unmarked error counts as unavailable")
	}
	if !workflow.IsUnavailable(workflow.ErrUnavailable) {
		t.Fatal("the sentinel itself is not unavailable")
	}
}

func TestRequiresNewCaps(t *testing.T) {
	step := func(r *workflow.RetryPolicy) workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop, Retry: r}}}
	}
	jitter := workflow.RetryPolicy{Max: 1, Backoff: workflow.Backoff{Initial: time.Second, Jitter: 0.2}}
	delay := workflow.RetryPolicy{Max: 1, DelayFor: workflow.RetryAfterDelay}
	for _, tc := range []struct {
		name string
		wf   workflow.Workflow
		want workflow.Caps
	}{
		{"plain retry needs nothing", step(&workflow.RetryPolicy{Max: 2, Backoff: workflow.Backoff{Initial: time.Second}}), 0},
		{"step jitter", step(&jitter), workflow.CapRetryJitter},
		{"step DelayFor", step(&delay), workflow.CapRetryDelay},
		{"workflow default inherited", func() workflow.Workflow { w := step(nil); w.Retry = jitter; return w }(), workflow.CapRetryJitter},
		{"workflow default every step overrides", func() workflow.Workflow { w := step(workflow.RetriesNone()); w.Retry = delay; return w }(), 0},
		{"UTC cron", func() workflow.Workflow { w := step(nil); w.Cron = []string{"* * * * *"}; return w }(), workflow.CapCron},
		{"explicit UTC CronLocation", func() workflow.Workflow {
			w := step(nil)
			w.Cron, w.CronLocation = []string{"* * * * *"}, time.UTC
			return w
		}(), workflow.CapCron},
		{"zoned cron", func() workflow.Workflow {
			w := step(nil)
			w.Cron, w.CronLocation = []string{"* * * * *"}, time.FixedZone("X", 3600)
			return w
		}(), workflow.CapCron | workflow.CapCronZone},
		{"misfire fire all", func() workflow.Workflow {
			w := step(nil)
			w.Cron, w.Misfire = []string{"* * * * *"}, workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 2}
			return w
		}(), workflow.CapCron | workflow.CapMisfireFireAll},
	} {
		if got := workflow.Requires(tc.wf); got != tc.want {
			t.Errorf("%s: Requires = %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestCheckScheduleCaps(t *testing.T) {
	zoned := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.FixedZone("X", 3600)}
	skip := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, Misfire: workflow.Misfire{Policy: workflow.MisfireSkip}}
	plain := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC}
	for _, tc := range []struct {
		name  string
		have  workflow.Caps
		s     workflow.Schedule
		field string // "" means allowed
	}{
		{"plain UTC on a cron driver", workflow.CapCron, plain, ""},
		{"no cron at all", 0, plain, "Schedule"},
		{"zone without CapCronZone", workflow.CapCron, zoned, "Schedule.Location"},
		{"zone with CapCronZone", workflow.CapCron | workflow.CapCronZone, zoned, ""},
		{"misfire without its cap", workflow.CapCron | workflow.CapMisfireFireOnce, skip, "Schedule.Misfire"},
		{"misfire with its cap", workflow.CapCron | workflow.CapMisfireSkip, skip, ""},
	} {
		err := workflow.CheckScheduleCaps(tc.have, tc.s)
		if tc.field == "" {
			if err != nil {
				t.Errorf("%s: %v, want allowed", tc.name, err)
			}
			continue
		}
		var ue *workflow.UnsupportedError
		if !errors.As(err, &ue) || !errors.Is(err, workflow.ErrUnsupported) || ue.Field != tc.field {
			t.Errorf("%s: err = %v, want an UnsupportedError on %s", tc.name, err, tc.field)
		}
	}
}

func TestNewCapNames(t *testing.T) {
	all := workflow.CapRetryJitter | workflow.CapRetryDelay | workflow.CapMisfireSkip |
		workflow.CapMisfireFireOnce | workflow.CapMisfireFireAll | workflow.CapCronZone
	want := "RetryJitter|RetryDelay|MisfireSkip|MisfireFireOnce|MisfireFireAll|CronZone"
	if got := all.String(); got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
	if workflow.CapRetryJitter <= workflow.CapRunFinished {
		t.Fatal("a new capability moved an existing bit")
	}
}

func TestValidateRetryAndMisfire(t *testing.T) {
	base := func() workflow.Workflow {
		return workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: noop}}, Cron: []string{"* * * * *"}}
	}
	for _, tc := range []struct {
		name string
		edit func(*workflow.Workflow)
		bad  bool
	}{
		{"jitter 1", func(w *workflow.Workflow) { w.Retry.Backoff.Jitter = 1 }, false},
		{"jitter above 1", func(w *workflow.Workflow) { w.Retry.Backoff.Jitter = 1.5 }, true},
		{"jitter negative", func(w *workflow.Workflow) { w.Retry.Backoff.Jitter = -0.1 }, true},
		{"jitter NaN", func(w *workflow.Workflow) { w.Retry.Backoff.Jitter = math.NaN() }, true},
		{"step jitter above 1", func(w *workflow.Workflow) {
			w.Steps[0].Retry = &workflow.RetryPolicy{Backoff: workflow.Backoff{Jitter: 2}}
		}, true},
		{"CronLocation Local", func(w *workflow.Workflow) { w.CronLocation = time.Local }, true},
		{"CronLocation zone", func(w *workflow.Workflow) { w.CronLocation = time.FixedZone("X", 60) }, false},
		{"misfire skip", func(w *workflow.Workflow) { w.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip} }, false},
		{"misfire skip with Within", func(w *workflow.Workflow) {
			w.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip, Within: time.Minute}
		}, true},
		{"misfire fire once with Within", func(w *workflow.Workflow) {
			w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: time.Minute}
		}, false},
		{"misfire fire once with Max", func(w *workflow.Workflow) {
			w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Max: 2}
		}, true},
		{"misfire fire all without Max", func(w *workflow.Workflow) { w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll} }, true},
		{"misfire fire all", func(w *workflow.Workflow) { w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 5} }, false},
		{"misfire negative Within", func(w *workflow.Workflow) {
			w.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: -time.Second}
		}, true},
		{"misfire unknown policy", func(w *workflow.Workflow) { w.Misfire = workflow.Misfire{Policy: "catch_up"} }, true},
		{"misfire fields without policy", func(w *workflow.Workflow) { w.Misfire = workflow.Misfire{Max: 3} }, true},
		{"misfire without Cron", func(w *workflow.Workflow) {
			w.Cron = nil
			w.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip}
		}, true},
	} {
		wf := base()
		tc.edit(&wf)
		err := workflow.Validate(wf)
		if tc.bad != (err != nil) {
			t.Errorf("%s: Validate = %v, want invalid %v", tc.name, err, tc.bad)
		}
		if err != nil && !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v does not wrap ErrInvalid", tc.name, err)
		}
	}
}

func TestReservedLabels(t *testing.T) {
	prefixes := workflow.ReservedLabelPrefixes()
	want := []string{workflow.LabelPrefixInternal, workflow.LabelPrefixBatch, workflow.LabelPrefixOTel}
	if !slices.Equal(prefixes, want) {
		t.Fatalf("ReservedLabelPrefixes = %v, want %v", prefixes, want)
	}
	// The spellings are on-the-wire contracts that live runs carry.
	if want[0] != "ubgo.workflow/" || want[1] != "workflow.batch." || want[2] != "otel." {
		t.Fatalf("a reserved prefix was renamed: %v", want)
	}
	prefixes[0] = "changed"
	if workflow.ReservedLabelPrefixes()[0] != workflow.LabelPrefixInternal {
		t.Fatal("changing the returned slice changed the package's list")
	}
	for key, want := range map[string]string{
		"ubgo.workflow/unique-key": workflow.LabelPrefixInternal,
		"workflow.batch.id":        workflow.LabelPrefixBatch,
		"otel.traceparent":         workflow.LabelPrefixOTel,
		"tenant":                   "",
		"otel":                     "",
		"workflow.batches":         "",
	} {
		p, ok := workflow.ReservedLabel(key)
		if p != want || ok != (want != "") {
			t.Errorf("ReservedLabel(%q) = %q %v, want %q", key, p, ok, want)
		}
	}
	if err := workflow.ValidateLabels(map[string]string{"tenant": "a", "source": "b"}); err != nil {
		t.Fatalf("app labels refused: %v", err)
	}
	err := workflow.ValidateLabels(map[string]string{"tenant": "a", "otel.mine": "b"})
	if !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), `"otel.mine"`) || !strings.Contains(err.Error(), `"otel."`) {
		t.Fatalf("reserved label: err = %v, want ErrInvalid naming the key and prefix", err)
	}
	s := workflow.Schedule{Workflow: "w", Cron: "* * * * *", Location: time.UTC, Labels: map[string]string{"workflow.batch.id": "x"}}
	if err := workflow.ValidateSchedule(s); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("ValidateSchedule with a reserved label: %v, want ErrInvalid", err)
	}
	s.Labels = map[string]string{"tenant": "t"}
	if err := workflow.ValidateSchedule(s); err != nil {
		t.Fatalf("ValidateSchedule with app labels: %v", err)
	}
}

// ticks is a TickSource over a fixed list, so Due is tested without a
// parser.
type ticks []time.Time

func (ts ticks) Between(from, to time.Time, _ *time.Location, limit int) []time.Time {
	var out []time.Time
	for _, t := range ts {
		if t.After(from) && !t.After(to) && len(out) < limit {
			out = append(out, t)
		}
	}
	return out
}

func TestMisfireDue(t *testing.T) {
	t0 := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	hourly := ticks{t0.Add(time.Hour), t0.Add(2 * time.Hour), t0.Add(3 * time.Hour), t0.Add(4 * time.Hour)}
	now := t0.Add(3*time.Hour + 30*time.Minute) // ticks 1h, 2h, 3h due; 3h is 30m late
	h := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Hour) }
	for _, tc := range []struct {
		name string
		m    workflow.Misfire
		now  time.Time
		want []time.Time
	}{
		{"default is the driver's", workflow.Misfire{}, now, nil},
		{"skip, 30m late", workflow.Misfire{Policy: workflow.MisfireSkip}, now, nil},
		{"skip, on time", workflow.Misfire{Policy: workflow.MisfireSkip}, h(3).Add(time.Second), []time.Time{h(3)}},
		{"skip, exactly the grace", workflow.Misfire{Policy: workflow.MisfireSkip}, h(3).Add(workflow.MisfireGrace), []time.Time{h(3)}},
		{"fire once", workflow.Misfire{Policy: workflow.MisfireFireOnce}, now, []time.Time{h(3)}},
		{"fire once within 10m", workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}, now, nil},
		{"fire once within 30m", workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 30 * time.Minute}, now, []time.Time{h(3)}},
		{"fire all", workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}, now, []time.Time{h(1), h(2), h(3)}},
		{"fire all, newest Max", workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 2}, now, []time.Time{h(2), h(3)}},
		{"fire all within 100m", workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10, Within: 100 * time.Minute}, now, []time.Time{h(2), h(3)}},
		{"nothing due", workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}, t0.Add(30 * time.Minute), nil},
	} {
		got := tc.m.Due(hourly, t0, tc.now, time.UTC)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: Due = %v, want %v", tc.name, got, tc.want)
		}
	}
	// A tick just past the limit, inside the one-second scan slack, is
	// still dropped.
	odd := ticks{now.Add(-10*time.Minute - 500*time.Millisecond)}
	if got := (workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 10 * time.Minute}).Due(odd, t0, now, time.UTC); got != nil {
		t.Errorf("a tick 10m0.5s old under Within 10m: %v, want dropped", got)
	}
	if got := (workflow.Misfire{Policy: workflow.MisfireFireOnce}).Due(hourly, now, now, time.UTC); got != nil {
		t.Errorf("last == now: %v, want nil", got)
	}
}

func TestMisfireDueScansPastOneChunk(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var many ticks
	for i := 1; i <= 5000; i++ { // more than one scan chunk
		many = append(many, t0.Add(time.Duration(i)*time.Minute))
	}
	now := t0.Add(5000 * time.Minute)
	got := workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3}.Due(many, t0, now, time.UTC)
	want := []time.Time{many[4997], many[4998], many[4999]}
	if !slices.Equal(got, want) {
		t.Fatalf("newest 3 of 5000: %v, want %v", got, want)
	}
	if got := (workflow.Misfire{Policy: workflow.MisfireFireOnce}).Due(many, t0, now, time.UTC); !slices.Equal(got, want[2:]) {
		t.Fatalf("fire once of 5000: %v, want %v", got, want[2:])
	}
}

func TestIsUTC(t *testing.T) {
	for loc, want := range map[*time.Location]bool{
		nil:                         true,
		time.UTC:                    true,
		time.FixedZone("UTC", 0):    true,
		time.FixedZone("X", 0):      false,
		time.FixedZone("UTC+1", 60): false,
	} {
		if got := workflow.IsUTC(loc); got != want {
			t.Errorf("IsUTC(%v) = %v, want %v", loc, got, want)
		}
	}
}
