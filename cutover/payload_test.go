package cutover_test

import (
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cutover"
	"github.com/ubgo/workflow/memory"
)

// TestPayloadLimit: a cutover client declares a payload cap
// (workflow.PayloadLimiter), the smaller of its backends' caps, a backend
// without one counting as no cap, so a claim check or batch splitter
// above it sizes against a cap both backends take (new work goes to the
// new backend, an edited payload to the backend holding the run).
func TestPayloadLimit(t *testing.T) {
	for _, tc := range []struct {
		name           string
		oldCap, newCap int
		want           int
	}{
		{"old smaller", 50, 100, 50},
		{"new smaller", 100, 50, 50},
		{"old without a cap", 0, 80, 80},
		{"new without a cap", 80, 0, 80},
		{"neither has one", 0, 0, 0},
	} {
		c := cutover.New(memory.New(memory.WithPayloadLimit(tc.oldCap)), memory.New(memory.WithPayloadLimit(tc.newCap)))
		l, ok := any(c).(workflow.PayloadLimiter)
		if !ok {
			t.Fatalf("cutover.Client does not implement workflow.PayloadLimiter")
		}
		if got := l.PayloadLimit(); got != tc.want {
			t.Errorf("%s: PayloadLimit = %d, want %d", tc.name, got, tc.want)
		}
	}
}
