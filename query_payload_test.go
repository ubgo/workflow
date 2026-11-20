package workflow_test

import (
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

func TestRunQueryPayload(t *testing.T) {
	info := workflow.RunInfo{Payload: []byte(` {"order":{"id":"o-1","total":12.50},"rush":false,"note":null} `)}
	for _, tc := range []struct {
		q    map[string]string
		want bool
	}{
		{map[string]string{"order.id": "o-1"}, true},
		{map[string]string{"order.total": "12.50"}, true}, // as written, not normalised
		{map[string]string{"order.total": "12.5"}, false},
		{map[string]string{"rush": "false", "note": "null"}, true},
		{map[string]string{"order": "o-1"}, false},
		{map[string]string{"order.id.x": "o-1"}, false},
	} {
		if got := (workflow.RunQuery{Payload: tc.q}).Match(info); got != tc.want {
			t.Errorf("Match(%v) = %v, want %v", tc.q, got, tc.want)
		}
	}
	for _, bad := range []string{"", "a..b", "1a", "a.b-c", ".a"} {
		if err := workflow.ValidateRunQuery(workflow.RunQuery{Payload: map[string]string{bad: "x"}}); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("path %q: %v, want ErrInvalid", bad, err)
		}
	}
	q := workflow.RunQuery{Payload: map[string]string{"order.id": "o-1"}}
	var ue *workflow.UnsupportedError
	if err := workflow.CheckRunQuery(workflow.CapsOf(workflow.CapRunQuery), q); !errors.As(err, &ue) || !ue.Missing.Has(workflow.CapPayloadSearch) {
		t.Errorf("CheckRunQuery without the capability = %v", err)
	}
	if err := workflow.ValidateCancelQuery(q); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("ValidateCancelQuery with Payload = %v, want ErrInvalid", err)
	}
}
