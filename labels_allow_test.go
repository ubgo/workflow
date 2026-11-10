package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
)

// TestAllowReservedLabelsNests: a nested allowance keeps the keys and
// prefixes allowed further out, and leaves the outer context unchanged.
func TestAllowReservedLabelsNests(t *testing.T) {
	parent := workflow.LabelPrefixInternal + "parent-run"
	unique := workflow.LabelPrefixInternal + "unique-key"
	outer := workflow.AllowReservedPrefix(workflow.AllowReservedLabels(context.Background(), parent), workflow.LabelPrefixBatch)
	inner := workflow.AllowReservedLabels(outer, unique)
	both := workflow.Job{Workflow: "w", Labels: map[string]string{parent: "r-1", unique: "k", workflow.LabelPrefixBatch + "id": "b"}}
	if err := workflow.CheckJobLabels(inner, both); err != nil {
		t.Fatalf("nested allowance lost an outer key or prefix: %v", err)
	}
	if err := workflow.CheckJobLabels(outer, both); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("outer context = %v, want ErrInvalid: the inner allowance leaked out", err)
	}
}
