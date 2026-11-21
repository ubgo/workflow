package events_test

import (
	"testing"

	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/workflowtest"
)

// TestMemoryLineageContract holds MemoryLineage, the reference semantics,
// to the LineageStore contract the durable stores keep.
func TestMemoryLineageContract(t *testing.T) {
	m := events.NewMemoryLineage()
	workflowtest.LineageStoreContract(t, m, m)
}
