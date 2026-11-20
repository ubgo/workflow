package middleware_test

import (
	"testing"

	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/workflowtest"
)

// TestMemoryDisableContract holds MemoryDisable, the reference semantics,
// to the DisableStore contract the durable stores keep.
func TestMemoryDisableContract(t *testing.T) {
	s := middleware.NewMemoryDisable()
	workflowtest.DisableStoreContract(t, s, s)
}
