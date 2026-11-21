// Package childflows runs the child, signal, promise, object, reset, redo, fork and slot flow tests (childflows.go)
// in a test binary of its own, so go test runs them as a process beside
// the enginetest package; each test keeps the name it had there.
package childflows

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestChildFlows(t *testing.T)   { enginetest.ChildFlows(t) }
func TestSignalFlows(t *testing.T)  { enginetest.SignalFlows(t) }
func TestPromiseStore(t *testing.T) { enginetest.PromiseStore(t) }
func TestObjectFlows(t *testing.T)  { enginetest.ObjectFlows(t) }
func TestResetFlows(t *testing.T)   { enginetest.ResetFlows(t) }
func TestRedoFlows(t *testing.T)    { enginetest.RedoFlows(t) }
func TestForkFlows(t *testing.T)    { enginetest.ForkFlows(t) }
func TestSlotFlows(t *testing.T)    { enginetest.SlotFlows(t) }
