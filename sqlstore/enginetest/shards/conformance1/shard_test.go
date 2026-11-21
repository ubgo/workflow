// Package conformance1 runs share 1 of 2 of the shared driver suite on
// SQLite in a test binary of its own, so go test runs it as a process
// beside the other shares (see enginetest.ConformanceSQLite).
package conformance1

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestConformance(t *testing.T) { enginetest.ConformanceSQLite(t, 0) }
