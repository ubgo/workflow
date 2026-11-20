// Package conformance2 runs share 2 of 2 of the shared driver suite on
// SQLite in a test binary of its own, so go test runs it as a process
// beside the other shares (see enginetest.ConformanceSQLite).
package conformance2

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestConformance(t *testing.T) { enginetest.ConformanceSQLite(t, 1) }
