// Package oneconn1 runs share 1 of 2 of the shared driver suite on SQLite
// with a one-connection pool, in a test binary of its own (see
// enginetest.ConformanceOnOneConnection).
package oneconn1

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestConformanceOnOneConnection(t *testing.T) { enginetest.ConformanceOnOneConnection(t, 0) }
