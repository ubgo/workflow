// Package retention runs the retention and prune tests (retention.go)
// in a test binary of its own, so go test runs them as a process beside
// the enginetest package; each test keeps the name it had there.
package retention

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestRetentionAgesPerStatusAndOverride(t *testing.T) {
	enginetest.RetentionAgesPerStatusAndOverride(t)
}
func TestRetentionGuards(t *testing.T)                { enginetest.RetentionGuards(t) }
func TestRetentionArchiveAndChunks(t *testing.T)      { enginetest.RetentionArchiveAndChunks(t) }
func TestRetentionTwoClientsPruneAtOnce(t *testing.T) { enginetest.RetentionTwoClientsPruneAtOnce(t) }
func TestAutoPruneRuns(t *testing.T)                  { enginetest.AutoPruneRuns(t) }
func TestPruneReportsHeldRuns(t *testing.T)           { enginetest.PruneReportsHeldRuns(t) }
