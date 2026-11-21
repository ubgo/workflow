// Package cluster runs the multi-client cluster tests (cluster.go)
// in a test binary of its own, so go test runs them as a process beside
// the enginetest package; each test keeps the name it had there.
package cluster

import (
	"testing"

	"github.com/ubgo/workflow/sqlstore/enginetest"
)

func TestClusterClaimsEachStepOnce(t *testing.T)  { enginetest.ClusterClaimsEachStepOnce(t) }
func TestClusterCronTickFiresOnce(t *testing.T)   { enginetest.ClusterCronTickFiresOnce(t) }
func TestClusterLiveLeaseIsKept(t *testing.T)     { enginetest.ClusterLiveLeaseIsKept(t) }
func TestCancelReachesAnotherClient(t *testing.T) { enginetest.CancelReachesAnotherClient(t) }
func TestRunFinishedIsDurable(t *testing.T)       { enginetest.RunFinishedIsDurable(t) }
func TestBatchStoreUpdateIsAtomic(t *testing.T)   { enginetest.BatchStoreUpdateIsAtomic(t) }
func TestPoisonStepIsAbandoned(t *testing.T)      { enginetest.PoisonStepIsAbandoned(t) }
