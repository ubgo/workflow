package redis_test

import (
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// TestLineageStore is the events.LineageStore contract through two stores
// on one prefix, as two processes would hold, one of them a client's.
func TestLineageStore(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	a, err := wfredis.NewLineageStore(rdb, prefix)
	if err != nil {
		t.Fatal(err)
	}
	workflowtest.LineageStoreContract(t, a, newClient(t, rdb, prefix).LineageStore())
	if _, err := wfredis.NewLineageStore(rdb, "bad prefix"); err == nil {
		t.Fatal("a bad prefix was accepted")
	}
}

// TestDatasetsSharedLineage runs events.Datasets in two processes (two
// clients on one prefix, each with its own bus) sharing lineage through
// the Redis LineageStore: each trigger starts once per version, even when
// both announce a trigger's last dataset at once.
func TestDatasetsSharedLineage(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	var stores []events.LineageStore
	for range 2 {
		s, err := wfredis.NewLineageStore(rdb, prefix)
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, s)
	}
	start := func(t *testing.T, wfs ...workflow.Workflow) workflow.Client { return startOn(t, rdb, prefix, wfs) }
	workflowtest.DatasetsSharedLineage(t, start, stores[0], stores[1])
}
