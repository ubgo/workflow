package redis_test

import (
	"testing"

	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// TestAggregateStore runs the AggregateStore contract against Redis:
// Append's count and first, Take's order and limit, and every item taken
// exactly once while appenders and takers race on one group.
func TestAggregateStore(t *testing.T) {
	rdb := redisClient(t)
	s, err := wfredis.NewAggregateStore(rdb, testPrefix(t, rdb))
	if err != nil {
		t.Fatal(err)
	}
	workflowtest.AggregateStoreContract(t, s)
	if _, err := wfredis.NewAggregateStore(rdb, "bad prefix"); err == nil {
		t.Fatal("a bad prefix was accepted")
	}
}
