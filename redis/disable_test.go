package redis_test

import (
	"testing"

	wfredis "github.com/ubgo/workflow/redis"
	"github.com/ubgo/workflow/workflowtest"
)

// TestDisableStore is the middleware.DisableStore contract (WH-02) through
// two stores on one prefix, as two processes would hold, one of them a
// client's.
func TestDisableStore(t *testing.T) {
	rdb := redisClient(t)
	prefix := testPrefix(t, rdb)
	a, err := wfredis.NewDisableStore(rdb, prefix)
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(t, rdb, prefix)
	workflowtest.DisableStoreContract(t, a, c.DisableStore())
	if _, err := wfredis.NewDisableStore(rdb, "bad prefix"); err == nil {
		t.Fatal("a bad prefix was accepted")
	}
}
