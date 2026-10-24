package redis_test

import (
	"context"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	wfredis "github.com/ubgo/workflow/redis"
)

// offlineClient is a Redis driver over a client that never connects: the
// checks under test run before the first command.
func offlineClient(t *testing.T, opts ...wfredis.Option) *wfredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })
	c, err := wfredis.New(rdb, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// TestQueueLimitIsRefused: a run is claimed whole and its parallel steps are
// not counted individually, so the driver has no cluster-wide count of a
// queue's in-flight attempts. It declares no CapQueueLimitGlobal and Start
// refuses a non-zero Limit instead of enforcing only a per-client cap.
func TestQueueLimitIsRefused(t *testing.T) {
	if offlineClient(t).Capabilities().Has(workflow.CapQueueLimitGlobal) {
		t.Fatal("redis declares CapQueueLimitGlobal, which it cannot enforce")
	}
	for name, qs := range map[string][]workflow.QueueConfig{
		"limit alone":        {{Name: "mail", Limit: 3}},
		"limit with workers": {{Name: "mail", Workers: 2, Limit: 3}},
		"limit on default":   {{Name: workflow.DefaultQueue, Limit: 1}},
		"second of two":      {{Name: "a"}, {Name: "b", Limit: 2}},
	} {
		err := offlineClient(t, wfredis.WithQueues(qs...)).Start(context.Background())
		var u *workflow.UnsupportedError
		if !errors.Is(err, workflow.ErrUnsupported) || !errors.As(err, &u) || u.Field != "QueueConfig.Limit" || !u.Missing.Has(workflow.CapQueueLimitGlobal) {
			t.Errorf("%s: Start = %v, want an UnsupportedError on QueueConfig.Limit for CapQueueLimitGlobal", name, err)
		}
	}
	err := offlineClient(t, wfredis.WithQueues(workflow.QueueConfig{Name: "a", Limit: -1})).Start(context.Background())
	if !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("negative Limit: Start = %v, want ErrInvalid", err)
	}
}

// TestDiscardOnExhaustIsAccepted: unlike Hatchet, Redis owns its run states,
// so it declares CapDiscardExhausted (with CapRunStates, which the status
// needs) and accepts the option on every level instead of refusing it.
func TestDiscardOnExhaustIsAccepted(t *testing.T) {
	c := offlineClient(t)
	have := c.Capabilities()
	if !have.Has(workflow.CapDiscardExhausted | workflow.CapRunStates | workflow.CapJobRetry) {
		t.Fatalf("redis lacks %s", have.Missing(workflow.CapDiscardExhausted|workflow.CapRunStates|workflow.CapJobRetry))
	}
	pol := workflow.RetryPolicy{Max: 1, DiscardOnExhaust: true}
	run := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	for name, wf := range map[string]workflow.Workflow{
		"workflow": {Name: "w", Retry: pol, Steps: []workflow.Step{{Name: "a", Run: run}}},
		"step":     {Name: "w", Steps: []workflow.Step{{Name: "a", Run: run, Retry: &pol}}},
	} {
		if err := workflow.CheckCaps(have, wf); err != nil {
			t.Errorf("%s: CheckCaps = %v, want accepted", name, err)
		}
	}
	if err := workflow.CheckJob(have, workflow.Job{Workflow: "w", Retry: &pol}); err != nil {
		t.Errorf("Job.Retry: CheckJob = %v, want accepted", err)
	}
}
