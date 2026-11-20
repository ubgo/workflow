package redis

import (
	"context"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// Workflow versions for rolling deploys (LO-11). A run stores the
// Workflow.Version of the client that enqueued it (its 'ver' field), and
// the sets that hand out work (ready, since, wfq, pending) are keyed by
// the run's routing name: its workflow, or for a versioned run the
// workflow plus a newline, 'v' and the version (rname in the Lua
// prelude). {prefix}:wfv:<workflow> holds the versions seen. A worker
// claims and admits only under the routing names of the versions its
// definition runs (Workflow.RunsVersion), so a run started by newer code
// waits for a worker that runs it. Runs are claimed whole, so every later
// transition of a run (a sleep ending, a signal, a retry) happens on a
// worker that claimed it, and so runs its version.

// versionRange is the lowest and highest run version this client's
// definition of workflow name runs.
func (c *Client) versionRange(name string) (lo, hi int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.wfs[name]; r != nil {
		return r.wf.MinVersion, r.wf.Version
	}
	return 0, 0
}

// routeName is rname of the Lua prelude: the name a run of workflow wf at
// version v is routed under.
func routeName(wf string, v int) string {
	if v == 0 {
		return wf
	}
	return wf + "\nv" + strconv.Itoa(v)
}

// routeNames returns, per workflow, the routing names of the versions
// this client runs that have been seen: the workflow itself when it runs
// version zero, then each seen version in range.
func (c *Client) routeNames(ctx context.Context, names []string) (map[string][]string, error) {
	pipe := c.rdb.Pipeline()
	cmds := make([]*goredis.StringSliceCmd, len(names))
	for i, n := range names {
		cmds[i] = pipe.SMembers(ctx, c.base+":wfv:"+n)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, classify(err)
	}
	out := make(map[string][]string, len(names))
	for i, n := range names {
		lo, hi := c.versionRange(n)
		if lo == 0 {
			out[n] = append(out[n], n)
		}
		for _, s := range cmds[i].Val() {
			if v, err := strconv.Atoi(s); err == nil && v > 0 && v >= lo && v <= hi {
				out[n] = append(out[n], routeName(n, v))
			}
		}
	}
	return out, nil
}
