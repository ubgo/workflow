package sqlstore

import (
	"context"
	"database/sql"
	"sort"

	"github.com/ubgo/workflow"
)

// limitLockPrefix names the lock row (locks table) that serialises the
// claims of one limited queue across every client sharing the store.
const limitLockPrefix = "qlimit:"

// limitLockName is the lock row of queue.
func limitLockName(queue string) string { return limitLockPrefix + queue }

// queueLimit is one queue's QueueConfig.Limit as the claim reads it.
type queueLimit struct {
	name  string
	limit int
}

// queueLimits extracts the queues with a Limit, sorted by name: claims
// lock their rows in that one order, so two claimers can never deadlock
// on each other's locks.
func queueLimits(qs []workflow.QueueConfig) []queueLimit {
	var out []queueLimit
	for _, q := range qs {
		if q.Limit > 0 {
			out = append(out, queueLimit{name: q.Name, limit: q.Limit})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// limitSQL is the claim predicate for QueueConfig.Limit: for each limited
// queue, a step of that queue is claimable only while fewer than Limit
// steps of the queue are running with a live lease, counted over every
// client's rows. The count is the exact complement of the reclaim test in
// claimWhere (lease_until < expiredBefore), so a step whose holder died is
// uncounted the moment its lease expires: a crashed worker never leaks a
// slot, and no separate counter exists to drift. The count is only safe
// under the queue's lock row (lockLimits), which makes the check and the
// claim one serialised step across clients.
func (c *Client) limitSQL() string {
	out := ""
	for range c.limits {
		out += " AND (" + queueOf + " <> ? OR (SELECT COUNT(*) FROM {steps} sl WHERE sl.status = ? AND sl.lease_until >= ? AND " +
			queueOfAs("sl") + " = ?) < ?)"
	}
	return out
}

// limitArgs are limitSQL's arguments, in its order.
func (c *Client) limitArgs() []any {
	var args []any
	for _, l := range c.limits {
		args = append(args, l.name, stepRunning, c.expiredBefore(), l.name, l.limit)
	}
	return args
}

// lockLimits takes the lock row of every limited queue in tx before the
// claim reads any count. The write holds the row until commit on Postgres
// and MySQL, so a second claimer on any client waits, then counts the
// first one's committed claim; SQLite has one writer, which gives the same
// order. A client with no limited queue pays nothing.
func (c *Client) lockLimits(ctx context.Context, tx *sql.Tx) error {
	for _, l := range c.limits {
		if _, err := c.st.exec(ctx, tx, "UPDATE {locks} SET n = n + 1 WHERE name = ?", limitLockName(l.name)); err != nil {
			return err
		}
	}
	return nil
}
