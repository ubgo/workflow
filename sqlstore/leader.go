package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// Elector answers whether this process is the leader of a cluster role.
// A client given one (WithElector, WithLeaderElection) fires cron ticks and
// runs the dispatcher's housekeeping only while IsLeader is true; every
// other node still claims and runs steps. IsLeader is called on every
// dispatcher pass, so it must answer from memory (campaigning happens
// elsewhere) and be safe for concurrent use. LeaderElector is the SQL one;
// a Redis lock, a Kubernetes Lease or a Consul session fits the same way.
type Elector interface {
	IsLeader() bool
}

// tblLeaders is migration 007's leader lease table.
const tblLeaders = "leaders"

// leaderDefs is the leader table of migration 007.
func leaderDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblLeaders, lines: []string{
		// One row per role; the holder leads while lease_until is ahead.
		"name " + t.name + " NOT NULL PRIMARY KEY",
		// holder is the leading node's id; empty when nobody ever led.
		"holder " + t.name + " NOT NULL",
		// lease_until is the holder's lease end, Unix nanoseconds on the
		// lease clock; 0 after a resignation.
		"lease_until " + t.int + " NOT NULL",
		// term counts changes of holder: a fencing token that grows each
		// time leadership moves.
		"term " + t.int + " NOT NULL",
	}}}
}

// defaultLeaderRole is the role WithLeaderElection campaigns for.
const defaultLeaderRole = "scheduler"

// defaultLeaderLease is how long a leader holds the role without renewing.
const defaultLeaderLease = 15 * time.Second

// LeaderElector elects one leader per role among the processes sharing a
// set of tables, through a lease row in <prefix>leaders: the holder
// renews it every third of its lease; when it stops (a crash, a network
// split), another node takes the role once the lease and the skew margin
// have passed. Each change of holder increments Term, a fencing token.
//
// A leader fences itself: IsLeader turns false once a whole lease has
// passed on its own (monotonic) lease clock since its last renewal began,
// before any other node can take over, so two nodes never both believe they
// lead for longer than the clocks' disagreement beyond the skew margin.
// Use it for work that must not run twice at once; where a duplicate is
// merely wasteful, a unique row (the cron tick table) is simpler and
// survives a split brain.
type LeaderElector struct {
	st    *store
	role  string
	node  string
	clk   clock.Clock
	lease time.Duration
	skew  time.Duration
	log   *slog.Logger
	// prefix and schema are LeaderTablePrefix and LeaderSchema, read once
	// by NewLeaderElector.
	prefix, schema string

	mu     sync.Mutex
	leader bool
	from   time.Time // lease clock time the held lease was computed from
	term   int64
}

var _ Elector = (*LeaderElector)(nil)

// LeaderOption configures a LeaderElector.
type LeaderOption func(*LeaderElector)

// LeaderLease sets how long a leader holds its role without renewing
// (default 15 seconds). A shorter lease hands the role on sooner after a
// crash and costs more writes. Values of zero or less are ignored.
func LeaderLease(d time.Duration) LeaderOption {
	return func(e *LeaderElector) {
		if d > 0 {
			e.lease = d
		}
	}
}

// LeaderSkew sets the margin other nodes wait past a lease's end before
// taking the role (see WithLeaseSkew); default zero.
func LeaderSkew(d time.Duration) LeaderOption {
	return func(e *LeaderElector) {
		if d >= 0 {
			e.skew = d
		}
	}
}

// LeaderClock sets the clock leases are measured on (default clock.Real;
// a DatabaseClock makes every node compare against the database's time).
func LeaderClock(c clock.Clock) LeaderOption {
	return func(e *LeaderElector) {
		if c != nil {
			e.clk = c
		}
	}
}

// LeaderTablePrefix and LeaderSchema name the tables, as WithTablePrefix and
// WithSchema do for a Client; they must match the client that migrated
// them.
func LeaderTablePrefix(p string) LeaderOption {
	return func(e *LeaderElector) { e.prefix = p }
}

// LeaderSchema puts the leader table in a Postgres schema (WithSchema).
func LeaderSchema(name string) LeaderOption {
	return func(e *LeaderElector) { e.schema = name }
}

// NewLeaderElector returns an elector for role, campaigning as node (an id
// unique among the nodes, such as workflow.DefaultNodeID()), over the
// leader table a Client's Migrate created in db. It reads nothing until
// Campaign or Run.
func NewLeaderElector(db *sql.DB, d sqlb.Dialect, role, node string, opts ...LeaderOption) (*LeaderElector, error) {
	if db == nil || d == nil {
		return nil, fmt.Errorf("%w: NewLeaderElector needs a *sql.DB and a dialect", workflow.ErrInvalid)
	}
	if role == "" || node == "" {
		return nil, fmt.Errorf("%w: NewLeaderElector needs a role and a node id", workflow.ErrInvalid)
	}
	e := &LeaderElector{role: role, node: node, clk: clock.Real{}, lease: defaultLeaderLease, prefix: defaultTablePrefix}
	for _, o := range opts {
		o(e)
	}
	// build validates the dialect, prefix and schema exactly as New does.
	c, err := build(db, d, []Option{WithTablePrefix(e.prefix), WithSchema(e.schema)})
	if err != nil {
		return nil, err
	}
	e.st, e.log = c.st, c.log
	return e, nil
}

// IsLeader reports whether this node holds the role: its last campaign
// won and less than a whole lease has passed since that renewal began.
func (e *LeaderElector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader && e.clk.Now().Sub(e.from) < e.lease
}

// Term is the fencing token of the leadership this node last won (0:
// never won). It grows each time the role changes hands.
func (e *LeaderElector) Term() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.term
}

// Campaign makes one attempt to take or renew the role and reports
// whether this node leads after it. Run calls it every third of the lease.
func (e *LeaderElector) Campaign(ctx context.Context) (bool, error) {
	from := e.clk.Now()
	now := from.UnixNano()
	if _, err := e.st.exec(ctx, e.st.db, "INSERT INTO {leaders} (name, holder, lease_until, term) VALUES (?, '', 0, 0)", e.role); err != nil && !e.st.isUnique(err) {
		return e.lost(), fmt.Errorf("sqlstore: create the leader row: %w", err)
	}
	// term is assigned first: MySQL evaluates SET left to right with the new
	// values, and the CASE must compare the holder before this update.
	n, err := e.st.execN(ctx, e.st.db, "UPDATE {leaders} SET term = term + CASE WHEN holder = ? THEN 0 ELSE 1 END, holder = ?, lease_until = ?"+
		" WHERE name = ? AND (holder = ? OR lease_until < ?)",
		e.node, e.node, now+int64(e.lease), e.role, e.node, now-int64(e.skew))
	if err != nil {
		return e.IsLeader(), fmt.Errorf("sqlstore: campaign for leader: %w", err)
	}
	if n == 0 {
		return e.lost(), nil
	}
	var term int64
	if err := e.st.one(ctx, e.st.db, "SELECT term FROM {leaders} WHERE name = ? AND holder = ?", []any{e.role, e.node}, &term); err != nil {
		if errors.Is(err, sqlb.ErrNoRows) {
			return e.lost(), nil // taken over between the two statements
		}
		return e.IsLeader(), fmt.Errorf("sqlstore: read the leader term: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.leader, e.from, e.term = true, from, term
	return true, nil
}

// lost records that this node does not lead.
func (e *LeaderElector) lost() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.leader = false
	return false
}

// Resign gives the role up at once, so another node takes it at its next
// campaign instead of after the lease.
func (e *LeaderElector) Resign(ctx context.Context) error {
	e.lost()
	_, err := e.st.exec(ctx, e.st.db, "UPDATE {leaders} SET lease_until = 0 WHERE name = ? AND holder = ?", e.role, e.node)
	return err
}

// Run campaigns every third of the lease until ctx ends, then resigns.
// Errors are logged through the Client's logger when the client runs it,
// and otherwise only change IsLeader.
func (e *LeaderElector) Run(ctx context.Context) {
	for {
		if _, err := e.Campaign(ctx); err != nil && ctx.Err() == nil {
			e.log.Error("sqlstore: leader campaign", "role", e.role, "error", err)
		}
		t := e.clk.NewTimer(e.lease / 3)
		select {
		case <-ctx.Done():
			t.Stop()
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
			if err := e.Resign(rctx); err != nil {
				e.log.Error("sqlstore: resign leadership (another node takes it after the lease)", "role", e.role, "error", err)
			}
			cancel()
			return
		case <-t.C():
		}
	}
}

// WithElector makes this client fire cron ticks and run housekeeping only
// while e says it leads (see Elector). Without it (the default) every
// client fires and the unique tick row picks one fire per tick, which
// needs no election and survives a split brain; prefer that unless the
// fire itself must happen on one known node. The tick row stays as the
// second guard either way. Steps run on every client regardless. Nil is
// ignored.
func WithElector(e Elector) Option {
	return func(cl *Client) {
		if e != nil {
			cl.elector = e
		}
	}
}

// WithLeaderElection is WithElector with a LeaderElector over this
// client's own tables, for the role "scheduler", as node (empty:
// workflow.DefaultNodeID()), on the client's lease clock and WithLeaseSkew.
// The client campaigns from Start and resigns when it stops. opts tune
// the elector (LeaderLease); its tables follow the client's.
func WithLeaderElection(node string, opts ...LeaderOption) Option {
	return func(cl *Client) {
		cl.leaderNode = node
		cl.leaderOpts = opts
		cl.leaderOn = true
	}
}

// IsLeader reports whether this client may fire cron ticks and run
// housekeeping: always true without an elector.
func (c *Client) IsLeader() bool {
	if c.elector == nil {
		return true
	}
	return c.elector.IsLeader()
}

// buildLeader makes WithLeaderElection's elector once the client's options
// are known.
func (c *Client) buildLeader() {
	if !c.leaderOn {
		return
	}
	node := c.leaderNode
	if node == "" {
		node = workflow.DefaultNodeID()
	}
	e := &LeaderElector{role: defaultLeaderRole, node: node, clk: c.leaseClk, lease: defaultLeaderLease, skew: c.leaseSkew}
	for _, o := range c.leaderOpts {
		o(e)
	}
	e.st, e.log = c.st, c.log // the client's tables, whatever an option said
	c.leaderRun = e
	c.elector = e
}
