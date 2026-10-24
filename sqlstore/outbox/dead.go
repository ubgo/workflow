package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// DeadEntry is an entry that failed WithMaxAttempts enqueues and is no
// longer relayed, as ListDead and a DeadFunc see it.
type DeadEntry struct {
	// ID is the entry's ID, which Requeue takes.
	ID string
	// Job is the job as written. Its IdempotencyKey is the job's own; the
	// entry ID the relay adds when it has none is not filled in.
	Job workflow.Job
	// Attempts is how many enqueues failed.
	Attempts int
	// LastErr is the text of the last enqueue error.
	LastErr string
	// CreatedAt is when Write added the entry.
	CreatedAt time.Time
}

// Limits of one ListDead page.
const (
	// defaultDeadPage is a page's size when DeadQuery.Limit is zero.
	defaultDeadPage = 100
	// maxDeadPage caps a page, so one call never reads an unbounded table.
	maxDeadPage = 1000
)

// DeadQuery selects a page of dead entries, oldest first.
type DeadQuery struct {
	// Workflow keeps only entries for this workflow; empty keeps all.
	Workflow string
	// After continues a listing after the entry with this ID (the last ID
	// of the previous page); empty starts at the oldest.
	After string
	// Limit is the page size: zero means 100; it is capped at 1000.
	Limit int
}

// ListDead returns a page of dead entries, oldest first, for an operator
// screen, an alert or a replay script. An After that names no dead entry
// returns ErrNotFound rather than restarting from the beginning.
func (o *Outbox) ListDead(ctx context.Context, q DeadQuery) ([]DeadEntry, error) {
	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultDeadPage
	case limit > maxDeadPage:
		limit = maxDeadPage
	}
	where := "status = ?"
	args := []any{statusDead}
	if q.Workflow != "" {
		where += " AND workflow = ?"
		args = append(args, q.Workflow)
	}
	if q.After != "" {
		var seq int64
		err := sqlb.One(ctx, o.db, sqlb.NewQuery(o.sql("SELECT seq FROM {t} WHERE id = ? AND status = ?"), []any{q.After, statusDead}, []any{&seq}))
		if errors.Is(err, sqlb.ErrNoRows) {
			return nil, fmt.Errorf("%w: dead entry %q", workflow.ErrNotFound, q.After)
		}
		if err != nil {
			return nil, fmt.Errorf("outbox: read dead entry %q: %w", q.After, err)
		}
		where += " AND seq > ?"
		args = append(args, seq)
	}
	args = append(args, limit)
	var out []DeadEntry
	var e entry
	var lastErr sql.NullString
	err := sqlb.Each(ctx, o.db, sqlb.NewQuery(o.sql("SELECT "+entryCols+", last_err FROM {t} WHERE "+where+" ORDER BY seq LIMIT ?"),
		args, append(e.targets(), &lastErr)), func() error {
		job, jerr := e.job(false)
		if jerr != nil {
			o.log.Error("outbox: dead entry listed without labels", "entry", e.id, "error", jerr)
		}
		job.Payload = append([]byte(nil), job.Payload...)
		out = append(out, DeadEntry{ID: e.id, Job: job, Attempts: int(e.attempts), LastErr: lastErr.String, CreatedAt: time.Unix(0, e.created).UTC()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: list dead entries: %w", err)
	}
	return out, nil
}

// Requeue makes a dead entry pending again with a fresh attempt count, due
// at once, so the next relay pass sends it (call Relay.Notify to send it
// now). It keeps the entry's ID, so the idempotency key the relay sends is
// the same as before. An id that names no dead entry returns ErrNotFound.
func (o *Outbox) Requeue(ctx context.Context, id string) error {
	res, err := o.db.ExecContext(ctx, o.sql("UPDATE {t} SET status = ?, attempts = 0, due = ?, lease_token = NULL, lease_until = 0 WHERE id = ? AND status = ?"),
		statusPending, o.clk.Now().UnixNano(), id, statusDead)
	if err != nil {
		return fmt.Errorf("outbox: requeue %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("outbox: requeue %q: rows affected: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: dead entry %q", workflow.ErrNotFound, id)
	}
	return nil
}
