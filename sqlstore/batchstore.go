package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// BatchStore is a durable batch.Store over the client's database and table
// prefix: one row per batch, its State as a JSON document. Update locks the
// row before reading it (the same first-statement-is-a-write rule every
// run transaction follows), so the read, fn and the write are atomic per
// batch on every engine and the once-only latches in batch.State hold
// across processes.
type BatchStore struct{ c *Client }

var (
	_ batch.Store  = (*BatchStore)(nil)
	_ batch.Pruner = (*BatchStore)(nil)

	_ batch.ManyEnqueuer = (*Client)(nil)
)

// BatchStore returns the batch.Store kept in this client's tables. It
// needs Migrate, nothing else: it works before Start and with no workflow
// registered.
func (c *Client) BatchStore() *BatchStore { return &BatchStore{c: c} }

// Create implements batch.Store. An existing ID is an error.
func (b *BatchStore) Create(ctx context.Context, st batch.State) error {
	doc, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("sqlstore: encode batch %q: %w", st.ID, err)
	}
	_, err = b.c.st.exec(ctx, b.c.st.db, "INSERT INTO {batches} (id, doc, ver, finished) VALUES (?, ?, 0, ?)",
		string(st.ID), string(doc), flag(!st.Open()))
	if b.c.st.isUnique(err) {
		return fmt.Errorf("sqlstore: batch id %q already exists", st.ID)
	}
	return err
}

// Get implements batch.Store.
func (b *BatchStore) Get(ctx context.Context, id batch.ID) (batch.State, error) {
	return b.read(ctx, b.c.st.db, id)
}

func (b *BatchStore) read(ctx context.Context, x sqlb.DBTX, id batch.ID) (batch.State, error) {
	var doc string
	err := b.c.st.one(ctx, x, "SELECT doc FROM {batches} WHERE id = ?", []any{string(id)}, &doc)
	if errors.Is(err, sqlb.ErrNoRows) {
		return batch.State{}, fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
	}
	if err != nil {
		return batch.State{}, fmt.Errorf("sqlstore: read batch %q: %w", id, err)
	}
	var st batch.State
	if err := json.Unmarshal([]byte(doc), &st); err != nil {
		return batch.State{}, fmt.Errorf("sqlstore: decode batch %q: %w", id, err)
	}
	return st, nil
}

// Update implements batch.Store: lock, read, apply fn, write, in one
// transaction. When fn fails nothing is written.
func (b *BatchStore) Update(ctx context.Context, id batch.ID, fn func(*batch.State) error) (batch.State, error) {
	var out batch.State
	err := b.c.st.tx(ctx, func(tx *sql.Tx) error {
		n, err := b.c.st.execN(ctx, tx, "UPDATE {batches} SET ver = ver + 1 WHERE id = ?", string(id))
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("batch %q: %w", id, workflow.ErrNotFound)
		}
		st, err := b.read(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := fn(&st); err != nil {
			return err
		}
		doc, err := json.Marshal(st)
		if err != nil {
			return fmt.Errorf("sqlstore: encode batch %q: %w", id, err)
		}
		if _, err := b.c.st.exec(ctx, tx, "UPDATE {batches} SET doc = ?, finished = ? WHERE id = ?",
			string(doc), flag(!st.Open()), string(id)); err != nil {
			return err
		}
		out = st
		return nil
	})
	if err != nil {
		return batch.State{}, err
	}
	return out, nil
}

// Unfinished implements batch.Store: every open batch (State.Open: a job
// has not ended, or a decided callback is still pending), read through the
// finished column's index. The column holds !State.Open(), "closed", and
// keeps the name finished from the 001 schema.
func (b *BatchStore) Unfinished(ctx context.Context) ([]batch.State, error) {
	var out []batch.State
	var doc string
	err := b.c.st.each(ctx, b.c.st.db, "SELECT doc FROM {batches} WHERE finished = 0 ORDER BY id", nil, func() error {
		var st batch.State
		if err := json.Unmarshal([]byte(doc), &st); err != nil {
			return fmt.Errorf("sqlstore: decode batch: %w", err)
		}
		out = append(out, st)
		return nil
	}, &doc)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: read unfinished batches: %w", err)
	}
	return out, nil
}

// pruneBatchPage is how many batch rows Prune reads per query, so a long
// history costs bounded memory.
const pruneBatchPage = 500

// Prune implements batch.Pruner: it deletes
// every batch f.Matches accepts and returns how many. The times live in
// the JSON document, so it reads the table a page at a time, decides in Go
// with f.Matches (the one rule every store applies) and deletes each match
// only if its row did not change since it was read (its ver), so a batch
// an Update moved on in between is judged again on the next Prune rather
// than deleted on stale state. A filter with no rule deletes nothing.
func (b *BatchStore) Prune(ctx context.Context, f batch.PruneFilter) (int, error) {
	if f.IsZero() {
		return 0, nil
	}
	deleted := 0
	after := ""
	for {
		type row struct {
			id, doc string
			ver     int64
		}
		var page []row
		var r row
		err := b.c.st.each(ctx, b.c.st.db, "SELECT id, doc, ver FROM {batches} WHERE id > ? ORDER BY id LIMIT ?",
			[]any{after, pruneBatchPage}, func() error { page = append(page, r); return nil }, &r.id, &r.doc, &r.ver)
		if err != nil {
			return deleted, fmt.Errorf("sqlstore: read batches to prune: %w", err)
		}
		for _, r := range page {
			var st batch.State
			if err := json.Unmarshal([]byte(r.doc), &st); err != nil {
				return deleted, fmt.Errorf("sqlstore: decode batch %q: %w", r.id, err)
			}
			if !f.Matches(st) {
				continue
			}
			n, err := b.c.st.execN(ctx, b.c.st.db, "DELETE FROM {batches} WHERE id = ? AND ver = ?", r.id, r.ver)
			if err != nil {
				return deleted, err
			}
			deleted += int(n)
		}
		if len(page) < pruneBatchPage {
			return deleted, nil
		}
		after = page[len(page)-1].id
	}
}

// flag stores a bool as the 0 or 1 every engine indexes the same way.
func flag(b bool) int {
	if b {
		return 1
	}
	return 0
}
