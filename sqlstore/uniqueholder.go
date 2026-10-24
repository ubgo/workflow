package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
)

// tblUniqueHolder is UniqueStore's second table (its migration
// 002_unique_holder): the run that holds each key, for
// middleware.UniqueWith's States and ReturnExisting.
const tblUniqueHolder = "unique_holders"

// uniqueHolderDefs is the unique_holders table.
func uniqueHolderDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblUniqueHolder, lines: []string{
		// One row per held key whose run was recorded. owner repeats the
		// lock's owner: a row whose owner no longer holds the lock (it
		// expired and was taken over) is ignored and replaced.
		"key_hash " + t.id + " NOT NULL PRIMARY KEY",
		"owner " + t.text + " NOT NULL",
		"run_id " + t.id + " NOT NULL",
		"scheduled_id " + t.id + " NOT NULL",
	}}}
}

var _ middleware.UniqueHolders = (*UniqueStore)(nil)

// SetHolder records ref as the run of key while owner holds it
// (middleware.UniqueHolders); a key that is free, expired or held by
// another owner is left alone.
func (u *UniqueStore) SetHolder(ctx context.Context, key, owner string, ref workflow.RunRef) error {
	h := keyHash(key)
	return u.st.tx(ctx, func(tx *sql.Tx) error {
		// The delete opens the transaction with a write, so SQLite takes its
		// write lock up front.
		if _, err := u.st.exec(ctx, tx, "DELETE FROM {unique_holders} WHERE key_hash = ?", h); err != nil {
			return err
		}
		var held int64
		if err := u.st.one(ctx, tx, "SELECT COUNT(*) FROM {unique_locks} WHERE key_hash = ? AND owner = ? AND expires > ?",
			[]any{h, owner, u.clk.Now().UnixNano()}, &held); err != nil {
			return err
		}
		if held == 0 {
			return nil
		}
		_, err := u.st.exec(ctx, tx, "INSERT INTO {unique_holders} (key_hash, owner, run_id, scheduled_id) VALUES (?, ?, ?, ?)",
			h, owner, string(ref.RunID), string(ref.ScheduledID))
		return err
	})
}

// Holder reports the owner and run of a held, unexpired key whose run was
// recorded (middleware.UniqueHolders); ok is false otherwise.
func (u *UniqueStore) Holder(ctx context.Context, key string) (string, workflow.RunRef, bool, error) {
	var owner, runID, sid string
	err := u.st.one(ctx, u.st.db, "SELECT l.owner, h.run_id, h.scheduled_id FROM {unique_locks} l JOIN {unique_holders} h ON h.key_hash = l.key_hash AND h.owner = l.owner"+
		" WHERE l.key_hash = ? AND l.expires > ?", []any{keyHash(key), u.clk.Now().UnixNano()}, &owner, &runID, &sid)
	if errors.Is(err, sqlb.ErrNoRows) {
		return "", workflow.RunRef{}, false, nil
	}
	if err != nil {
		return "", workflow.RunRef{}, false, fmt.Errorf("sqlstore: read a unique key's holder: %w", err)
	}
	ref := workflow.RunRef{RunID: workflow.RunID(runID), ScheduledID: workflow.ScheduledID(sid)}
	if ref.IsZero() {
		return "", workflow.RunRef{}, false, nil
	}
	return owner, ref, true, nil
}

// pruneHolders deletes holder rows whose lock is gone.
func (u *UniqueStore) pruneHolders(ctx context.Context) error {
	_, err := u.st.exec(ctx, u.st.db, "DELETE FROM {unique_holders} WHERE key_hash NOT IN (SELECT key_hash FROM {unique_locks})")
	return err
}
