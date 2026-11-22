package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
)

// Inbox is a durable workflow.Inbox (MQ-15, TX-07): the message IDs each
// consumer claimed or processed are rows of <prefix>inbox, so a message
// redelivered to any process sharing the database is processed once. Like
// UniqueStore it needs no running Client: build it with NewInbox over any
// *sql.DB (an app on Hatchet with a database, a broker consumer), or take
// Client.Inbox.
//
//	consumer := workflow.InboxConsumer{Inbox: inbox, Consumer: "mailer"}
//	ran, err := consumer.Once(ctx, msg.ID, send)
//
// Claim takes over an expired entry with one guarded UPDATE, else inserts
// one, and the primary key decides between two first claims, so of two
// processes claiming one message exactly one wins. MarkTx records a
// message inside the app's own transaction, for work whose writes live in
// this database: the record and the writes commit or roll back together,
// so the message is processed exactly once.
//
// Rows whose lease or keep ended are deleted by PruneExpired, which a
// running Client calls on its dispatcher's housekeeping.
type Inbox struct {
	st *store
}

var _ workflow.Inbox = (*Inbox)(nil)

// InboxMigrationOwner is the ledger owner of Inbox's migrations: a list of
// its own, so NewInbox(...).Migrate creates only the inbox table, while
// Client.Migrate applies it too.
//
//ds:def id=sql-inboxmigrationowner-t7bmxnmy owner=@khanakia stability=stable
const InboxMigrationOwner = "ubgo/workflow/sqlstore/inbox"

// inboxSavepoint is the savepoint MarkTx sets around its insert, so a
// duplicate never aborts the caller's transaction (Postgres aborts a
// transaction on any failed statement).
const inboxSavepoint = "ubgo_workflow_inbox"

// Stored states of an inbox row.
const (
	inboxHeld = "held"
	inboxDone = "done"
)

// inboxMigrations is every Inbox schema change, in order, under the same
// rules as migrations: never edit a released one, append a new one.
//
//ds:def id=sql-store-inboxmigrations-gb5pjrwy owner=@khanakia stability=stable
func (s *store) inboxMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(InboxMigrationOwner), Name: "001_inbox", Script: s.script(s.render(inboxDefs(typesFor(s.engine))))},
	}
}

// inboxDefs is Inbox's table. Its rendered text is frozen once released,
// like ddl.
func inboxDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblInbox, lines: []string{
		// key_hash is the SHA-256 in hex of workflow.InboxKey.String(). One
		// row per consumer and message: the primary key is what makes two
		// first claims of one message race to one row (ADR-0003, a
		// structural constraint).
		"key_hash " + t.id + " PRIMARY KEY",
		// consumer and msg_id are the key's parts, kept readable for an
		// operator; lookups go by key_hash only.
		"consumer " + t.text + " NOT NULL",
		"msg_id " + t.text + " NOT NULL",
		// state is held (a claim is processing it) or done.
		"state " + t.short + " NOT NULL",
		// token fences a claim: Complete and Release act only for the
		// claim that holds the row.
		"token " + t.id + " NOT NULL",
		// until_ns is when the claim's lease or the done entry's keep
		// ends, in Unix nanoseconds of the caller's now.
		"until_ns " + t.int + " NOT NULL",
	}, indexes: []index{{name: "until", cols: "until_ns"}}}}
}

// NewInbox returns the inbox over db for dialect d. Of the Options,
// WithTablePrefix, WithSchema, WithTxAttempts and WithErrorClassifier
// apply; the others configure a Client and are
// ignored. Call Migrate before the first Claim.
//
//ds:def id=sql-newinbox-rz7tpwct owner=@khanakia stability=stable
func NewInbox(db *sql.DB, d sqlb.Dialect, opts ...Option) (*Inbox, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.Inbox(), nil
}

// Inbox returns the inbox in this client's database and prefix.
// Client.Migrate creates its table.
func (c *Client) Inbox() *Inbox { return &Inbox{st: c.st} }

// Migrate creates the inbox table (its own migration list, under the same
// ledger and lock as Client.Migrate), and nothing else.
func (s *Inbox) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.inboxMigrations())
}

// inboxAttempts bounds Claim's loop for a row that disappears between the
// failed insert and the read (a Release in between).
const inboxAttempts = 5

// Claim implements workflow.Inbox.
func (s *Inbox) Claim(ctx context.Context, key workflow.InboxKey, lease time.Duration, now time.Time) (workflow.InboxState, string, error) {
	if key.Consumer == "" || key.ID == "" {
		return 0, "", fmt.Errorf("%w: sqlstore: inbox key needs a Consumer and an ID", workflow.ErrInvalid)
	}
	token := randomID("inbox_") // an internal token, never the app's ID generator
	h, at, until := keyHash(key.String()), now.UnixNano(), now.Add(lease).UnixNano()
	for range inboxAttempts {
		var n int64
		err := s.st.retry(ctx, func() (err error) {
			n, err = s.st.execN(ctx, s.st.db, "UPDATE {inbox} SET state = ?, token = ?, until_ns = ? WHERE key_hash = ? AND until_ns <= ?",
				inboxHeld, token, until, h, at)
			return err
		})
		if err != nil {
			return 0, "", fmt.Errorf("sqlstore: inbox claim: %w", err)
		}
		if n == 1 {
			return workflow.InboxClaimed, token, nil
		}
		err = s.st.retry(ctx, func() error {
			_, err := s.st.exec(ctx, s.st.db, "INSERT INTO {inbox} (key_hash, consumer, msg_id, state, token, until_ns) VALUES (?, ?, ?, ?, ?, ?)",
				h, key.Consumer, key.ID, inboxHeld, token, until)
			return err
		})
		if err == nil {
			return workflow.InboxClaimed, token, nil
		}
		if !s.st.isUnique(err) {
			return 0, "", fmt.Errorf("sqlstore: inbox claim: %w", err)
		}
		var state string
		var untilNs int64
		err = s.st.one(ctx, s.st.db, "SELECT state, until_ns FROM {inbox} WHERE key_hash = ?", []any{h}, &state, &untilNs)
		if errors.Is(err, sqlb.ErrNoRows) {
			continue // released in between: claim again
		}
		if err != nil {
			return 0, "", fmt.Errorf("sqlstore: inbox claim: %w", err)
		}
		if untilNs <= at {
			continue // expired in between: take it over
		}
		if state == inboxDone {
			return workflow.InboxDone, "", nil
		}
		return workflow.InboxBusy, "", nil
	}
	return workflow.InboxBusy, "", nil
}

// Complete implements workflow.Inbox.
func (s *Inbox) Complete(ctx context.Context, key workflow.InboxKey, token string, keep time.Duration, now time.Time) error {
	var n int64
	err := s.st.retry(ctx, func() (err error) {
		n, err = s.st.execN(ctx, s.st.db, "UPDATE {inbox} SET state = ?, until_ns = ? WHERE key_hash = ? AND token = ? AND state = ?",
			inboxDone, now.Add(keep).UnixNano(), keyHash(key.String()), token, inboxHeld)
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlstore: inbox complete: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s", workflow.ErrInboxLost, key)
	}
	return nil
}

// Release implements workflow.Inbox.
func (s *Inbox) Release(ctx context.Context, key workflow.InboxKey, token string) error {
	err := s.st.retry(ctx, func() error {
		_, err := s.st.exec(ctx, s.st.db, "DELETE FROM {inbox} WHERE key_hash = ? AND token = ? AND state = ?", keyHash(key.String()), token, inboxHeld)
		return err
	})
	if err != nil {
		return fmt.Errorf("sqlstore: inbox release: %w", err)
	}
	return nil
}

// MarkTx records key as processed, kept until now+keep, inside the app's
// transaction tx, and reports whether it was new. Do the message's work
// in tx only when it returns true; on false the message was processed (or
// is being processed through Claim) already, so skip the work and commit
// or roll back as you like. The record commits or rolls back with tx, so
// work whose writes live in this database is processed exactly once: of
// two transactions marking one message, the second waits for the first
// and finds the record (Postgres, MySQL), or fails to begin writing until
// the first ends (SQLite). The insert runs under a savepoint, so a
// duplicate never aborts tx. tx must be on this inbox's database.
//
//ds:def id=sql-inbox-marktx-s8saadjk owner=@khanakia stability=stable
func (s *Inbox) MarkTx(ctx context.Context, tx *sql.Tx, key workflow.InboxKey, keep time.Duration, now time.Time) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("%w: sqlstore: inbox MarkTx needs a transaction", workflow.ErrInvalid)
	}
	if key.Consumer == "" || key.ID == "" {
		return false, fmt.Errorf("%w: sqlstore: inbox key needs a Consumer and an ID", workflow.ErrInvalid)
	}
	h, at, until := keyHash(key.String()), now.UnixNano(), now.Add(keep).UnixNano()
	n, err := s.st.execN(ctx, tx, "UPDATE {inbox} SET state = ?, token = ?, until_ns = ? WHERE key_hash = ? AND until_ns <= ?",
		inboxDone, "", until, h, at)
	if err != nil {
		return false, fmt.Errorf("sqlstore: inbox mark: %w", err)
	}
	if n == 1 {
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+inboxSavepoint); err != nil {
		return false, fmt.Errorf("sqlstore: inbox mark: savepoint: %w", err)
	}
	_, err = s.st.exec(ctx, tx, "INSERT INTO {inbox} (key_hash, consumer, msg_id, state, token, until_ns) VALUES (?, ?, ?, ?, ?, ?)",
		h, key.Consumer, key.ID, inboxDone, "", until)
	if err == nil {
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+inboxSavepoint); err != nil {
			return false, fmt.Errorf("sqlstore: inbox mark: release savepoint: %w", err)
		}
		return true, nil
	}
	if !s.st.isUnique(err) {
		return false, fmt.Errorf("sqlstore: inbox mark: %w", err)
	}
	if _, rerr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+inboxSavepoint); rerr != nil {
		return false, fmt.Errorf("sqlstore: inbox mark: roll back to savepoint: %w", rerr)
	}
	return false, nil
}

// PruneExpired deletes every row whose lease or keep ended at or before
// now and returns how many. Such rows never change a decision (an expired
// row is taken over), so this only bounds the table's size; a running
// Client does it on its dispatcher's housekeeping with its own clock.
//
//ds:def id=sql-inbox-pruneexpired-s434c38w owner=@khanakia stability=stable
func (s *Inbox) PruneExpired(ctx context.Context, now time.Time) (int64, error) {
	return s.st.execN(ctx, s.st.db, "DELETE FROM {inbox} WHERE until_ns <= ?", now.UnixNano())
}
