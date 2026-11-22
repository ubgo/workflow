package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
)

// DataStore is a durable workflow.DataStore (WX-01): each scoped value
// (folder, workflow or global data) is a row of <prefix>data with a
// version, so every process sharing the database reads and writes one
// value, and a conditional Put is decided by the database. Like
// JournalStore it needs no running Client: build it with NewDataStore over
// any *sql.DB (an app on Hatchet with a database), or take
// Client.DataStore.
//
//	c, err := workflow.Use(client, workflow.Data(store))
//
// Put with a version is one UPDATE guarded by that version; Put with 0 is
// one INSERT decided by the primary key; Put with AnyVersion updates, or
// inserts when no row is there, in one transaction (a concurrent first
// insert of the same key is tried again and then updates).
//
//ds:def id=sql-datastore-y337tc24 owner=@khanakia stability=stable
type DataStore struct {
	st *store
}

var _ workflow.DataStore = (*DataStore)(nil)

// DataMigrationOwner is the ledger owner of DataStore's migrations: a list
// of its own, so NewDataStore(...).Migrate creates only the data table,
// while Client.Migrate applies it too.
//
//ds:def id=sql-datamigrationowner-awxy22fn owner=@khanakia stability=stable
const DataMigrationOwner = "ubgo/workflow/sqlstore/data"

// dataMigrations is every DataStore schema change, in order, under the
// same rules as migrations: never edit a released one, append a new one.
//
//ds:def id=sql-store-datamigrations-hmesyyuv owner=@khanakia stability=stable
func (s *store) dataMigrations() []migrate.Migration {
	return []migrate.Migration{
		{Owner: s.owner(DataMigrationOwner), Name: "001_data", Script: s.script(s.render(dataDefs(typesFor(s.engine))))},
	}
}

// dataDefs is DataStore's table. Its rendered text is frozen once
// released, like ddl.
func dataDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblData, lines: []string{
		// key_hash is the SHA-256 in hex of workflow.DataScope.Key(name),
		// which is as long as the scope name and the value name together,
		// past any indexable width. One row per key: the primary key is
		// what makes two first Puts of one key race to one row (ADR-0003,
		// a structural constraint).
		"key_hash " + t.id + " PRIMARY KEY",
		// kind, scope and name are the key's parts, kept readable for an
		// operator; lookups go by key_hash only.
		"kind " + t.short + " NOT NULL",
		"scope " + t.text + " NOT NULL",
		"name " + t.text + " NOT NULL",
		"val " + t.blob + " NOT NULL",
		// ver is the workflow.DataVersion: 1 on insert, one more on every
		// write.
		"ver " + t.int + " NOT NULL",
	}}}
}

// NewDataStore returns the data store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Put.
//
//ds:def id=sql-newdatastore-nhsan4b5 owner=@khanakia stability=stable
func NewDataStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*DataStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.DataStore(), nil
}

// DataStore returns the data store in this client's database and prefix.
// Client.Migrate creates its table.
func (c *Client) DataStore() *DataStore { return &DataStore{st: c.st} }

// Migrate creates the data table (its own migration list, under the same
// ledger and lock as Client.Migrate), and nothing else.
func (s *DataStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.st.dataMigrations())
}

// Get implements workflow.DataStore.
func (s *DataStore) Get(ctx context.Context, scope workflow.DataScope, name string) ([]byte, workflow.DataVersion, bool, error) {
	var val []byte
	var ver int64
	err := s.st.one(ctx, s.st.db, "SELECT val, ver FROM {data} WHERE key_hash = ?", []any{keyHash(scope.Key(name))}, &val, &ver)
	if errors.Is(err, sqlb.ErrNoRows) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf("sqlstore: data get: %w", err)
	}
	return append([]byte{}, val...), workflow.DataVersion(ver), true, nil
}

// Put implements workflow.DataStore.
func (s *DataStore) Put(ctx context.Context, scope workflow.DataScope, name string, val []byte, match workflow.DataVersion) (workflow.DataVersion, error) {
	if err := scope.Validate(); err != nil {
		return 0, err
	}
	if val == nil {
		val = []byte{} // the column is NOT NULL; an empty value is a value
	}
	h := keyHash(scope.Key(name))
	insert := func(x sqlb.DBTX) error {
		_, err := s.st.exec(ctx, x, "INSERT INTO {data} (key_hash, kind, scope, name, val, ver) VALUES (?, ?, ?, ?, ?, 1)",
			h, string(scope.Kind), scope.Name, name, val)
		return err
	}
	switch {
	case match == 0:
		err := s.st.retry(ctx, func() error { return insert(s.st.db) })
		if s.st.isUnique(err) {
			return 0, fmt.Errorf("%w: %s %q already exists", workflow.ErrDataConflict, scope, name)
		}
		if err != nil {
			return 0, fmt.Errorf("sqlstore: data put: %w", err)
		}
		return 1, nil
	case match > 0:
		var n int64
		err := s.st.retry(ctx, func() (err error) {
			n, err = s.st.execN(ctx, s.st.db, "UPDATE {data} SET val = ?, ver = ver + 1 WHERE key_hash = ? AND ver = ?", val, h, int64(match))
			return err
		})
		if err != nil {
			return 0, fmt.Errorf("sqlstore: data put: %w", err)
		}
		if n == 0 {
			return 0, fmt.Errorf("%w: %s %q is not at version %d", workflow.ErrDataConflict, scope, name, match)
		}
		return match + 1, nil
	case match == workflow.AnyVersion:
		var ver int64
		var err error
		for range s.st.txAttempts {
			err = s.st.tx(ctx, func(tx *sql.Tx) error {
				n, err := s.st.execN(ctx, tx, "UPDATE {data} SET val = ?, ver = ver + 1 WHERE key_hash = ?", val, h)
				if err != nil {
					return err
				}
				if n == 0 {
					ver = 1
					return insert(tx)
				}
				return s.st.one(ctx, tx, "SELECT ver FROM {data} WHERE key_hash = ?", []any{h}, &ver)
			})
			if !s.st.isUnique(err) || ctx.Err() != nil {
				break
			}
		}
		if err != nil {
			return 0, fmt.Errorf("sqlstore: data put: %w", err)
		}
		return workflow.DataVersion(ver), nil
	default:
		return 0, fmt.Errorf("%w: sqlstore: data version %d", workflow.ErrInvalid, match)
	}
}

// Delete implements workflow.DataStore.
func (s *DataStore) Delete(ctx context.Context, scope workflow.DataScope, name string) error {
	if _, err := s.st.exec(ctx, s.st.db, "DELETE FROM {data} WHERE key_hash = ?", keyHash(scope.Key(name))); err != nil {
		return fmt.Errorf("sqlstore: data delete: %w", err)
	}
	return nil
}
