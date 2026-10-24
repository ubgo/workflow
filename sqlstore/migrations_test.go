package sqlstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow/sqlstore"
)

// The migration tests build clients with sqlstore.New directly, because
// newClient already migrates.

func TestMigrateTwiceIsANoOp(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix) // first Migrate
			if err := c.Migrate(context.Background()); err != nil {
				t.Fatalf("second Migrate: %v", err)
			}
			var n int
			q := "SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"migrations") + " WHERE " + e.dialect.QuoteIdent("owner") + " = " + e.dialect.Placeholder(1)
			if err := db.QueryRow(q, "ubgo/workflow/sqlstore").Scan(&n); err != nil {
				t.Fatal(err)
			}
			want := 0
			scripts, err := sqlstore.MigrationScripts(e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range scripts {
				if m.Owner == sqlstore.MigrationOwner {
					want++
				}
			}
			if n != want {
				t.Fatalf("ledger holds %d sqlstore migrations after two Migrate calls, want %d", n, want)
			}
		})
	}
}

func TestMigrateRefusesANewerSchema(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			newClient(t, e, db, prefix)
			// A newer release recorded a migration this code has never heard of.
			cols := e.dialect.QuoteIdent("owner") + ", " + e.dialect.QuoteIdent("name") + ", " + e.dialect.QuoteIdent("checksum") + ", " + e.dialect.QuoteIdent("applied_at")
			ins := "INSERT INTO " + e.dialect.QuoteIdent(prefix+"migrations") + " (" + cols + ") VALUES (" +
				e.dialect.Placeholder(1) + ", " + e.dialect.Placeholder(2) + ", " + e.dialect.Placeholder(3) + ", " + e.dialect.Placeholder(4) + ")"
			if _, err := db.Exec(ins, "ubgo/workflow/sqlstore", "999_future", "x", "2027-01-01T00:00:00Z"); err != nil {
				t.Fatal(err)
			}
			c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Migrate(context.Background())
			var tooNew *sqlstore.SchemaTooNewError
			if !errors.Is(err, sqlstore.ErrSchemaTooNew) || !errors.As(err, &tooNew) || len(tooNew.Unknown) != 1 || tooNew.Unknown[0] != "999_future" {
				t.Fatalf("Migrate on a newer schema: %v, want a SchemaTooNewError naming 999_future", err)
			}
			if err := c.Start(context.Background()); !errors.Is(err, sqlstore.ErrSchemaTooNew) {
				t.Fatalf("Start on a newer schema: %v, want ErrSchemaTooNew", err)
			}
		})
	}
}

func TestMigrateRefusesAnEditedMigration(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			// Stands for 001 having been edited after it was applied: the
			// recorded checksum no longer matches the code's script.
			upd := "UPDATE " + e.dialect.QuoteIdent(prefix+"migrations") + " SET " + e.dialect.QuoteIdent("checksum") + " = 'edited'"
			if _, err := db.Exec(upd); err != nil {
				t.Fatal(err)
			}
			if err := c.Migrate(context.Background()); !errors.Is(err, migrate.ErrChecksumMismatch) {
				t.Fatalf("Migrate after an edit: %v, want migrate.ErrChecksumMismatch", err)
			}
		})
	}
}

func TestConcurrentMigrateBothSucceed(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i := range errs {
				c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
				if err != nil {
					t.Fatal(err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = c.Migrate(context.Background())
				}()
			}
			wg.Wait()
			for i, err := range errs {
				if err != nil {
					t.Errorf("concurrent Migrate %d: %v", i, err)
				}
			}
		})
	}
}

// TestMigrateVersionCheckIsPerOwner pins how checkVersion scopes the ledger
// by owner: a newer UniqueStore migration refuses both the lock store's
// Migrate and the Client's (which applies both lists), named with its
// owner's last path element; a newer queue migration does not refuse a
// standalone lock store, which never touches the queue's tables.
func TestMigrateVersionCheckIsPerOwner(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			newClient(t, e, db, prefix)
			record := func(owner string) {
				t.Helper()
				cols := e.dialect.QuoteIdent("owner") + ", " + e.dialect.QuoteIdent("name") + ", " + e.dialect.QuoteIdent("checksum") + ", " + e.dialect.QuoteIdent("applied_at")
				ins := "INSERT INTO " + e.dialect.QuoteIdent(prefix+"migrations") + " (" + cols + ") VALUES (" +
					e.dialect.Placeholder(1) + ", " + e.dialect.Placeholder(2) + ", " + e.dialect.Placeholder(3) + ", " + e.dialect.Placeholder(4) + ")"
				if _, err := db.Exec(ins, owner, "999_future", "x", "2027-01-01T00:00:00Z"); err != nil {
					t.Fatal(err)
				}
			}
			u, err := sqlstore.NewUniqueStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}

			record("ubgo/workflow/sqlstore")
			if err := u.Migrate(ctx); err != nil {
				t.Fatalf("lock store Migrate with a newer queue schema: %v, want nil", err)
			}

			record("ubgo/workflow/sqlstore/unique")
			var tooNew *sqlstore.SchemaTooNewError
			if err := u.Migrate(ctx); !errors.As(err, &tooNew) || len(tooNew.Unknown) != 1 || tooNew.Unknown[0] != "unique/999_future" {
				t.Fatalf("lock store Migrate with a newer lock schema: %v, want a SchemaTooNewError naming unique/999_future", err)
			}
			c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Migrate(ctx)
			if !errors.As(err, &tooNew) || len(tooNew.Unknown) != 2 {
				t.Fatalf("Client Migrate with both lists newer: %v, want a SchemaTooNewError naming both", err)
			}
			got := map[string]bool{tooNew.Unknown[0]: true, tooNew.Unknown[1]: true}
			if !got["999_future"] || !got["unique/999_future"] {
				t.Fatalf("Unknown = %v, want 999_future and unique/999_future", tooNew.Unknown)
			}
		})
	}
}
