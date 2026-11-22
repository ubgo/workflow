package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// TestDisableStore is the middleware.DisableStore contract (WH-02) on
// every engine, through two stores over one table, as two processes would
// hold: Fail's crossing reported once, Succeed and Enable resetting, and
// exactly one newly of 50 concurrent Fails. It also checks that the
// store's migration is its own list and that a second Migrate is a no-op.
//
//ds:def id=enginetest-testdisablestore-gx6njhs4 owner=@khanakia stability=stable
func TestDisableStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + e.dialect.QuoteIdent(prefix+"disabled_endpoints")) })
			a, err := sqlstore.NewDisableStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := sqlstore.NewDisableStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Migrate(ctx); err != nil {
				t.Fatalf("a second Migrate: %v", err)
			}
			scripts := a.MigrationScripts()
			if len(scripts) != 1 || scripts[0].Owner != sqlstore.DisableMigrationOwner || scripts[0].Name != "001_disable" {
				t.Fatalf("migration scripts = %+v", scripts)
			}
			workflowtest.DisableStoreContract(t, a, b)
		})
	}
}

// TestDisableStoreFromClient: Client.DisableStore is the same store, in
// the client's prefix, and Client.Migrate does not create its table.
func TestDisableStoreFromClient(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + e.dialect.QuoteIdent(prefix+"disabled_endpoints")) })
			c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			s := c.DisableStore()
			if _, err := s.Disabled(ctx, "k"); err == nil {
				t.Fatal("Client.Migrate created the disable table; it belongs to the store's own Migrate")
			}
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if newly, err := s.Fail(ctx, "k", 1); err != nil || !newly {
				t.Fatalf("Fail = %v, %v", newly, err)
			}
			other, err := sqlstore.NewDisableStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if off, err := other.Disabled(ctx, "k"); err != nil || !off {
				t.Fatalf("a standalone store on the same prefix: %v, %v", off, err)
			}
		})
	}
}
