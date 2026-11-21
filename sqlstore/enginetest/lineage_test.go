package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// TestLineageStore is the events.LineageStore contract on every engine,
// through two stores over one table, as two processes would hold: the
// order (At, then reverse recording), every filter, Limit, Meta and At
// round trips, idempotency across both stores and under concurrent
// Records. It also checks that the store's migration is its own list and
// that a second Migrate is a no-op.
func TestLineageStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + e.dialect.QuoteIdent(prefix+"lineage")) })
			a, err := sqlstore.NewLineageStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := sqlstore.NewLineageStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Migrate(ctx); err != nil {
				t.Fatalf("a second Migrate: %v", err)
			}
			scripts := a.MigrationScripts()
			if len(scripts) != 1 || scripts[0].Owner != sqlstore.LineageMigrationOwner || scripts[0].Name != "001_lineage" {
				t.Fatalf("migration scripts = %+v", scripts)
			}
			workflowtest.LineageStoreContract(t, a, b)
		})
	}
}

// TestDatasetsSharedLineage runs events.Datasets in two processes (two
// clients over one database, each with its own bus) sharing lineage
// through Client.LineageStore: each trigger starts once per version, even
// when both announce a trigger's last dataset at once. Client.Migrate
// does not create the lineage table; the store's Migrate does.
func TestDatasetsSharedLineage(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + e.dialect.QuoteIdent(prefix+"lineage")) })
			var clients []*sqlstore.Client
			start := func(t *testing.T, wfs ...workflow.Workflow) workflow.Client {
				c := newClient(t, e, db, prefix, sqlstore.WithLease(queueCaseLease))
				for _, wf := range wfs {
					if err := c.Register(wf); err != nil {
						t.Fatal(err)
					}
				}
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				clients = append(clients, c)
				return c
			}
			first := newClient(t, e, db, prefix)
			if _, err := first.LineageStore().Find(ctx, events.LineageQuery{}); err == nil {
				t.Fatal("Client.Migrate created the lineage table; it belongs to the store's own Migrate")
			}
			if err := first.LineageStore().Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			la, err := sqlstore.NewLineageStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			lb := first.LineageStore()
			workflowtest.DatasetsSharedLineage(t, start, la, lb)
			if len(clients) != 2 {
				t.Fatalf("start called %d times", len(clients))
			}
		})
	}
}
