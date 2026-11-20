package enginetest

import (
	"context"
	"testing"

	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/workflowtest"
)

// TestDataStore is the workflow.DataStore contract (WX-01) on every
// engine, through two stores over one table, as two processes would hold.
func TestDataStore(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			a, err := sqlstore.NewDataStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			b, err := sqlstore.NewDataStore(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			workflowtest.DataStoreContract(t, a, b)
		})
	}
}
