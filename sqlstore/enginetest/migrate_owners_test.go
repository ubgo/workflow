package enginetest

import (
	"maps"
	"slices"
	"testing"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/outbox"
)

// TestClientMigrateOwnersAreFixed pins which migration lists
// Client.Migrate (and MigrationScripts) applies: the queue's and the
// stores that shipped inside it by v0.2.0. A store added since, and the
// outbox and the aggregate store, migrate only through their own Migrate,
// so upgrading the library never makes Client.Migrate create tables an
// app did not ask for (see the package doc, "Which tables Migrate
// creates"). A new store belongs in the second list, never the first.
func TestClientMigrateOwnersAreFixed(t *testing.T) {
	scripts, err := sqlstore.MigrationScripts(sqlb.SQLiteDialect{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, s := range scripts {
		got[s.Owner] = true
	}
	want := []string{
		sqlstore.MigrationOwner,
		sqlstore.UniqueMigrationOwner,
		sqlstore.DebounceMigrationOwner,
		sqlstore.OutputMigrationOwner,
		sqlstore.JournalMigrationOwner,
		sqlstore.BucketMigrationOwner,
		sqlstore.DataMigrationOwner,
		sqlstore.InboxMigrationOwner,
	}
	if owners := slices.Sorted(maps.Keys(got)); !slices.Equal(owners, slices.Sorted(slices.Values(want))) {
		t.Fatalf("Client.Migrate applies the lists of %v, want exactly %v: a store added after v0.2.0 migrates on its own", owners, slices.Sorted(slices.Values(want)))
	}
	for _, own := range []string{outbox.MigrationOwner, sqlstore.AggregateMigrationOwner, sqlstore.DisableMigrationOwner, sqlstore.LineageMigrationOwner} {
		if got[own] {
			t.Errorf("Client.Migrate applies %s, which migrates only through its own store's Migrate", own)
		}
	}
}
