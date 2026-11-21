package e2e_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ubgo/workflow/recorder/sqlrecorder"
)

// TestSQLRecorderMigrateRace: Migrate promises that processes running it
// at once all succeed. Many rounds, each on fresh tables, with several
// stores (one per process) migrating at the same moment: on Postgres two
// CREATE TABLE IF NOT EXISTS racing in the catalog fail with 42P07
// "relation already exists", and Migrate must retry that, not give up.
func TestSQLRecorderMigrateRace(t *testing.T) {
	const rounds, procs = 15, 6
	ctx := context.Background()
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			for r := range rounds {
				db, prefix := e.open(t)
				start := make(chan struct{})
				errs := make(chan error, procs)
				var wg sync.WaitGroup
				for range procs {
					s, err := sqlrecorder.New(db, dialectOf(e.name), sqlrecorder.WithTablePrefix(prefix+"rec_"))
					if err != nil {
						t.Fatal(err)
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						errs <- s.Migrate(ctx)
					}()
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatalf("round %d: concurrent Migrate: %v", r, err)
					}
				}
			}
		})
	}
}
