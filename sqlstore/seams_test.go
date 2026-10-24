package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/sqlb/ddl"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/sqlstore"
)

// frozenChecksums pins the ledger checksum of every released migration
// with the default prefix, per engine. A released migration is never
// edited: a change here means every database that applied it would fail
// Migrate with migrate.ErrChecksumMismatch. Append the new migration's
// checksum when one is added; never change an existing line.
var frozenChecksums = map[string]string{
	"sqlite ubgo/workflow/sqlstore 001_initial":                "1aff1a03905c59c6496de3fefc28a162904bc5b5c5b90a1cd55bb82e310b5b7b",
	"sqlite ubgo/workflow/sqlstore 002_schedule_misfire":       "025a80f2993736358cf39272ac529595f9999c651f427565ecce22aa8b507d2f",
	"sqlite ubgo/workflow/sqlstore/unique 001_unique":          "e2aa974f428cbd18a47b6429ed0c0aac24c80b49e622104de9d592fa0100074b",
	"postgres ubgo/workflow/sqlstore 001_initial":              "6ba93751ea0f3f611d5a468eddab90c8b32f1c0f55f7da83110d01f4336008d7",
	"postgres ubgo/workflow/sqlstore 002_schedule_misfire":     "797c4a77743447c8f43dd4574c85d4a02b74dbf3ef84700b77abf986535e0006",
	"postgres ubgo/workflow/sqlstore/unique 001_unique":        "09416816283ba499d47d16a0c90acd1c686e14c80c68d3edf84f62bbbab145e6",
	"mysql ubgo/workflow/sqlstore 001_initial":                 "d8177697008de44b3047c0a3627169355f208ca9cedd86dc0093e1ce4ef6d646",
	"mysql ubgo/workflow/sqlstore 002_schedule_misfire":        "6a058b8bf529f2a5c56b49054b7c900b06eac16851d11811be478c2585551267",
	"mysql ubgo/workflow/sqlstore/unique 001_unique":           "c3098a7819d5d9eaa1464e3c43a287a3ef9e14adc67072e1be0a27d7942cb2fe",
	"sqlite ubgo/workflow/sqlstore/debounce 001_debounce":      "51dcce462478883444dd6da25f134c1d41115c9e7dfe00451f52a2d529808a86",
	"postgres ubgo/workflow/sqlstore/debounce 001_debounce":    "d21ebbe76c119731da4930dde7749bc6d500965be05f92feacac9284fc084b67",
	"mysql ubgo/workflow/sqlstore/debounce 001_debounce":       "3e0b07577e3ea8e3d702b4f7c8e5eb4a29aa6b3478798e7c45a3a1f92a90ec68",
	"sqlite ubgo/workflow/sqlstore 003_queues_admin":           "359f7c9fc530a570236c9470172210c8902fd4c0cd7d3d841d788bdc4e1a96b3",
	"postgres ubgo/workflow/sqlstore 003_queues_admin":         "980148421afed35b4886bc3308217d56c1d6797d980d9083c36be0d1ee42dddd",
	"mysql ubgo/workflow/sqlstore 003_queues_admin":            "5c4fcb315e8b33337083a33a7e5fae70c44b78ab01829ec316d06b55629c7813",
	"sqlite ubgo/workflow/sqlstore 004_run_states_chain":       "5724d956b3d3a8b7c7b6368177af509a140065a7261fb91b02edc9de7782018a",
	"postgres ubgo/workflow/sqlstore 004_run_states_chain":     "5fde2f64b260d3287614d348dd9693d3e301082dbdadd7f9b07d75a4b1ac754e",
	"mysql ubgo/workflow/sqlstore 004_run_states_chain":        "a2633cbd7b071c2974ff0742472c81a470fd8049a1628b1a5e73c11cfb15f03f",
	"sqlite ubgo/workflow/sqlstore 005_run_labels":             "e624e005cb691e460af016f3c39225382599d3f7f651a299203a244bd2a710bb",
	"postgres ubgo/workflow/sqlstore 005_run_labels":           "e1ae23b0a53621484ab90388a21ffa938125fe0131d21da226f6e11191a4f719",
	"mysql ubgo/workflow/sqlstore 005_run_labels":              "7d053bc428486519bfbfe456e335828502a9bbdba693098d61ea9ea8c5e0d5f4",
	"sqlite ubgo/workflow/sqlstore 006_schedule_history":       "a01508deeabcd3b97788602c83903028f4d81df75cee18632f8c15d3d24d6276",
	"postgres ubgo/workflow/sqlstore 006_schedule_history":     "7db247dce8ad58e22783b4e49b0b78245a1b4a114a52986f6a26c523bfc11997",
	"mysql ubgo/workflow/sqlstore 006_schedule_history":        "6d475ca8456f3219f99ea1e978a51a6374893022db33a118cc75928df72522a0",
	"sqlite ubgo/workflow/sqlstore/output 001_output":          "8cec5a7a81e81c2558ba28ca1d19b96055ad3917c34109f3f39ca2f1b648f75e",
	"postgres ubgo/workflow/sqlstore/output 001_output":        "40b5d3eab28806aac12a5447f9222c9a4c84951319a639d2bacab80a4afd80ae",
	"mysql ubgo/workflow/sqlstore/output 001_output":           "2893dc2bddf21e991cfbe1fcb46f04279d720f409a633c9634e189422c2641e2",
	"sqlite ubgo/workflow/sqlstore 007_leader_archive":         "f2a8ac58296be399fb2db3494962d6e7d404207ca39df615ec7075f26b1dbd38",
	"postgres ubgo/workflow/sqlstore 007_leader_archive":       "8a9183ee29d401b49eb3b2239fcfe2c4cb096b4060faafe2e60336f8df1aad42",
	"mysql ubgo/workflow/sqlstore 007_leader_archive":          "9bfcdae00c33adb47cacf8321a36a3291f618d144b60cbd63eced79db49389af",
	"sqlite ubgo/workflow/sqlstore 008_run_info_nodes":         "63b01eee36b3252bd93d8f6796b10d8361bf8a4c4084d2c33ea54c95bf77104e",
	"postgres ubgo/workflow/sqlstore 008_run_info_nodes":       "c99847e6bf7610e31ad4bbbcb1f4b63742524d1a3d2ca301c620eba20774a029",
	"mysql ubgo/workflow/sqlstore 008_run_info_nodes":          "b654e403d369564e335a5c176e91716d6a24eb05f363f5c90e664c6bbb81b358",
	"sqlite ubgo/workflow/sqlstore/unique 002_unique_holder":   "d6423e263e432cebbdcaa1b2eb6d7236c927e8acfe226974408b73e29f803633",
	"postgres ubgo/workflow/sqlstore/unique 002_unique_holder": "2b2467b6ad1c6e67126aa63d5b7f1e7103becdcfbaabb5e70b09a67bbaaf53e1",
	"mysql ubgo/workflow/sqlstore/unique 002_unique_holder":    "07fbe8621a3e4fc367c96f2bb025ce800d390e115961713706be0eb15d5e61c7",
	"sqlite ubgo/workflow/sqlstore 009_schedule_tenant":        "ddb147de02e8c702346b0a52c48e94e86d5ed4597bc4ae2e8e5bd2c50f80a082",
	"sqlite ubgo/workflow/sqlstore/journal 001_journal":        "2a559bdaaa463913a28d2c5d72bd350609b0dc2d2b38245a85c2a53ec301bcdb",
	"sqlite ubgo/workflow/sqlstore/bucket 001_buckets":         "d1d74bc8f75b17ada1cf5fe4fc716e68646fdda36c712f83fb56d3c4f8da08fa",
	"postgres ubgo/workflow/sqlstore 009_schedule_tenant":      "6157cd6351fa3636ac58e9015021effb8c8fe96fadfa07ab3fbaa28a9eb7db0d",
	"postgres ubgo/workflow/sqlstore/journal 001_journal":      "88daf7de07728cd1bb358518f3294495f91f264bc94b63382584ad10b45a1f7a",
	"postgres ubgo/workflow/sqlstore/bucket 001_buckets":       "6aef137b46458c4029373aa0228aa73e3b46ea666dc10597a385804e62638a99",
	"mysql ubgo/workflow/sqlstore 009_schedule_tenant":         "efb477c49811861bfbff552194aee728b755fefa2d60978fc2fb5a1ff3abaf92",
	"mysql ubgo/workflow/sqlstore/journal 001_journal":         "15848682e620817a67d8ecd16d913e24625af32f406af928b63b119d5f7eb4ec",
	"mysql ubgo/workflow/sqlstore/bucket 001_buckets":          "4740aa81988d327e2c9f1764807d053c7b3e1c53688b6a38aebab25d839ac1a6",
}

// checksum is the ledger's checksum of a MigrationScript, rebuilt the way
// Migrate builds its ddl.Script (every statement transactional).
func checksum(m sqlstore.MigrationScript) string {
	var sc ddl.Script
	for _, st := range m.Statements {
		sc = append(sc, ddl.Statement{SQL: st, Transactional: true})
	}
	return migrate.Checksum(sc)
}

// TestMigrationScriptsAreFrozen proves MigrationScripts is the exact text
// Migrate records, and that no released migration changed.
func TestMigrationScriptsAreFrozen(t *testing.T) {
	seen := 0
	for _, d := range []sqlb.Dialect{sqlb.SQLiteDialect{}, sqlb.PostgresDialect{}, sqlb.MySQLDialect{}} {
		scripts, err := sqlstore.MigrationScripts(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scripts {
			key := sqlb.DialectName(d) + " " + m.Owner + " " + m.Name
			want, ok := frozenChecksums[key]
			if !ok {
				t.Errorf("%s has no pinned checksum; add %q", key, checksum(m))
				continue
			}
			seen++
			if got := checksum(m); got != want {
				t.Errorf("%s changed: checksum %s, pinned %s. A released migration is never edited; append a new one", key, got, want)
			}
		}
	}
	if seen != len(frozenChecksums) {
		t.Errorf("checked %d pinned migrations, want %d: a released migration disappeared", seen, len(frozenChecksums))
	}
}

// TestMigrationScriptsWithSchema checks the schema-qualified text and the
// refusals: a schema on SQLite or MySQL, and a schema name a statement
// could not hold unquoted.
func TestMigrationScriptsWithSchema(t *testing.T) {
	scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{}, sqlstore.WithSchema("jobs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range scripts {
		if !strings.HasSuffix(m.Owner, "@jobs") {
			t.Errorf("owner %q under WithSchema, want an @jobs suffix", m.Owner)
		}
		for _, st := range m.Statements {
			if !strings.Contains(st, `"jobs"."wf_`) {
				t.Errorf("statement does not name the schema: %s", st)
			}
		}
	}
	for _, d := range []sqlb.Dialect{sqlb.SQLiteDialect{}, sqlb.MySQLDialect{}} {
		if _, err := sqlstore.MigrationScripts(d, sqlstore.WithSchema("jobs")); !errors.Is(err, workflow.ErrUnsupported) {
			t.Errorf("WithSchema on %s: %v, want ErrUnsupported", sqlb.DialectName(d), err)
		}
	}
	if _, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{}, sqlstore.WithSchema(`jobs"; DROP`)); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("a schema outside the pattern: %v, want ErrInvalid", err)
	}
}

// applyScripts runs the statements of the scripts kept by keep by hand,
// twice each, the way a DBA or goose would: no ledger, and proof that each
// statement is safe to run again.
func applyScripts(t *testing.T, e engine, db *sql.DB, prefix string, keep func(sqlstore.MigrationScript) bool) {
	t.Helper()
	scripts, err := sqlstore.MigrationScripts(e.dialect, sqlstore.WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range scripts {
		if !keep(m) {
			continue
		}
		for range 2 {
			for _, st := range m.Statements {
				if _, err := db.Exec(st); err != nil {
					t.Fatalf("%s: %v\n%s", m.Name, err, st)
				}
			}
		}
	}
}

// TestSchemaAppliedElsewhereStarts is the point of MigrationScripts: a
// schema applied by another tool starts without Migrate. It also walks the
// upgrade from a release that had only the 001 migrations: Start refuses
// until 002 and then 003 are applied, and Migrate over the hand-applied schema then
// succeeds and records every migration.
func TestSchemaAppliedElsewhereStarts(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return strings.HasPrefix(m.Name, "001_") })

			old, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := old.Start(ctx); err == nil || !strings.Contains(err.Error(), "schedule_misfire") {
				old.Stop()
				t.Fatalf("Start on a 001-only schema: %v, want an error naming schedule_misfire", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "002_schedule_misfire" })
			mid, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := mid.Start(ctx); err == nil || !strings.Contains(err.Error(), "run_ext") {
				mid.Stop()
				t.Fatalf("Start on a schema up to 002: %v, want an error naming run_ext", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "003_queues_admin" })
			late, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := late.Start(ctx); err == nil || !strings.Contains(err.Error(), "run_chain") {
				late.Stop()
				t.Fatalf("Start on a schema up to 003: %v, want an error naming run_chain", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "004_run_states_chain" })
			before, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := before.Start(ctx); err == nil || !strings.Contains(err.Error(), "run_labels") {
				before.Stop()
				t.Fatalf("Start on a schema up to 004: %v, want an error naming run_labels", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "005_run_labels" })
			pre6, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := pre6.Start(ctx); err == nil || !strings.Contains(err.Error(), "schedule_last") {
				pre6.Stop()
				t.Fatalf("Start on a schema up to 005: %v, want an error naming schedule_last", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "006_schedule_history" })
			pre7, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := pre7.Start(ctx); err == nil || !strings.Contains(err.Error(), "leaders") {
				pre7.Stop()
				t.Fatalf("Start on a schema up to 006: %v, want an error naming leaders", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name == "007_leader_archive" })
			pre8, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := pre8.Start(ctx); err == nil || !strings.Contains(err.Error(), "run_info") {
				pre8.Stop()
				t.Fatalf("Start on a schema up to 007: %v, want an error naming run_info", err)
			}

			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool {
				return m.Name == "008_run_info_nodes" || m.Name == "002_unique_holder"
			})
			pre9, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := pre9.Start(ctx); err == nil || !strings.Contains(err.Error(), "schedule_tenant") {
				pre9.Stop()
				t.Fatalf("Start on a schema up to 008: %v, want an error naming schedule_tenant", err)
			}
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool {
				return m.Name == "009_schedule_tenant" || m.Name == "001_journal" || m.Name == "001_buckets"
			})
			c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix), sqlstore.WithPollInterval(testPoll))
			if err != nil {
				t.Fatal(err)
			}
			wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start on a schema applied without Migrate: %v", err)
			}
			t.Cleanup(c.Stop)
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "the run to succeed", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			if err := c.Migrate(ctx); err != nil {
				t.Fatalf("Migrate over a hand-applied schema: %v", err)
			}
			var n int
			q := "SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"migrations")
			if err := db.QueryRow(q).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != len(frozenChecksums)/3 {
				t.Fatalf("ledger holds %d rows after Migrate, want %d", n, len(frozenChecksums)/3)
			}
		})
	}
}

// createTable finds the table each CREATE TABLE statement creates.
var createTable = regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS "([^"]+)"\."([^"]+)"`)

// scriptTables is the set of tables every Postgres migration script
// creates in schema, under prefix.
func scriptTables(t *testing.T, schema, prefix string) map[string]bool {
	t.Helper()
	scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{}, sqlstore.WithTablePrefix(prefix), sqlstore.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, m := range scripts {
		for _, st := range m.Statements {
			if g := createTable.FindStringSubmatch(st); g != nil {
				if g[1] != schema {
					t.Fatalf("%s creates a table in schema %q, want %q", m.Name, g[1], schema)
				}
				out[g[2]] = true
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no CREATE TABLE found in the migration scripts; the pattern no longer matches their text")
	}
	return out
}

// schemaTables is the set of tables in a Postgres schema.
func schemaTables(t *testing.T, db *sql.DB, schema string) map[string]bool {
	t.Helper()
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// setDiff is the sorted members of a that b lacks.
func setDiff(a, b map[string]bool) []string {
	var out []string
	for k := range a {
		if !b[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// TestWithSchemaPostgres runs a client in its own Postgres schema: the
// tables are created there, nothing is created under the prefix in the
// search_path apart from the shared ledger, and a run goes through.
func TestWithSchemaPostgres(t *testing.T) {
	for _, e := range engines(t) {
		if e.name != "postgres" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			schema := "s" + randHex(6)
			if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA " + schema + " CASCADE") })
			c := newClient(t, e, db, prefix, sqlstore.WithSchema(schema))
			wf := workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "the run to succeed", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			var outside int
			if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE $1 AND table_name <> $2",
				prefix+"%", prefix+"migrations").Scan(&outside); err != nil {
				t.Fatal(err)
			}
			// The schema holds exactly the tables the migration scripts create
			// (the ledger lives in the search_path, not the schema), so a new
			// migration needs no edit here.
			want := scriptTables(t, schema, prefix)
			got := schemaTables(t, db, schema)
			if missing, extra := setDiff(want, got), setDiff(got, want); len(missing) > 0 || len(extra) > 0 {
				t.Fatalf("schema %s: missing tables %v, unexpected tables %v", schema, missing, extra)
			}
			if outside != 0 {
				t.Fatalf("%d tables beside the ledger in the search_path, want 0", outside)
			}
			var owner string
			if err := db.QueryRow(`SELECT "owner" FROM "` + prefix + `migrations" WHERE "name" = '001_initial'`).Scan(&owner); err != nil {
				t.Fatal(err)
			}
			if owner != sqlstore.MigrationOwner+"@"+schema {
				t.Fatalf("ledger owner %q, want %q", owner, sqlstore.MigrationOwner+"@"+schema)
			}
		})
	}
}

// sqlState is an error carrying a Postgres SQLSTATE the way pgx and lib/pq
// expose it.
type sqlState string

func (s sqlState) Error() string    { return "pg error " + string(s) }
func (s sqlState) SQLState() string { return string(s) }

// TestDefaultErrorClassifier pins every signature the default knows, and
// that an unknown error is neither (the fail-closed answer).
func TestDefaultErrorClassifier(t *testing.T) {
	for _, c := range []struct {
		err  error
		want sqlstore.ErrorClass
	}{
		{fmt.Errorf("wrapped: %w", sqlState("23505")), sqlstore.ErrorClass{UniqueViolation: true}},
		{sqlState("40P01"), sqlstore.ErrorClass{Retryable: true}},
		{sqlState("40001"), sqlstore.ErrorClass{Retryable: true}},
		{sqlState("08006"), sqlstore.ErrorClass{Unavailable: true}},
		{sqlState("57P01"), sqlstore.ErrorClass{Unavailable: true}},
		{sqlState("42P01"), sqlstore.ErrorClass{}},
		{errors.New("Error 1062 (23000): Duplicate entry"), sqlstore.ErrorClass{UniqueViolation: true}},
		{errors.New("Error 1213 (40001): Deadlock found"), sqlstore.ErrorClass{Retryable: true}},
		{errors.New("Error 1205 (HY000): Lock wait timeout"), sqlstore.ErrorClass{Retryable: true}},
		{errors.New("Error 1040: Too many connections"), sqlstore.ErrorClass{Unavailable: true}},
		{errors.New("invalid connection"), sqlstore.ErrorClass{Unavailable: true}},
		{errors.New("constraint failed: UNIQUE constraint failed: wf_runs.id (2067)"), sqlstore.ErrorClass{UniqueViolation: true}},
		{errors.New("database is locked (5) (SQLITE_BUSY)"), sqlstore.ErrorClass{Retryable: true}},
		{&net.OpError{Op: "dial", Err: errors.New("connection refused")}, sqlstore.ErrorClass{Unavailable: true}},
		{fmt.Errorf("x: %w", driver.ErrBadConn), sqlstore.ErrorClass{Unavailable: true}},
		{sql.ErrConnDone, sqlstore.ErrorClass{Unavailable: true}},
		{errors.New("no such table: wf_runs"), sqlstore.ErrorClass{}},
	} {
		if got := sqlstore.DefaultErrorClassifier(c.err); got != c.want {
			t.Errorf("DefaultErrorClassifier(%q) = %+v, want %+v", c.err, got, c.want)
		}
	}
}

// idemWorkflow is a workflow that blocks until released, so a run holds
// its idempotency key while a second enqueue races it.
func idemWorkflow(release <-chan struct{}) workflow.Workflow {
	return workflow.Workflow{Name: "idem", Steps: []workflow.Step{{Name: "a", Run: func(ctx context.Context, in []byte) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return in, nil
	}}}}
}

func startWith(t *testing.T, c *sqlstore.Client, wfs ...workflow.Workflow) context.Context {
	t.Helper()
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx
}

// TestErrorClassifier: the default answers a held idempotency key with the
// holder's run; a classifier that recognises the unique violation (here by
// delegating) does the same; one that does not (it says "neither") makes
// the second Enqueue fail rather than start a second run, which is the
// fail-closed direction.
func TestErrorClassifier(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				opts    []sqlstore.Option
				wantDup bool // the second enqueue returns the holder
			}{
				{"default", nil, true},
				{"custom recognises", []sqlstore.Option{sqlstore.WithErrorClassifier(func(err error) sqlstore.ErrorClass {
					return sqlstore.DefaultErrorClassifier(err)
				})}, true},
				{"custom does not recognise", []sqlstore.Option{sqlstore.WithErrorClassifier(func(error) sqlstore.ErrorClass {
					return sqlstore.ErrorClass{}
				})}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db, prefix := e.open(t)
					release := make(chan struct{})
					t.Cleanup(func() { close(release) })
					c := newClient(t, e, db, prefix, tc.opts...)
					ctx := startWith(t, c, idemWorkflow(release))
					first, err := c.Enqueue(ctx, workflow.Job{Workflow: "idem", IdempotencyKey: "k"})
					if err != nil {
						t.Fatal(err)
					}
					second, err := c.Enqueue(ctx, workflow.Job{Workflow: "idem", IdempotencyKey: "k"})
					if tc.wantDup {
						if err != nil || second.RunID != first.RunID {
							t.Fatalf("second enqueue = %v, %v; want the holder %v", second, err, first.RunID)
						}
						return
					}
					if err == nil {
						t.Fatalf("second enqueue = %v with a classifier that does not see the duplicate; want an error", second)
					}
					var runs int
					if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"runs")).Scan(&runs); err != nil {
						t.Fatal(err)
					}
					if runs != 1 {
						t.Fatalf("%d runs stored, want 1", runs)
					}
				})
			}
		})
	}
}

// TestTxAttempts: a classifier that calls every error retryable makes a
// failing transaction run WithTxAttempts times (default 5), then fail.
func TestTxAttempts(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				opts []sqlstore.Option
				want int
			}{{"default", nil, 5}, {"three", []sqlstore.Option{sqlstore.WithTxAttempts(3)}, 3}, {"ignored zero", []sqlstore.Option{sqlstore.WithTxAttempts(0)}, 5}} {
				t.Run(tc.name, func(t *testing.T) {
					db, prefix := e.open(t)
					opts := append([]sqlstore.Option{sqlstore.WithErrorClassifier(func(error) sqlstore.ErrorClass {
						return sqlstore.ErrorClass{Retryable: true}
					})}, tc.opts...)
					c := newClient(t, e, db, prefix, opts...)
					ctx := startWith(t, c, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
					c.Stop() // nothing else may call the classifier
					if _, err := db.Exec("DROP TABLE " + e.dialect.QuoteIdent(prefix+"runs")); err != nil {
						t.Fatal(err)
					}
					got, err := sqlstore.TxOnRuns(ctx, c)
					if err == nil {
						t.Fatal("a transaction on a dropped table succeeded")
					}
					if got != tc.want {
						t.Fatalf("transaction tried %d times, want %d", got, tc.want)
					}
				})
			}
		})
	}
}

// TestIDGenerator: a custom generator's IDs are the RunIDs and step IDs;
// one that errors, or returns an ID that does not fit, fails the enqueue
// and writes nothing; the default keeps its format.
func TestIDGenerator(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var n atomic.Int64
			custom := func(kind sqlstore.IDKind) (string, error) {
				return fmt.Sprintf("my-%s-%d", kind, n.Add(1)), nil
			}
			for _, tc := range []struct {
				name    string
				gen     sqlstore.IDGenerator
				wantErr error
				prefix  string
			}{
				{"default", nil, nil, "run_"},
				{"custom", custom, nil, "my-run-"},
				{"error", func(sqlstore.IDKind) (string, error) { return "", errors.New("id service down") }, errors.New("id service down"), ""},
				{"too long", func(sqlstore.IDKind) (string, error) { return strings.Repeat("x", 65), nil }, workflow.ErrInvalid, ""},
				{"empty", func(sqlstore.IDKind) (string, error) { return "", nil }, workflow.ErrInvalid, ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db, prefix := e.open(t)
					var opts []sqlstore.Option
					if tc.gen != nil {
						opts = append(opts, sqlstore.WithIDGenerator(tc.gen))
					}
					c := newClient(t, e, db, prefix, opts...)
					ctx := startWith(t, c, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
					ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w"})
					if tc.wantErr != nil {
						if err == nil || (errors.Is(tc.wantErr, workflow.ErrInvalid) && !errors.Is(err, workflow.ErrInvalid)) ||
							!strings.Contains(err.Error(), strings.TrimPrefix(tc.wantErr.Error(), "workflow: ")) {
							t.Fatalf("Enqueue = %v, %v; want an error like %v", ref, err, tc.wantErr)
						}
						var runs int
						if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"runs")).Scan(&runs); err != nil {
							t.Fatal(err)
						}
						if runs != 0 {
							t.Fatalf("%d runs written by a failed generator, want 0", runs)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if !strings.HasPrefix(string(ref.RunID), tc.prefix) {
						t.Fatalf("RunID %q, want prefix %q", ref.RunID, tc.prefix)
					}
					if tc.name == "default" && len(ref.RunID) != len("run_")+32 {
						t.Fatalf("default RunID %q is not run_ plus 32 hex", ref.RunID)
					}
					waitFor(t, 5*time.Second, "the run to succeed", func() bool {
						st, err := c.Status(ctx, ref)
						return err == nil && st == workflow.StatusSucceeded
					})
					if tc.name != "custom" {
						return
					}
					var stepID string
					if err := db.QueryRow("SELECT id FROM "+e.dialect.QuoteIdent(prefix+"steps")+" WHERE run_id = "+e.dialect.Placeholder(1), string(ref.RunID)).Scan(&stepID); err != nil {
						t.Fatal(err)
					}
					if !strings.HasPrefix(stepID, "my-step-") {
						t.Fatalf("step id %q, want the generator's", stepID)
					}
					later, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", RunAt: time.Now().Add(24 * time.Hour)})
					if err != nil || !strings.HasPrefix(string(later.ScheduledID), "my-sched-") {
						t.Fatalf("delayed enqueue = %+v, %v; want a ScheduledID from the generator", later, err)
					}
				})
			}
		})
	}
}

// scheduleRows counts schedules rows of workflow wf.
func scheduleRows(t *testing.T, e engine, db *sql.DB, prefix, wf string) (n int, nextFire int64) {
	t.Helper()
	q := "SELECT COUNT(*), COALESCE(MAX(next_fire), 0) FROM " + e.dialect.QuoteIdent(prefix+"schedules") + " WHERE workflow = " + e.dialect.Placeholder(1)
	if err := db.QueryRow(q, wf).Scan(&n, &nextFire); err != nil {
		t.Fatal(err)
	}
	return n, nextFire
}

// TestScheduleUpsertsByName: the same name is the same row and ID across
// calls and clients (a reboot), an unchanged re-declaration keeps the next
// fire time, a changed cron moves it, an Unscheduled name comes back
// active, two nodes declaring one name at once leave one row, and a
// schedule without a name is new every time.
func TestScheduleUpsertsByName(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			wf := workflow.Workflow{Name: "tick", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}
			c1 := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			ctx := startWith(t, c1, wf)
			s := workflow.Schedule{Name: "nightly", Workflow: "tick", Cron: "0 3 * * *", Location: time.UTC}
			id1, err := c1.Schedule(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			_, next1 := scheduleRows(t, e, db, prefix, "tick")

			// A reboot an hour later: a new client, the same declaration.
			clk.Advance(time.Hour)
			c1.Stop()
			c2 := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			ctx = startWith(t, c2, wf)
			id2, err := c2.Schedule(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			n, next2 := scheduleRows(t, e, db, prefix, "tick")
			if id2 != id1 || n != 1 || next2 != next1 {
				t.Fatalf("re-declared after a reboot: id %s (first %s), %d rows, next %d (first %d); want the same id, 1 row, the same next fire", id2, id1, n, next2, next1)
			}

			// An unchanged re-declaration keeps whatever next fire time is
			// stored (here moved a week ahead by hand, so a recompute would
			// show).
			sched := e.dialect.QuoteIdent(prefix + "schedules")
			if _, err := db.Exec("UPDATE "+sched+" SET next_fire = next_fire + "+e.dialect.Placeholder(1)+" WHERE id = "+e.dialect.Placeholder(2),
				int64(7*24*time.Hour), string(id1)); err != nil {
				t.Fatal(err)
			}
			if _, err := c2.Schedule(ctx, s); err != nil {
				t.Fatal(err)
			}
			if _, next := scheduleRows(t, e, db, prefix, "tick"); next != next1+int64(7*24*time.Hour) {
				t.Fatalf("unchanged re-declaration moved next_fire to %d, want the stored %d kept", next, next1+int64(7*24*time.Hour))
			}

			// A changed cron moves the next fire time on the same row.
			s.Cron = "0 4 * * *"
			if id, err := c2.Schedule(ctx, s); err != nil || id != id1 {
				t.Fatalf("changed cron: %v, %v", id, err)
			}
			if n, next := scheduleRows(t, e, db, prefix, "tick"); n != 1 || next == next1 || next == next1+int64(7*24*time.Hour) {
				t.Fatalf("changed cron: %d rows, next %d; want 1 row and a new next fire", n, next)
			}

			// Unscheduled, then declared again: active again.
			if err := c2.Unschedule(ctx, id1); err != nil {
				t.Fatal(err)
			}
			if _, err := c2.Schedule(ctx, s); err != nil {
				t.Fatal(err)
			}
			var active int
			if err := db.QueryRow("SELECT active FROM "+e.dialect.QuoteIdent(prefix+"schedules")+" WHERE id = "+e.dialect.Placeholder(1), string(id1)).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if active != 1 {
				t.Fatal("a re-declared Unscheduled name stayed inactive")
			}

			// Two nodes declaring a new name at the same moment.
			c3 := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			startWith(t, c3, wf)
			var wg sync.WaitGroup
			ids := make([]workflow.ScheduleID, 8)
			errs := make([]error, len(ids))
			for i := range ids {
				wg.Add(1)
				go func() {
					defer wg.Done()
					cl := c2
					if i%2 == 1 {
						cl = c3
					}
					ids[i], errs[i] = cl.Schedule(ctx, workflow.Schedule{Name: "race", Workflow: "tick", Cron: "*/5 * * * *", Location: time.UTC})
				}()
			}
			wg.Wait()
			for i := range ids {
				if errs[i] != nil || ids[i] != ids[0] {
					t.Fatalf("racing declaration %d: %v, %v (first %v)", i, ids[i], errs[i], ids[0])
				}
			}
			if n, _ := scheduleRows(t, e, db, prefix, "tick"); n != 2 {
				t.Fatalf("%d schedule rows after the race, want 2 (nightly and race)", n)
			}

			// Without a name: a new schedule every call.
			anon := workflow.Schedule{Workflow: "tick", Cron: "0 5 * * *", Location: time.UTC}
			a1, err1 := c2.Schedule(ctx, anon)
			a2, err2 := c2.Schedule(ctx, anon)
			if err1 != nil || err2 != nil || a1 == a2 {
				t.Fatalf("unnamed schedules: %v %v, %v %v; want two ids", a1, err1, a2, err2)
			}
		})
	}
}

// TestRetentionKeepLast: KeepLast keeps the newest N finished runs of each
// workflow whatever their age rule says, a PerWorkflow entry sets its own
// N, and both rules apply together.
func TestRetentionKeepLast(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{
				KeepLast:    2,
				Undelivered: true,
				PerWorkflow: map[string]sqlstore.Retention{"keep": {KeepLast: 3, Undelivered: true}},
			}))
			var w, keep []workflow.RunRef
			for i := range 5 {
				w = append(w, r.finish(workflow.Job{Workflow: "w", Payload: []byte(fmt.Sprintf("%d", i))}, workflow.StatusSucceeded))
				keep = append(keep, r.finish(workflow.Job{Workflow: "keep", Payload: []byte(fmt.Sprintf("%d", i))}, workflow.StatusSucceeded))
				r.clk.Advance(time.Second) // distinct finish times
			}
			if got := r.prune(r.c); got != 5 {
				t.Fatalf("Prune deleted %d runs, want 5 (3 of w, 2 of keep)", got)
			}
			for i, ref := range w {
				if r.exists(ref) != (i >= 3) {
					t.Fatalf("w run %d exists=%v; want only the newest 2 kept", i, !(i >= 3))
				}
			}
			for i, ref := range keep {
				if r.exists(ref) != (i >= 2) {
					t.Fatalf("keep run %d exists=%v; want only the newest 3 kept", i, !(i >= 2))
				}
			}
			if got := r.prune(r.c); got != 0 {
				t.Fatalf("a second Prune deleted %d runs, want 0", got)
			}
		})
	}
}

// TestRetentionKeepLastWithAge: an age rule still deletes runs inside the
// newest N.
func TestRetentionKeepLastWithAge(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{KeepLast: 10, Succeeded: time.Hour, Undelivered: true}))
			old := r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			r.clk.Advance(2 * time.Hour)
			fresh := r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			if got := r.prune(r.c); got != 1 || r.exists(old) || !r.exists(fresh) {
				t.Fatalf("Prune deleted %d; old exists=%v fresh exists=%v; want only the old one gone", got, r.exists(old), r.exists(fresh))
			}
		})
	}
}

// TestPruneChunk: the archive hook receives chunks of WithPruneChunk runs.
func TestPruneChunk(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			var mu sync.Mutex
			var sizes []int
			r := newRetEnv(t, e, sqlstore.WithPruneChunk(2), sqlstore.WithRetention(sqlstore.Retention{Succeeded: time.Minute, Undelivered: true}),
				sqlstore.WithArchive(func(_ context.Context, runs []sqlstore.FinishedRun) error {
					mu.Lock()
					defer mu.Unlock()
					sizes = append(sizes, len(runs))
					return nil
				}))
			for range 5 {
				r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)
			}
			r.clk.Advance(time.Hour)
			if got := r.prune(r.c); got != 5 {
				t.Fatalf("Prune deleted %d, want 5", got)
			}
			mu.Lock()
			defer mu.Unlock()
			if fmt.Sprint(sizes) != "[2 2 1]" {
				t.Fatalf("archive chunks %v, want [2 2 1]", sizes)
			}
		})
	}
}

// TestTickRetention: with WithHousekeepingEvery(1) and a short
// WithTickRetention, a fired tick's row is pruned once it is older than
// the retention; under the default 24 hours it stays.
func TestTickRetention(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				opts     []sqlstore.Option
				wantGone bool
			}{
				{"short", []sqlstore.Option{sqlstore.WithTickRetention(time.Minute), sqlstore.WithHousekeepingEvery(1)}, true},
				{"default retention", []sqlstore.Option{sqlstore.WithHousekeepingEvery(1)}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					db, prefix := e.open(t)
					clk := clock.NewFake(start)
					var runs atomic.Int64
					c := newClient(t, e, db, prefix, append([]sqlstore.Option{sqlstore.WithClock(clk)}, tc.opts...)...)
					ctx := startWith(t, c, workflow.Workflow{Name: "tick", Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
						runs.Add(1)
						return in, nil
					}}}})
					if _, err := c.Schedule(ctx, workflow.Schedule{Name: "h", Workflow: "tick", Cron: "0 * * * *", Location: time.UTC}); err != nil {
						t.Fatal(err)
					}
					clk.Set(start.Truncate(time.Hour).Add(time.Hour))
					waitFor(t, 5*time.Second, "the tick to fire", func() bool { return runs.Load() == 1 })
					clk.Advance(10 * time.Minute) // past the short retention, before the next tick
					ticks := func() int {
						var n int
						if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"ticks")).Scan(&n); err != nil {
							t.Fatal(err)
						}
						return n
					}
					if tc.wantGone {
						waitFor(t, 5*time.Second, "the tick row to be pruned", func() bool { return ticks() == 0 })
						return
					}
					time.Sleep(20 * testPoll) // many housekeeping passes
					if n := ticks(); n != 1 {
						t.Fatalf("%d tick rows under the default retention, want 1", n)
					}
				})
			}
		})
	}
}

// TestConnectionFailureIsUnavailable: a database that refuses the
// connection surfaces as workflow.ErrUnavailable, while the driver's own
// error stays reachable. Port 1 on loopback has no listener.
func TestConnectionFailureIsUnavailable(t *testing.T) {
	// sql.Open, not driverpgx.Open, which pings and would fail here.
	db, err := sql.Open(driverpgx.DriverName, "postgres://nobody@127.0.0.1:1/none?connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	u, err := sqlstore.NewUniqueStore(db, sqlb.PostgresDialect{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = u.Acquire(context.Background(), "k", "o", time.Minute)
	if !errors.Is(err, workflow.ErrUnavailable) {
		t.Fatalf("Acquire on a refused connection: %v, want ErrUnavailable", err)
	}
	var op *net.OpError
	if !errors.As(err, &op) && !strings.Contains(err.Error(), "failed to connect") {
		t.Fatalf("the driver's error is not reachable through %v", err)
	}
	// A classifier that never says unavailable leaves the error unmarked.
	u2, err := sqlstore.NewUniqueStore(db, sqlb.PostgresDialect{}, sqlstore.WithErrorClassifier(func(error) sqlstore.ErrorClass { return sqlstore.ErrorClass{} }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := u2.Acquire(context.Background(), "k", "o", time.Minute); err == nil || errors.Is(err, workflow.ErrUnavailable) {
		t.Fatalf("Acquire under a classifier that never says unavailable: %v, want an unmarked error", err)
	}
}

// countingWorkflow is a one-step workflow that counts its runs.
func countingWorkflow(name string, runs *atomic.Int64) workflow.Workflow {
	return workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) {
		runs.Add(1)
		return in, nil
	}}}}
}

// TestMisfireFireAllAcrossNodes: a backfill of three missed ticks on a
// cluster fires each tick once, not once per node.
func TestMisfireFireAllAcrossNodes(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			var runs atomic.Int64
			cs := startCluster(t, e, db, prefix, countingWorkflow("tick", &runs), sqlstore.WithClock(clk))
			if _, err := cs[0].Schedule(context.Background(), workflow.Schedule{Name: "h", Workflow: "tick", Cron: "0 * * * *", Location: time.UTC,
				Misfire: workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 10}}); err != nil {
				t.Fatal(err)
			}
			first := start.Truncate(time.Hour).Add(time.Hour)
			clk.Set(first.Add(2*time.Hour + 30*time.Minute))
			waitFor(t, 5*time.Second, "three backfilled runs", func() bool { return runs.Load() >= 3 })
			time.Sleep(20 * testPoll)
			if got := runs.Load(); got != 3 {
				t.Fatalf("%d runs across %d nodes for 3 missed ticks, want 3", got, nodes)
			}
		})
	}
}

// TestDeclaredMisfireAndZone: a declared cron honours the workflow's
// Misfire (MisfireSkip drops what a jump of the clock missed) and a
// re-registration with another policy rewrites the stored one.
func TestDeclaredMisfireAndZone(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			var runs atomic.Int64
			wf := countingWorkflow("tick", &runs)
			wf.Cron = []string{"0 * * * *"}
			wf.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip}
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			startWith(t, c, wf)
			first := start.Truncate(time.Hour).Add(time.Hour)
			clk.Set(first.Add(2*time.Hour + 30*time.Minute))
			time.Sleep(20 * testPoll)
			if got := runs.Load(); got != 0 {
				t.Fatalf("MisfireSkip on a declared cron fired %d runs for missed ticks, want 0", got)
			}
			clk.Set(first.Add(3 * time.Hour))
			waitFor(t, 5*time.Second, "the on-time tick", func() bool { return runs.Load() == 1 })
			c.Stop()

			// Re-registered with FireAll: the stored policy follows.
			wf.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 5}
			c2 := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			startWith(t, c2, wf)
			var policy string
			if err := db.QueryRow("SELECT policy FROM " + e.dialect.QuoteIdent(prefix+"schedule_misfire")).Scan(&policy); err != nil {
				t.Fatal(err)
			}
			if policy != string(workflow.MisfireFireAll) {
				t.Fatalf("stored policy %q after re-registering, want %q", policy, workflow.MisfireFireAll)
			}
			c2.Stop()

			// Back to the default: the policy row goes.
			wf.Misfire = workflow.Misfire{}
			c3 := newClient(t, e, db, prefix, sqlstore.WithClock(clk))
			startWith(t, c3, wf)
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"schedule_misfire")).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("%d misfire rows under the default policy, want 0", n)
			}
		})
	}
}

// TestCronParserOption: a custom parser decides what Register and Schedule
// accept and when stored schedules fire; one that returns an error, or no
// schedule, refuses the expression.
func TestCronParserOption(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			clk := clock.NewFake(start)
			// "@quarter-hour" is not standard cron; the parser maps it.
			parser := func(expr string) (cron.Schedule, error) {
				if expr == "@quarter-hour" {
					return cron.Parse("*/15 * * * *")
				}
				if expr == "@none" {
					return nil, nil
				}
				return cron.Parse(expr)
			}
			var runs atomic.Int64
			c := newClient(t, e, db, prefix, sqlstore.WithClock(clk), sqlstore.WithCronParser(parser))
			wf := countingWorkflow("tick", &runs)
			ctx := startWith(t, c, wf)
			if _, err := c.Schedule(ctx, workflow.Schedule{Name: "q", Workflow: "tick", Cron: "@quarter-hour", Location: time.UTC}); err != nil {
				t.Fatalf("Schedule with the custom dialect: %v", err)
			}
			if _, err := c.Schedule(ctx, workflow.Schedule{Name: "n", Workflow: "tick", Cron: "@none", Location: time.UTC}); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("a parser returning no schedule: %v, want ErrInvalid", err)
			}
			clk.Set(start.Truncate(15 * time.Minute).Add(15 * time.Minute))
			waitFor(t, 5*time.Second, "the custom-dialect tick", func() bool { return runs.Load() == 1 })

			def, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix)) // Register needs no tables
			if err != nil {
				t.Fatal(err)
			}
			if err := def.Register(workflow.Workflow{Name: "x", Cron: []string{"@quarter-hour"}, Steps: wf.Steps}); !errors.Is(err, workflow.ErrInvalid) {
				t.Fatalf("the default parser accepted @quarter-hour: %v", err)
			}
		})
	}
}

// TestRetentionKeepLastTies: runs that finished at the same instant are cut
// by enqueue order, so exactly KeepLast stay.
func TestRetentionKeepLastTies(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			r := newRetEnv(t, e, sqlstore.WithRetention(sqlstore.Retention{KeepLast: 2, Undelivered: true}))
			var refs []workflow.RunRef
			for range 3 {
				refs = append(refs, r.finish(workflow.Job{Workflow: "w"}, workflow.StatusSucceeded)) // one fake instant
			}
			if got := r.prune(r.c); got != 1 || r.exists(refs[0]) || !r.exists(refs[1]) || !r.exists(refs[2]) {
				t.Fatalf("Prune deleted %d; want only the first of three tied runs gone", got)
			}
		})
	}
}

// TestDeclaredCronZoneChange: moving a declared cron to another zone
// replaces its row, so it fires in the new zone; a zone that is neither
// loadable by name nor a fixed offset is refused at Register.
func TestDeclaredCronZoneChange(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			var runs atomic.Int64
			wf := countingWorkflow("tick", &runs)
			wf.Cron = []string{"0 9 * * *"}
			c := newClient(t, e, db, prefix)
			startWith(t, c, wf)
			c.Stop()
			wf.CronLocation = time.FixedZone("UTC+5", 5*60*60)
			c2 := newClient(t, e, db, prefix)
			startWith(t, c2, wf)
			var n int
			var loc string
			if err := db.QueryRow("SELECT COUNT(*), MAX(location) FROM "+e.dialect.QuoteIdent(prefix+"schedules")).Scan(&n, &loc); err != nil {
				t.Fatal(err)
			}
			if n != 1 || loc != "fixed:18000:UTC+5" {
				t.Fatalf("%d declared rows in zone %q after moving the cron to UTC+5, want 1 in fixed:18000:UTC+5", n, loc)
			}

			data, err := os.ReadFile("/usr/share/zoneinfo/Europe/London")
			if err != nil {
				t.Skip("no system zoneinfo to build an unnamed DST zone from")
			}
			odd, err := time.LoadLocationFromTZData("Not/AZone", data)
			if err != nil {
				t.Fatal(err)
			}
			wf.CronLocation = odd
			fresh, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := fresh.Register(wf); !errors.Is(err, workflow.ErrUnsupported) {
				t.Fatalf("Register with a zone neither loadable nor fixed: %v, want ErrUnsupported", err)
			}
		})
	}
}
