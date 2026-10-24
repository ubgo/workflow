// Command migrations-export shows the two ways sqlstore's schema reaches a
// database: Migrate, which applies numbered migrations under a ledger and
// the engine's lock, and MigrationScripts, which hands the same SQL to a
// tool of your own (goose, Atlas, Flyway, a DBA review). It also shows a
// lock store migrated on its own, the refusal of a schema a newer release
// wrote, and the Postgres-only WithSchema option.
//
// It is the program behind docs/operations/migrations.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "migrations-export:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "migrations-export-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	steps := []func() error{
		func() error { return migrateAndList(ctx, out, filepath.Join(dir, "app.db")) },
		func() error { return export(out, filepath.Join(dir, "sql")) },
		func() error { return applyYourself(ctx, out, filepath.Join(dir, "tool.db")) },
		func() error { return uniqueOnly(ctx, out, filepath.Join(dir, "locks.db")) },
		func() error { return tooNew(ctx, out, filepath.Join(dir, "app.db")) },
		func() error { return schemaOption(out, filepath.Join(dir, "app.db")) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// migrateAndList runs Migrate twice and prints the ledger it wrote.
func migrateAndList(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:migrate
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	// Run it at deploy time, before Start, from as many processes as you
	// like: the ledger applies each migration once, under the engine's lock.
	if err := c.Migrate(ctx); err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil { // a second call is a no-op
		return err
	}
	// snippet:end

	fmt.Fprintln(out, "ledger after Migrate:")
	return printLedger(ctx, out, db, "wf_")
}

// export writes every migration as a .sql file, the way a team that ships
// DDL through its own tool would.
func export(out io.Writer, dir string) error {
	// snippet:export
	scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{},
		sqlstore.WithTablePrefix("jobs_"), // the same table options the app's New gets
	)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for i, m := range scripts {
		// One file per migration, in order. Record (Owner, Name) in your tool
		// so a later release's new migrations are the only ones it applies.
		name := fmt.Sprintf("%03d_%s_%s.sql", i+1, strings.ReplaceAll(m.Owner, "/", "_"), m.Name)
		body := "-- owner: " + m.Owner + "\n-- name: " + m.Name + "\n" + strings.Join(m.Statements, ";\n\n") + ";\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			return err
		}
		fmt.Fprintln(out, "wrote", name)
	}
	// snippet:end
	return nil
}

// applyYourself applies MigrationScripts with plain Exec calls, as an
// external tool would, then starts a client that never ran Migrate.
func applyYourself(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:apply
	scripts, err := sqlstore.MigrationScripts(sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	for _, m := range scripts {
		for _, stmt := range m.Statements {
			if _, err := db.ExecContext(ctx, stmt); err != nil { // each is safe to run twice
				return fmt.Errorf("%s %s: %w", m.Owner, m.Name, err)
			}
		}
	}
	// Start checks that the tables are readable, not the ledger, so a client
	// starts on a schema applied this way without calling Migrate.
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := c.Register(workflow.Workflow{Name: "noop", Steps: []workflow.Step{{Name: "only", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}}); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	c.Stop()
	// snippet:end
	fmt.Fprintln(out, "started on a schema applied without Migrate")
	return nil
}

// uniqueOnly migrates only the uniqueness-lock store into an empty database,
// as an app on Hatchet that wants durable unique jobs would.
func uniqueOnly(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:unique
	locks, err := sqlstore.NewUniqueStore(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := locks.Migrate(ctx); err != nil { // its own list: only the lock table
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "ledger after UniqueStore.Migrate:")
	return printLedger(ctx, out, db, "wf_")
}

// tooNew records a migration this code has never heard of, as a newer
// release would, and shows that Migrate and Start refuse the database.
func tooNew(ctx context.Context, out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()
	// Stands in for a newer release's Migrate.
	if _, err := db.ExecContext(ctx, `INSERT INTO wf_migrations (owner, name, checksum, applied_at) VALUES (?, ?, ?, ?)`,
		sqlstore.MigrationOwner, "099_from_the_future", "x", "2027-01-01T00:00:00Z"); err != nil {
		return err
	}

	c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	// snippet:too-new
	err = c.Migrate(ctx)
	if tooNew, ok := errors.AsType[*sqlstore.SchemaTooNewError](err); ok {
		// The code is older than the schema: deploy the newer release again
		// rather than run this one. Nothing was changed.
		fmt.Fprintln(out, "refused, unknown migrations:", tooNew.Unknown)
	}
	// snippet:end
	if !errors.Is(err, sqlstore.ErrSchemaTooNew) {
		return fmt.Errorf("calling Migrate on a newer schema: %v, want ErrSchemaTooNew", err)
	}
	if err := c.Start(ctx); !errors.Is(err, sqlstore.ErrSchemaTooNew) {
		return fmt.Errorf("calling Start on a newer schema: %v, want ErrSchemaTooNew", err)
	}
	fmt.Fprintln(out, "Start refused too:", sqlstore.ErrSchemaTooNew)
	return nil
}

// schemaOption shows WithSchema: schema-qualified DDL for Postgres, and the
// refusal on SQLite.
func schemaOption(out io.Writer, path string) error {
	db, err := driversqlite.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:schema
	scripts, err := sqlstore.MigrationScripts(sqlb.PostgresDialect{},
		sqlstore.WithTablePrefix("wf_"),
		sqlstore.WithSchema("jobs"), // the schema must exist; Migrate does not create it
	)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "postgres:", firstLine(scripts[0].Statements[0]))
	fmt.Fprintln(out, "ledger owner:", scripts[0].Owner)

	_, err = sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithSchema("jobs"))
	fmt.Fprintln(out, "sqlite:", err)
	// snippet:end
	if err == nil {
		return errors.New("WithSchema on SQLite was accepted")
	}
	return nil
}

// firstLine returns the first line of a migration's SQL without a trailing
// "(", so the program prints one short line per statement.
func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "("))
}

// printLedger lists the ledger's (owner, name) rows in the order applied.
func printLedger(ctx context.Context, out io.Writer, db *sql.DB, prefix string) error {
	rows, err := db.QueryContext(ctx, `SELECT owner, name FROM `+prefix+`migrations ORDER BY owner, name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var owner, name string
		if err := rows.Scan(&owner, &name); err != nil {
			return err
		}
		fmt.Fprintf(out, "  %s %s\n", owner, name)
	}
	return rows.Err()
}
