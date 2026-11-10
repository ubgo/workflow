package sqlrecorder

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the version of the tables this release writes. Migrate
// records it in the <prefix>schema table, so a later release knows which
// steps to apply.
//
//ds:def id=sqlrec-schemaversion-scf4u7mm owner=@khanakia stability=stable
const SchemaVersion = 1

// The table names, after the prefix (WithTablePrefix).
const (
	tableRuns   = "runs"
	tableSteps  = "steps"
	tableEvents = "events"
	tableLabels = "labels"
	tableSchema = "schema"
)

// Column widths of the identifier columns. Identifiers longer than these
// are refused by the core's validation before they reach a store (workflow
// and queue names, tenants, sources), or are engine IDs well below them.
const (
	keyWidth    = 255
	nameWidth   = 255
	sourceWidth = 64
	refWidth    = 256
	statusWidth = 32
	// mysqlLabelPrefix is how much of a label value MySQL indexes; values
	// are TEXT, and InnoDB indexes only a prefix of TEXT.
	mysqlLabelPrefix = 191
)

// table returns the quoted name of a table.
func (s *Store) table(name string) string { return s.d.quote(s.prefix + name) }

// ddl returns the statements that create the version-1 schema. Every
// statement is idempotent (IF NOT EXISTS), so a Migrate that died half way
// runs again cleanly; MySQL declares its indexes inside CREATE TABLE
// because it has no CREATE INDEX IF NOT EXISTS.
func (s *Store) ddl() []string {
	d := s.d
	id, txt, blob, num := d.idType, d.textType(), d.blobType(), d.intType()
	q := d.quote
	runs := []string{
		q("run_key") + " " + id(keyWidth) + " NOT NULL PRIMARY KEY",
		q("run_id") + " " + id(keyWidth) + " NULL",
		q("scheduled_id") + " " + id(keyWidth) + " NULL",
		q("workflow") + " " + id(nameWidth) + " NOT NULL",
		q("queue") + " " + id(nameWidth) + " NOT NULL",
		q("tenant") + " " + id(nameWidth) + " NOT NULL",
		q("source") + " " + id(sourceWidth) + " NOT NULL",
		q("source_ref") + " " + id(refWidth) + " NOT NULL",
		q("status") + " " + id(statusWidth) + " NOT NULL",
		q("outcome") + " " + id(statusWidth) + " NOT NULL",
		q("outcome_note") + " " + txt + " NOT NULL",
		q("outcome_set") + " " + num + " NOT NULL",
		q("error") + " " + txt + " NOT NULL",
		q("failed_step") + " " + id(nameWidth) + " NOT NULL",
		q("labels") + " " + txt + " NOT NULL",
		q("payload") + " " + blob + " NULL",
		q("payload_size") + " " + num + " NOT NULL",
		q("output") + " " + blob + " NULL",
		q("output_size") + " " + num + " NOT NULL",
		q("priority") + " " + num + " NOT NULL",
		q("attempts") + " " + num + " NOT NULL",
		q("attempt_errors") + " " + txt + " NOT NULL",
		q("attempted_by") + " " + txt + " NOT NULL",
		q("enqueued_at") + " " + num + " NOT NULL",
		q("started_at") + " " + num + " NOT NULL",
		q("finished_at") + " " + num + " NOT NULL",
		q("run_at") + " " + num + " NOT NULL",
		q("updated_at") + " " + num + " NOT NULL",
		q("checked_at") + " " + num + " NOT NULL",
		q("stale") + " " + num + " NOT NULL",
	}
	steps := []string{
		q("run_id") + " " + id(keyWidth) + " NOT NULL",
		q("step") + " " + id(nameWidth) + " NOT NULL",
		q("attempt") + " " + num + " NOT NULL",
		q("status") + " " + id(statusWidth) + " NOT NULL",
		q("input") + " " + blob + " NULL",
		q("output") + " " + blob + " NULL",
		q("input_size") + " " + num + " NOT NULL",
		q("output_size") + " " + num + " NOT NULL",
		q("error") + " " + txt + " NOT NULL",
		q("stack") + " " + txt + " NOT NULL",
		q("node") + " " + id(nameWidth) + " NOT NULL",
		q("started_at") + " " + num + " NOT NULL",
		q("finished_at") + " " + num + " NOT NULL",
		"PRIMARY KEY (" + q("run_id") + ", " + q("step") + ", " + q("attempt") + ")",
	}
	events := []string{
		q("run_id") + " " + id(keyWidth) + " NOT NULL",
		q("step") + " " + id(nameWidth) + " NOT NULL",
		q("attempt") + " " + num + " NOT NULL",
		q("seq") + " " + num + " NOT NULL",
		q("message") + " " + txt + " NOT NULL",
		q("level") + " " + num + " NOT NULL",
		q("attrs") + " " + txt + " NOT NULL",
		q("at") + " " + num + " NOT NULL",
		"PRIMARY KEY (" + q("run_id") + ", " + q("step") + ", " + q("attempt") + ", " + q("seq") + ")",
	}
	labels := []string{
		q("run_key") + " " + id(keyWidth) + " NOT NULL",
		q("k") + " " + id(nameWidth) + " NOT NULL",
		q("v") + " " + txt + " NOT NULL",
		"PRIMARY KEY (" + q("run_key") + ", " + q("k") + ")",
	}
	// The version is the key, so two Migrates racing to record it cannot
	// both insert: the loser's duplicate is retried and finds the row.
	schema := []string{q("version") + " " + num + " NOT NULL PRIMARY KEY"}

	// Indexes: the listing order, the filters a runs page uses, Pending's
	// order and the retention scan.
	type index struct {
		table, name string
		cols        []string
		unique      bool
	}
	idx := []index{
		{tableRuns, "run_id", []string{"run_id"}, true},
		{tableRuns, "scheduled_id", []string{"scheduled_id"}, true},
		{tableRuns, "enqueued", []string{"enqueued_at", "run_key"}, false},
		{tableRuns, "workflow", []string{"workflow", "enqueued_at"}, false},
		{tableRuns, "status", []string{"status", "enqueued_at"}, false},
		{tableRuns, "tenant", []string{"tenant", "enqueued_at"}, false},
		{tableRuns, "source", []string{"source", "enqueued_at"}, false},
		{tableRuns, "pending", []string{"status", "checked_at"}, false},
		{tableRuns, "finished", []string{"finished_at"}, false},
		{tableLabels, "kv", []string{"k", "v"}, false},
	}
	cols := func(ix index) string {
		parts := make([]string, len(ix.cols))
		for i, c := range ix.cols {
			parts[i] = q(c)
			if d == MySQL && ix.table == tableLabels && c == "v" {
				parts[i] += fmt.Sprintf("(%d)", mysqlLabelPrefix)
			}
		}
		return strings.Join(parts, ", ")
	}
	defs := map[string][]string{tableRuns: runs, tableSteps: steps, tableEvents: events, tableLabels: labels, tableSchema: schema}
	if d == MySQL {
		for _, ix := range idx {
			kind := "INDEX"
			if ix.unique {
				kind = "UNIQUE INDEX"
			}
			defs[ix.table] = append(defs[ix.table], kind+" "+q(s.prefix+ix.table+"_"+ix.name)+" ("+cols(ix)+")")
		}
	}
	suffix := ""
	if d == MySQL {
		suffix = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	}
	var out []string
	for _, t := range []string{tableRuns, tableSteps, tableEvents, tableLabels, tableSchema} {
		out = append(out, "CREATE TABLE IF NOT EXISTS "+s.table(t)+" (\n  "+strings.Join(defs[t], ",\n  ")+"\n)"+suffix)
	}
	if d != MySQL {
		for _, ix := range idx {
			kind := "INDEX"
			if ix.unique {
				kind = "UNIQUE INDEX"
			}
			out = append(out, "CREATE "+kind+" IF NOT EXISTS "+q(s.prefix+ix.table+"_"+ix.name)+" ON "+s.table(ix.table)+" ("+cols(ix)+")")
		}
	}
	return out
}

// MigrationScripts returns the statements Migrate runs, for an app that
// applies schema changes with its own migration tool instead. After running
// them, the app inserts SchemaVersion into the <prefix>schema table, or
// calls Migrate once, which then only records the version.
//
//ds:def id=sqlrec-store-migrationscripts-xbepgg67 owner=@khanakia stability=stable
func (s *Store) MigrationScripts() []string { return s.ddl() }

// Migrate creates the store's tables and indexes when they are missing and
// records SchemaVersion. It is idempotent and safe to run on every start;
// two processes running it at once both succeed. It refuses a database
// whose recorded version is newer than this release knows (an older binary
// against a newer schema), with an error naming both versions.
//
//ds:def id=sqlrec-store-migrate-63gjrdbn owner=@khanakia stability=stable
func (s *Store) Migrate(ctx context.Context) error {
	have, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if have > SchemaVersion {
		return fmt.Errorf("sqlrecorder: the database schema is version %d, newer than this release's %d", have, SchemaVersion)
	}
	if have == SchemaVersion {
		return nil
	}
	for _, stmt := range s.ddl() {
		if err := s.execRetry(ctx, stmt); err != nil {
			return fmt.Errorf("sqlrecorder: migrate: %w\n%s", err, stmt)
		}
	}
	a := &args{d: s.d}
	from := ""
	if s.d == MySQL {
		from = " FROM DUAL" // MySQL needs a FROM before WHERE
	}
	tbl := s.table(tableSchema)
	ins := "INSERT INTO " + tbl + " (" + s.d.quote("version") + ") SELECT " + a.add(SchemaVersion) + from +
		" WHERE NOT EXISTS (SELECT 1 FROM " + tbl + " AS x)"
	if err := s.execRetry(ctx, ins, a.vals...); err != nil {
		return fmt.Errorf("sqlrecorder: record schema version: %w", err)
	}
	return nil
}

// execRetry runs one idempotent migration statement, again on a
// contention error: two processes creating the same table at once can
// collide inside the engine's catalog (Postgres reports a duplicate type)
// even with IF NOT EXISTS, and the retry then finds the table.
func (s *Store) execRetry(ctx context.Context, stmt string, vals ...any) error {
	return s.retry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, stmt, vals...)
		return err
	})
}

// retry runs fn up to the store's attempts while it fails with a
// contention error, with a short growing pause between tries, and returns
// its last error.
func (s *Store) retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		if err = fn(); err == nil || !s.retryable(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(attempt*attempt) * time.Millisecond):
		}
	}
	return err
}

// schemaVersion reads the recorded version: 0 when the schema table does
// not exist yet or is empty. The read is retried on a contention error like
// the statements after it: on a fresh SQLite file another process switching
// the journal to WAL makes it fail at once with SQLITE_BUSY.
func (s *Store) schemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.retry(ctx, func() error {
		v = 0
		err := s.db.QueryRowContext(ctx, "SELECT MAX("+s.d.quote("version")+") FROM "+s.table(tableSchema)).Scan(&nullInt{&v})
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	if err == nil {
		return v, nil
	}
	// A missing table reads as version 0; Migrate creates it. Any other
	// failure (no connection) is the caller's to see.
	if missingTable(err) {
		return 0, nil
	}
	return 0, fmt.Errorf("sqlrecorder: read schema version: %w", err)
}

// missingTable reports whether err is "table does not exist" on any engine.
func missingTable(err error) bool {
	msg := err.Error()
	for _, m := range []string{"does not exist", "no such table", "doesn't exist", "42P01", "Error 1146"} {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// nullInt scans a nullable integer into an int, NULL as 0.
type nullInt struct{ dst *int }

func (n *nullInt) Scan(v any) error {
	var ni sql.NullInt64
	if err := ni.Scan(v); err != nil {
		return err
	}
	*n.dst = int(ni.Int64)
	return nil
}
