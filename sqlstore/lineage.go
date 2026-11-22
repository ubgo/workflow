package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/sqlb/contrib/migrate"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// LineageStore is a durable events.LineageStore: every lineage record is a
// row of the append-only table <prefix>lineage, so events.Datasets in
// every process sharing the database sees every produced version and every
// version a trigger consumed, and lineage queries (events.Producers,
// Consumers, Outputs, Inputs) answer for the whole fleet. Like
// AggregateStore it needs no running Client: build it with
// NewLineageStore over any *sql.DB (an app on Hatchet or Redis with a
// database), or take Client.LineageStore.
//
//	store := c.LineageStore() // after store.Migrate(ctx)
//	ds, err := events.NewDatasets(bus, events.WithLineage(store))
//
// Record is idempotent across processes: each row carries the SHA-256 of
// its identifying fields (Kind, Dataset, Version, Run, Workflow, Step,
// Trigger) under a unique constraint, so a second Record of the same fact,
// from this process or another, loses the insert and is ignored, keeping
// the first one's Meta and At. Find orders by At, latest first, ties in
// reverse order of recording (the auto-numbered id), with indexes for the
// queries Datasets and the lineage helpers make: by dataset and kind, by
// run, and by trigger and dataset. At is stored as Unix seconds and
// nanoseconds, so every time.Time keeps its exact instant (returned in
// UTC); Meta is stored as JSON.
//
//ds:def id=sql-lineagestore-dvvv742n owner=@khanakia stability=stable
type LineageStore struct {
	st    *store
	names *strings.Replacer // {lineage} -> quoted name
}

var _ events.LineageStore = (*LineageStore)(nil)

// LineageMigrationOwner is the ledger owner of LineageStore's migrations:
// a list of its own, applied by LineageStore.Migrate, so a database with
// no sqlstore queue gets only the lineage table. Client.Migrate does not
// apply it: call Migrate on the store.
//
//ds:def id=sql-lineagemigrationowner-sfgeqp26 owner=@khanakia stability=stable
const LineageMigrationOwner = "ubgo/workflow/sqlstore/lineage"

// tblLineage is LineageStore's table stem, resolved by the store itself
// because the table belongs to its own migration list.
//
//ds:def id=sql-tbllineage-6729y5yp owner=@khanakia stability=stable
const tblLineage = "lineage"

// lineageMigrations is every LineageStore schema change, in order, under
// the same rules as migrations: never edit a released one, append a new
// one.
//
//ds:def id=sql-lineagestore-lineagemigrations-9yyjjh6e owner=@khanakia stability=stable
func (s *LineageStore) lineageMigrations() []migrate.Migration {
	stmts := s.st.render(lineageDefs(typesFor(s.st.engine)))
	for i, q := range stmts {
		stmts[i] = s.names.Replace(q)
	}
	return []migrate.Migration{
		{Owner: s.st.owner(LineageMigrationOwner), Name: "001_lineage", Script: s.st.script(stmts)},
	}
}

// lineageDefs is LineageStore's table. Its rendered text is frozen once
// released, like ddl.
func lineageDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblLineage, lines: []string{
		// id orders records by recording: Find breaks ties of At by it,
		// latest first.
		"id " + t.serial,
		// ident is the SHA-256 in hex of the identifying fields. Its
		// unique constraint is what makes Record idempotent across
		// processes (ADR-0003, a structural constraint): the second
		// insert of one fact loses.
		"ident " + t.id + " NOT NULL",
		"kind " + t.name + " NOT NULL",
		// ds_hash is the SHA-256 in hex of the dataset, the indexed
		// form: a dataset name (an S3 prefix, a URI) can be longer than
		// an indexable column; dataset keeps the name as given.
		"ds_hash " + t.id + " NOT NULL",
		"dataset " + t.text + " NOT NULL",
		"version " + t.text + " NOT NULL",
		// run and trig are indexed (Outputs and Inputs, a trigger's last
		// consumed version); workflow and step are only returned.
		"run " + t.name + " NOT NULL",
		"workflow " + t.text + " NOT NULL",
		"step " + t.text + " NOT NULL",
		"trig " + t.name + " NOT NULL",
		// meta is Lineage.Meta as a JSON object, empty for a nil map.
		"meta " + t.text + " NOT NULL",
		// at_s and at_n are Lineage.At in Unix seconds and the
		// nanoseconds within the second.
		"at_s " + t.int + " NOT NULL",
		"at_n " + t.int + " NOT NULL",
		"UNIQUE (ident)",
	}, indexes: []index{
		{"dataset", "ds_hash, kind, at_s, at_n, id"},
		{"run", "run, at_s, at_n, id"},
		{"trig", "trig, ds_hash, at_s, at_n, id"},
	}}}
}

// NewLineageStore returns the lineage store over db for dialect d. Of the
// Options, WithTablePrefix, WithSchema, WithTxAttempts and
// WithErrorClassifier apply; the others configure a Client and are
// ignored. Call Migrate before the first Record.
//
//ds:def id=sql-newlineagestore-j9tgbj56 owner=@khanakia stability=stable
func NewLineageStore(db *sql.DB, d sqlb.Dialect, opts ...Option) (*LineageStore, error) {
	c, err := New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	return c.LineageStore(), nil
}

// LineageStore returns the lineage store in this client's database and
// prefix. Its table is created by the store's Migrate, not by
// Client.Migrate.
//
//ds:def id=sql-client-lineagestore-6bkqfduv owner=@khanakia stability=stable
func (c *Client) LineageStore() *LineageStore {
	return &LineageStore{st: c.st, names: strings.NewReplacer("{"+tblLineage+"}", c.st.table(tblLineage))}
}

// Migrate creates the lineage table (its own migration list, under the
// same ledger and lock as Client.Migrate), and nothing else.
//
//ds:def id=sql-lineagestore-migrate-5nmd5b3c owner=@khanakia stability=stable
func (s *LineageStore) Migrate(ctx context.Context) error {
	return s.st.migrate(ctx, s.lineageMigrations())
}

// MigrationScripts returns the store's migrations as SQL text, with table
// names resolved for its prefix and schema, for teams whose DDL goes
// through their own pipeline (see the package's MigrationScripts).
//
//ds:def id=sql-lineagestore-migrationscripts-8q862stq owner=@khanakia stability=stable
func (s *LineageStore) MigrationScripts() []MigrationScript {
	return scriptsOf(s.lineageMigrations())
}

// lineageIdent is the hash of l's identifying fields, each length-prefixed
// so no two different records share an encoding.
func lineageIdent(l events.Lineage) string {
	h := sha256.New()
	for _, f := range []string{string(l.Kind), l.Dataset, l.Version, string(l.Run), l.Workflow, l.Step, l.Trigger} {
		h.Write([]byte(strconv.Itoa(len(f))))
		h.Write([]byte{':'})
		h.Write([]byte(f))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Record implements events.LineageStore: an insert, ignored when the same
// fact is already recorded (see LineageStore).
func (s *LineageStore) Record(ctx context.Context, l events.Lineage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	meta := ""
	if l.Meta != nil {
		b, err := json.Marshal(l.Meta)
		if err != nil {
			return fmt.Errorf("sqlstore: lineage meta: %w", err)
		}
		meta = string(b)
	}
	err := s.st.retry(ctx, func() error {
		_, err := s.st.exec(ctx, s.st.db, s.q(`INSERT INTO {lineage} (ident, kind, ds_hash, dataset, version, run, workflow, step, trig, meta, at_s, at_n)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			lineageIdent(l), string(l.Kind), keyHash(l.Dataset), l.Dataset, l.Version, string(l.Run), l.Workflow, l.Step, l.Trigger, meta,
			l.At.Unix(), int64(l.At.Nanosecond()))
		return err
	})
	if s.st.isUnique(err) {
		return nil // the same fact is recorded: the first one stays
	}
	if err != nil {
		return fmt.Errorf("sqlstore: record lineage: %w", err)
	}
	return nil
}

// Find implements events.LineageStore: the matching records, latest At
// first, ties latest recorded first. A negative Limit is refused with
// workflow.ErrInvalid.
func (s *LineageStore) Find(ctx context.Context, q events.LineageQuery) ([]events.Lineage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.Limit < 0 {
		return nil, fmt.Errorf("%w: sqlstore: lineage query Limit %d is negative", workflow.ErrInvalid, q.Limit)
	}
	var where []string
	var args []any
	if q.Kind != "" {
		where, args = append(where, "kind = ?"), append(args, string(q.Kind))
	}
	if q.Dataset != "" {
		where, args = append(where, "ds_hash = ?"), append(args, keyHash(q.Dataset))
	}
	if q.Run != "" {
		where, args = append(where, "run = ?"), append(args, string(q.Run))
	}
	if q.Trigger != "" {
		where, args = append(where, "trig = ?"), append(args, q.Trigger)
	}
	stmt := "SELECT kind, dataset, version, run, workflow, step, trig, meta, at_s, at_n FROM {lineage}"
	if len(where) > 0 {
		stmt += " WHERE " + strings.Join(where, " AND ")
	}
	stmt += " ORDER BY at_s DESC, at_n DESC, id DESC"
	if q.Limit > 0 {
		stmt, args = stmt+" LIMIT ?", append(args, q.Limit)
	}
	var out []events.Lineage
	var kind, dataset, version, run, wf, step, trig, meta string
	var atS, atN int64
	err := s.st.each(ctx, s.st.db, s.q(stmt), args, func() error {
		l := events.Lineage{Kind: events.LineageKind(kind), Dataset: dataset, Version: version, Run: workflow.RunID(run),
			Workflow: wf, Step: step, Trigger: trig, At: time.Unix(atS, atN).UTC()}
		if meta != "" {
			if err := json.Unmarshal([]byte(meta), &l.Meta); err != nil {
				return fmt.Errorf("sqlstore: lineage meta: %w", err)
			}
		}
		out = append(out, l)
		return nil
	}, &kind, &dataset, &version, &run, &wf, &step, &trig, &meta, &atS, &atN)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: find lineage: %w", err)
	}
	return out, nil
}

// q resolves the store's own table stem; the client's sqlText then
// resolves placeholders.
func (s *LineageStore) q(stmt string) string { return s.names.Replace(stmt) }
