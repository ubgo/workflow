package outbox

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// ChangeOp is a row change a database trigger fires on.
type ChangeOp string

// The row changes ChangeTrigger can fire on.
const (
	OnInsert ChangeOp = "INSERT"
	OnUpdate ChangeOp = "UPDATE"
	OnDelete ChangeOp = "DELETE"
)

// suffix is the per-operation suffix of a trigger's name on the engines
// that take one operation per trigger (MySQL, SQLite).
func (op ChangeOp) suffix() string {
	switch op {
	case OnInsert:
		return "_ins"
	case OnUpdate:
		return "_upd"
	default:
		return "_del"
	}
}

// TriggerSpec describes a row trigger on an app table that writes an
// outbox entry for every change it fires on, so the relay enqueues a job
// for it after the change commits: a change made by any program, a
// migration or a person at a SQL prompt, not only by Go code that calls
// Write.
//
//ds:def id=outbox-triggerspec-u6e88rcx owner=@khanakia stability=stable
type TriggerSpec struct {
	// Table is the app table the trigger watches. Required.
	Table string

	// TableSchema is the Postgres schema holding Table (and the trigger
	// function); empty uses the search_path. Refused on MySQL and SQLite,
	// where a trigger lives beside its table.
	TableSchema string

	// Ops are the changes the trigger fires on: OnInsert, OnUpdate,
	// OnDelete, each at most once. Required.
	Ops []ChangeOp

	// Workflow is the job's workflow name. Required. It is written into the
	// SQL as a string literal, so it is limited to letters, digits and
	// _ . : / @ - (1 to 191 characters).
	Workflow string

	// Columns are the table's columns copied into the job's payload, a
	// JSON object with one field per column under the column's name: the
	// new row's values on insert and update, the old row's on delete.
	// Required, 1 to 50 (Postgres's json_build_object takes 100
	// arguments). A binary column is refused by SQLite's json_object at
	// change time, failing the app's statement: leave it out.
	Columns []string

	// OpField, when set, adds the change to the payload under this name:
	// "insert", "update" or "delete". It must be a plain identifier and
	// not one of Columns.
	OpField string

	// KeyColumn, when set, makes the column's value (as text) the job's
	// idempotency key, so a target honouring keys runs one job per row
	// while that row's run is unfinished: a second change of the row
	// before then is dropped by the target. Empty: every change gets its
	// own job (the relay sends the entry ID as the key).
	KeyColumn string

	// Priority is the job's Priority.
	Priority int

	// Name is the trigger's name. The default is the outbox's table
	// prefix, Table and "_outbox" (wf_orders_outbox); set it to put two
	// triggers on one table. On MySQL and SQLite it gets a suffix per
	// operation (_ins, _upd, _del); on Postgres the trigger function is
	// Name plus "_fn". It must be a plain identifier, short enough for the
	// suffix within 63 characters (Name at most 59).
	Name string
}

// TriggerSQL is a change trigger as SQL text for one engine.
//
//ds:def id=outbox-triggersql-j7czkbyr owner=@khanakia stability=stable
type TriggerSQL struct {
	// Create installs the trigger, in order. Running it again replaces the
	// trigger with this definition (each CREATE is preceded by a DROP ...
	// IF EXISTS, and the Postgres function is CREATE OR REPLACE), so it is
	// safe in a migration that may re-run. Apply it in the app's own
	// migrations, after the outbox table exists.
	Create []string
	// Drop removes what Create installed. Dropping the table also drops
	// its triggers, but not the Postgres function.
	Drop []string
}

// identPattern is a plain SQL identifier: what every engine accepts
// unquoted, so a quoted name can never carry another statement.
//
//ds:def id=outbox-identpattern-xtcg2ch7 owner=@khanakia stability=stable
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// workflowLiteral is what a workflow name may hold to be written as a
// string literal: no quote and no backslash (MySQL reads backslash escapes
// by default), so the literal cannot end early on any engine.
//
//ds:def id=outbox-workflowliteral-6kg8txr4 owner=@khanakia stability=stable
var workflowLiteral = regexp.MustCompile(`^[A-Za-z0-9_.:/@-]{1,191}$`)

// maxTriggerColumns bounds TriggerSpec.Columns (plus OpField), because
// Postgres's json_build_object takes at most 100 arguments.
//
//ds:def id=outbox-maxtriggercolumns-w9exfgnc owner=@khanakia stability=stable
const maxTriggerColumns = 50

// maxTriggerName is the longest trigger or function name ChangeTrigger
// writes: Postgres's identifier limit (MySQL's is 64).
//
//ds:def id=outbox-maxtriggername-zjszg8gu owner=@khanakia stability=stable
const maxTriggerName = 63

// ChangeTrigger returns the SQL that installs a row trigger on spec.Table
// writing an outbox entry for each change in spec.Ops, for dialect d (sqlb's
// Postgres, MySQL or SQLite dialect) and the outbox options among opts
// (WithTablePrefix, WithSchema: the outbox table the entries go to; the
// others are ignored). It renders text only and touches no database.
//
// The entry it writes is what Write writes for workflow.Job{Workflow:
// spec.Workflow, Payload: <the JSON object of spec.Columns>, Priority:
// spec.Priority, IdempotencyKey: <spec.KeyColumn's value, if set>}, with no
// labels and no RunAt, due at once. Its ID is "obx_" and 32 random hex
// digits, made by the database: Postgres's gen_random_uuid (Postgres 13 or
// later), MySQL's UUID(), SQLite's randomblob. It is written inside the
// statement that changed the row, so it commits or rolls back with it.
//
// Per engine: on Postgres a plpgsql function plus one AFTER ... FOR EACH
// ROW trigger for every operation, the payload built with
// json_build_object; on MySQL one trigger per operation with JSON_OBJECT
// (creating a trigger needs the TRIGGER privilege, and with binary logging
// on also SUPER or log_bin_trust_function_creators); on SQLite one trigger
// per operation with json_object.
//
// Every name is checked before it reaches the SQL: Table, TableSchema,
// Columns, OpField, KeyColumn and Name must be plain identifiers
// ([A-Za-z_][A-Za-z0-9_]*, at most 63 characters) and are quoted, and
// Workflow must match its literal pattern; anything else, an empty or
// repeated operation, no columns, more than 50, or a repeated column is
// refused with workflow.ErrInvalid. TableSchema on MySQL or SQLite, and a
// dialect other than these three, are refused with workflow.ErrUnsupported.
//
//ds:def id=outbox-changetrigger-jvb8ehcp owner=@khanakia stability=stable
func ChangeTrigger(d sqlb.Dialect, spec TriggerSpec, opts ...Option) (TriggerSQL, error) {
	o, err := build(nil, d, opts)
	if err != nil {
		return TriggerSQL{}, err
	}
	if err := o.checkTrigger(&spec); err != nil {
		return TriggerSQL{}, err
	}
	switch o.engine {
	case sqlb.DialectPostgres:
		return o.pgTrigger(spec), nil
	case sqlb.DialectMySQL:
		return o.perOpTrigger(spec, mysqlExprs), nil
	default:
		return o.perOpTrigger(spec, sqliteExprs), nil
	}
}

// checkTrigger validates spec and fills in its default Name.
//
//ds:def id=outbox-outbox-checktrigger-54jagn5j owner=@khanakia stability=stable
func (o *Outbox) checkTrigger(spec *TriggerSpec) error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: outbox.ChangeTrigger: "+format, append([]any{workflow.ErrInvalid}, args...)...)
	}
	ident := func(what, v string) error {
		if !identPattern.MatchString(v) {
			return invalid("%s %q is not a plain identifier ([A-Za-z_][A-Za-z0-9_]*, at most 63 characters)", what, v)
		}
		return nil
	}
	if err := ident("Table", spec.Table); err != nil {
		return err
	}
	if spec.TableSchema != "" {
		if o.engine != sqlb.DialectPostgres {
			return workflow.Unsupported("TriggerSpec.TableSchema on "+o.engine, "only Postgres has schemas inside a database; a trigger lives beside its table")
		}
		if err := ident("TableSchema", spec.TableSchema); err != nil {
			return err
		}
	}
	if !workflowLiteral.MatchString(spec.Workflow) {
		return invalid("Workflow %q must match %s to be written as a SQL literal", spec.Workflow, workflowLiteral)
	}
	if len(spec.Ops) == 0 {
		return invalid("no operation: set Ops to OnInsert, OnUpdate and/or OnDelete")
	}
	seenOp := map[ChangeOp]bool{}
	for _, op := range spec.Ops {
		if op != OnInsert && op != OnUpdate && op != OnDelete {
			return invalid("unknown operation %q", op)
		}
		if seenOp[op] {
			return invalid("operation %s given twice", op)
		}
		seenOp[op] = true
	}
	n := len(spec.Columns)
	if spec.OpField != "" {
		n++
	}
	if len(spec.Columns) == 0 || n > maxTriggerColumns {
		return invalid("%d payload fields; give 1 to %d (Columns plus OpField)", n, maxTriggerColumns)
	}
	seenCol := map[string]bool{}
	for _, col := range spec.Columns {
		if err := ident("column", col); err != nil {
			return err
		}
		if seenCol[col] {
			return invalid("column %q given twice", col)
		}
		seenCol[col] = true
	}
	if spec.OpField != "" {
		if err := ident("OpField", spec.OpField); err != nil {
			return err
		}
		if seenCol[spec.OpField] {
			return invalid("OpField %q is also a column", spec.OpField)
		}
	}
	if spec.KeyColumn != "" {
		if err := ident("KeyColumn", spec.KeyColumn); err != nil {
			return err
		}
	}
	if spec.Name == "" {
		spec.Name = o.prefix + spec.Table + "_outbox"
	}
	if err := ident("Name", spec.Name); err != nil {
		return err
	}
	if len(spec.Name)+len(OnInsert.suffix()) > maxTriggerName { // the longest suffix: "_ins", "_upd" and "_del" are 4, "_fn" 3
		return invalid("trigger name %q is too long for its suffix; set a Name of at most %d characters", spec.Name, maxTriggerName-len(OnInsert.suffix()))
	}
	return nil
}

// insertColumns is the outbox column list every trigger writes, in the
// order of the values triggerInsert renders.
const insertColumns = "id, workflow, payload, labels, idem, priority, run_at, status, attempts, due, lease_until, created_at"

// triggerInsert is the INSERT a trigger runs: id and now are the engine's
// expressions for a new entry ID and the current Unix nanoseconds, payload
// and key the row's payload and idempotency key (NULL: none). The entry
// is due at once (due 0), never leased, with no labels and no RunAt.
//
//ds:def id=outbox-outbox-triggerinsert-qacdmzs5 owner=@khanakia stability=stable
func (o *Outbox) triggerInsert(spec TriggerSpec, id, payload, key, now string) string {
	return "INSERT INTO " + o.table + " (" + insertColumns + ") VALUES (" + id + ", '" + spec.Workflow + "', " + payload + ", '{}', " + key + ", " +
		strconv.Itoa(spec.Priority) + ", 0, '" + statusPending + "', 0, 0, 0, " + now + ")"
}

// jsonPairs renders the key/value arguments of a JSON object builder: each
// column's name as a literal and the row's value (row is NEW or OLD), then
// the operation under OpField.
func (o *Outbox) jsonPairs(spec TriggerSpec, row, op string) string {
	parts := make([]string, 0, 2*len(spec.Columns)+2)
	for _, col := range spec.Columns {
		parts = append(parts, "'"+col+"'", row+"."+o.d.QuoteIdent(col))
	}
	if spec.OpField != "" {
		parts = append(parts, "'"+spec.OpField+"'", op)
	}
	return strings.Join(parts, ", ")
}

// pgTrigger is the Postgres form: one plpgsql function that reads OLD on a
// delete and NEW otherwise, and one trigger for every operation chosen.
//
//ds:def id=outbox-outbox-pgtrigger-49mug8p7 owner=@khanakia stability=stable
func (o *Outbox) pgTrigger(spec TriggerSpec) TriggerSQL {
	qual := func(name string) string {
		if spec.TableSchema == "" {
			return o.d.QuoteIdent(name)
		}
		return o.d.QuoteIdent(spec.TableSchema) + "." + o.d.QuoteIdent(name)
	}
	table, fn, trg := qual(spec.Table), qual(spec.Name+"_fn"), o.d.QuoteIdent(spec.Name)
	key := "NULL"
	if spec.KeyColumn != "" {
		key = "r." + o.d.QuoteIdent(spec.KeyColumn) + "::text"
	}
	payload := "convert_to(json_build_object(" + o.jsonPairs(spec, "r", "lower(TG_OP)") + ")::text, 'UTF8')"
	insert := o.triggerInsert(spec, "'obx_' || replace(gen_random_uuid()::text, '-', '')", payload, key,
		"(extract(epoch from clock_timestamp()) * 1000000000)::bigint")
	body := "CREATE OR REPLACE FUNCTION " + fn + "() RETURNS trigger LANGUAGE plpgsql AS $ubgo_outbox$\n" +
		"DECLARE r record;\nBEGIN\n" +
		"\tIF TG_OP = 'DELETE' THEN r := OLD; ELSE r := NEW; END IF;\n" +
		"\t" + insert + ";\n" +
		"\tRETURN NULL;\nEND\n$ubgo_outbox$"
	ops := make([]string, len(spec.Ops))
	for i, op := range spec.Ops {
		ops[i] = string(op)
	}
	return TriggerSQL{
		Create: []string{
			body,
			"DROP TRIGGER IF EXISTS " + trg + " ON " + table,
			"CREATE TRIGGER " + trg + " AFTER " + strings.Join(ops, " OR ") + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION " + fn + "()",
		},
		Drop: []string{
			"DROP TRIGGER IF EXISTS " + trg + " ON " + table,
			"DROP FUNCTION IF EXISTS " + fn + "()",
		},
	}
}

// engineExprs are one engine's spellings of what a per-operation trigger
// computes.
type engineExprs struct {
	id      string                  // a new entry ID
	now     string                  // the current Unix nanoseconds
	object  string                  // the JSON object builder function
	key     func(col string) string // a column's value as text
	wrapped bool                    // the body needs BEGIN ... END (SQLite)
}

//ds:def id=outbox-mysqlexprs-68vfqywh owner=@khanakia stability=stable
var mysqlExprs = engineExprs{
	id:     "CONCAT('obx_', REPLACE(UUID(), '-', ''))",
	now:    "CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000000 AS SIGNED)",
	object: "JSON_OBJECT",
	key:    func(col string) string { return "CAST(" + col + " AS CHAR)" },
}

//ds:def id=outbox-sqliteexprs-t23t49x7 owner=@khanakia stability=stable
var sqliteExprs = engineExprs{
	id:      "'obx_' || lower(hex(randomblob(16)))",
	now:     "CAST((julianday('now') - 2440587.5) * 86400000000000 AS INTEGER)",
	object:  "json_object",
	key:     func(col string) string { return "CAST(" + col + " AS TEXT)" },
	wrapped: true,
}

// perOpTrigger is the MySQL and SQLite form: those engines take one
// operation per trigger, so each chosen operation gets its own trigger
// whose body is the one INSERT (a single statement, so MySQL needs no
// BEGIN ... END and no client-side DELIMITER).
//
//ds:def id=outbox-outbox-peroptrigger-f3ecak24 owner=@khanakia stability=stable
func (o *Outbox) perOpTrigger(spec TriggerSpec, x engineExprs) TriggerSQL {
	var out TriggerSQL
	table := o.d.QuoteIdent(spec.Table)
	for _, op := range spec.Ops {
		row := "NEW"
		if op == OnDelete {
			row = "OLD"
		}
		key := "NULL"
		if spec.KeyColumn != "" {
			key = x.key(row + "." + o.d.QuoteIdent(spec.KeyColumn))
		}
		payload := x.object + "(" + o.jsonPairs(spec, row, "'"+strings.ToLower(string(op))+"'") + ")"
		insert := o.triggerInsert(spec, x.id, payload, key, x.now)
		if x.wrapped {
			insert = "BEGIN " + insert + "; END"
		}
		trg := o.d.QuoteIdent(spec.Name + op.suffix())
		out.Create = append(out.Create,
			"DROP TRIGGER IF EXISTS "+trg,
			"CREATE TRIGGER "+trg+" AFTER "+string(op)+" ON "+table+" FOR EACH ROW "+insert)
		out.Drop = append(out.Drop, "DROP TRIGGER IF EXISTS "+trg)
	}
	return out
}
