package sqlstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/ubgo/workflow"
)

// sqlTypes is one engine's spelling of the few column kinds the tables use.
type sqlTypes struct {
	id     string // a generated id (run, step, schedule, lease token)
	name   string // an indexed short text: workflow, step, event name or key
	short  string // a closed-set status word
	blob   string // payloads and step outputs, any size
	text   string // labels (JSON) and error messages, unindexed
	int    string // 64-bit integer: Unix nanoseconds, counters, flags
	serial string // an auto-numbered 64-bit primary key
	suffix string // ends every CREATE TABLE
	// inlineIndex is true when secondary indexes are declared inside
	// CREATE TABLE (MySQL, which has no CREATE INDEX IF NOT EXISTS).
	inlineIndex bool
}

func typesFor(engine string) sqlTypes {
	switch engine {
	case enginePostgres:
		return sqlTypes{id: "VARCHAR(64)", name: "VARCHAR(191)", short: "VARCHAR(16)", blob: "BYTEA", text: "TEXT",
			int: "BIGINT", serial: "BIGSERIAL PRIMARY KEY"}
	case engineMySQL:
		// utf8mb4_bin: names and keys compare byte for byte, so the event
		// key "Order-1" never matches "order-1" the way MySQL's default
		// collation would.
		return sqlTypes{id: "VARCHAR(64)", name: "VARCHAR(191)", short: "VARCHAR(16)", blob: "LONGBLOB", text: "LONGTEXT",
			int: "BIGINT", serial: "BIGINT AUTO_INCREMENT PRIMARY KEY",
			suffix: " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", inlineIndex: true}
	default: // SQLite
		return sqlTypes{id: "TEXT", name: "TEXT", short: "TEXT", blob: "BLOB", text: "TEXT",
			int: "INTEGER", serial: "INTEGER PRIMARY KEY AUTOINCREMENT"}
	}
}

// index is one secondary index of a table.
type index struct {
	name string
	cols string
}

// tableDef is one table: its column and constraint lines and its secondary
// indexes.
type tableDef struct {
	stem    string
	lines   []string
	indexes []index
}

// tableDefs is the whole schema. Times are BIGINT Unix nanoseconds (UTC by
// definition), so no engine's date type, precision or time-zone handling
// can shift a due time. Every unique constraint here is a correctness
// guard, not business validation; the comment on each says what it
// guarantees.
func tableDefs(t sqlTypes) []tableDef {
	return []tableDef{
		{stem: tblRuns, lines: []string{
			// seq orders runs first come, first served across nodes.
			"seq " + t.serial,
			"id " + t.id + " NOT NULL",
			"scheduled_id " + t.id,
			"workflow " + t.name + " NOT NULL",
			"payload " + t.blob,
			"labels " + t.text + " NOT NULL",
			// idem_active holds the idempotency key while the run is not
			// terminal and NULL after, so the unique constraint below holds a
			// key exactly as long as Client.Enqueue documents.
			"idem_active " + t.name,
			"priority " + t.int + " NOT NULL",
			"bucket " + t.name + " NOT NULL",
			"rate_keys " + t.text + " NOT NULL",
			// status is one of the run* constants (sqlstore.go).
			"status " + t.short + " NOT NULL",
			"run_at " + t.int + " NOT NULL",
			// sig_floor is the highest signal id when the run was enqueued:
			// the run takes only signals above it (the Signal look-back).
			"sig_floor " + t.int + " NOT NULL",
			"created_at " + t.int + " NOT NULL",
			"finished_at " + t.int + " NOT NULL",
			"err " + t.text,
			"fail_step " + t.name,
			"fail_input " + t.blob,
			"fail_token " + t.id,
			"fail_until " + t.int + " NOT NULL",
			"notified " + t.int + " NOT NULL",
			"notify_until " + t.int + " NOT NULL",
			"ver " + t.int + " NOT NULL",
			"UNIQUE (id)",
			"UNIQUE (scheduled_id)",
			"UNIQUE (workflow, idem_active)",
		}, indexes: []index{{"status", "status, workflow"}}},
		{stem: tblSteps, lines: []string{
			"id " + t.id + " NOT NULL PRIMARY KEY",
			"run_id " + t.id + " NOT NULL",
			"run_seq " + t.int + " NOT NULL",
			"workflow " + t.name + " NOT NULL",
			"name " + t.name + " NOT NULL",
			// status is one of the step* constants (sqlstore.go).
			"status " + t.short + " NOT NULL",
			"in_data " + t.blob,
			"out_data " + t.blob,
			// attempts counts executions started (StepInfo.Attempt);
			// failures counts attempts that returned an error, which is what
			// the retry policy is checked against.
			"attempts " + t.int + " NOT NULL",
			"failures " + t.int + " NOT NULL",
			// reclaims counts how often this attempt's lease expired and
			// another client took the step over (its worker died); past
			// WithMaxReclaims the attempt fails instead of running again.
			"reclaims " + t.int + " NOT NULL",
			// due is when a ready step may be claimed, when a sleep ends, or
			// when a wait expires (0: never).
			"due " + t.int + " NOT NULL",
			"lease_token " + t.id,
			"lease_until " + t.int + " NOT NULL",
			"priority " + t.int + " NOT NULL",
			"wait_event " + t.name + " NOT NULL",
			"wait_key " + t.name + " NOT NULL",
			"UNIQUE (run_id, name)",
		}, indexes: []index{{"claim", "status, due"}, {"lease", "lease_token"}}},
		{stem: tblSignals, lines: []string{
			"id " + t.serial,
			"name " + t.name + " NOT NULL",
			"ev_key " + t.name + " NOT NULL",
			"payload " + t.blob,
			"created_at " + t.int + " NOT NULL",
		}, indexes: []index{{"match", "name, ev_key"}}},
		{stem: tblConsumed, lines: []string{
			"run_id " + t.id + " NOT NULL",
			"signal_id " + t.int + " NOT NULL",
			// One run never takes the same signal twice.
			"PRIMARY KEY (run_id, signal_id)",
		}},
		{stem: tblSchedules, lines: []string{
			"id " + t.id + " NOT NULL PRIMARY KEY",
			"workflow " + t.name + " NOT NULL",
			"name " + t.name + " NOT NULL",
			"cron " + t.name + " NOT NULL",
			"location " + t.name + " NOT NULL",
			"in_data " + t.blob,
			"labels " + t.text + " NOT NULL",
			"next_fire " + t.int + " NOT NULL",
			"active " + t.int + " NOT NULL",
			"declared " + t.int + " NOT NULL",
		}},
		{stem: tblTicks, lines: []string{
			"schedule_id " + t.id + " NOT NULL",
			"fire_at " + t.int + " NOT NULL",
			"run_id " + t.id + " NOT NULL",
			// One row per tick: the node whose insert wins fires it, so a
			// tick runs once however many nodes see it due.
			"PRIMARY KEY (schedule_id, fire_at)",
		}},
		{stem: tblLocks, lines: []string{
			"name " + t.name + " NOT NULL PRIMARY KEY",
			"n " + t.int + " NOT NULL",
		}},
		{stem: tblStarts, lines: []string{
			"id " + t.serial,
			"workflow " + t.name + " NOT NULL",
			"lim " + t.int + " NOT NULL",
			"rkey " + t.name + " NOT NULL",
			"started_at " + t.int + " NOT NULL",
		}, indexes: []index{{"window", "workflow, lim, rkey, started_at"}}},
		{stem: tblBatches, lines: []string{
			"id " + t.name + " NOT NULL PRIMARY KEY",
			// doc is the batch.State as JSON; ver is bumped first by every
			// Update, which is how Update locks the row (BatchStore).
			"doc " + t.text + " NOT NULL",
			"ver " + t.int + " NOT NULL",
			// finished is 1 once the batch is closed (!State.Open(): every job
			// ended and no callback is pending), so Unfinished and the
			// retention guard read an index, not every document.
			"finished " + t.int + " NOT NULL",
		}, indexes: []index{{"open", "finished"}}},
	}
}

// ddl returns the statements that create the version-one schema when it is
// missing. It is the script of migration 001_initial (migrations.go), so its
// text is frozen: a later schema change is a new migration, never an edit
// here, or every database that applied 001 fails Migrate on its checksum.
func (s *store) ddl() []string { return s.render(tableDefs(typesFor(s.engine))) }

// uniqueDefs is UniqueStore's table, created by its own migration list
// (uniqueMigrations), so a database used only for uniqueness locks gets
// only this table. Its rendered text is frozen once released, like ddl.
func uniqueDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblUnique, lines: []string{
		// key_hash is the SHA-256 of the key in hex: a fixed-width primary
		// key, so a key of any length fits every engine's index limit. The
		// primary key is the lock itself: one holder per key (ADR-0003, a
		// structural constraint).
		"key_hash " + t.id + " NOT NULL PRIMARY KEY",
		// ukey is the key as given, for an operator reading the table.
		"ukey " + t.text + " NOT NULL",
		"owner " + t.text + " NOT NULL",
		// expires is when the lock frees by itself (Unix nanoseconds on the
		// store's clock); the key is held while now < expires.
		"expires " + t.int + " NOT NULL",
	}, indexes: []index{{"expires", "expires"}}}}
}

// misfireDefs is the table of migration 002_schedule_misfire: one row per
// schedule (declared or Client.Schedule) whose workflow.Misfire is not the
// default. It is a table of its own rather than new columns on schedules
// because CREATE TABLE IF NOT EXISTS is safe to run twice on every engine,
// which an ALTER TABLE ADD COLUMN is not on MySQL (whose DDL commits
// implicitly, so a failed migration re-runs). Its rendered text is frozen
// once released, like ddl.
func misfireDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblMisfire, lines: []string{
		// One policy per schedule.
		"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
		// policy is a workflow.MisfirePolicy, never empty here.
		"policy " + t.short + " NOT NULL",
		// within_ns is Misfire.Within in nanoseconds (0: no limit).
		"within_ns " + t.int + " NOT NULL",
		// max_fires is Misfire.Max (0 unless MisfireFireAll).
		"max_fires " + t.int + " NOT NULL",
	}}}
}

// adminDefs is the tables of migration 003_queues_admin: queues, per-run
// retry policies, discards, attempt counts for Until and MaxErrors, pauses
// and schedule windows. Like 002 they are new tables rather than new
// columns, because CREATE TABLE IF NOT EXISTS is safe to run twice on every
// engine and MySQL has no ADD COLUMN IF NOT EXISTS. A run or schedule
// written before 003 has no row in them and reads as the defaults (the
// default queue, no retry override, no windows), so the statements that
// read them LEFT JOIN or COALESCE. Its rendered text is frozen once
// released, like ddl.
func adminDefs(t sqlTypes) []tableDef {
	return []tableDef{
		{stem: tblRunExt, lines: []string{
			// One row per run, written with the run.
			"run_id " + t.id + " NOT NULL PRIMARY KEY",
			// queue is workflow.EffectiveQueue of the run, fixed at enqueue.
			"queue " + t.name + " NOT NULL",
			// idem_key is the run's idempotency key as given (NULL: none).
			// runs.idem_active drops it when the run ends; RunRetrier takes
			// it again from here.
			"idem_key " + t.name,
			// retry is Job.Retry as JSON (retryDoc); NULL means no override.
			"retry " + t.text,
			// started_at is when the run's first step attempt was claimed
			// (0: none yet), RunInfo.StartedAt.
			"started_at " + t.int + " NOT NULL",
			// discarded is 1 when a step ended the run with workflow.Discard.
			"discarded " + t.int + " NOT NULL",
		}, indexes: []index{{"queue", "queue"}}},
		{stem: tblStepExt, lines: []string{
			// One row per step that has failed at least once.
			"step_id " + t.id + " NOT NULL PRIMARY KEY",
			// errors counts failures that were not releases
			// (workflow.Attempts.Errors, for RetryPolicy.MaxErrors).
			"errors " + t.int + " NOT NULL",
			// first_at is when the step's first attempt started
			// (workflow.Attempts.First, for RetryPolicy.Until).
			"first_at " + t.int + " NOT NULL",
		}},
		{stem: tblPauses, lines: []string{
			// kind is one of the pause* constants (pause.go); name is the
			// workflow or queue, empty for the kinds that target everything.
			"kind " + t.short + " NOT NULL",
			"name " + t.name + " NOT NULL",
			// One row per paused target, so Pause is idempotent.
			"PRIMARY KEY (kind, name)",
		}},
		{stem: tblScheduleExt, lines: []string{
			// One row per schedule that has windows or a pause.
			"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
			// windows is workflow.Windows as JSON; empty: none.
			"windows " + t.text + " NOT NULL",
			// paused is 1 while SchedulePauser holds this schedule.
			"paused " + t.int + " NOT NULL",
		}},
	}
}

// backfillRunExt gives every run written before 003 its run_ext row (the
// default queue, its idempotency key while it holds one). It is part of
// 003's script and safe to run twice: it inserts only the missing rows.
const backfillRunExt = "INSERT INTO {run_ext} (run_id, queue, idem_key, retry, started_at, discarded)" +
	" SELECT id, '" + workflow.DefaultQueue + "', idem_active, NULL, 0, 0 FROM {runs} WHERE id NOT IN (SELECT run_id FROM {run_ext})"

// statesChainDefs is the tables of migration 004_run_states_chain: the
// rest of a run's job chain (workflow.Job.Chain plus the edits its steps
// made), one row per run that has one, and the error of each step's last
// failed attempt that is retried (RunInfo.Error while the run is
// StatusRetrying). New tables, not columns, for the reason adminDefs gives.
// Their rendered text is frozen once released, like ddl.
func statesChainDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblStepErr, lines: []string{
		// One row per step whose last attempt failed and was retried,
		// rewritten by each such failure.
		"step_id " + t.id + " NOT NULL PRIMARY KEY",
		// err is that attempt's error message.
		"err " + t.text + " NOT NULL",
	}}, {stem: tblRunChain, lines: []string{
		// One chain per run.
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
		// chain is the jobs as JSON (chainDoc), in the order they run.
		"chain " + t.text + " NOT NULL",
		// next_wf is empty while the run has not succeeded. Once it has and
		// its next job's workflow was not registered on the client that
		// finished it, next_wf names that workflow, and a client that
		// registers it hands the chain on.
		"next_wf " + t.name + " NOT NULL",
	}, indexes: []index{{"next", "next_wf"}}}}
}

// discardedStatus is the data half of migration 004: runs that earlier
// releases ended with workflow.Discard were stored cancelled with
// run_ext.discarded = 1; they become runDiscarded, so a status filter and
// Status read them the way runs discarded from now on read. Safe to run
// twice: a second run finds no such cancelled row.
const discardedStatus = "UPDATE {runs} SET status = '" + runDiscarded + "' WHERE status = '" + runCancelled +
	"' AND id IN (SELECT run_id FROM {run_ext} WHERE discarded = 1)"

// labelDefs is the table of migration 005_run_labels: the label index, one
// row per (key, value) of a run's Job.Labels, each cut to labelIndexWidth
// characters (labels.go). RunQuery.Labels filters through its (lkey, lval)
// index; runs.labels stays the exact copy RunInfo.Labels is read from. Its
// rendered text is frozen once released, like ddl.
func labelDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblRunLabels, lines: []string{
		"run_id " + t.id + " NOT NULL",
		"lkey " + t.name + " NOT NULL",
		"lval " + t.name + " NOT NULL",
		// One row per indexed pair of a run, so the backfill that rewrites a
		// run's rows can never leave two copies of one pair. Not
		// (run_id, lkey): two long keys may share their indexed prefix.
		"PRIMARY KEY (run_id, lkey, lval)",
	}, indexes: []index{{"kv", "lkey, lval, run_id"}}}}
}

// historyDefs is the tables of migration 006_schedule_history: each
// schedule's overlap policy and last fire (CR-02, CR-20) and the bounded
// history of failed attempts of each run (RL-19), and the marker of runs
// ended by ForceFail (WX-11). New tables, not columns,
// for the reason adminDefs gives. Their rendered text is frozen once
// released, like ddl.
func historyDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblScheduleLast, lines: []string{
		// One row per schedule, written with it (and backfilled by 006 for
		// older ones), so a fire only ever updates it.
		"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
		// overlap is the workflow.OverlapPolicy; empty means allow.
		"overlap " + t.short + " NOT NULL",
		// last_fire is when the schedule last fired (tick or trigger), Unix
		// nanoseconds on the scheduling clock; 0: never.
		"last_fire " + t.int + " NOT NULL",
		// last_run is the run that fire enqueued; empty: none. A fire moves
		// it with a compare-and-set on the value its overlap decision read,
		// so two fires deciding at once cannot both start a run.
		"last_run " + t.id + " NOT NULL",
	}}, {stem: tblAttemptErr, lines: []string{
		"id " + t.serial,
		"run_id " + t.id + " NOT NULL",
		"step " + t.name + " NOT NULL",
		// attempt is StepInfo.Attempt of the failed execution.
		"attempt " + t.int + " NOT NULL",
		"err " + t.text + " NOT NULL",
		// stack is workflow.StackOf the error; empty when it has none.
		"stack " + t.text + " NOT NULL",
		// at is when the failure was written, on the scheduling clock.
		"at " + t.int + " NOT NULL",
	}, indexes: []index{{"run", "run_id, id"}}}, {stem: tblRunForced, lines: []string{
		// One row per run whose failure ForceFail wrote: the explicit flag
		// that makes its error match workflow.ErrForced. Never inferred
		// from the error text, which a step's own error could imitate.
		// Removed when the run is retried, force-completed or deleted.
		"run_id " + t.id + " NOT NULL PRIMARY KEY",
	}}}
}

// backfillScheduleLast gives every schedule written before 006 its
// schedule_last row (allow, never fired). Safe to run twice: it inserts
// only the missing rows.
const backfillScheduleLast = "INSERT INTO {schedule_last} (schedule_id, overlap, last_fire, last_run)" +
	" SELECT id, '', 0, '' FROM {schedules} WHERE id NOT IN (SELECT schedule_id FROM {schedule_last})"

// render turns table definitions into CREATE TABLE and CREATE INDEX
// statements for this engine.
func (s *store) render(defs []tableDef) []string {
	t := typesFor(s.engine)
	var out []string
	for _, td := range defs {
		lines := td.lines
		if t.inlineIndex {
			for _, ix := range td.indexes {
				lines = append(lines, "INDEX "+s.d.QuoteIdent(s.prefix+td.stem+"_"+ix.name)+" ("+ix.cols+")")
			}
		}
		out = append(out, "CREATE TABLE IF NOT EXISTS {"+td.stem+"} (\n\t"+strings.Join(lines, ",\n\t")+"\n)"+t.suffix)
		if !t.inlineIndex {
			for _, ix := range td.indexes {
				out = append(out, "CREATE INDEX IF NOT EXISTS "+s.d.QuoteIdent(s.prefix+td.stem+"_"+ix.name)+" ON {"+td.stem+"} ("+ix.cols+")")
			}
		}
	}
	return out
}

// checkSchema fails when the schema is missing, so a missing table is an
// error at Start rather than at the first enqueue. It reads the tables, not
// the ledger, so a schema applied from MigrationScripts by another tool
// passes.
func (c *Client) checkSchema(ctx context.Context) error {
	for _, stem := range allTables {
		var n int64
		if err := c.st.one(ctx, c.st.db, "SELECT COUNT(*) FROM {"+stem+"} WHERE 1 = 0", nil, &n); err != nil {
			return fmt.Errorf("sqlstore: table %s is not readable (run Migrate, or apply MigrationScripts, first): %w", c.st.table(stem), err)
		}
	}
	return nil
}
