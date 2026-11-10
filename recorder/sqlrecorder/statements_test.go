package sqlrecorder

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/recorder"
)

// These tests pin the SQL the store sends on Postgres and MySQL, which the
// SQLite tests (module sqlitetest) cannot see: placeholders, quoting, row
// locks, and the order of arguments against the order of placeholders.
// A recording database/sql driver written here stands in for the engine,
// so this module still imports no driver.

// recDB is a database/sql connector that records every statement and its
// arguments. Exec succeeds; a query returns no rows, except SELECT MAX(...)
// which returns one NULL, as an engine does on an empty table.
type recDB struct {
	mu    sync.Mutex
	stmts []recStmt
	// rows, when set, answers a query with rows instead.
	rows func(q string) [][]driver.Value
}

type recStmt struct {
	query string
	args  []any
}

func (c *recDB) Connect(context.Context) (driver.Conn, error) { return &recConn{c}, nil }
func (c *recDB) Driver() driver.Driver                        { return recDriver{} }

func (c *recDB) record(q string, args []driver.NamedValue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vals := make([]any, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	c.stmts = append(c.stmts, recStmt{q, vals})
}

func (c *recDB) take() []recStmt {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.stmts
	c.stmts = nil
	return out
}

type recDriver struct{}

func (recDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("recDB: open through the connector")
}

type recConn struct{ c *recDB }

func (*recConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("recDB: no prepared statements")
}
func (*recConn) Close() error              { return nil }
func (*recConn) Begin() (driver.Tx, error) { return recTx{}, nil }

func (c *recConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.c.record(q, args)
	return driver.RowsAffected(1), nil
}

func (c *recConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.c.record(q, args)
	if c.c.rows != nil {
		if rs := c.c.rows(q); rs != nil {
			return &recRows{rows: rs}, nil
		}
	}
	if strings.HasPrefix(q, "SELECT MAX(") {
		return &recRows{rows: [][]driver.Value{{nil}}}, nil
	}
	return &recRows{}, nil
}

type recTx struct{}

func (recTx) Commit() error   { return nil }
func (recTx) Rollback() error { return nil }

type recRows struct{ rows [][]driver.Value }

func (r *recRows) Columns() []string {
	if len(r.rows) > 0 {
		return make([]string, len(r.rows[0]))
	}
	return nil
}
func (*recRows) Close() error { return nil }
func (r *recRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

func recStore(t *testing.T, d Dialect, opts ...Option) (*Store, *recDB) {
	t.Helper()
	rec := &recDB{}
	db := sql.OpenDB(rec)
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(db, d, append([]Option{WithTablePrefix("p_")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return s, rec
}

// placeholders lists the placeholders of a statement in text order.
var placeholderRE = regexp.MustCompile(`\$\d+|\?`)

// checkPlaceholders proves a statement's placeholders match its arguments:
// on Postgres they read $1, $2, ... in text order, so the n-th argument
// fills the n-th placeholder in the text, which is also how "?" binds.
func checkPlaceholders(t *testing.T, d Dialect, st recStmt) {
	t.Helper()
	ps := placeholderRE.FindAllString(st.query, -1)
	if len(ps) != len(st.args) {
		t.Fatalf("%s: %d placeholders, %d args in %s", d, len(ps), len(st.args), st.query)
	}
	for i, p := range ps {
		want := "?"
		if d == Postgres {
			want = fmt.Sprintf("$%d", i+1)
		}
		if p != want {
			t.Fatalf("%s: placeholder %d is %s, want %s in %s", d, i, p, want, st.query)
		}
	}
}

// TestWhereArgumentOrder builds the WHERE of a query using every filter
// and proves each argument sits where its placeholder is, on every
// dialect, and that the label filters bind key then value.
func TestWhereArgumentOrder(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	q := recorder.Query{
		RunQuery: workflow.RunQuery{
			Ref:           workflow.RunRef{RunID: "r1", ScheduledID: "s1"},
			Workflow:      "wf",
			Queue:         "qu",
			Tenant:        "te",
			Statuses:      []workflow.Status{workflow.StatusQueued, workflow.StatusFailed},
			Labels:        map[string]string{"zz": "last", "aa": "first"},
			LabelPrefixes: map[string]string{"pp": "50%_x"},
			From:          from,
			To:            from.Add(time.Hour),
			DueFrom:       from.Add(2 * time.Hour),
			DueTo:         from.Add(3 * time.Hour),
		},
		Sources:   []workflow.Source{"api"},
		Outcomes:  []workflow.Outcome{workflow.OutcomeSkipped},
		Search:    "AbC_%",
		OnlyStale: true,
	}
	wantArgs := []any{
		"r1", "s1", "wf", "qu", "te", "queued", "failed", "api", "skipped",
		"aa", "first", "zz", "last", "pp", "50!%!_x%",
		nanos(from), nanos(from.Add(time.Hour)), nanos(from.Add(2 * time.Hour)), nanos(from.Add(3 * time.Hour)),
		"%abc!_!%%", "%abc!_!%%", "%abc!_!%%", "%abc!_!%%",
	}
	for _, d := range Dialects() {
		s, err := New(&sql.DB{}, d, WithTablePrefix("p_"))
		if err != nil {
			t.Fatal(err)
		}
		a := &args{d: d}
		w := s.where(q, a)
		if fmt.Sprint(a.vals) != fmt.Sprint(wantArgs) {
			t.Fatalf("%s args:\n got %v\nwant %v", d, a.vals, wantArgs)
		}
		checkPlaceholders(t, d, recStmt{w, a.vals})
		lq := d.quote
		for _, frag := range []string{
			"l." + lq("k") + " = ",
			" ESCAPE '!'",
			"r." + lq("stale") + " = 1",
			"r." + lq("run_at") + " <> 0",
			"LOWER(COALESCE(r." + lq("source_ref") + ", ''))",
			"FROM " + lq("p_labels") + " l WHERE l." + lq("run_key") + " = r." + lq("run_key"),
		} {
			if !strings.Contains(w, frag) {
				t.Errorf("%s: WHERE lacks %q:\n%s", d, frag, w)
			}
		}
	}
	// No filter, no WHERE.
	s, _ := New(&sql.DB{}, Postgres)
	if w := s.where(recorder.Query{}, &args{d: Postgres}); w != "" {
		t.Fatalf("empty query WHERE = %q", w)
	}
	// One due bound alone still excludes runs with no due time.
	a := &args{d: Postgres}
	if w := s.where(recorder.Query{RunQuery: workflow.RunQuery{DueTo: from}}, a); w != ` WHERE r."run_at" <> 0 AND r."run_at" < $1` {
		t.Fatalf("DueTo alone = %q", w)
	}
}

// TestMigrateStatements pins the schema read, the DDL and the version
// insert per dialect: MySQL needs FROM DUAL and declares its indexes in
// CREATE TABLE; Postgres binds $1.
func TestMigrateStatements(t *testing.T) {
	cases := []struct {
		d       Dialect
		read    string
		insert  string
		creates int
	}{
		{Postgres, `SELECT MAX("version") FROM "p_schema"`, `INSERT INTO "p_schema" ("version") SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM "p_schema" AS x)`, 15},
		{MySQL, "SELECT MAX(`version`) FROM `p_schema`", "INSERT INTO `p_schema` (`version`) SELECT ? FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `p_schema` AS x)", 5},
		{SQLite, `SELECT MAX("version") FROM "p_schema"`, `INSERT INTO "p_schema" ("version") SELECT ? WHERE NOT EXISTS (SELECT 1 FROM "p_schema" AS x)`, 15},
	}
	for _, c := range cases {
		t.Run(string(c.d), func(t *testing.T) {
			s, rec := recStore(t, c.d)
			if err := s.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			st := rec.take()
			if len(st) != c.creates+2 || st[0].query != c.read {
				t.Fatalf("%d statements, first %q", len(st), st[0].query)
			}
			last := st[len(st)-1]
			if last.query != c.insert || len(last.args) != 1 || last.args[0] != int64(SchemaVersion) {
				t.Fatalf("version insert = %q %v", last.query, last.args)
			}
			for i, x := range st[1 : len(st)-1] {
				if x.query != s.MigrationScripts()[i] {
					t.Fatalf("statement %d = %q, want script %d", i+1, x.query, i)
				}
			}
		})
	}
}

// TestWriteStatements pins one first write per dialect: the load locks
// the rows it reads (not on SQLite, which takes the database's write lock
// with a no-op UPDATE first), the run row binds its values in column order,
// and the label rows go in key order.
func TestWriteStatements(t *testing.T) {
	at := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	for _, d := range Dialects() {
		t.Run(string(d), func(t *testing.T) {
			s, rec := recStore(t, d)
			err := s.UpsertRun(context.Background(), recorder.RunWrite{
				Ref: workflow.RunRef{RunID: "r1"}, Workflow: "wf", Labels: map[string]string{"b": "2", "a": "1"},
				Status: workflow.StatusQueued, Payload: []byte("p"), PayloadSize: 1, EnqueuedAt: at, At: at,
			})
			if err != nil {
				t.Fatal(err)
			}
			st := rec.take()
			var kinds []string
			for _, x := range st {
				checkPlaceholders(t, d, x)
				kinds = append(kinds, strings.SplitN(x.query, " ", 2)[0])
			}
			// A new run's insert is preceded by its load and followed by the
			// delete-then-insert of its labels.
			want := []string{"SELECT", "INSERT", "DELETE", "INSERT", "INSERT"}
			if d == SQLite {
				want = append([]string{"UPDATE"}, want...)
				if st[0].query != `UPDATE "p_schema" SET "version" = "version" WHERE 1 = 0` {
					t.Fatalf("sqlite lock = %q", st[0].query)
				}
				st = st[1:]
			}
			if !slices.Equal(kinds, want) {
				t.Fatalf("statements = %v, want %v", kinds, want)
			}
			load := st[0]
			if got := strings.HasSuffix(load.query, " FOR UPDATE"); got != (d != SQLite) {
				t.Fatalf("load row lock %v: %q", got, load.query)
			}
			if !strings.Contains(load.query, "r."+d.quote("run_id")+" = ") || fmt.Sprint(load.args) != "[r1]" {
				t.Fatalf("load = %q %v", load.query, load.args)
			}
			ins := st[1]
			if !strings.HasPrefix(ins.query, "INSERT INTO "+d.quote("p_runs")+" ("+d.quote("run_key")+", "+d.quote("run_id")+", ") || len(ins.args) != len(runColumns) {
				t.Fatalf("insert = %q (%d args)", ins.query, len(ins.args))
			}
			byCol := map[string]any{}
			for i, c := range runColumns {
				byCol[c] = ins.args[i]
			}
			if byCol["run_key"] != "r:r1" || byCol["run_id"] != "r1" || byCol["scheduled_id"] != nil || byCol["workflow"] != "wf" ||
				byCol["status"] != "queued" || byCol["labels"] != `{"a":"1","b":"2"}` || string(byCol["payload"].([]byte)) != "p" ||
				byCol["enqueued_at"] != at.UnixNano() || byCol["finished_at"] != int64(0) || byCol["stale"] != int64(0) {
				t.Fatalf("insert values by column = %v", byCol)
			}
			if fmt.Sprint(st[3].args, st[4].args) != "[r:r1 a 1] [r:r1 b 2]" {
				t.Fatalf("label inserts = %v %v", st[3].args, st[4].args)
			}
		})
	}
}

// TestRecordsCursorStatement pins the keyset page condition: older enqueue
// time, or the same time and a smaller key, joined to the filters with AND
// (or opening the WHERE when there is none), and LIMIT one past the page.
func TestRecordsCursorStatement(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	cur := recorder.EncodeCursor(recorder.Cursor{EnqueuedAt: at, Key: "r:k"})
	s, rec := recStore(t, Postgres)
	if _, err := s.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Workflow: "wf", Cursor: cur, Limit: 5}}); err != nil {
		t.Fatal(err)
	}
	st := rec.take()
	want := ` WHERE r."workflow" = $1 AND (r."enqueued_at" < $2 OR (r."enqueued_at" = $3 AND r."run_key" < $4)) ORDER BY r."enqueued_at" DESC, r."run_key" DESC LIMIT 6`
	if len(st) != 1 || !strings.HasSuffix(st[0].query, want) || fmt.Sprint(st[0].args) != fmt.Sprintf("[wf %d %d r:k]", at.UnixNano(), at.UnixNano()) {
		t.Fatalf("records = %q %v", st[0].query, st[0].args)
	}
	if _, err := s.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Cursor: cur}}); err != nil {
		t.Fatal(err)
	}
	if st := rec.take(); !strings.Contains(st[0].query, `" r WHERE (r."enqueued_at" < $1 OR`) {
		t.Fatalf("cursor alone = %q", st[0].query)
	}
	if _, err := s.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Cursor: "%%%"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("bad cursor: %v", err)
	}
	if st := rec.take(); len(st) != 0 {
		t.Fatalf("a bad cursor reached the database: %v", st)
	}
}

// TestPruneStatements pins the stage-two pick of the default rule on
// MySQL: finished, ended by the cutoff, and not of a workflow with its own
// rule, oldest first, one batch.
func TestPruneStatements(t *testing.T) {
	now := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	s, rec := recStore(t, MySQL, WithClock(clock.NewFake(now)), WithRetention(recorder.Retention{
		RetentionRule: recorder.RetentionRule{Runs: time.Hour},
		PerWorkflow:   map[string]recorder.RetentionRule{"b": {}, "a": {Steps: time.Minute}},
		Batch:         7,
	}))
	res, err := s.Prune(context.Background())
	if err != nil || res != (recorder.PruneResult{}) {
		t.Fatalf("prune of an empty store = %+v, %v", res, err)
	}
	st := rec.take()
	if len(st) != 2 {
		t.Fatalf("statements = %d: %v", len(st), st)
	}
	want := "SELECT r.`run_key`, r.`run_id` FROM `p_runs` r WHERE r.`status` IN (?, ?, ?, ?) AND r.`finished_at` <> 0 AND r.`finished_at` <= ? AND r.`workflow` NOT IN (?, ?) ORDER BY r.`finished_at`, r.`run_key` LIMIT 7"
	if st[0].query != want {
		t.Fatalf("stage two pick:\n got %s\nwant %s", st[0].query, want)
	}
	wantArgs := fmt.Sprint(append(append([]any{}, toAny(terminalStatuses)...), now.Add(-time.Hour).UnixNano(), "a", "b"))
	if fmt.Sprint(st[0].args) != wantArgs {
		t.Fatalf("args = %v, want %v", st[0].args, wantArgs)
	}
	// Stage one of workflow a: its own name, and only runs that still have
	// steps.
	if !strings.Contains(st[1].query, "r.`workflow` = ? AND r.`run_id` IS NOT NULL AND EXISTS (SELECT 1 FROM `p_steps` st WHERE st.`run_id` = r.`run_id`)") ||
		st[1].args[len(st[1].args)-1] != "a" || st[1].args[4] != now.Add(-time.Minute).UnixNano() {
		t.Fatalf("stage one pick = %s %v", st[1].query, st[1].args)
	}
}

func toAny[T any](vs []T) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

// rowOf is r as the runs row an engine returns.
func rowOf(t *testing.T, r recorder.RunRecord) []driver.Value {
	t.Helper()
	vals, err := runValues(r)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]driver.Value, len(vals))
	for i, v := range vals {
		if vr, ok := v.(driver.Valuer); ok {
			if v, err = vr.Value(); err != nil {
				t.Fatal(err)
			}
		}
		out[i] = v
	}
	return out
}

// TestJoinTwoRowsInEitherOrder: when a write names both IDs of a run kept
// in two rows, the rows are merged with the RunID row winning whichever
// order the engine returns them in (its labels kept whole, the earlier
// enqueue time taken), both old rows and their labels are
// deleted, and one row is inserted under the RunID key.
func TestJoinTwoRowsInEitherOrder(t *testing.T) {
	at := time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC)
	byRun := recorder.NewRecord(workflow.RunRef{RunID: "r1"})
	byRun.Workflow, byRun.Status, byRun.EnqueuedAt, byRun.Labels = "wf", workflow.StatusRunning, at.Add(time.Minute), map[string]string{"a": "run"}
	bySched := recorder.NewRecord(workflow.RunRef{ScheduledID: "s1"})
	bySched.Workflow, bySched.Status, bySched.EnqueuedAt, bySched.Labels = "wf", workflow.StatusScheduled, at, map[string]string{"a": "sched", "b": "sched"}
	for _, order := range [][]recorder.RunRecord{{byRun, bySched}, {bySched, byRun}} {
		s, rec := recStore(t, Postgres)
		rec.rows = func(q string) [][]driver.Value {
			if strings.HasSuffix(q, " FOR UPDATE") {
				return [][]driver.Value{rowOf(t, order[0]), rowOf(t, order[1])}
			}
			return nil
		}
		if err := s.UpsertRun(context.Background(), recorder.RunWrite{Ref: workflow.RunRef{RunID: "r1", ScheduledID: "s1"}, At: at}); err != nil {
			t.Fatal(err)
		}
		var deleted []string
		var ins []any
		for _, st := range rec.take() {
			switch {
			case strings.HasPrefix(st.query, `DELETE FROM "p_runs"`):
				deleted = append(deleted, fmt.Sprint(st.args...))
			case strings.HasPrefix(st.query, `INSERT INTO "p_runs"`):
				ins = st.args
			}
		}
		if fmt.Sprint(deleted) != "[r:r1 s:s1]" || ins == nil {
			t.Fatalf("first %s: deleted %v, inserted %v", order[0].Key(), deleted, ins)
		}
		if ins[0] != "r:r1" || ins[1] != "r1" || ins[2] != "s1" || ins[8] != "running" || ins[14] != `{"a":"run"}` || ins[23] != at.UnixNano() {
			t.Fatalf("first %s: merged row = %v", order[0].Key(), ins)
		}
	}
}

// TestWritesRefusedBeforeTheDatabase: a malformed write or read is refused
// with workflow.ErrInvalid and sends no statement.
func TestWritesRefusedBeforeTheDatabase(t *testing.T) {
	ctx := context.Background()
	s, rec := recStore(t, Postgres)
	ref := workflow.RunRef{RunID: "r1"}
	cases := map[string]func() error{
		"upsert no ref":   func() error { return s.UpsertRun(ctx, recorder.RunWrite{}) },
		"start no step":   func() error { return s.StartStep(ctx, recorder.StepStart{Ref: ref, Attempt: 1}) },
		"start attempt 0": func() error { return s.StartStep(ctx, recorder.StepStart{Ref: ref, Step: "s"}) },
		"end no ref": func() error {
			return s.EndStep(ctx, recorder.StepEnd{Step: "s", Attempt: 1, Status: recorder.StepSucceeded})
		},
		"end running": func() error {
			return s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: recorder.StepRunning})
		},
		"end unknown status": func() error { return s.EndStep(ctx, recorder.StepEnd{Ref: ref, Step: "s", Attempt: 1, Status: "done"}) },
		"event no step":      func() error { return s.AddEvent(ctx, recorder.EventWrite{Ref: ref, Attempt: 1}) },
		"outcome no ref":     func() error { return s.SetOutcome(ctx, recorder.OutcomeWrite{Outcome: workflow.OutcomeSkipped}) },
		"outcome unknown":    func() error { return s.SetOutcome(ctx, recorder.OutcomeWrite{Ref: ref, Outcome: "meh"}) },
		"finish no ref":      func() error { return s.FinishRun(ctx, recorder.Finish{Status: workflow.StatusFailed}) },
		"finish not terminal": func() error {
			return s.FinishRun(ctx, recorder.Finish{Ref: ref, Status: workflow.StatusRunning})
		},
		"check no ref": func() error { return s.MarkChecked(ctx, recorder.Check{}) },
		"run no ref": func() error {
			_, err := s.Run(ctx, workflow.RunRef{})
			return err
		},
		"records bad limit": func() error {
			_, err := s.Records(ctx, recorder.Query{RunQuery: workflow.RunQuery{Limit: -1}})
			return err
		},
	}
	for name, call := range cases {
		if err := call(); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
		if st := rec.take(); len(st) != 0 {
			t.Errorf("%s reached the database: %v", name, st)
		}
	}
}

// TestMigrateOnAnEmptyVersionAnswer: a schema read that returns no row at
// all (a driver that drops the NULL row of an aggregate) reads as version
// 0, so Migrate creates the schema.
func TestMigrateOnAnEmptyVersionAnswer(t *testing.T) {
	s, rec := recStore(t, Postgres)
	rec.rows = func(q string) [][]driver.Value {
		if strings.HasPrefix(q, "SELECT MAX(") {
			return [][]driver.Value{}
		}
		return nil
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := rec.take(); len(st) != 17 || !strings.HasPrefix(st[len(st)-1].query, `INSERT INTO "p_schema"`) {
		t.Fatalf("%d statements", len(st))
	}
}
