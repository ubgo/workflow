package sqlrecorder

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/recorder"
)

// These tests need no database: they cover option checks, the dialect
// helpers and the generated SQL. The store against real engines runs in
// the recorder/e2e module, which may import drivers.

func TestNewRefusals(t *testing.T) {
	db := &sql.DB{}
	if _, err := New(nil, Postgres); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("nil db: %v", err)
	}
	if _, err := New(db, "oracle"); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("bad dialect: %v", err)
	}
	bad := map[string]Option{
		"prefix upper":  WithTablePrefix("Wf_"),
		"prefix digit":  WithTablePrefix("1wf"),
		"prefix dash":   WithTablePrefix("wf-"),
		"prefix empty":  WithTablePrefix(""),
		"prefix long":   WithTablePrefix("a" + strings.Repeat("b", maxPrefix)),
		"retention":     WithRetention(recorder.Retention{Batch: -1}),
		"clock":         WithClock(nil),
		"attempts":      WithTxAttempts(0),
		"retryable nil": WithRetryable(nil),
	}
	for name, o := range bad {
		if _, err := New(db, SQLite, o); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	s, err := New(db, MySQL, nil, WithTablePrefix("app_runs_"), WithTxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	if s.Dialect() != MySQL || s.TablePrefix() != "app_runs_" {
		t.Fatalf("store = %v %q", s.Dialect(), s.TablePrefix())
	}
}

func TestDialects(t *testing.T) {
	if len(Dialects()) != 3 {
		t.Fatal(Dialects())
	}
	for _, d := range Dialects() {
		if !d.Valid() {
			t.Errorf("%q invalid", d)
		}
	}
	a := &args{d: Postgres}
	if p := a.add(1) + a.add(2); p != "$1$2" {
		t.Errorf("postgres placeholders %q", p)
	}
	b := &args{d: MySQL}
	if p := in(b, []string{"x", "y"}); p != "(?, ?)" || len(b.vals) != 2 {
		t.Errorf("mysql in %q", p)
	}
	if MySQL.quote("t") != "`t`" || Postgres.quote("t") != `"t"` || SQLite.forUpdate() != "" || Postgres.forUpdate() == "" {
		t.Error("quoting or locking")
	}
}

func TestEscapeLike(t *testing.T) {
	if got := escapeLike(`50%_off!`); got != `50!%!_off!!` {
		t.Fatalf("escapeLike = %q", got)
	}
}

func TestDefaultRetryable(t *testing.T) {
	for _, msg := range []string{
		`ERROR: duplicate key value violates unique constraint "x" (SQLSTATE 23505)`,
		"Error 1213 (40001): Deadlock found when trying to get lock",
		"database is locked (5) (SQLITE_BUSY)",
		"UNIQUE constraint failed: wf_rec_runs.run_key",
	} {
		if !DefaultRetryable(errors.New(msg)) {
			t.Errorf("not retryable: %s", msg)
		}
	}
	if DefaultRetryable(nil) || DefaultRetryable(errors.New("syntax error at or near")) {
		t.Error("retryable false positive")
	}
}

func TestTerminalStatuses(t *testing.T) {
	var want []string
	for _, st := range []workflow.Status{workflow.StatusScheduled, workflow.StatusQueued, workflow.StatusRunning, workflow.StatusWaiting,
		workflow.StatusRetrying, workflow.StatusSucceeded, workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusDiscarded} {
		if st.IsTerminal() {
			want = append(want, string(st))
		}
	}
	if strings.Join(want, ",") != strings.Join(terminalStatuses, ",") {
		t.Fatalf("terminalStatuses = %v, want %v", terminalStatuses, want)
	}
}

func TestDDLPerDialect(t *testing.T) {
	for _, d := range Dialects() {
		s, err := New(&sql.DB{}, d, WithTablePrefix("p_"))
		if err != nil {
			t.Fatal(err)
		}
		stmts := s.MigrationScripts()
		joined := strings.Join(stmts, "\n")
		for _, tbl := range []string{"p_runs", "p_steps", "p_events", "p_labels", "p_schema"} {
			if !strings.Contains(joined, tbl) {
				t.Errorf("%s: no table %s", d, tbl)
			}
		}
		for _, st := range stmts {
			if !strings.Contains(st, "IF NOT EXISTS") {
				t.Errorf("%s: not idempotent: %s", d, st)
			}
		}
		switch d {
		case MySQL:
			if strings.Contains(joined, "CREATE INDEX") || !strings.Contains(joined, "utf8mb4_bin") {
				t.Errorf("mysql DDL: %s", joined)
			}
		case Postgres:
			if !strings.Contains(joined, `COLLATE "C"`) || !strings.Contains(joined, "BYTEA") {
				t.Errorf("postgres DDL: %s", joined)
			}
		}
	}
}

func TestMissingTable(t *testing.T) {
	for _, msg := range []string{`relation "x" does not exist`, "no such table: x", "Error 1146 (42S02): Table 'x' doesn't exist"} {
		if !missingTable(errors.New(msg)) {
			t.Errorf("not missing: %s", msg)
		}
	}
	if missingTable(errors.New("connection refused")) {
		t.Error("false positive")
	}
}

func TestTimeEncoding(t *testing.T) {
	if nanos(fromNanos(0)) != 0 || !fromNanos(0).IsZero() {
		t.Fatal("zero time")
	}
	if got := fromNanos(1_700_000_000_123_456_789); got.UnixNano() != 1_700_000_000_123_456_789 || got.Location().String() != "UTC" {
		t.Fatalf("round trip %v", got)
	}
	if nullID("").Valid || !nullID("x").Valid || boolInt(true) != 1 || boolInt(false) != 0 {
		t.Fatal("helpers")
	}
}
