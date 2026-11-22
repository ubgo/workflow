package outbox

import (
	"errors"
	"strings"
	"testing"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

func validSpec() TriggerSpec {
	return TriggerSpec{Table: "accounts", Ops: []ChangeOp{OnInsert, OnDelete}, Workflow: "account.changed", Columns: []string{"id", "email"}, OpField: "op"}
}

// TestChangeTriggerRefusals proves every name that reaches the SQL is
// checked: anything that is not a plain identifier (or, for the workflow,
// a safe literal) is refused before any SQL is rendered.
//
//ds:def id=outbox-testchangetriggerrefusals-28frsyw5 owner=@khanakia stability=stable
func TestChangeTriggerRefusals(t *testing.T) {
	many := make([]string, maxTriggerColumns)
	for i := range many {
		many[i] = "c" + strings.Repeat("x", i)
	}
	cases := []struct {
		name string
		d    sqlb.Dialect
		edit func(*TriggerSpec)
		want error
	}{
		{"table with a quote", sqlb.PostgresDialect{}, func(s *TriggerSpec) { s.Table = `accounts"; DROP TABLE x; --` }, workflow.ErrInvalid},
		{"table with a space", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Table = "my accounts" }, workflow.ErrInvalid},
		{"empty table", sqlb.MySQLDialect{}, func(s *TriggerSpec) { s.Table = "" }, workflow.ErrInvalid},
		{"column with a backtick", sqlb.MySQLDialect{}, func(s *TriggerSpec) { s.Columns = []string{"id", "a`b"} }, workflow.ErrInvalid},
		{"repeated column", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Columns = []string{"id", "id"} }, workflow.ErrInvalid},
		{"no columns", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Columns = nil }, workflow.ErrInvalid},
		{"too many fields", sqlb.PostgresDialect{}, func(s *TriggerSpec) { s.Columns = many }, workflow.ErrInvalid},
		{"workflow with a quote", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Workflow = "ship'); DELETE FROM x; --" }, workflow.ErrInvalid},
		{"workflow with a backslash", sqlb.MySQLDialect{}, func(s *TriggerSpec) { s.Workflow = `ship\` }, workflow.ErrInvalid},
		{"no operation", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Ops = nil }, workflow.ErrInvalid},
		{"unknown operation", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Ops = []ChangeOp{"TRUNCATE"} }, workflow.ErrInvalid},
		{"repeated operation", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Ops = []ChangeOp{OnInsert, OnInsert} }, workflow.ErrInvalid},
		{"op field is a column", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.OpField = "email" }, workflow.ErrInvalid},
		{"op field not an identifier", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.OpField = "o'p" }, workflow.ErrInvalid},
		{"key column not an identifier", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.KeyColumn = "id)" }, workflow.ErrInvalid},
		{"name not an identifier", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.Name = "trg-1" }, workflow.ErrInvalid},
		{"name too long for its suffix", sqlb.PostgresDialect{}, func(s *TriggerSpec) { s.Name = strings.Repeat("n", 60) }, workflow.ErrInvalid},
		{"default name too long", sqlb.MySQLDialect{}, func(s *TriggerSpec) { s.Table = strings.Repeat("t", 60) }, workflow.ErrInvalid},
		{"schema on MySQL", sqlb.MySQLDialect{}, func(s *TriggerSpec) { s.TableSchema = "app" }, workflow.ErrUnsupported},
		{"schema on SQLite", sqlb.SQLiteDialect{}, func(s *TriggerSpec) { s.TableSchema = "app" }, workflow.ErrUnsupported},
		{"schema not an identifier", sqlb.PostgresDialect{}, func(s *TriggerSpec) { s.TableSchema = "app.x" }, workflow.ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			tc.edit(&spec)
			got, err := ChangeTrigger(tc.d, spec)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if len(got.Create) != 0 || len(got.Drop) != 0 {
				t.Fatalf("a refused spec rendered SQL: %+v", got)
			}
		})
	}
	if _, err := ChangeTrigger(nil, validSpec()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("nil dialect: %v, want ErrInvalid", err)
	}
}

// TestChangeTriggerShape checks what each engine renders: the outbox table
// under its prefix and schema, a trigger per operation where the engine
// needs one, the old row on delete, the default name, and identifiers
// quoted.
func TestChangeTriggerShape(t *testing.T) {
	spec := validSpec()
	spec.KeyColumn = "id"
	pg, err := ChangeTrigger(sqlb.PostgresDialect{}, spec, WithTablePrefix("app_"), WithSchema("jobs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(pg.Create) != 3 || len(pg.Drop) != 2 {
		t.Fatalf("postgres: %d create and %d drop statements, want 3 and 2", len(pg.Create), len(pg.Drop))
	}
	for _, want := range []string{`INSERT INTO "jobs"."app_outbox"`, `"app_accounts_outbox_fn"()`, "json_build_object('id', r.\"id\", 'email', r.\"email\", 'op', lower(TG_OP))",
		`r."id"::text`, "'account.changed'"} {
		if !strings.Contains(pg.Create[0], want) {
			t.Errorf("postgres function lacks %q:\n%s", want, pg.Create[0])
		}
	}
	if want := `CREATE TRIGGER "app_accounts_outbox" AFTER INSERT OR DELETE ON "accounts" FOR EACH ROW EXECUTE FUNCTION "app_accounts_outbox_fn"()`; pg.Create[2] != want {
		t.Errorf("postgres trigger:\n%s\nwant:\n%s", pg.Create[2], want)
	}

	my, err := ChangeTrigger(sqlb.MySQLDialect{}, spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(my.Create) != 4 || len(my.Drop) != 2 {
		t.Fatalf("mysql: %d create and %d drop statements, want 4 and 2", len(my.Create), len(my.Drop))
	}
	for i, want := range []string{"CREATE TRIGGER `wf_accounts_outbox_ins` AFTER INSERT ON `accounts` FOR EACH ROW INSERT INTO `wf_outbox`",
		"CREATE TRIGGER `wf_accounts_outbox_del` AFTER DELETE ON `accounts`"} {
		if !strings.HasPrefix(my.Create[2*i+1], want) {
			t.Errorf("mysql trigger %d:\n%s\nwant prefix:\n%s", i, my.Create[2*i+1], want)
		}
	}
	if !strings.Contains(my.Create[3], "JSON_OBJECT('id', OLD.`id`, 'email', OLD.`email`, 'op', 'delete')") || !strings.Contains(my.Create[3], "CAST(OLD.`id` AS CHAR)") {
		t.Errorf("mysql delete trigger does not read the old row:\n%s", my.Create[3])
	}

	lite, err := ChangeTrigger(sqlb.SQLiteDialect{}, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lite.Create[1], "FOR EACH ROW BEGIN INSERT INTO \"wf_outbox\"") || !strings.HasSuffix(lite.Create[1], "; END") ||
		!strings.Contains(lite.Create[1], "json_object('id', NEW.\"id\", 'email', NEW.\"email\", 'op', 'insert')") {
		t.Errorf("sqlite insert trigger:\n%s", lite.Create[1])
	}
}
