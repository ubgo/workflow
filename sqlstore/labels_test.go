package sqlstore_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// labelRows counts the run_labels rows of run id (every run when id is
// empty).
func labelRows(t *testing.T, e engine, db *sql.DB, prefix, id string) int {
	t.Helper()
	q := "SELECT COUNT(*) FROM " + e.dialect.QuoteIdent(prefix+"run_labels")
	var args []any
	if id != "" {
		q += " WHERE run_id = " + e.dialect.Placeholder(1)
		args = append(args, id)
	}
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// runIDs is the RunIDs of a page, in order.
func runIDs(p workflow.RunPage) []string {
	out := make([]string, len(p.Runs))
	for i, r := range p.Runs {
		out[i] = string(r.Ref.RunID)
	}
	return out
}

// TestRunsLabelFilter: a label filter returns exactly the runs whose labels
// hold every pair and every key prefix, also when two values share the
// index's cut prefix and when a prefix holds LIKE wildcards, and
// Delete removes a run's index rows with it.
func TestRunsLabelFilter(t *testing.T) {
	long := strings.Repeat("x", 300)
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			ctx := startWith(t, c, workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "a",
				Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
			enq := func(labels map[string]string) workflow.RunRef {
				ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Labels: labels})
				if err != nil {
					t.Fatal(err)
				}
				waitStatus(t, c, ref, workflow.StatusSucceeded)
				return ref
			}
			a := enq(map[string]string{"tenant": "t1", "kind": "x"})
			b := enq(map[string]string{"tenant": "t1"})
			t2 := enq(map[string]string{"tenant": "t2", "kind": "x"})
			longA := enq(map[string]string{"doc": long + "a"})
			longB := enq(map[string]string{"doc": long + "b"})
			enq(nil)
			pct := enq(map[string]string{"code": "a_b%1"})
			enq(map[string]string{"code": "aXb%2"})

			cases := []struct {
				name     string
				labels   map[string]string
				prefixes map[string]string
				want     []string
			}{
				{"one pair", map[string]string{"tenant": "t1"}, nil, []string{string(b.RunID), string(a.RunID)}},
				{"two pairs", map[string]string{"tenant": "t1", "kind": "x"}, nil, []string{string(a.RunID)}},
				{"no match", map[string]string{"tenant": "t3"}, nil, nil},
				{"long value", map[string]string{"doc": long + "a"}, nil, []string{string(longA.RunID)}},
				{"prefix", nil, map[string]string{"tenant": "t"}, []string{string(t2.RunID), string(b.RunID), string(a.RunID)}},
				{"prefix and pair", map[string]string{"kind": "x"}, map[string]string{"tenant": "t1"}, []string{string(a.RunID)}},
				{"empty prefix", nil, map[string]string{"kind": ""}, []string{string(t2.RunID), string(a.RunID)}},
				{"wildcards are literal", nil, map[string]string{"code": "a_b%"}, []string{string(pct.RunID)}},
				{"long prefix", nil, map[string]string{"doc": long + "a"}, []string{string(longA.RunID)}},
				{"prefix of a cut value", nil, map[string]string{"doc": long[:250]}, []string{string(longB.RunID), string(longA.RunID)}},
			}
			for _, tc := range cases {
				page, err := c.Runs(ctx, workflow.RunQuery{Labels: tc.labels, LabelPrefixes: tc.prefixes})
				if err != nil {
					t.Fatal(err)
				}
				got := runIDs(page)
				if strings.Join(got, ",") != strings.Join(tc.want, ",") {
					t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
				}
			}

			if n := labelRows(t, e, db, prefix, string(a.RunID)); n != 2 {
				t.Fatalf("run a has %d label rows, want 2", n)
			}
			if err := c.Delete(ctx, a); err != nil {
				t.Fatal(err)
			}
			if n := labelRows(t, e, db, prefix, string(a.RunID)); n != 0 {
				t.Fatalf("a deleted run kept %d label rows", n)
			}
		})
	}
}

// TestLabelFilterReadsTheIndex proves an exact and a prefix label filter are answered by the
// run_labels (lkey, lval) index, not by decoding every run's labels: the
// plan SQLite picks for the Runs statement searches that index.
func TestLabelFilterReadsTheIndex(t *testing.T) {
	e := engines(t)[0] // SQLite: EXPLAIN QUERY PLAN is its syntax
	db, prefix := e.open(t)
	c := newClient(t, e, db, prefix)
	for _, q := range []workflow.RunQuery{
		{Labels: map[string]string{"tenant": "t1"}},
		{LabelPrefixes: map[string]string{"tenant": "t"}},
	} {
		plan, err := sqlstore.RunsPlan(context.Background(), c, q)
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(plan, "\n")
		if !strings.Contains(joined, prefix+"run_labels_kv") {
			t.Fatalf("the filter %+v does not read the label index; plan:\n%s", q, joined)
		}
	}
}

// TestMigration005Backfill: runs written before 005 (and by an older
// client after it) are indexed by the backfill, which Migrate runs; a
// second run of it changes nothing, and two at once index each run once.
func TestMigration005Backfill(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			applyScripts(t, e, db, prefix, func(m sqlstore.MigrationScript) bool { return m.Name != "005_run_labels" })
			q := e.dialect.QuoteIdent
			p := e.dialect.Placeholder
			ins := "INSERT INTO " + q(prefix+"runs") + " (id, workflow, labels, priority, bucket, rate_keys, status, run_at, sig_floor," +
				" created_at, finished_at, fail_until, notified, notify_until, ver) VALUES (" + p(1) + ", 'old', " + p(2) +
				", 0, '', '[]', 'pending', 0, 0, " + p(3) + ", 0, 0, 0, 0, 0)"
			insert := func(id, labels string, at int) {
				t.Helper()
				if _, err := db.Exec(ins, id, labels, at); err != nil {
					t.Fatal(err)
				}
			}
			insert("run_a", `{"tenant":"t1","kind":"x"}`, 1)
			insert("run_b", `{}`, 2)
			insert("run_c", `{"tenant":"t1"}`, 3)

			c, err := sqlstore.New(db, e.dialect, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Migrate(ctx); err != nil {
				t.Fatalf("Migrate: %v", err)
			}
			find := func() []string {
				t.Helper()
				page, err := c.Runs(ctx, workflow.RunQuery{Labels: map[string]string{"tenant": "t1"}})
				if err != nil {
					t.Fatal(err)
				}
				return runIDs(page)
			}
			if got := strings.Join(find(), ","); got != "run_c,run_a" {
				t.Fatalf("after Migrate the filter finds %s, want run_c,run_a", got)
			}
			if n := labelRows(t, e, db, prefix, ""); n != 3 {
				t.Fatalf("%d label rows after the backfill, want 3", n)
			}

			if n, err := c.IndexLabels(ctx); err != nil || n != 0 {
				t.Fatalf("a second backfill indexed %d runs (%v), want 0", n, err)
			}
			if n := labelRows(t, e, db, prefix, ""); n != 3 {
				t.Fatalf("%d label rows after a second backfill, want 3", n)
			}

			// An older client, still running through a rolling deploy, writes
			// a run without index rows; the next backfill picks it up, and two
			// at once index it exactly once.
			insert("run_d", `{"tenant":"t1"}`, 4)
			var wg sync.WaitGroup
			counts := make([]int, 2)
			errs := make([]error, 2)
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					counts[i], errs[i] = c.IndexLabels(ctx)
				}()
			}
			wg.Wait()
			if errs[0] != nil || errs[1] != nil || counts[0]+counts[1] != 1 {
				t.Fatalf("two concurrent backfills indexed %v (%v), want 1 in all", counts, errs)
			}
			if got := strings.Join(find(), ","); got != "run_d,run_c,run_a" {
				t.Fatalf("after the second backfill the filter finds %s, want run_d,run_c,run_a", got)
			}
		})
	}
}
