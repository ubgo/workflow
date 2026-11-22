package enginetest

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// The statements docs/drivers/sqlstore-insert-contract.md gives a writer in
// another language. {p} is the table prefix and ? a parameter; the test
// rewrites both for the engine. Keep them equal to the page.
const (
	//ds:def id=enginetest-foreignrun-rkf6n8qx owner=@khanakia stability=stable
	foreignRun = `INSERT INTO {p}runs (id, scheduled_id, workflow, payload, labels, idem_active, priority, bucket, rate_keys,
  status, run_at, sig_floor, created_at, finished_at, fail_until, notified, notify_until, ver)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, (SELECT COALESCE(MAX(id), 0) FROM {p}signals), ?, 0, 0, 0, 0, 0)`
	//ds:def id=enginetest-foreignrunext-zuvsh9y6 owner=@khanakia stability=stable
	foreignRunExt = `INSERT INTO {p}run_ext (run_id, queue, idem_key, retry, started_at, discarded)
VALUES (?, ?, ?, NULL, 0, 0)`
	//ds:def id=enginetest-foreignruninfo-fz5tjbvv owner=@khanakia stability=stable
	foreignRunInfo = `INSERT INTO {p}run_info (run_id, tenant, output, has_output, progress, nodes, deadline, timeout)
VALUES (?, ?, NULL, 0, NULL, '[]', 0, '')`
	//ds:def id=enginetest-foreignrunlabel-7vk4p34x owner=@khanakia stability=stable
	foreignRunLabel = `INSERT INTO {p}run_labels (run_id, lkey, lval) VALUES (?, ?, ?)`
)

// foreignJob is one run as a foreign writer describes it.
type foreignJob struct {
	id, scheduledID, workflow, labels, key, queue, tenant string
	payload                                               []byte
	priority                                              int
	runAt                                                 time.Time // zero: now
	labelPairs                                            [][2]string
}

// insertForeign writes job by the contract's plain SQL in one transaction,
// as a program in another language would, and returns the error of the
// first statement that failed (a unique violation for a held key).
func insertForeign(ctx context.Context, db *sql.DB, d sqlb.Dialect, prefix string, job foreignJob) error {
	q := func(s string) string {
		s = strings.ReplaceAll(s, "{p}", prefix)
		if d.Placeholder(1) == "?" {
			return s
		}
		var b strings.Builder
		n := 0
		for _, r := range s {
			if r == '?' {
				n++
				b.WriteString(d.Placeholder(n))
				continue
			}
			b.WriteRune(r)
		}
		return b.String()
	}
	nullable := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	now := time.Now().UnixNano()
	status, runAt := "pending", now
	if !job.runAt.IsZero() {
		status, runAt = "scheduled", job.runAt.UnixNano()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, q(foreignRun), job.id, nullable(job.scheduledID), job.workflow, job.payload, job.labels,
		nullable(job.key), job.priority, status, runAt, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q(foreignRunExt), job.id, job.queue, nullable(job.key)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q(foreignRunInfo), job.id, job.tenant); err != nil {
		return err
	}
	for _, p := range job.labelPairs {
		if _, err := tx.ExecContext(ctx, q(foreignRunLabel), job.id, p[0], p[1]); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// TestForeignInsert proves the insert contract (QC-15): runs written by
// plain SQL, without this package, are run by a client that registers the
// workflow, on every engine: a run due now (with labels found by the label
// index, and its queue filled in by the admitting worker), a delayed run,
// and a keyed run whose key a second writer and Enqueue both see held.
//
//ds:def id=enginetest-testforeigninsert-e8axw8f6 owner=@khanakia stability=stable
func TestForeignInsert(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix)
			var mu sync.Mutex
			got := map[string]string{} // run id -> payload the step saw
			if err := c.Register(workflow.Workflow{Name: "report", Queue: "reports", Steps: []workflow.Step{{Name: "build",
				Run: func(ctx context.Context, in []byte) ([]byte, error) {
					info, _ := workflow.StepInfoFrom(ctx)
					mu.Lock()
					got[string(info.RunID)] = string(in)
					mu.Unlock()
					return nil, nil
				}}}}); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			seen := func(id string) string {
				mu.Lock()
				defer mu.Unlock()
				return got[id]
			}

			now := foreignJob{id: "run_" + randHex(16), workflow: "report", payload: []byte(`{"month":"2026-11"}`),
				labels: `{"source":"billing"}`, labelPairs: [][2]string{{"source", "billing"}}, tenant: "acme"}
			if err := insertForeign(ctx, db, e.dialect, prefix, now); err != nil {
				t.Fatalf("insert a run by SQL: %v", err)
			}
			ref := workflow.RunRef{RunID: workflow.RunID(now.id)}
			waitFor(t, 10*time.Second, "the SQL-inserted run to succeed", func() bool {
				st, err := c.Status(ctx, ref)
				return err == nil && st == workflow.StatusSucceeded
			})
			if p := seen(now.id); p != string(now.payload) {
				t.Fatalf("step saw payload %q, want %q", p, now.payload)
			}
			page, err := c.Runs(ctx, workflow.RunQuery{Labels: map[string]string{"source": "billing"}})
			if err != nil || len(page.Runs) != 1 || page.Runs[0].Ref.RunID != ref.RunID {
				t.Fatalf("label query found %+v, %v; want the inserted run", page.Runs, err)
			}
			if info := page.Runs[0]; info.Queue != "reports" || info.Tenant != "acme" {
				t.Fatalf("run queue %q tenant %q, want the workflow's queue (filled in at admission) and acme", info.Queue, info.Tenant)
			}

			later := foreignJob{id: "run_" + randHex(16), scheduledID: "sched_" + randHex(16), workflow: "report",
				payload: []byte(`{"month":"2026-12"}`), labels: `{}`, runAt: time.Now().Add(300 * time.Millisecond)}
			if err := insertForeign(ctx, db, e.dialect, prefix, later); err != nil {
				t.Fatalf("insert a delayed run by SQL: %v", err)
			}
			if st, err := c.Status(ctx, workflow.RunRef{ScheduledID: workflow.ScheduledID(later.scheduledID)}); err != nil || st != workflow.StatusScheduled {
				t.Fatalf("delayed run %s, %v; want scheduled", st, err)
			}
			waitFor(t, 10*time.Second, "the delayed run to succeed", func() bool {
				st, err := c.Status(ctx, workflow.RunRef{RunID: workflow.RunID(later.id)})
				return err == nil && st == workflow.StatusSucceeded
			})
			if seen(later.id) == "" {
				t.Fatal("the delayed run's step never ran")
			}

			// A key held by an unfinished run: the second writer's insert
			// fails on the unique index (its transaction rolls back) and
			// Enqueue returns the held run. The run waits an hour, so it
			// holds the key for the whole test.
			keyed := foreignJob{id: "run_" + randHex(16), scheduledID: "sched_" + randHex(16), workflow: "report", labels: `{}`,
				key: "report-2027-01", runAt: time.Now().Add(time.Hour)}
			if err := insertForeign(ctx, db, e.dialect, prefix, keyed); err != nil {
				t.Fatalf("insert a keyed run by SQL: %v", err)
			}
			dup := keyed
			dup.id, dup.scheduledID = "run_"+randHex(16), "sched_"+randHex(16)
			if err := insertForeign(ctx, db, e.dialect, prefix, dup); err == nil {
				t.Fatal("a second SQL insert of a held key succeeded; want a unique violation")
			}
			held, err := c.Enqueue(ctx, workflow.Job{Workflow: "report", IdempotencyKey: keyed.key})
			if err != nil {
				t.Fatal(err)
			}
			if held.ScheduledID != workflow.ScheduledID(keyed.scheduledID) {
				t.Fatalf("Enqueue with the held key returned %+v, want the SQL-inserted run", held)
			}
		})
	}
}
