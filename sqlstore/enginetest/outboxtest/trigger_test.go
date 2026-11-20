package outboxtest

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore/outbox"
)

// TestChangeTrigger proves a generated change trigger on every engine
// (EV-09): an app table's insert, update and delete, made by plain SQL with
// no outbox.Write, each leave an outbox entry carrying the chosen columns
// (the old row on delete) and the operation; a rolled-back change leaves
// none; applying the trigger SQL twice replaces it rather than adding a
// second; and the relay enqueues each entry on a memory client, whose
// workflow runs it with that payload.
func TestChangeTrigger(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t, e)
			prefix := f.orders[1 : len(f.orders)-len("orders")-1]
			table := prefix + "accounts"
			quoted := e.dialect.QuoteIdent(table)
			if _, err := f.db.Exec("CREATE TABLE " + quoted + " (id VARCHAR(64) NOT NULL PRIMARY KEY, email VARCHAR(191) NOT NULL, plan VARCHAR(32) NOT NULL, secret VARCHAR(64))"); err != nil {
				t.Fatalf("create app table: %v", err)
			}
			trg, err := outbox.ChangeTrigger(e.dialect, outbox.TriggerSpec{
				Table:    table,
				Ops:      []outbox.ChangeOp{outbox.OnInsert, outbox.OnUpdate, outbox.OnDelete},
				Workflow: "account.changed",
				Columns:  []string{"id", "email", "plan"},
				OpField:  "op",
				Priority: 3,
			}, outbox.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, q := range trg.Drop {
					_, _ = f.db.Exec(q)
				}
				_, _ = f.db.Exec("DROP TABLE IF EXISTS " + quoted)
			})
			for range 2 { // the second pass must replace, not add
				for _, q := range trg.Create {
					if _, err := f.db.Exec(q); err != nil {
						t.Fatalf("apply trigger SQL: %v\n%s", err, q)
					}
				}
			}

			for _, q := range []string{
				"INSERT INTO " + quoted + " (id, email, plan, secret) VALUES ('a1', 'ana@example.com', 'free', 'pw')",
				"UPDATE " + quoted + " SET plan = 'pro' WHERE id = 'a1'",
				"DELETE FROM " + quoted + " WHERE id = 'a1'",
			} {
				if _, err := f.db.Exec(q); err != nil {
					t.Fatalf("app change: %v", err)
				}
			}
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("INSERT INTO " + quoted + " (id, email, plan) VALUES ('a2', 'bo@example.com', 'free')"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if n := f.pending(t, "pending"); n != 3 {
				t.Fatalf("%d outbox entries, want 3 (insert, update, delete; none for the rollback or a second trigger)", n)
			}

			m := memory.New()
			var mu sync.Mutex
			var got []map[string]string
			var prios []int
			if err := m.Register(workflow.Workflow{Name: "account.changed", Steps: []workflow.Step{{Name: "sync", Run: func(_ context.Context, in []byte) ([]byte, error) {
				var p map[string]string
				if err := json.Unmarshal(in, &p); err != nil {
					return nil, workflow.Permanent(err)
				}
				mu.Lock()
				got = append(got, p)
				mu.Unlock()
				return nil, nil
			}}}}); err != nil {
				t.Fatal(err)
			}
			rec := &priorities{Client: m, mu: &mu, prios: &prios}
			mctx, cancel := context.WithCancel(ctx)
			t.Cleanup(func() { cancel(); m.Stop() })
			if err := m.Start(mctx); err != nil {
				t.Fatal(err)
			}
			relay, err := f.ob.NewRelay(rec)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := relay.Flush(ctx); err != nil || n != 3 {
				t.Fatalf("Flush = %d, %v; want 3", n, err)
			}
			waitFor(t, "the three jobs to run", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(got) == 3
			})
			mu.Lock()
			defer mu.Unlock()
			sort.Slice(got, func(i, j int) bool { return got[i]["op"] < got[j]["op"] })
			want := []map[string]string{
				{"id": "a1", "email": "ana@example.com", "plan": "pro", "op": "delete"},
				{"id": "a1", "email": "ana@example.com", "plan": "free", "op": "insert"},
				{"id": "a1", "email": "ana@example.com", "plan": "pro", "op": "update"},
			}
			for i := range want {
				if len(got[i]) != len(want[i]) {
					t.Fatalf("payload %d = %v, want %v (only the chosen columns)", i, got[i], want[i])
				}
				for k, v := range want[i] {
					if got[i][k] != v {
						t.Fatalf("payload %d = %v, want %v", i, got[i], want[i])
					}
				}
			}
			for _, p := range prios {
				if p != 3 {
					t.Fatalf("job priorities %v, want 3 each", prios)
				}
			}
		})
	}
}

// priorities records the Priority of every job the relay enqueues.
type priorities struct {
	*memory.Client
	mu    *sync.Mutex
	prios *[]int
}

func (p *priorities) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	p.mu.Lock()
	*p.prios = append(*p.prios, job.Priority)
	p.mu.Unlock()
	return p.Client.Enqueue(ctx, job)
}
