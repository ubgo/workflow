package enginetest

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// TestLongPollSingleProcess proves IO-03 and the SQL half of EM-04: a
// single-process app (one SQLite file, every enqueue made by this client)
// can set a poll interval of an hour and still runs new work at once and
// delayed work at its due time, because the dispatcher sleeps until the
// next due time it knows and a local enqueue wakes it; the poll only looks
// for work other processes made due, and there are none.
func TestLongPollSingleProcess(t *testing.T) {
	for _, e := range engines(t) {
		if e.name != "sqlite" {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			c := newClient(t, e, db, prefix, sqlstore.WithPollInterval(time.Hour))
			ran := make(chan string, 2)
			if err := c.Register(workflow.Workflow{Name: "w", Steps: []workflow.Step{{Name: "s", Run: func(_ context.Context, in []byte) ([]byte, error) {
				ran <- string(in)
				return nil, nil
			}}}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("delayed"), RunAt: time.Now().Add(300 * time.Millisecond)}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "w", Payload: []byte("now")}); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"now", "delayed"} {
				select {
				case got := <-ran:
					if got != want {
						t.Fatalf("ran %q, want %q first", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("%q did not run with an hour-long poll", want)
				}
			}
			if el := time.Since(start); el < 300*time.Millisecond || el > 3*time.Second {
				t.Fatalf("the delayed run ran after %s, want at its due time", el)
			}
		})
	}
}
