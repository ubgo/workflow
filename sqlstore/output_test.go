package sqlstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

// TestOutputStoreKeepsOutputWithTheRun captures the output of a step that
// fails twice and then succeeds, through a standalone OutputStore wired as
// CaptureOutput's sink, and checks the bounds, the truncation, the order,
// the errors, and that deleting the run deletes its output.
func TestOutputStoreKeepsOutputWithTheRun(t *testing.T) {
	for _, e := range engines(t) {
		t.Run(e.name, func(t *testing.T) {
			db, prefix := e.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			const cap = 200
			store, err := sqlstore.NewOutputStore(db, e.dialect, sqlstore.OutputLimits{PerRun: 2, Bytes: cap}, sqlstore.WithTablePrefix(prefix))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			wf := workflow.Workflow{Name: "chatty", Retry: workflow.RetryPolicy{Max: 2}, Steps: []workflow.Step{{Name: "s",
				Run: func(ctx context.Context, _ []byte) ([]byte, error) {
					info, _ := workflow.StepInfoFrom(ctx)
					log := middleware.OutputLogger(ctx, nil)
					log.Info(fmt.Sprintf("attempt %d", info.Attempt))
					if info.Attempt == 3 {
						log.Info(strings.Repeat("é", cap)) // two bytes each: the cut lands inside one
						return nil, nil
					}
					return nil, fmt.Errorf("boom %d", info.Attempt)
				}}}}
			c := newClient(t, e, db, prefix, sqlstore.WithMiddleware(middleware.CaptureOutput(store)))
			if err := c.Register(wf); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: wf.Name})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, 20*time.Second, "the run to succeed", func() bool {
				st, _ := c.Status(ctx, ref)
				return st == workflow.StatusSucceeded
			})
			got, err := store.RunOutput(ctx, ref.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 {
				t.Fatalf("%d attempts kept, want the newest 2: %+v", len(got), got)
			}
			if got[0].Attempt != 2 || got[0].Error != "boom 2" || !strings.Contains(got[0].Text, "attempt 2") || got[0].Truncated {
				t.Errorf("first kept attempt %+v, want attempt 2 failed with boom 2", got[0])
			}
			last := got[1]
			if last.Attempt != 3 || last.Error != "" || last.Step != "s" || last.Started.IsZero() || last.Finished.Before(last.Started) {
				t.Errorf("last attempt %+v, want attempt 3 of s, succeeded, timed", last)
			}
			if !last.Truncated || len(last.Text) > cap || !utf8.ValidString(last.Text) {
				t.Errorf("last text: %d bytes, truncated %v, valid UTF-8 %v; want cut to at most %d valid bytes", len(last.Text), last.Truncated, utf8.ValidString(last.Text), cap)
			}

			if err := c.Delete(ctx, ref); err != nil {
				t.Fatal(err)
			}
			if got, err := store.RunOutput(ctx, ref.RunID); err != nil || got == nil || len(got) != 0 {
				t.Errorf("output after the run was deleted: %+v, %v; want an empty slice", got, err)
			}
			if err := store.Deliver(ctx, middleware.Output{}); !errors.Is(err, sqlstore.ErrOutputNoRun) {
				t.Errorf("Deliver without StepInfo: %v, want ErrOutputNoRun", err)
			}
		})
	}
}

// TestOutputStorePruneBefore proves a store used without a Client bounds
// its table by finish time.
func TestOutputStorePruneBefore(t *testing.T) {
	e := engines(t)[0]
	db, prefix := e.open(t)
	ctx := context.Background()
	store, err := sqlstore.NewOutputStore(db, e.dialect, sqlstore.OutputLimits{}, sqlstore.WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, run := range []workflow.RunID{"old", "new"} {
		at := base.Add(time.Duration(i) * time.Hour)
		out := middleware.Output{HasInfo: true, Info: workflow.StepInfo{RunID: run, Step: "s", Attempt: 1}, Started: at, Finished: at}
		if err := store.Deliver(ctx, out); err != nil {
			t.Fatal(err)
		}
	}
	n, err := store.PruneBefore(ctx, base.Add(30*time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("PruneBefore: %d, %v; want 1", n, err)
	}
	if got, _ := store.RunOutput(ctx, "old"); len(got) != 0 {
		t.Errorf("old output kept: %+v", got)
	}
	if got, _ := store.RunOutput(ctx, "new"); len(got) != 1 {
		t.Errorf("new output: %+v, want 1 attempt", got)
	}
}
