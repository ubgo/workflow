// Command output-store keeps each step attempt's captured output in the
// database, on the SQL driver over a SQLite file: sqlstore.OutputStore as
// the middleware.CaptureOutput sink, read back per run, and wired into the
// admin API as its output source, so every process and the admin run page
// see the same history.
//
// It is the program behind the "Keep output in the database" section of
// docs/schedules/output.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/admin"
	"github.com/ubgo/workflow/middleware"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "output-store:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "output-store-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	// snippet:store
	// A client only to build the store from; the worker below gets its
	// middleware. Client.Migrate creates <prefix>outputs too.
	setup, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := setup.Migrate(ctx); err != nil {
		return err
	}
	outputs := setup.OutputStore(sqlstore.OutputLimits{}) // 20 attempts per run, 64 KiB each
	c, err := sqlstore.New(db, sqlb.SQLiteDialect{},
		sqlstore.WithPollInterval(20*time.Millisecond),
		sqlstore.WithMiddleware(middleware.CaptureOutput(outputs)),
	)
	// snippet:end
	if err != nil {
		return err
	}
	err = c.Register(workflow.Workflow{
		Name:  "import-feed",
		Retry: workflow.RetryPolicy{Max: 1},
		Steps: []workflow.Step{{Name: "fetch", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			middleware.OutputLogger(ctx, nil).Info("fetching feed", "attempt", info.Attempt)
			if info.Attempt == 1 {
				return nil, errors.New("feed timed out")
			}
			return nil, nil
		}}},
	})
	if err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(cctx); err != nil {
		return err
	}
	defer c.Stop()

	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import-feed"})
	if err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st.IsTerminal() {
			fmt.Fprintln(out, "run", st)
			break
		}
		if time.Now().After(deadline) {
			return errors.New("the run did not finish")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// snippet:read
	attempts, err := outputs.RunOutput(ctx, ref.RunID) // oldest first
	if err != nil {
		return err
	}
	for _, a := range attempts {
		fmt.Fprintf(out, "%s attempt %d error=%q logged fetching feed: %t\n", a.Step, a.Attempt, a.Error, strings.Contains(a.Text, "fetching feed"))
	}
	// snippet:end

	// snippet:admin
	// sqlstore does not import admin: map its rows to admin's, one for one.
	src := admin.OutputSourceFunc(func(ctx context.Context, id workflow.RunID) ([]admin.AttemptOutput, error) {
		rows, err := outputs.RunOutput(ctx, id)
		if err != nil {
			return nil, err
		}
		out := make([]admin.AttemptOutput, len(rows))
		for i, r := range rows {
			out[i] = admin.AttemptOutput(r) // the same fields, types and order
		}
		return out, nil
	})
	adm := admin.New(c, admin.WithOutputs(src))
	// snippet:end
	got, err := adm.RunOutput(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "admin sees", len(got), "attempts")
	return nil
}
