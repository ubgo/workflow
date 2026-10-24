// Command sql-queue-workers runs two worker clients over one SQLite file,
// each serving its own queue, the way an app deploys separate worker
// processes per queue on the SQL driver. It shows what the database makes
// shared: a queue paused from one client holds the other, a failed run is
// listed, retried from its failed step and forgotten from any client, and
// a worker drains gracefully.
//
// It is the program behind the queue, pause and admin sections of
// docs/drivers/sqlstore.md: the guide copies its regions (marked
// "snippet:") and task docs:snippets fails when they drift.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/sqlstore"
)

// waitLimit bounds every wait for a status, generous for a slow CI host.
const waitLimit = 10 * time.Second

// poll is how often the clients look for work another client made due,
// short so the demo finishes quickly.
const poll = 20 * time.Millisecond

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sql-queue-workers:", err)
		os.Exit(1)
	}
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir, err := os.MkdirTemp("", "sql-queue-workers-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	var broken atomic.Bool
	broken.Store(true)
	var gathered, rendered atomic.Int64
	ranOn := make(chan string, 4)

	// snippet:workflows
	email := workflow.Workflow{Name: "send-email", Queue: "mail", Steps: []workflow.Step{{Name: "send",
		Run: func(ctx context.Context, in []byte) ([]byte, error) {
			info, _ := workflow.StepInfoFrom(ctx)
			ranOn <- info.Queue
			return in, nil
		}}}}
	report := workflow.Workflow{Name: "build-report", Steps: []workflow.Step{ // the default queue
		{Name: "gather", Run: func(_ context.Context, in []byte) ([]byte, error) { gathered.Add(1); return in, nil }},
		{Name: "render", After: []string{"gather"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
			rendered.Add(1)
			if broken.Load() {
				return nil, workflow.Permanent(errors.New("template missing"))
			}
			return in, nil
		}},
	}}
	// snippet:end

	// snippet:clients
	mail, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
		sqlstore.WithQueues(workflow.QueueConfig{Name: "mail", Workers: 1})) // one email at a time
	if err != nil {
		return err
	}
	reports, err := sqlstore.New(db, sqlb.SQLiteDialect{}, sqlstore.WithPollInterval(poll),
		sqlstore.WithQueues(workflow.QueueConfig{Name: workflow.DefaultQueue}))
	if err != nil {
		return err
	}
	// snippet:end
	defer mail.Stop()
	defer reports.Stop()
	if err := reports.Migrate(ctx); err != nil {
		return err
	}
	for _, c := range []*sqlstore.Client{mail, reports} {
		for _, wf := range []workflow.Workflow{email, report} {
			if err := c.Register(wf); err != nil {
				return err
			}
		}
		if err := c.Start(ctx); err != nil {
			return err
		}
	}

	// snippet:pause
	// Paused from the reports client; the mail client obeys, because the
	// pause is a row both read when they claim.
	if err := reports.Pause(ctx, workflow.PauseQueue("mail")); err != nil {
		return err
	}
	ref, err := reports.Enqueue(ctx, workflow.Job{Workflow: "send-email", Payload: []byte(`"hi"`)})
	if err != nil {
		return err
	}
	// snippet:end
	time.Sleep(10 * poll) // long enough for the mail client to have claimed it, were it not paused
	st, err := reports.Status(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "email while mail is paused:", st)
	if err := reports.Resume(ctx, workflow.PauseQueue("mail")); err != nil {
		return err
	}
	if err := wait(ctx, reports, ref, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintln(out, "email after resume: succeeded, on queue", <-ranOn)

	failed, err := reports.Enqueue(ctx, workflow.Job{Workflow: "build-report", Labels: map[string]string{"tenant": "acme"}})
	if err != nil {
		return err
	}
	if err := wait(ctx, reports, failed, workflow.StatusFailed); err != nil {
		return err
	}

	// snippet:admin
	page, err := mail.Runs(ctx, workflow.RunQuery{Statuses: []workflow.Status{workflow.StatusFailed}, Labels: map[string]string{"tenant": "acme"}})
	if err != nil {
		return err
	}
	for _, r := range page.Runs {
		fmt.Fprintf(out, "failed: %s at step %s on queue %s\n", r.Workflow, r.FailedStep, r.Queue)
	}
	// The fix is deployed.
	broken.Store(false)
	again, err := mail.Retry(ctx, page.Runs[0].Ref) // from the failed step; gather keeps its output
	if err != nil {
		return err
	}
	// snippet:end
	if err := wait(ctx, reports, again, workflow.StatusSucceeded); err != nil {
		return err
	}
	fmt.Fprintf(out, "after retry: gather ran %d time(s), render %d\n", gathered.Load(), rendered.Load())
	if err := reports.Delete(ctx, again); err != nil {
		return err
	}
	if _, err := reports.Status(ctx, again); errors.Is(err, workflow.ErrNotFound) {
		fmt.Fprintln(out, "after delete: not found")
	}

	// snippet:drain
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	defer dcancel()
	err = mail.Drain(dctx) // stop claiming, let running attempts finish, stop
	fmt.Fprintln(out, "mail worker drained:", err == nil)
	<-mail.Done()
	// snippet:end
	return nil
}

// wait polls ref's status until it is want, a different terminal status,
// or waitLimit passes.
func wait(ctx context.Context, c workflow.Client, ref workflow.RunRef, want workflow.Status) error {
	deadline := time.Now().Add(waitLimit)
	for {
		st, err := c.Status(ctx, ref)
		if err != nil {
			return err
		}
		if st == want {
			return nil
		}
		if st.IsTerminal() || time.Now().After(deadline) {
			return fmt.Errorf("run %s is %s, want %s", ref.RunID, st, want)
		}
		time.Sleep(poll / 2)
	}
}
