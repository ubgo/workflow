// Command label-search finds runs by their labels on the SQL driver over
// SQLite: an exact tenant label, an order number prefix, and "has this
// label at all", answered from the label index migration 005 adds. It
// then shows the backfill an app calls after applying the migrations
// with its own tool, and the refusal of a prefix filter on a driver that
// cannot answer it.
//
// It is the program behind docs/jobs/manage-runs.md#search-by-label.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ubgo/sqlb"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/sqlstore"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "label-search:", err)
		os.Exit(1)
	}
}

// orders are the runs the program enqueues, each with its labels.
var orders = []map[string]string{
	{"tenant": "acme", "order": "A-1001"},
	{"tenant": "acme", "order": "A-1002", "coupon": "SPRING"},
	{"tenant": "acme", "order": "B-2001"},
	{"tenant": "globex", "order": "A-1003", "coupon": "VIP"},
}

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	dir, err := os.MkdirTemp("", "label-search-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := driversqlite.Open(filepath.Join(dir, "app.db"))
	if err != nil {
		return err
	}
	defer db.Close()

	c, err := sqlstore.New(db, sqlb.SQLiteDialect{})
	if err != nil {
		return err
	}
	if err := c.Migrate(ctx); err != nil { // applies 005_run_labels, then backfills
		return err
	}
	if err := c.Register(workflow.Workflow{Name: "ship-order", Steps: []workflow.Step{{
		Name: "ship", Run: func(context.Context, []byte) ([]byte, error) { return nil, nil },
	}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		return err
	}
	defer c.Stop()
	for _, labels := range orders {
		if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "ship-order", Labels: labels}); err != nil {
			return err
		}
	}
	fmt.Fprintln(out, "label prefix capability:", c.Capabilities().Has(workflow.CapLabelPrefix))

	// snippet:search
	acmeA, err := c.Runs(ctx, workflow.RunQuery{
		Labels:        map[string]string{"tenant": "acme"}, // exact value
		LabelPrefixes: map[string]string{"order": "A-"},    // value starts with A-
	})
	if err != nil {
		return err
	}
	withCoupon, err := c.Runs(ctx, workflow.RunQuery{
		LabelPrefixes: map[string]string{"coupon": ""}, // the key is present, any value
	})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintln(out, "acme, order A-*:", labelValues(acmeA, "order"))
	fmt.Fprintln(out, "has a coupon:", labelValues(withCoupon, "order"))

	// snippet:backfill
	n, err := c.IndexLabels(ctx) // after MigrationScripts applied by your own tool
	if err != nil {
		return err
	}
	again, err := c.IndexLabels(ctx) // safe to repeat: nothing past the watermark
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "IndexLabels: %d runs, then %d\n", n, again)

	// snippet:refused
	q := workflow.RunQuery{LabelPrefixes: map[string]string{"order": "A-"}}
	err = workflow.CheckRunQuery(hatchet.New(hatchet.Config{}).Capabilities(), q)
	var unsupported *workflow.UnsupportedError
	if errors.As(err, &unsupported) {
		fmt.Fprintf(out, "hatchet refuses: field %s, missing %s\n", unsupported.Field, unsupported.Missing)
	}
	// snippet:end
	return nil
}

// labelValues returns the value of key on each run of page, sorted.
func labelValues(page workflow.RunPage, key string) string {
	var vs []string
	for _, r := range page.Runs {
		vs = append(vs, r.Labels[key])
	}
	sort.Strings(vs)
	return strings.Join(vs, " ")
}
