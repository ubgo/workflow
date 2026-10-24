package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ubgo/sqlb"
	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
	driversqlite "github.com/ubgo/sqlb/contrib/driver-sqlite"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/sqlstore"
	"github.com/ubgo/workflow/sqlstore/pgnotify"
)

// benchWorkflow is the one-step workflow every ubgo driver runs.
const benchWorkflow = "ubgo-bench"

// wfSystem runs the workload on a workflow.Client. newClient builds the
// client for conc worker slots; cleanup (optional) removes what the run
// created once the client has stopped.
type wfSystem struct {
	newClient   func(ctx context.Context, conc int) (workflow.Client, error)
	newProducer func(ctx context.Context) (workflow.Client, error) // nil: no worker-less client
	cleanup     func(ctx context.Context) error

	client  workflow.Client
	prod    workflow.Client // the separate producer, when one was asked for
	cancel  context.CancelFunc
	started bool // Start succeeded: there is something on the engine to remove
}

func (s *wfSystem) start(ctx context.Context, conc int, h handler) (enqueueFunc, error) {
	c, err := s.newClient(ctx, conc)
	if err != nil {
		return nil, err
	}
	s.client = c
	err = c.Register(workflow.Workflow{
		Name: benchWorkflow,
		Steps: []workflow.Step{{Name: "run", Run: func(_ context.Context, in []byte) ([]byte, error) {
			return nil, h(in)
		}}},
	})
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	if err := c.Start(runCtx); err != nil {
		return nil, err
	}
	s.started = true
	return func(ctx context.Context, p []byte) error {
		_, err := c.Enqueue(ctx, workflow.Job{Workflow: benchWorkflow, Payload: p})
		return err
	}, nil
}

// producer builds the worker-less client, registers the workflow on it
// (so it admits runs itself, as a web node would) and starts it, which
// for an enqueue-only client starts nothing.
func (s *wfSystem) producer(ctx context.Context) (enqueueFunc, error) {
	if s.newProducer == nil {
		return nil, errNoProducer
	}
	p, err := s.newProducer(ctx)
	if err != nil {
		return nil, err
	}
	s.prod = p
	err = p.Register(workflow.Workflow{
		Name: benchWorkflow,
		Steps: []workflow.Step{{Name: "run", Run: func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("bench: the producer must never run a step")
		}}},
	})
	if err != nil {
		return nil, err
	}
	if err := p.Start(ctx); err != nil {
		return nil, err
	}
	return func(ctx context.Context, b []byte) error {
		_, err := p.Enqueue(ctx, workflow.Job{Workflow: benchWorkflow, Payload: b})
		return err
	}, nil
}

func (s *wfSystem) close(ctx context.Context) error {
	var errs []error
	if st, ok := s.prod.(interface{ Stop() }); ok {
		st.Stop()
	}
	if s.client != nil {
		if h, ok := s.client.(*hatchet.Client); ok && s.started {
			errs = append(errs, deleteHatchetWorkflow(ctx, h))
		}
		if st, ok := s.client.(interface{ Stop() }); ok {
			st.Stop() // every driver here has Stop; cancelling below is the fallback
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.cleanup != nil {
		errs = append(errs, s.cleanup(ctx))
	}
	return errors.Join(errs...)
}

// deleteHatchetWorkflow removes the bench workflow (and with it its runs)
// from the engine, so repeated runs leave nothing behind.
func deleteHatchetWorkflow(ctx context.Context, h *hatchet.Client) error {
	sdk := h.Native()
	if sdk == nil {
		return nil // never started: nothing was declared
	}
	_, err := sdk.Workflows().Delete(ctx, benchWorkflow)
	// The SDK (v0.107.1) expects 200 and reports the engine's 204 No
	// Content, which is the delete succeeding, as an error.
	if err == nil || strings.Contains(err.Error(), "got status 204") {
		return nil
	}
	return fmt.Errorf("delete hatchet workflow %s: %w", benchWorkflow, err)
}

func newMemory() system {
	return &wfSystem{newClient: func(_ context.Context, conc int) (workflow.Client, error) {
		return memory.New(memory.WithWorkers(conc)), nil
	}}
}

// newSQLite runs sqlstore on a SQLite file in a fresh temporary directory,
// removed after the run.
func newSQLite() system {
	s := &wfSystem{}
	var (
		dir string
		db  *sql.DB
	)
	s.newClient = func(ctx context.Context, conc int) (workflow.Client, error) {
		var err error
		if dir, err = os.MkdirTemp("", "ubgo-bench-*"); err != nil {
			return nil, err
		}
		if db, err = driversqlite.Open(filepath.Join(dir, "bench.db")); err != nil {
			return nil, err
		}
		return newSQLClient(ctx, db, sqlb.SQLiteDialect{}, sqlstore.WithWorkers(conc))
	}
	s.newProducer = func(ctx context.Context) (workflow.Client, error) {
		return newSQLClient(ctx, db, sqlb.SQLiteDialect{}, sqlstore.WithEnqueueOnly())
	}
	s.cleanup = func(context.Context) error {
		var errs []error
		if db != nil {
			errs = append(errs, db.Close())
		}
		if dir != "" {
			errs = append(errs, os.RemoveAll(dir))
		}
		return errors.Join(errs...)
	}
	return s
}

// newPostgres runs sqlstore on a database created for this run and
// dropped after it. With notify, both the worker and the producer get the
// LISTEN/NOTIFY notifier; without it they rely on polling alone. pool is
// the connection pool size, 0 for the sqlb pgx driver's default.
func newPostgres(pg *pgServer, notify bool, pool int) system {
	s := &wfSystem{}
	var (
		dbName string
		db     *sql.DB
	)
	opts := func(extra ...sqlstore.Option) []sqlstore.Option {
		if notify {
			extra = append(extra, sqlstore.WithNotifier(pgnotify.New(db)))
		}
		return extra
	}
	s.newClient = func(ctx context.Context, conc int) (workflow.Client, error) {
		var err error
		if dbName, err = pg.create(ctx, "sqlstore"); err != nil {
			return nil, err
		}
		var popts []driverpgx.Option
		if pool > 0 {
			popts = append(popts, driverpgx.WithMaxOpenConns(pool))
		}
		if db, err = driverpgx.Open(pg.dsn(dbName), popts...); err != nil {
			return nil, err
		}
		return newSQLClient(ctx, db, sqlb.PostgresDialect{}, opts(sqlstore.WithWorkers(conc))...)
	}
	s.newProducer = func(ctx context.Context) (workflow.Client, error) {
		return newSQLClient(ctx, db, sqlb.PostgresDialect{}, opts(sqlstore.WithEnqueueOnly())...)
	}
	s.cleanup = func(ctx context.Context) error {
		var errs []error
		if db != nil {
			errs = append(errs, db.Close())
		}
		if dbName != "" {
			errs = append(errs, pg.drop(ctx, dbName))
		}
		return errors.Join(errs...)
	}
	return s
}

// newSQLClient builds a migrated sqlstore client with the driver's default
// poll interval, lease and pool: the bench measures what an app gets
// untuned.
func newSQLClient(ctx context.Context, db *sql.DB, d sqlb.Dialect, opts ...sqlstore.Option) (workflow.Client, error) {
	c, err := sqlstore.New(db, d, opts...)
	if err != nil {
		return nil, err
	}
	if err := c.Migrate(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// newHatchet runs on the engine the HATCHET_CLIENT_* environment points
// at, with conc worker slots.
// The producer is an EnqueueOnly client: it triggers runs through the
// engine and runs nothing.
func newHatchet() system {
	return &wfSystem{
		newClient: func(_ context.Context, conc int) (workflow.Client, error) {
			return hatchet.New(hatchet.Config{
				Namespace:  os.Getenv("HATCHET_CLIENT_NAMESPACE"),
				WorkerName: "ubgo-bench",
				Slots:      conc,
			}), nil
		},
		newProducer: func(context.Context) (workflow.Client, error) {
			return hatchet.New(hatchet.Config{
				Namespace:   os.Getenv("HATCHET_CLIENT_NAMESPACE"),
				EnqueueOnly: true,
			}), nil
		},
	}
}
