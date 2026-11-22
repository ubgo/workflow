package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// aggregateNeeds is what Aggregate asks of the client the jobs reach: a
// delayed start, so the flush run waits for its window to close.
//
//ds:def id=mw-aggregateneeds-4qsn28f8 owner=@khanakia stability=stable
var aggregateNeeds = workflow.CapDelayedStart

// The reserved labels a flush run carries. Aggregate stamps them; the
// step half (AggregatedItems, AggregateInput) reads them back, so a worker
// in another process needs no copy of the AggregateConfig.
const (
	// AggregateGroupLabel holds the group a flush run takes its items
	// from.
	AggregateGroupLabel = workflow.LabelPrefixInternal + "aggregate-group"
	// AggregateMaxLabel holds the most items one flush takes (the
	// config's MaxItems; "0" takes the whole group).
	AggregateMaxLabel = workflow.LabelPrefixInternal + "aggregate-max"
)

// aggregateItemsData is the run data name under which a flush run keeps
// the items it took, so a retried step gets the same chunk back instead of
// taking another one.
const aggregateItemsData = workflow.LabelPrefixInternal + "aggregate-items"

// ErrFlushNotEnqueued means Aggregate stored the job's payload in its
// group but could not enqueue the flush run the payload needed (the
// backend refused or was down). The item is not lost: the next job of the
// group in this process enqueues a flush at once, and so does any later
// trigger of the group in another process. Do not enqueue the same job
// again: its payload is already in the group and would go out twice.
//
//ds:def id=mw-errflushnotenqueued-wyzy7c7x owner=@khanakia stability=stable
var ErrFlushNotEnqueued = errors.New("workflow/middleware: aggregate: the item is stored but its flush run could not be enqueued")

// AggregateStore holds the pending items of each aggregation group. It is
// where the aggregation state lives, so its reach is Aggregate's:
// MemoryAggregate aggregates inside one process; a store shared by several
// processes (sqlstore.AggregateStore, redis.AggregateStore) aggregates
// a group enqueued from all of them.
//
// Both methods must be atomic per group, which is what makes the
// guarantee hold under concurrency: every appended item is returned by
// exactly one Take, never by two and never by none (as long as Takes keep
// coming, which Aggregate's flush runs arrange).
//
//ds:def id=mw-aggregatestore-4ujmkzzc owner=@khanakia stability=stable
type AggregateStore interface {
	// Append adds item to the end of group and returns how many items the
	// group holds now (this one included) and when its oldest pending
	// item was appended. now is the caller's clock reading, stored with
	// the item; a store never reads a clock of its own.
	Append(ctx context.Context, group string, item []byte, now time.Time) (count int, first time.Time, err error)
	// Take removes and returns up to max of group's oldest items, in the
	// order they were appended; max of zero or less takes them all. A
	// group with no items returns none and no error.
	Take(ctx context.Context, group string, max int) ([][]byte, error)
}

// AggregateConfig configures Aggregate.
//
//ds:def id=mw-aggregateconfig-rxz5zsns owner=@khanakia stability=stable
type AggregateConfig struct {
	// Workflows names the workflows whose jobs are aggregated. A job of
	// another workflow passes through untouched. At least one is
	// required.
	Workflows []string

	// Group returns the group a job's payload joins: one flush run
	// carries the items of one group (the user ID for a per-user digest).
	// A job for which it returns "" is not aggregated and passes through
	// as an ordinary run. Nil puts every job of a workflow in one group
	// named after the workflow.
	Group func(workflow.Job) string

	// Window is how long a group collects items: the flush run starts
	// Window after the group's oldest pending item was appended.
	// Required, positive.
	Window time.Duration

	// MaxItems caps one flush: when a group reaches it, a flush run is
	// enqueued to start now instead of at the window's end, and a flush
	// takes at most MaxItems items. Zero means no cap: one flush per
	// window takes the whole group. Every process enqueuing a group must
	// use the same value.
	MaxItems int

	// RawItems accepts payloads that are not JSON. By default Aggregate
	// refuses such a payload (ErrInvalid) before storing it, because
	// AggregateInput hands the items to the step as one JSON array; with
	// RawItems the step reads them with AggregatedItems instead.
	RawItems bool

	// Clock is the time source; nil means clock.Real.
	Clock clock.Clock
}

// Aggregate turns many small jobs into one run per group and window (a
// digest instead of a mail per comment): a job of an aggregated workflow
// is not enqueued as a run of its own. Its payload is appended to the
// job's group in store, and a flush run of the same workflow, carrying the
// group in AggregateGroupLabel, is enqueued when the group needs one. The
// flush run's step reads the group's items with AggregatedItems, or gets
// them as its input through the AggregateInput step middleware.
//
// When a flush run is enqueued: the job that opens a group (the first
// item after the group was empty) enqueues one starting at the window's
// end, Window after the oldest pending item. With MaxItems set, the job
// that fills a chunk (the group's count reaches a multiple of MaxItems)
// enqueues one starting now, and the job that opens the next chunk
// enqueues one for the window's end. So there is at most one flush per
// window and chunk, never one per job, and there are always enough flush
// runs pending for the items stored: a run that finds its chunk already
// taken by an earlier flush gets no items and should return early.
//
// Enqueue returns the flush run's RunRef for a job that enqueued one, and
// a zero RunRef with a nil error for a job whose item joined a flush that
// is already pending. A job of an aggregated workflow may not carry an
// IdempotencyKey or a Chain (refused with ErrInvalid: the flush run is not
// that job's run), and its RunAt is replaced; its Queue, Tenant,
// Priority, Retry and Labels are copied onto the flush run it triggers.
// Items appended after a flush took its chunk go to the next flush.
//
// It needs CapDelayedStart on c, the client the jobs finally reach, and
// returns an UnsupportedError (matching workflow.ErrUnsupported) without
// it; it runs on every driver that has it (memory, sqlstore, Redis,
// Hatchet). The flush run's step needs the Journal plugin, so a retried
// step gets back the chunk it took (see AggregatedItems).
//
// If storing the item fails, nothing is enqueued and the error is
// returned. If the item is stored and its flush run cannot be enqueued,
// the error matches ErrFlushNotEnqueued: the item is kept, and the next
// job of the group in this process enqueues an immediate flush.
//
//ds:def id=mw-aggregate-pdu5kk9k owner=@khanakia stability=stable
func Aggregate(c workflow.Client, store AggregateStore, cfg AggregateConfig) (workflow.EnqueueMiddleware, error) {
	if c == nil || store == nil {
		return nil, fmt.Errorf("%w: Aggregate needs a client and a store", workflow.ErrInvalid)
	}
	if missing := c.Capabilities().Missing(aggregateNeeds); !missing.IsZero() {
		return nil, &workflow.UnsupportedError{Field: "Aggregate", Missing: missing}
	}
	if len(cfg.Workflows) == 0 || cfg.Window <= 0 || cfg.MaxItems < 0 {
		return nil, fmt.Errorf("%w: Aggregate needs at least one workflow, a positive Window and a MaxItems of zero or more", workflow.ErrInvalid)
	}
	a := &aggregator{store: store, cfg: cfg, wfs: map[string]bool{}, failed: map[string]bool{}}
	for _, w := range cfg.Workflows {
		if w == "" {
			return nil, fmt.Errorf("%w: Aggregate: an empty workflow name", workflow.ErrInvalid)
		}
		a.wfs[w] = true
	}
	if a.cfg.Clock == nil {
		a.cfg.Clock = clock.Real{}
	}
	return a.wrap, nil
}

type aggregator struct {
	store AggregateStore
	cfg   AggregateConfig
	wfs   map[string]bool

	mu sync.Mutex
	// failed holds the groups whose last flush enqueue failed in this
	// process; the group's next job enqueues an immediate flush.
	failed map[string]bool
}

func (a *aggregator) group(job workflow.Job) string {
	if a.cfg.Group == nil {
		return job.Workflow
	}
	return a.cfg.Group(job)
}

//ds:def id=mw-aggregator-wrap-mhdsx7xx owner=@khanakia stability=stable
func (a *aggregator) wrap(next workflow.EnqueueFunc) workflow.EnqueueFunc {
	return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		if !a.wfs[job.Workflow] {
			return next(ctx, job)
		}
		g := a.group(job)
		if g == "" {
			return next(ctx, job)
		}
		item, err := a.check(job)
		if err != nil {
			return workflow.RunRef{}, err
		}
		now := a.cfg.Clock.Now()
		count, first, err := a.store.Append(ctx, g, item, now)
		if err != nil {
			return workflow.RunRef{}, fmt.Errorf("workflow/middleware: aggregate store append: %w", err)
		}
		runAt, trigger := a.trigger(count, first)
		a.mu.Lock()
		if a.failed[g] {
			runAt, trigger = time.Time{}, true
		}
		a.mu.Unlock()
		if !trigger {
			return workflow.RunRef{}, nil
		}
		if !runAt.After(now) {
			runAt = time.Time{}
		}
		ref, err := next(workflow.AllowReservedLabels(ctx, AggregateGroupLabel, AggregateMaxLabel), a.flushJob(job, g, runAt))
		a.mu.Lock()
		if err != nil {
			a.failed[g] = true
		} else {
			delete(a.failed, g)
		}
		a.mu.Unlock()
		if err != nil {
			return workflow.RunRef{}, fmt.Errorf("%w (group %q): %w", ErrFlushNotEnqueued, g, err)
		}
		return ref, nil
	}
}

// check refuses what a flush run cannot carry and returns the payload to
// store (an empty payload is the JSON null).
//
//ds:def id=mw-aggregator-check-vqkgkvza owner=@khanakia stability=stable
func (a *aggregator) check(job workflow.Job) ([]byte, error) {
	if job.IdempotencyKey != "" || len(job.Chain) > 0 {
		return nil, fmt.Errorf("%w: Aggregate: a job of aggregated workflow %q may not set an IdempotencyKey or a Chain", workflow.ErrInvalid, job.Workflow)
	}
	for _, k := range []string{AggregateGroupLabel, AggregateMaxLabel} {
		if _, ok := job.Labels[k]; ok {
			return nil, fmt.Errorf("%w: Aggregate: label %q is reserved for the flush run", workflow.ErrInvalid, k)
		}
	}
	item := job.Payload
	if a.cfg.RawItems {
		return item, nil
	}
	if len(item) == 0 {
		return []byte("null"), nil
	}
	if !json.Valid(item) {
		return nil, fmt.Errorf("%w: Aggregate: the payload of a job of %q is not JSON (set RawItems to aggregate other bytes)", workflow.ErrInvalid, job.Workflow)
	}
	return item, nil
}

// trigger decides from the count Append returned whether this job
// enqueues a flush run and when it starts (zero: now).
//
// The rule keeps enough flush runs pending: with C items stored and a
// chunk of M, at least ceil(C/M) flushes are pending, because the job that
// opens a chunk (count 1, M+1, 2M+1, ...) enqueues one for the window's
// end, and every flush that takes a chunk removes one flush and up to M
// items. The job that fills a chunk (count M, 2M, ...) adds an immediate
// one, so a full chunk does not wait for the window. With M of one every
// job is a full chunk and enqueues one immediate flush.
//
//ds:def id=mw-aggregator-trigger-8xtzdnbj owner=@khanakia stability=stable
func (a *aggregator) trigger(count int, first time.Time) (runAt time.Time, ok bool) {
	m := a.cfg.MaxItems
	switch {
	case m > 0 && count%m == 0:
		return time.Time{}, true
	case m == 0 && count == 1, m > 0 && count%m == 1:
		return first.Add(a.cfg.Window), true
	}
	return time.Time{}, false
}

// flushJob is the flush run for group: job's routing fields, no payload,
// and the group labels.
//
//ds:def id=mw-aggregator-flushjob-nfx9yqzx owner=@khanakia stability=stable
func (a *aggregator) flushJob(job workflow.Job, group string, runAt time.Time) workflow.Job {
	labels := make(map[string]string, len(job.Labels)+2)
	maps.Copy(labels, job.Labels)
	labels[AggregateGroupLabel] = group
	labels[AggregateMaxLabel] = strconv.Itoa(a.cfg.MaxItems)
	return workflow.Job{
		Workflow: job.Workflow,
		RunAt:    runAt,
		Labels:   labels,
		Tenant:   job.Tenant,
		Priority: job.Priority,
		Queue:    job.Queue,
		Retry:    job.Retry,
	}
}

// AggregateGroup returns the group of the flush run ctx's step belongs to,
// read from AggregateGroupLabel, and false outside a flush run.
func AggregateGroup(ctx context.Context) (string, bool) {
	info, ok := workflow.StepInfoFrom(ctx)
	if !ok {
		return "", false
	}
	g, ok := info.Labels[AggregateGroupLabel]
	return g, ok && g != ""
}

// AggregatedItems returns the items of the flush run ctx's step belongs
// to: on the first call it takes up to the run's chunk (AggregateMaxLabel)
// of the group's oldest items from store, removing them, and keeps them as
// run data (workflow.SetRunData), so a retried step, a later step of the
// same run, or a run retried with RunRetrier, gets the same items back and
// nothing is taken twice. A flush that finds its group empty (an earlier
// flush took the chunk) returns no items: return early.
//
// It needs the Journal plugin (a Permanent workflow.ErrNoJournal
// otherwise, before anything is taken), and a flush run: outside one it
// returns a Permanent error wrapping ErrInvalid. Call it from one step at
// a time: two steps of one run calling it in parallel would each take a
// chunk. A crash between the take and the journal's save loses that
// chunk, the one window in which an item can go missing; use a durable
// JournalStore so the window is two store calls wide. A run that fails
// for good after taking keeps its items only in its journal: keep failed
// journals (workflow.JournalKeepFailed) to retry it with the same items.
//
//ds:def id=mw-aggregateditems-mgvmxuw5 owner=@khanakia stability=stable
func AggregatedItems(ctx context.Context, store AggregateStore) ([][]byte, error) {
	group, ok := AggregateGroup(ctx)
	if !ok {
		return nil, workflow.Permanent(fmt.Errorf("%w: AggregatedItems: the run is not an aggregate flush run (no %s label)", workflow.ErrInvalid, AggregateGroupLabel))
	}
	info, _ := workflow.StepInfoFrom(ctx)
	limit, err := strconv.Atoi(info.Labels[AggregateMaxLabel])
	if err != nil || limit < 0 {
		return nil, workflow.Permanent(fmt.Errorf("%w: AggregatedItems: label %s is %q", workflow.ErrInvalid, AggregateMaxLabel, info.Labels[AggregateMaxLabel]))
	}
	items, found, err := workflow.RunData[[][]byte](ctx, aggregateItemsData)
	if err != nil || found {
		return items, err
	}
	items, err = store.Take(ctx, group, limit)
	if err != nil {
		return nil, fmt.Errorf("workflow/middleware: aggregate store take: %w", err)
	}
	if items == nil {
		items = [][]byte{}
	}
	if err := workflow.SetRunData(ctx, aggregateItemsData, items); err != nil {
		return nil, fmt.Errorf("workflow/middleware: aggregate: keep the taken items: %w", err)
	}
	return items, nil
}

// AggregateInput is step middleware for the workflows Aggregate feeds:
// in a flush run it replaces the step's input with the run's items
// (AggregatedItems) as one JSON array, in the order they were appended
// (an empty array when the flush found nothing). A step of any other run
// passes through untouched. It applies to every step of a flush run, and
// every step gets the same items, since the first one keeps them as run
// data; give a flush workflow one step that reads the items, or steps
// that run one after another. An item that is not JSON (RawItems) fails
// the step with a Permanent error; read such items with AggregatedItems.
//
//ds:def id=mw-aggregateinput-zg7dq6qm owner=@khanakia stability=stable
func AggregateInput(store AggregateStore) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if _, ok := AggregateGroup(ctx); !ok {
				return next(ctx, in)
			}
			items, err := AggregatedItems(ctx, store)
			if err != nil {
				return nil, err
			}
			raw := make([]json.RawMessage, len(items))
			for i, it := range items {
				if !json.Valid(it) {
					return nil, workflow.Permanent(fmt.Errorf("%w: AggregateInput: item %d of the flush is not JSON", workflow.ErrInvalid, i))
				}
				raw[i] = it
			}
			arr, err := json.Marshal(raw)
			if err != nil {
				return nil, workflow.Permanent(fmt.Errorf("workflow/middleware: aggregate input: %w", err))
			}
			return next(ctx, arr)
		}
	}
}

// MemoryAggregate is an AggregateStore inside one process, for tests, a
// single server, or aggregation that only needs to hold per process. Its
// items are lost when the process exits. The zero value is not ready: use
// NewMemoryAggregate.
//
//ds:def id=mw-memoryaggregate-d68n2mzr owner=@khanakia stability=stable
type MemoryAggregate struct {
	mu     sync.Mutex
	groups map[string]*memGroup
}

type memGroup struct {
	items [][]byte
	at    []time.Time
}

// NewMemoryAggregate returns an empty store.
func NewMemoryAggregate() *MemoryAggregate {
	return &MemoryAggregate{groups: map[string]*memGroup{}}
}

// Append implements AggregateStore. It keeps a copy of item.
func (m *MemoryAggregate) Append(_ context.Context, group string, item []byte, now time.Time) (int, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		g = &memGroup{}
		m.groups[group] = g
	}
	g.items = append(g.items, slices.Clone(item))
	g.at = append(g.at, now)
	return len(g.items), g.at[0], nil
}

// Take implements AggregateStore. A group it empties is dropped.
func (m *MemoryAggregate) Take(_ context.Context, group string, limit int) ([][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return nil, nil
	}
	n := len(g.items)
	if limit > 0 && limit < n {
		n = limit
	}
	out := g.items[:n:n]
	g.items, g.at = g.items[n:], g.at[n:]
	if len(g.items) == 0 {
		delete(m.groups, group)
	}
	return out, nil
}

// Pending returns how many items group holds, for tests and metrics.
func (m *MemoryAggregate) Pending(group string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.groups[group]; g != nil {
		return len(g.items)
	}
	return 0
}

// AggregatePlugin bundles both halves for workflow.Use: Aggregate as
// enqueue middleware and, unless cfg.RawItems, AggregateInput as step
// middleware. c is the client handed to Use (the one the jobs reach). Put
// it after the Journal plugin in the same Use call, so the journal is on
// the step's ctx when AggregateInput takes the items:
//
//	c, err := workflow.Use(inner, workflow.Journal(journal), agg)
//
// It returns Aggregate's errors.
//
//ds:def id=mw-aggregateplugin-fpzxvv2y owner=@khanakia stability=stable
func AggregatePlugin(c workflow.Client, store AggregateStore, cfg AggregateConfig) (workflow.Plugin, error) {
	mw, err := Aggregate(c, store, cfg)
	if err != nil {
		return nil, err
	}
	p := &aggregatePlugin{enqueue: mw}
	if !cfg.RawItems {
		p.step = AggregateInput(store)
	}
	return p, nil
}

type aggregatePlugin struct {
	enqueue workflow.EnqueueMiddleware
	step    workflow.Middleware // nil with RawItems
}

// Name names the plugin in refusals.
func (p *aggregatePlugin) Name() string { return "aggregate" }

// EnqueueMiddleware returns Aggregate's middleware.
func (p *aggregatePlugin) EnqueueMiddleware() []workflow.EnqueueMiddleware {
	return []workflow.EnqueueMiddleware{p.enqueue}
}

// StepMiddleware returns AggregateInput, or nothing with RawItems.
func (p *aggregatePlugin) StepMiddleware() []workflow.Middleware {
	if p.step == nil {
		return nil
	}
	return []workflow.Middleware{p.step}
}
