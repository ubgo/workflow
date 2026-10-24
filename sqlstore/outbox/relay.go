package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ubgo/sqlb"
	"github.com/ubgo/workflow"
)

// Relay defaults, each overridable by a RelayOption.
const (
	// defaultPoll is how often a relay looks for entries when nothing woke
	// it (Notify) sooner: entries written by other processes, ended backoffs.
	defaultPoll = time.Second
	// defaultBatch caps the entries one claim takes, so one pass stays
	// short and other relays get a share.
	defaultBatch = 100
	// defaultLease is how long a claimed batch stays claimed. A relay that
	// dies loses its batch to another relay after at most this long.
	defaultLease = time.Minute
	// defaultEnqueueTimeout bounds one enqueue on the target, so a hung
	// call cannot hold the batch past its lease.
	defaultEnqueueTimeout = 10 * time.Second
)

// defaultBackoff spaces retries of a failing entry: 1s doubling to 5m.
var defaultBackoff = workflow.Backoff{Initial: time.Second, Factor: 2, Max: 5 * time.Minute}

// RelayOption configures a Relay.
type RelayOption func(*Relay)

// WithPollInterval sets how often an idle relay looks for entries. Values
// of zero or less are ignored.
func WithPollInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.poll = d
		}
	}
}

// WithBatchSize sets how many entries one claim takes. Values below 1 are
// ignored.
func WithBatchSize(n int) RelayOption {
	return func(r *Relay) {
		if n >= 1 {
			r.batch = n
		}
	}
}

// WithLease sets how long a claimed batch stays claimed. It must exceed the
// enqueue timeout: NewRelay refuses otherwise. Values of zero or less are
// ignored.
func WithLease(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.lease = d
		}
	}
}

// WithEnqueueTimeout bounds one enqueue on the target. Values of zero or
// less are ignored.
func WithEnqueueTimeout(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.timeout = d
		}
	}
}

// WithBackoff sets the wait before an entry is tried again after its n-th
// failed enqueue (Backoff.Delay(n)).
func WithBackoff(b workflow.Backoff) RelayOption { return func(r *Relay) { r.backoff = b } }

// WithMaxAttempts moves an entry to the dead status after n failed
// enqueues. Zero (the default) retries forever.
func WithMaxAttempts(n int) RelayOption {
	return func(r *Relay) {
		if n >= 0 {
			r.maxAttempts = n
		}
	}
}

// DeadFunc is told about an entry the moment it goes dead (WithMaxAttempts
// failed enqueues), with the last enqueue error, after the dead status is
// written. It runs on the relay's goroutine, so it should hand slow work
// (paging, a ticket) off rather than block the relay. ctx is not cancelled
// by Stop.
type DeadFunc func(ctx context.Context, e DeadEntry, cause error)

// WithOnDead sets the hook told about each entry that goes dead, for
// alerting or moving it elsewhere; ListDead and Requeue are the read and
// replay side. A nil hook is ignored; without one, dead entries are logged.
func WithOnDead(fn DeadFunc) RelayOption {
	return func(r *Relay) {
		if fn != nil {
			r.onDead = fn
		}
	}
}

// AllowDuplicates lets NewRelay accept a target without
// CapIdempotencyKey, accepting the trade-off it names: delivery stays at
// least once, but nothing drops the second enqueue of an entry, so an entry
// whose relay died after the target accepted it, or whose delete failed,
// runs twice. The relay then sends the job's own idempotency key only (none
// when the job has none), since such a target refuses keyed jobs. Without
// this option NewRelay refuses such a target, because every entry would
// otherwise be refused at enqueue time and go dead.
func AllowDuplicates() RelayOption { return func(r *Relay) { r.allowDuplicates = true } }

// Relay moves committed outbox entries to a target client. Build it with
// Outbox.NewRelay; nothing runs until Start, and Flush relays once without
// starting anything (for an app that drives relaying from its own
// scheduler, or right after a commit). Any number of relays, in any number
// of processes, may share one outbox table.
type Relay struct {
	o           *Outbox
	target      workflow.Client
	poll        time.Duration
	batch       int
	lease       time.Duration
	timeout     time.Duration
	backoff     workflow.Backoff
	maxAttempts int
	onDead      DeadFunc
	// allowDuplicates is AllowDuplicates; entryKey is false when the target
	// lacks CapIdempotencyKey, so the entry ID is not sent as a key.
	allowDuplicates bool
	entryKey        bool

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	wake    chan struct{} // closed and replaced by Notify
	done    chan struct{} // closed when the loop exits
}

// NewRelay returns a relay that enqueues this outbox's entries on target.
// It refuses, with an UnsupportedError, a target without CapIdempotencyKey
// unless AllowDuplicates is given: the relay's duplicate protection is the
// key it sends with every enqueue.
func (o *Outbox) NewRelay(target workflow.Client, opts ...RelayOption) (*Relay, error) {
	if target == nil {
		return nil, fmt.Errorf("%w: NewRelay needs a target client", workflow.ErrInvalid)
	}
	r := &Relay{o: o, target: target, poll: defaultPoll, batch: defaultBatch, lease: defaultLease,
		timeout: defaultEnqueueTimeout, backoff: defaultBackoff, wake: make(chan struct{})}
	for _, opt := range opts {
		opt(r)
	}
	if r.lease <= r.timeout {
		return nil, fmt.Errorf("%w: the relay lease (%s) must exceed the enqueue timeout (%s)", workflow.ErrInvalid, r.lease, r.timeout)
	}
	r.entryKey = target.Capabilities().Has(workflow.CapIdempotencyKey)
	if !r.entryKey && !r.allowDuplicates {
		return nil, workflow.Unsupported("an outbox relay to a target without CapIdempotencyKey",
			"the relay dedups redeliveries by idempotency key; pass outbox.AllowDuplicates() to accept that an entry may run twice")
	}
	return r, nil
}

// Start runs the relay loop until ctx ends or Stop is called. It does not
// block, and it does not migrate (Outbox.Migrate does).
func (r *Relay) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return workflow.ErrAlreadyStarted
	}
	r.started = true
	ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan struct{})
	go r.loop(ctx)
	return nil
}

// Stop ends the loop and waits for it. Entries it had claimed but not yet
// enqueued are released for any relay at once. Safe to call more than
// once, and before Start.
func (r *Relay) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Notify wakes the loop to relay now rather than at its next poll: call it
// after committing a transaction that wrote to the outbox. It never blocks.
func (r *Relay) Notify() {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.wake)
	r.wake = make(chan struct{})
}

func (r *Relay) wakeCh() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wake
}

func (r *Relay) loop(ctx context.Context) {
	defer close(r.done)
	for {
		wake := r.wakeCh()
		if _, err := r.Flush(ctx); err != nil && ctx.Err() == nil {
			r.o.log.Error("outbox: relay pass failed", "error", err)
		}
		poll := r.o.clk.NewTimer(r.poll)
		select {
		case <-ctx.Done():
			poll.Stop()
			return
		case <-wake:
		case <-poll.C():
		}
		poll.Stop()
	}
}

// Flush relays every entry due now and returns how many the target
// accepted. It claims a batch at a time until a batch comes back short or
// delivers nothing, so entries failing with no backoff cannot keep it
// looping. An error is a database failure; a target failure is recorded on
// the entry and retried later, not returned.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	total := 0
	for {
		batch, token, err := r.claim(ctx)
		if err != nil || len(batch) == 0 {
			return total, err
		}
		n, err := r.deliver(ctx, batch, token)
		total += n
		if err != nil || len(batch) < r.batch || n == 0 {
			return total, err
		}
	}
}

// entryCols is the column list entry.targets scans, in order.
const entryCols = "id, workflow, payload, labels, idem, priority, run_at, attempts, created_at"

// entry is one claimed outbox row.
type entry struct {
	id       string
	workflow string
	payload  []byte
	labels   string
	idem     sql.NullString
	priority int64
	runAt    int64
	attempts int64
	created  int64
}

func (e *entry) targets() []any {
	return []any{&e.id, &e.workflow, &e.payload, &e.labels, &e.idem, &e.priority, &e.runAt, &e.attempts, &e.created}
}

// job rebuilds the Job written, with the entry ID as the idempotency key
// when the job had none and entryKey is set.
func (e *entry) job(entryKey bool) (workflow.Job, error) {
	job := workflow.Job{Workflow: e.workflow, Payload: e.payload, Priority: int(e.priority)}
	if entryKey {
		job.IdempotencyKey = e.id
	}
	if e.idem.Valid {
		job.IdempotencyKey = e.idem.String
	}
	if e.runAt != 0 {
		job.RunAt = time.Unix(0, e.runAt).UTC()
	}
	if err := json.Unmarshal([]byte(e.labels), &job.Labels); err != nil {
		return job, fmt.Errorf("outbox: entry %s labels: %w", e.id, err)
	}
	return job, nil
}

// claimWhere selects due pending entries whose lease is free (never taken,
// released, or expired with a dead relay), first written first.
const claimWhere = "status = ? AND due <= ? AND lease_until < ? ORDER BY seq LIMIT ?"

// claim leases up to a batch of due entries to this relay and reads them.
// Postgres and MySQL lock the candidates with FOR UPDATE SKIP LOCKED, so
// concurrent relays pass over each other; SQLite, with one writer, claims
// with a single conditional UPDATE.
func (r *Relay) claim(ctx context.Context) ([]*entry, string, error) {
	o := r.o
	now := o.clk.Now().UnixNano()
	token := randomID("obx")
	until := now + int64(r.lease)
	args := []any{statusPending, now, now, r.batch}
	if o.engine == sqlb.DialectSQLite {
		_, err := o.db.ExecContext(ctx, o.sql("UPDATE {t} SET lease_token = ?, lease_until = ? WHERE id IN (SELECT id FROM {t} WHERE "+claimWhere+")"),
			append([]any{token, until}, args...)...)
		if err != nil {
			return nil, "", fmt.Errorf("outbox: claim: %w", err)
		}
	} else if err := r.claimLocked(ctx, token, until, args); err != nil {
		return nil, "", err
	}
	var out []*entry
	var e entry
	err := sqlb.Each(ctx, o.db, sqlb.NewQuery(o.sql("SELECT "+entryCols+" FROM {t} WHERE lease_token = ? ORDER BY seq"), []any{token}, e.targets()),
		func() error { cp := e; out = append(out, &cp); return nil })
	if err != nil {
		return nil, "", fmt.Errorf("outbox: read claimed entries: %w", err)
	}
	return out, token, nil
}

// claimLocked is the Postgres and MySQL claim: lock the candidates, skipping
// rows another relay holds, then lease them, in one transaction.
func (r *Relay) claimLocked(ctx context.Context, token string, until int64, args []any) error {
	o := r.o
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outbox: begin claim: %w", err)
	}
	// The rollback error is dropped: after a commit it is ErrTxDone, and
	// after a failure the claim error is the one the caller acts on.
	defer func() { _ = tx.Rollback() }()
	var ids []any
	var id string
	err = sqlb.Each(ctx, tx, sqlb.NewQuery(o.sql("SELECT id FROM {t} WHERE "+claimWhere+" FOR UPDATE SKIP LOCKED"), args, []any{&id}),
		func() error { ids = append(ids, id); return nil })
	if err != nil {
		return fmt.Errorf("outbox: claim: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}
	in := "?" + strings.Repeat(", ?", len(ids)-1)
	if _, err := tx.ExecContext(ctx, o.sql("UPDATE {t} SET lease_token = ?, lease_until = ? WHERE id IN ("+in+")"),
		append([]any{token, until}, ids...)...); err != nil {
		return fmt.Errorf("outbox: lease claimed entries: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("outbox: commit claim: %w", err)
	}
	return nil
}

// deliver enqueues each claimed entry on the target in order and returns
// how many were accepted. It stops early, releasing the rest of the batch
// for any relay, when ctx ends or when the lease could run out during the
// next enqueue (another relay might then take the entry while this one
// still sends it).
func (r *Relay) deliver(ctx context.Context, batch []*entry, token string) (int, error) {
	o := r.o
	leaseEnd := o.clk.Now().Add(r.lease)
	sent := 0
	for i, e := range batch {
		if ctx.Err() != nil || o.clk.Now().Add(r.timeout).After(leaseEnd) {
			return sent, r.release(batch[i:], token)
		}
		job, jerr := e.job(r.entryKey)
		if jerr != nil {
			// Written by this package from a map, so a bad value means the row
			// was edited by hand; the job goes on without labels.
			o.log.Error("outbox: entry relayed without labels", "entry", e.id, "error", jerr)
		}
		ectx, cancel := context.WithTimeout(ctx, r.timeout)
		_, err := r.target.Enqueue(ectx, job)
		cancel()
		if err != nil && ctx.Err() != nil {
			// Stopping: the failure is the relay's, not the entry's.
			return sent, r.release(batch[i:], token)
		}
		if err != nil {
			if ferr := r.fail(ctx, e, token, err); ferr != nil {
				return sent, ferr
			}
			continue
		}
		if _, err := o.db.ExecContext(context.WithoutCancel(ctx), o.sql("DELETE FROM {t} WHERE id = ? AND lease_token = ?"), e.id, token); err != nil {
			// The target has it; the entry stays and is sent again after the
			// lease: the at-least-once the package documents.
			return sent, fmt.Errorf("outbox: mark entry %s sent: %w", e.id, err)
		}
		sent++
	}
	return sent, nil
}

// fail records a failed enqueue of e: due again after the backoff, or dead
// once WithMaxAttempts is reached.
func (r *Relay) fail(ctx context.Context, e *entry, token string, cause error) error {
	o := r.o
	attempts := e.attempts + 1
	status := statusPending
	dead := r.maxAttempts > 0 && attempts >= int64(r.maxAttempts)
	if dead {
		status = statusDead
		o.log.Error("outbox: entry is dead", "entry", e.id, "workflow", e.workflow, "attempts", attempts, "error", cause)
	} else {
		o.log.Warn("outbox: enqueue failed; will retry", "entry", e.id, "workflow", e.workflow, "attempts", attempts, "error", cause)
	}
	due := o.clk.Now().Add(r.backoff.Delay(int(attempts))).UnixNano()
	_, err := o.db.ExecContext(context.WithoutCancel(ctx), o.sql(`UPDATE {t} SET status = ?, attempts = ?, due = ?, last_err = ?, lease_token = NULL, lease_until = 0
		WHERE id = ? AND lease_token = ?`), status, attempts, due, cause.Error(), e.id, token)
	if err != nil {
		return fmt.Errorf("outbox: record failed entry %s: %w", e.id, err)
	}
	if dead && r.onDead != nil {
		job, jerr := e.job(false)
		if jerr != nil {
			o.log.Error("outbox: dead entry reported without labels", "entry", e.id, "error", jerr)
		}
		r.onDead(context.WithoutCancel(ctx), DeadEntry{ID: e.id, Job: job, Attempts: int(attempts), LastErr: cause.Error(),
			CreatedAt: time.Unix(0, e.created).UTC()}, cause)
	}
	return nil
}

// release hands entries back unclaimed so any relay takes them at once. It
// runs on a fresh context, because the relay's own may already be done.
func (r *Relay) release(rest []*entry, token string) error {
	if len(rest) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	if _, err := r.o.db.ExecContext(ctx, r.o.sql("UPDATE {t} SET lease_token = NULL, lease_until = 0 WHERE lease_token = ?"), token); err != nil {
		return fmt.Errorf("outbox: release claimed entries: %w", err)
	}
	return nil
}

// releaseTimeout bounds the write that hands claimed entries back.
const releaseTimeout = 5 * time.Second
