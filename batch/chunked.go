package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/ubgo/workflow"
)

// ChunkedOptions configures EnqueueChunked.
type ChunkedOptions struct {
	// ChunkOptions is the paging: Size jobs per chunk (at least 1), the
	// Total when known (for progress) and MaxChunks, which is required
	// when Total is not set. See workflow.ProcessChunks.
	workflow.ChunkOptions

	// Limit, when set, is drawn from before each chunk is enqueued, at a
	// cost of one per job, so a fan-out to a million recipients goes out
	// at the rate the downstream allows. A chunk that does not fit fails
	// the step with the limit's retry-after error (or snoozes it, with
	// Snooze), and the retried step resumes at that chunk: give the
	// step's workflow RetryPolicy.DelayFor workflow.RetryAfterDelay, so
	// the retry waits for room, and enough attempts. Size must not exceed
	// the limit's capacity, or every chunk is refused (Permanent).
	Limit *workflow.UpstreamLimit

	// LimitKey is the Limit bucket the chunks draw from; empty means the
	// name given to EnqueueChunked.
	LimitKey string

	// Snooze makes a spent Limit snooze the step (workflow.Snooze, which
	// spends no attempt) instead of failing it. Set it only when the
	// step's own driver has workflow.CapSnooze; elsewhere a snooze is an
	// ordinary error.
	Snooze bool

	// Key returns the IdempotencyKey of the job at index (its position in
	// the whole fan-out, from 0). Nil means a key made from the step's
	// run, the name and the index, so a chunk sent again by a retry of
	// the same run is folded into the runs the first send started. A job
	// that already has a key keeps it.
	Key func(index int64, job workflow.Job) string
}

// EnqueueChunked fans a large set of jobs out from inside a step, a chunk
// at a time (NT-05: a notification to every recipient of a list): page
// returns the jobs at offset (at most size of them; fewer ends the
// fan-out), and each chunk is started with EnqueueMany, so a sqlstore
// client writes a chunk in one transaction and Hatchet sends one bulk
// request. It returns how many jobs it enqueued.
//
// It is built on workflow.ProcessChunks, so each finished chunk is
// recorded in the run's journal and a retried step resumes at the first
// chunk that did not finish, reporting progress after every chunk. It
// needs the Journal plugin on the step's client (a Permanent
// workflow.ErrNoJournal otherwise).
//
// Every job gets an IdempotencyKey (Key), and c must have
// workflow.CapIdempotencyKey (a Permanent UnsupportedError otherwise, since
// a retry would then start a chunk's jobs twice). The key covers the one
// window the journal cannot: a crash after a chunk was enqueued and
// before the journal recorded it sends that chunk again on the retry, and
// the key folds each job into its run from the first send while that run
// is unfinished. A run that already finished in that window does not hold
// its key any more, so its recipient would get a second message: keep the
// job's own handler idempotent (or long-lived dedup such as
// middleware.Unique) when that matters.
//
// It runs on every driver: the step on any driver with the Journal
// plugin, the jobs on any c with CapIdempotencyKey (memory, sqlstore,
// Redis, Hatchet).
func EnqueueChunked(ctx context.Context, c workflow.Client, name string, opts ChunkedOptions, page func(ctx context.Context, offset, size int64) ([]workflow.Job, error)) (int64, error) {
	if c == nil || page == nil || name == "" {
		return 0, workflow.Permanent(fmt.Errorf("%w: batch: EnqueueChunked needs a client, a name and a page func", workflow.ErrInvalid))
	}
	if missing := c.Capabilities().Missing(workflow.CapIdempotencyKey); !missing.IsZero() {
		return 0, workflow.Permanent(&workflow.UnsupportedError{Field: "EnqueueChunked", Missing: missing,
			Reason: "each job needs an idempotency key so a resumed chunk does not start its jobs twice"})
	}
	key := opts.Key
	if key == nil {
		info, ok := workflow.StepInfoFrom(ctx)
		if !ok || info.RunID.IsZero() {
			return 0, workflow.Permanent(fmt.Errorf("%w: batch: EnqueueChunked runs inside a step (no StepInfo on ctx); pass ChunkedOptions.Key to call it elsewhere", workflow.ErrInvalid))
		}
		key = chunkKey(info.RunID, name)
	}
	limitKey := opts.LimitKey
	if limitKey == "" {
		limitKey = name
	}
	return workflow.ProcessChunks(ctx, name, opts.ChunkOptions, func(ctx context.Context, offset, size int64) (int64, error) {
		jobs, err := page(ctx, offset, size)
		if err != nil {
			return 0, err
		}
		if int64(len(jobs)) > size {
			return 0, workflow.Permanent(fmt.Errorf("%w: batch: EnqueueChunked %q: page returned %d jobs for a chunk of %d", workflow.ErrInvalid, name, len(jobs), size))
		}
		if len(jobs) == 0 {
			return 0, nil
		}
		keyed := make([]workflow.Job, len(jobs))
		for i, job := range jobs {
			if job.IdempotencyKey == "" {
				job.IdempotencyKey = key(offset+int64(i), job)
			}
			keyed[i] = job
		}
		if opts.Limit != nil {
			take := opts.Limit.Take
			if opts.Snooze {
				take = opts.Limit.TakeOrSnooze
			}
			if err := take(ctx, limitKey, float64(len(keyed))); err != nil {
				return 0, err
			}
		}
		if _, err := EnqueueMany(ctx, c, keyed); err != nil {
			return 0, fmt.Errorf("batch: EnqueueChunked %q: chunk at %d: %w", name, offset, err)
		}
		return int64(len(keyed)), nil
	})
}

// chunkKey is the default Key: a digest of the run and the fan-out's name
// (fixed width, whatever their length) and the job's index.
func chunkKey(run workflow.RunID, name string) func(int64, workflow.Job) string {
	sum := sha256.Sum256([]byte(string(run) + "\x00" + name))
	base := "ubgo-chunk/" + hex.EncodeToString(sum[:16]) + "/"
	return func(i int64, _ workflow.Job) string { return base + strconv.FormatInt(i, 10) }
}
