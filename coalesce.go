package workflow

// Coalesce is the uniqueness policy of a coalescing workflow (a cache
// rebuild, a search reindex, a sync): work that arrives while the job runs
// must cause one more run afterwards, never zero and never one per
// arrival. Set on Workflow.Coalesce, it changes what an enqueue does whose
// idempotency key (Job.IdempotencyKey, or a schedule's key) is already held:
//
//   - no run holds the key: a run starts as usual and holds it;
//   - the holder is still in the queue (scheduled or not yet admitted):
//     the enqueue folds into it and returns its RunRef, as without
//     Coalesce;
//   - the holder has left the queue (admitted: running, waiting, retrying)
//     and no run is pending behind it: a new pending run is stored and its
//     RunRef returned; it reports StatusScheduled and starts only once the
//     holder ends, whatever the holder's end (succeeded, failed, cancelled,
//     discarded), and then holds the key itself;
//   - a pending run already waits: the enqueue folds into that one and
//     returns its RunRef.
//
// So a key has at most one run executing and one waiting. Folding marks
// the EnqueueOutcome a duplicate; storing a run marks it inserted. A
// job's RunAt on the pending run is kept: it starts at the later of RunAt
// and the holder's end. Editing the pending run's RunAt (RunEditor,
// RunRescheduler) can start it before the holder ends; leave it to the
// driver. Needs CapCoalesce.
type Coalesce struct {
	// Payload says which payload a run that enqueues fold into keeps.
	// Empty means CoalesceKeepFirst.
	Payload CoalescePayload
}

// CoalescePayload says which payload a coalesced run keeps when later
// enqueues fold into it.
type CoalescePayload string

// The payload choices of Coalesce.
const (
	// CoalesceKeepFirst keeps the payload of the enqueue that stored the
	// run; later payloads are dropped. It suits work whose payload only
	// names what to rebuild. It is the default.
	CoalesceKeepFirst CoalescePayload = "keep-first"
	// CoalesceKeepLast replaces the run's payload, and its priority, with
	// each later enqueue's while the run is still in the queue, so the run
	// that starts carries the newest request. Once the run has left the
	// queue its payload is fixed, and a later enqueue only folds.
	CoalesceKeepLast CoalescePayload = "keep-last"
)

// Valid reports whether p is a known choice or empty.
func (p CoalescePayload) Valid() bool {
	return p == "" || p == CoalesceKeepFirst || p == CoalesceKeepLast
}

// KeepsLast reports whether c replaces the payload of the run enqueues fold
// into. A nil c keeps nothing.
func (c *Coalesce) KeepsLast() bool { return c != nil && c.Payload == CoalesceKeepLast }
