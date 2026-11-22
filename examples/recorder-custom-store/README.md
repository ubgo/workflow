# recorder-custom-store

A `recorder.Store` of the app's own, and the recorder options around it. `store.go` is `docStore`: one document per run under the run's key and an index from a delayed run's `ScheduledID`, the shape of a store over a document database. Each write loads the document, applies the recorder's shared merge rule (`recorder.ApplyRunWrite`, `ApplyStepStart` and the rest) and saves it, so it merges exactly as the bundled stores do. `main_test.go` holds it to the store contract with `recordertest.RunStore` and runs the recorder's driver cases over it with `recordertest.RunDriver(..., recordertest.WithStore(...))`.

`main.go` then records a checkout with a `Redactor` of its own (it reads `PayloadInfo` to drop one step's output and mask an email in an event attribute), chained after `JSONPaths` for that workflow only with `PerWorkflow` and `Chain`. It takes the store down for maintenance: every history write of the next run fails, the error hook (`WithOnError`, a `recorder.ErrorFunc`) hears of each by its `WriteOp`, and the run still succeeds. Once the store is back, one `Reconciler` pass, tuned with its options, records the run the failed writes missed.

```sh
cd examples
go run ./recorder-custom-store
go test ./recorder-custom-store
```

```text
input as stored: {"card":{"number":"[REDACTED]"},"order":"order-1"}
  step charge output=none (51 bytes before redaction)
  step notify output={"sent":true} (13 bytes before redaction)
    event "receipt sent" [{email a***@example.com}]
-- the history store goes into maintenance
history writes that failed: add-event end-step finish-run start-step upsert-run
run status from the driver: succeeded; in the history: true
-- maintenance over: one Reconciler pass
reconcile: checked=0 finished=0 backfilled=2 errors=0
second run in the history: succeeded source=api/order-2 steps=0
```

`backfilled=2`: the backfill lists every run the client enqueued within the window and upserts it. The first run was already recorded and is left as it was; the second gets a record with its source from its labels, its status and its end. Its step detail was never written, so it has no steps.

## The code

<!-- snippet: examples/recorder-custom-store/main.go#redactor -->
```go
// receipts is a Redactor of the app's own. It reads PayloadInfo to treat
// one step differently: the charge step's output is the payment provider's
// raw response, so none of it is stored (nil keeps the size only), and an
// email in any event attribute is masked down to its domain. It must not
// change the slice it is given, and it runs on the step's goroutine, so it
// stays quick.
type receipts struct{}

// RedactPayload implements recorder.Redactor: it drops the charge step's
// output and keeps every other body as it is.
func (receipts) RedactPayload(info recorder.PayloadInfo, body []byte) []byte {
	if info.Kind == recorder.PayloadStepOutput && info.Step == "charge" {
		return nil
	}
	return body
}

// RedactAttr implements recorder.Redactor: it masks an email attribute
// down to its first letter and domain.
func (receipts) RedactAttr(_ recorder.PayloadInfo, a workflow.EventAttr) workflow.EventAttr {
	if at := strings.IndexByte(a.Value, '@'); a.Key == "email" && at > 0 {
		a.Value = a.Value[:1] + "***" + a.Value[at:]
	}
	return a
}
```

<!-- snippet: examples/recorder-custom-store/main.go#recorder -->
```go
cards, err := recorder.JSONPaths("card.number")
if err != nil {
	return err
}
// checkout gets both redactors, in order; every other workflow the
// card paths only.
redact := recorder.PerWorkflow(map[string]recorder.Redactor{
	"checkout": recorder.Chain(cards, receipts{}),
}, cards)
// onError is the error hook, a recorder.ErrorFunc: told about each
// failed history write while the run goes on, on the goroutine that
// made the write, so it only counts.
var onError recorder.ErrorFunc = func(_ context.Context, op recorder.WriteOp, ref workflow.RunRef, err error) {
	failed.add(op) // in an app: a counter by op, and a log line with ref and err
}
rec, err := recorder.New(db,
	recorder.WithRedactor(redact),
	recorder.WithMaxPayload(maxBody),        // larger bodies are kept by size only
	recorder.WithStepBodies(true),           // the default; false keeps only the run's input and result
	recorder.WithErrorHistory(errorHistory), // attempt errors kept per run
	recorder.WithClock(clk),                 // the driver's clock, so records and driver agree
	recorder.WithOnError(onError),           // every failed write, by its WriteOp
)
if err != nil {
	return err
}
```

<!-- snippet: examples/recorder-custom-store/store.go#store -->
```go
// docStore is a recorder.Store of the app's own, shaped like a store over a
// document database: one document per run (the run with its steps and their
// events) under the run's key, and a secondary index from a delayed run's
// ScheduledID to that key. Every write is the same three moves in one
// transaction: load the document, apply the recorder's merge rule for the
// write (the recorder.Apply* functions), save it. The rules are shared, so
// this store merges a write exactly as the bundled stores do, and
// recordertest.RunStore holds it to their contract (see main_test.go).
//
// Here the "database" is a map behind a mutex; over a real one, update is
// a transaction that reads the document, runs fn and writes it back.
type docStore struct {
	mu      sync.Mutex
	docs    map[string]recorder.RunRecord
	bySched map[workflow.ScheduledID]string
}

var _ recorder.Store = (*docStore)(nil)

// newDocStore returns an empty store.
func newDocStore() *docStore {
	return &docStore{docs: map[string]recorder.RunRecord{}, bySched: map[workflow.ScheduledID]string{}}
}

// update is one write: it loads the run's document (joining a record kept
// under the ScheduledID into the one under the RunID when ref carries
// both, as recorder.MergeRecords says), hands it to fn, and saves it under
// its key, moving it off a ScheduledID-only key once it has a RunID.
func (s *docStore) update(ref workflow.RunRef, fn func(r *recorder.RunRecord, isNew bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var byRun, bySched *recorder.RunRecord
	if !ref.RunID.IsZero() {
		if d, ok := s.docs[recorder.KeyOf(workflow.RunRef{RunID: ref.RunID})]; ok {
			byRun = &d
		}
	}
	if k, ok := s.bySched[ref.ScheduledID]; ok && !ref.ScheduledID.IsZero() {
		if d, ok := s.docs[k]; ok {
			bySched = &d
		}
	}
	var r recorder.RunRecord
	isNew := false
	switch {
	case byRun != nil && bySched != nil && byRun.Key() != bySched.Key():
		delete(s.docs, bySched.Key())
		r = recorder.MergeRecords(*byRun, *bySched)
	case byRun != nil:
		r = *byRun
	case bySched != nil:
		r = *bySched
	default:
		r, isNew = recorder.NewRecord(ref), true
	}
	fn(&r, isNew)
	key := r.Key()
	if !r.Ref.ScheduledID.IsZero() {
		if old := recorder.KeyOf(workflow.RunRef{ScheduledID: r.Ref.ScheduledID}); old != key {
			delete(s.docs, old)
		}
		s.bySched[r.Ref.ScheduledID] = key
	}
	s.docs[key] = r.Clone() // the document owns its data, never the caller's slices
}
```

<!-- snippet: examples/recorder-custom-store/main_test.go#contract-store -->
```go
// TestStoreContract holds docStore to the recorder.Store contract the
// bundled stores pass: fill-only upserts, attempts, events, outcome,
// finish and re-open, the join of a delayed run's two IDs, every filter,
// paging, Pending, copies on read, concurrent writes and the refusals.
// docStore is no recorder.Pruner, so the retention cases skip.
func TestStoreContract(t *testing.T) {
	recordertest.RunStore(t, func(t *testing.T, _ recordertest.StoreConfig) recorder.Store {
		return newDocStore()
	})
}
```

## Guides

- [Record run history](../../docs/observability/run-recorder.md): [redaction](../../docs/observability/run-recorder.md#redaction-and-payload-bounds), [failed writes](../../docs/observability/run-recorder.md#be-told-about-a-failed-write), [the Reconciler](../../docs/observability/run-recorder.md#close-the-gaps-with-the-reconciler), [your own store](../../docs/observability/run-recorder.md#bring-your-own-store)
