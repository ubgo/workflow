// Package recorder keeps a run history that the app owns, separate from
// the driver's own store: one record per run, whatever started it, with its
// trigger source, input, every step attempt with its input, output, error
// and timeline events, the result, the outcome and the timings.
//
// Why it exists: a driver's run history is the driver's store. On an engine
// driver it ends where the engine's retention ends, and no driver keeps a
// step's timeline or says who started a run. A recorder fixes that for
// every driver at once, because it is built only from the seams every
// driver already runs: enqueue middleware (a run is accepted), step
// middleware (an attempt starts and ends, events, outcome) and the
// run-finished hook (a run ends). No driver changes, and an app that never
// imports this module pays nothing.
//
// Wiring it in:
//
//	store := recorder.NewMemoryStore()           // or sqlrecorder.New(db, dialect)
//	rec, err := recorder.New(store)
//	c, err := workflow.Use(memory.New(), rec.Plugin())
//	// register and start through c; then read with store.Runs / store.Run
//
// The pieces:
//
//   - Store is the contract a run store keeps: idempotent upserts keyed by
//     RunRef, reads that answer workflow.RunQuerier plus richer Records and
//     Run. MemoryStore is the in-process one; recorder/sqlrecorder is a SQL
//     one in its own module; an app can bring its own and prove it with
//     recordertest.RunStore. The Apply* functions hold the merge rules, so
//     every store merges a write the same way.
//   - Recorder turns the three seams into Store writes. A failed write never
//     fails a run: it goes to the OnError hook.
//   - Redactor scrubs what is stored (JSONPaths, PerWorkflow, Chain), and
//     WithMaxPayload bounds it; a runs table is read by more people than the
//     queue, so it is where a token in a payload leaks.
//   - Reconciler closes the gaps the seams leave (a worker that died before
//     its step ended, a cancel from an engine UI, runs started from an
//     engine's CLI): it re-checks unfinished records against the client,
//     flags stale ones (never cancelling anything) and backfills runs it
//     never saw with SourceExternal.
//
// Delivery: every write is an upsert, so at-least-once delivery from any
// seam is safe. Run ends are guaranteed only on a driver with
// workflow.CapRunFinished; elsewhere the Reconciler is the fallback.
package recorder
