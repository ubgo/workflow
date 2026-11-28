package workflow

import "context"

// TxCompleter completes the step attempt running on ctx as part of a unit
// of work the caller writes in, so the step's success and the app's own
// side effect happen together or not at all: the step that writes an
// invoice row completes in the transaction that writes it, and a crash
// between the two cannot run the step again and write a second invoice.
// Code written against Client finds it with a type assertion, or asks
// first with Supports(c, OpCompleteInTx).
//
// CompleteInTx opens the unit of work, calls fn with a ctx that carries the
// driver's transaction handle (CompletionTx), and, when fn returns nil,
// records the step's success with output in the same unit and commits it.
// When fn returns an error, nothing is recorded, the unit is discarded and
// fn's error is returned; the handler then returns as it likes and its
// return value is recorded as usual. After a successful CompleteInTx the
// step is done: what the handler returns is ignored (no retry, no
// failure), so return promptly.
//
// The completion is fenced like a returned result: an attempt that no
// longer owns its step (its lease was lost, the run ended, it timed out)
// is refused with an error matching ErrAttemptLost and fn's writes are
// discarded with it. A second completion of the same attempt, and a ctx
// that is not a step handler's ctx of this client, are refused with
// ErrInvalid. output is stored as given, past any step middleware that
// would rewrite a returned output.
//
// What the unit of work is depends on the driver:
//
//   - sqlstore: a database transaction on the client's *sql.DB; fn reads
//     it with CompletionTx[*sql.Tx](ctx) and writes the app's rows in it.
//     sqlstore.Client.CompleteTx is the same completion for a transaction
//     the app opened itself.
//   - memory: a function-scoped unit with no handle: fn runs, and the
//     step's success is recorded only when fn returns nil, so a test on
//     the memory driver goes through the same code path as production on
//     sqlstore; CompletionTx reports ok false.
//   - Redis and Hatchet do not implement it: neither shares a transaction
//     with the app's database.
type TxCompleter interface {
	CompleteInTx(ctx context.Context, output []byte, fn func(ctx context.Context) error) error
}

type completionTxKey struct{}

// WithCompletionTx returns a ctx carrying tx, the handle of the unit of
// work CompleteInTx opened. Drivers call it before calling fn.
func WithCompletionTx(ctx context.Context, tx any) context.Context {
	return context.WithValue(ctx, completionTxKey{}, tx)
}

// CompletionTx returns the transaction handle a TxCompleter put on the ctx
// it passes to fn, as T (*sql.Tx on sqlstore). ok is false outside fn, on
// a driver whose unit of work has no handle (memory), and when the handle
// is not a T, so app code can fall back to its non-transactional path:
//
//	if tx, ok := workflow.CompletionTx[*sql.Tx](ctx); ok {
//		_, err = tx.ExecContext(ctx, insertInvoice, id)
//	}
func CompletionTx[T any](ctx context.Context) (tx T, ok bool) {
	tx, ok = ctx.Value(completionTxKey{}).(T)
	return tx, ok
}
