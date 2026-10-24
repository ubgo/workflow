// Package middleware is the app-side layer that sits above every driver:
// the conveniences a job queue offers on top of its broker, written once
// for Go. Step middleware (workflow.Middleware) wraps each step execution;
// enqueue middleware (workflow.EnqueueMiddleware) wraps each
// Client.Enqueue, and WrapClient applies it to any driver.
//
// Step middleware: Logging, Recover, Timeout, FailOn, Skip, Release (puts
// the step back for a delay without running it, as workflow.RetryAfter
// wrapping ErrReleased), ThrottleErrors, ReleaseUniqueOnStart (the step
// half of UniqueUntilStarted), DecodeStep (the step half of a Codec) and ClaimCheck.Step (resolves
// claim check references before the handler and stores large outputs).
// Logging picks each event's level through WithLogLevel (by default Debug
// for start and finish, Error for every failed attempt). Encrypt(key,
// oldKeys...) seals with key and also opens values sealed with any old
// key, so a key can be rotated without breaking runs already in flight.
//
// Enqueue middleware: Unique, UniqueUntilFinished (the lock is freed when
// the run ends) and UniqueUntilStarted (freed when a step starts, through
// its step half), Debounce, Route,
// EncodeEnqueue (the enqueue half of a Codec) and ClaimCheck.Enqueue
// (moves a large payload into a BlobStore). WithCodec wraps a whole
// Client so Signal payloads and Schedule inputs are sealed too, and
// ClaimCheck.CleanupOnFinish deletes a run's blobs when the run ends.
//
// Chains: a driver enqueues the jobs of a Job.Chain itself, past enqueue
// middleware, so the codec and the claim check rewrite those jobs when the
// first job is enqueued (each chained job gets its own claim token) and
// wrap the step's chain editor so jobs a step adds with
// workflow.PrependToChain or AppendToChain are rewritten too. The other
// enqueue middleware (Unique, Debounce, Route, OpenTelemetry's) applies to
// the first job only. Deferred dispatch (workflow.DeferEnqueue,
// workflow.DeferHTTP) lives in the core: it is enqueue middleware too.
//
// Schedule hooks: Pinger turns heartbeat URLs into the funcs
// workflow.ScheduleHooks takes (a ping when a tick fires, after a run,
// on success and on failure), sending through a PingFunc (HTTPPing
// by default) under a timeout, and logging a failure without the URL.
//
// Task output: CaptureOutput collects what a step logged through
// OutputLogger, how the attempt ended and, with CaptureResult only, its
// result, and hands it after each attempt to an OutputSink: WriterSink,
// FileSink (FileReplace or FileAppend) or an OutputSinkFunc for mail, chat
// or any other destination. A sink error never fails the step.
//
// Unique by arguments: UniqueByArgs is a key function for Unique and its
// siblings that makes a job unique by its workflow and canonical payload
// (ArgsFields picks the top-level fields that count; ArgsQueue and
// ArgsLabels add the queue and labels).
//
// Failures: Alerts is a run-finished observer that calls an Alerter (the
// app's Slack, mail or pager sink) on every run that ends failed, with a
// pluggable AlertLimiter for dedup and rate limits (MemoryAlertLimiter,
// ten minutes per workflow and step by default); AlertPlugin bundles it
// for workflow.Use. Report hands failures to a workflow.Reporter (an
// error tracker): final failures always, each failed attempt with
// ReportConfig.Attempts. Neither sends the payload. Recover's error
// carries the panic's stack (workflow.StackOf), which drivers with
// workflow.CapErrorHistory keep in RunInfo.Errors.
//
// Nothing here keeps hidden state: every piece of state (a unique store,
// a debounce table, a circuit breaker) hangs off the value its constructor
// returns, and time comes from an injected clock.Clock. State that lives in
// such a value is per process; a guarantee that must hold across processes
// needs a store that is shared, which each constructor takes: UniqueStore
// for Unique, DebounceStore (WithDebounceStore) for Debounce and
// CircuitStore (ThrottleConfig.Store) for ThrottleErrors and AlertLimiter
// (AlertConfig.Limiter) for Alerts, each with an in-process default
// (MemoryUnique, MemoryDebounce, MemoryCircuit, MemoryAlertLimiter).
//
// UniqueWith adds two choices to Unique: which states of the holder's run
// count (UniqueConfig.States, looked up through Client.Status, so a key
// can open again once its run started) and answering a duplicate with the
// holder's RunRef (ReturnExisting, marking workflow.EnqueueOutcome) instead
// of ErrDuplicate; both need a store implementing UniqueHolders.
// ArgsTagged builds UniqueByArgs's field list from `unique:"true"` struct
// tags. MaxPayload refuses an oversized payload before the driver sees it,
// and PayloadVersion stamps workflow.LabelPayloadVersion so a step built
// with workflow.DefineVersioned can upcast older payloads. Stuck reports a
// step attempt that runs longer than expected without touching it.
//
// Batches (Then, Catch, Finally) are not middleware: they live in package
// batch, on any Client.
package middleware
