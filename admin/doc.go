// Package admin is the operations side of ubgo/workflow (list failed
// runs, retry, forget, clear a backlog, pause, list schedules, and the data
// a queue dashboard shows): a Go API, a
// JSON HTTP handler that is closed by default, and a threshold Monitor.
// It is its own module, needs nothing beyond the standard library and the
// core, and holds no state: every call goes to the client.
//
// # Operations over any client
//
// New(client) returns an Admin over any workflow.Client. Each operation
// needs a capability and an optional core interface (Requirements):
//
//	runs, run, failures, queues  CapRunQuery       workflow.RunQuerier
//	discarded                    CapRunQuery and CapRunStates  workflow.RunQuerier
//	retry                        CapRetryRun       workflow.RunRetrier
//	delete                       CapDeleteRun      workflow.RunDeleter
//	cancel                       CapCancel         Client.Cancel
//	cancel_all                   CapCancelAll      workflow.BulkCanceller
//	pause                        CapPauseWorkflow  workflow.Pauser (CapPauseQueue for a queue target)
//	schedules                    CapScheduleList   workflow.ScheduleLister
//	schedule_pause               CapScheduleList and CapSchedulePause  workflow.ScheduleLister and workflow.SchedulePauser
//	schedule_trigger             CapScheduleList and CapScheduleTrigger  workflow.ScheduleLister and workflow.ScheduleTriggerer
//	force                        CapForceRun       workflow.RunForcer
//	retry_all                    CapRunQuery and CapRetryRun   workflow.RunQuerier and workflow.RunRetrier
//	delete_all                   CapRunQuery and CapDeleteRun  workflow.RunQuerier and workflow.RunDeleter
//	nodes                        none              workflow.NodeLister
//	health                       none              workflow.CheckHealth (every client)
//	reschedule                   none              workflow.RunRescheduler
//	cancel_step                  none              workflow.StepCanceller
//
// Support reports which ones the client has and what it lacks, so a
// screen hides what cannot work; an unsupported call returns a
// *workflow.UnsupportedError naming "admin.<operation>" and the missing
// capabilities. QueueStats pages RunQuerier over queued runs to count each
// queue's depth and find its oldest run, reading at most WithMaxScan runs
// (DefaultMaxScan) and saying Truncated when there were more. On a client
// that implements workflow.WorkerScaler (CapWorkerScale) it adds each
// queue's worker count and running attempts on that client
// (QueueReport.Workers), the state a workflow.BalanceLoop acts on.
// Failures lists failed runs and Discarded, beside it, the runs a step
// dropped with workflow.Discard; a driver without CapRunStates reports a
// discarded run as failed or cancelled, so Discarded is refused there.
// CountRuns counts the runs a query matches, within the same max scan,
// and returns the cursor that starts each page, so a screen can show
// "1–50 of N" and numbered pages over a driver's opaque cursors. Runs,
// Failures, Discarded and CountRuns search by label: RunQuery.Labels
// (every key=value pair must match) and RunQuery.LabelPrefixes (a key's
// value starts with a prefix; the HTTP filter label=key=prefix*), which
// is refused with an UnsupportedError on a client without CapLabelPrefix.
//
// RetryAll and DeleteAll act on every run a filter matches: they list the
// matches first (at most WithMaxBulk, DefaultMaxBulk, per call; Truncated
// says to call again) and then retry or delete each, counting runs that
// changed in between as Skipped and stopping on any other error. A
// BulkOptions.DryRun lists and changes nothing, returning the count and a
// sample of refs; PreviewCancelAll is the same for CancelAll. DeleteAll
// refuses an empty filter, so forgetting every finished run is said
// explicitly with its statuses.
//
// Metrics reads run-end history for the charts from a MetricsSource wired
// with WithMetrics: counts per step by terminal status, zero-filled.
// MemoryMetrics is the in-process default, fed by registering its Observe
// with the run-finished hook; it keeps one-minute buckets for a day,
// drops repeated deliveries, and starts empty on a restart (Since says
// when its data begins). An app with a metrics store adapts it with
// MetricsSourceFunc.
//
// Nodes lists the online nodes (any client implementing
// workflow.NodeLister) and Health returns the
// client's health report, which an admin screen shows; a probe should
// call workflow.CheckHealth on the app's own unauthenticated route.
// Nodes, Reschedule (move a delayed run) and CancelStep (withdraw one
// step) have no capability bit: whether they are offered is
// workflow.Supports, which a wrapper (workflow.Forward, failover) answers
// for the clients it wraps, and a client without them is refused at the
// call.
//
// Schedule returns one schedule with its overlap policy and, on a client
// with CapScheduleHistory, its last fire and the run it started
// (ScheduleInfo.Last). TriggerSchedule runs a schedule now with its
// payload and labels; the overlap policy still applies, so with
// OverlapSkip and the previous run unfinished it returns an error
// matching workflow.ErrOverlap and enqueues nothing. ForceComplete and
// ForceFail end a run from outside (workflow.RunForcer); ForceFail's
// reason is bounded by MaxForceReason. A run's per-attempt errors
// (RunInfo.Errors, CapErrorHistory) come with it. Captured step output
// is read through an OutputSource the app wires with WithOutputs; the
// library keeps no output itself, and MemoryOutputs is an in-process,
// bounded store that is both the middleware.CaptureOutput sink and the
// source.
//
// # The HTTP handler is closed by default
//
// NewHandler(admin, Auth[P]{Identify, Authorize}) serves the JSON API. P is
// the app's own principal type. With either function nil, every request,
// on every path, is refused with 403. Otherwise each request is
// identified first (401 before routing on any error or panic), then
// parsed, then authorized with a Request[P] saying who, which Action and
// which Resource (the run, the parsed filter or the pause target); any
// error or panic from Authorize refuses it with 403, so a policy that
// fails, fails closed. Writes need Content-Type: application/json, which
// a cross-site form cannot send. Payloads are left out of responses unless
// WithPayloads is given, and an error without a known core sentinel is
// logged (WithLogger) and answered with a bare 500, so a database error
// never reaches the browser. Run lists are paginated with the driver's
// opaque cursor (count=1 adds the total and every page's cursor);
// schedules with an offset cursor. Attempt errors are sent with each run;
// their stack traces only with WithStacks. Force-fail sends its reason
// to Authorize in Resource.Reason, so a policy can require one or log
// it. Bulk retry, delete and cancel send the dry-run flag to Authorize
// in Resource.DryRun, so a policy can let more people preview than act.
// A run's tenant, due time, progress, nodes and result are sent with it;
// its progress data and its result only with WithPayloads, since both
// are app data like a payload.
//
// # Web UI
//
// The same handler serves a web UI at ui/ under its mount point (GET /
// and GET /ui redirect there): queues with depth, oldest wait and worker
// counts, and pause and resume of queues, workflows and everything; runs
// filtered by status, workflow, queue and label, with numbered pages and
// a page size; one run with its attempts, error and, only with
// WithPayloads, its payload; its timing (queue wait, duration), the
// error of each failed attempt (stacks with WithStacks), its captured
// output per attempt when WithOutputs is wired, and force complete and
// force fail; failed and discarded runs with retry and delete; schedules
// with their last fire and last run, run now, pause and resume, and a
// page per schedule with its recent runs; "all matching" retry, delete
// and cancel on a run list, each showing the server's dry-run count
// before the confirming click; a run's progress, tenant, due time,
// nodes and result, with reschedule for a delayed run and cancel step
// for a running one; charts of run ends per step by status over one,
// six or 24 hours, with totals and a table view, when WithMetrics is
// wired; and a system page with the health report and the online nodes.
// It is three files embedded in
// the package, written for it, with no third-party code and nothing
// fetched at run time. They are served only after Identify and Authorize
// (ActionUI), like every route, and each call the page makes is a request
// to the JSON API, authorized on its own action, so a read-only role sees
// the screens and is refused every button. Writes go through the API's
// JSON-only rule. A browser sends no Authorization header when it opens a
// page, so Identify must accept the app's session cookie for the UI to
// load. Pages carry a Content-Security-Policy that allows only those files
// and same-origin requests, and forbids framing. Buttons for an operation
// the client does not support are hidden (the capabilities route).
//
// # Alerts
//
// NewMonitor polls QueueStats on the Admin's clock (WithClock) and calls
// an Alerter when a queue's depth or oldest wait goes over a Threshold,
// once on crossing and once when it resolves (WithRepeat re-sends while it
// stays over). Nothing runs until Monitor.Run; Check polls once. Which
// alerts are open is kept in memory, so run one Monitor per deployment.
//
// The OpenTelemetry queue-depth gauge lives in the otel module;
// Admin.QueueDepths has the otel.QueueDepthFunc signature, so neither
// module imports the other.
package admin
