// Package cli is a command line client for the admin JSON API
// (github.com/ubgo/workflow/admin): list and inspect runs, retry, cancel
// and delete them one at a time or by filter, pause and resume, list and
// trigger schedules, watch a queue drain and read run-end history.
//
// It talks HTTP only. It imports nothing beyond the standard library (not
// even the workflow core), so installing it pulls no driver, SDK or
// database code, and it works against an app in any language that serves
// the same API. A test (TestGuard_StdlibOnly) fails if that changes.
//
// # Connecting
//
// The API is closed by default and authenticated by the app, so the CLI
// sends whatever the app's Identify accepts:
//
//	--url URL        where the app mounted admin.NewHandler; env WORKFLOW_ADMIN_URL
//	--token T        sent as "Authorization: Bearer T"; env WORKFLOW_ADMIN_TOKEN
//	--header 'K: V'  any header, repeatable (a session cookie, a tenant header); env WORKFLOW_ADMIN_HEADER holds one
//
// The token is never printed, not even in an error.
//
// # Commands
//
//	caps                         operations the server's driver supports
//	runs|failures|discarded      list runs; --workflow --queue --status --label k=v --tenant --from --to --limit --cursor --count
//	inspect RUN                  one run, its attempt errors and, with --output, captured output
//	retry RUN | retry --all      retry one failed run, or every failed run a filter matches
//	cancel RUN | cancel --all    cancel one run, or every run a filter matches
//	delete RUN | delete --all    forget one finished run, or every finished run a filter matches
//	reschedule RUN --at T        move a delayed run that has not started
//	cancel-step RUN STEP         withdraw one step of a run
//	pause|resume                 --workflow W | --queue Q | --everything
//	pauses                       the active pauses
//	queues                       depth and oldest wait per queue
//	schedules | schedule ID      list schedules, or show one
//	trigger SCHEDULE             run a schedule now
//	drain                        wait until no run is queued or running (--queue, --wait, --interval)
//	metrics                      run ends per step by status (--window, --step, --workflow)
//	health                       live, ready and each check; exit 1 when not ready
//	nodes                        the nodes that are online
//
// Every destructive command takes --dry-run, which reads what it would
// touch and changes nothing; --all on retry, cancel and delete asks the
// server to preview with the same filter. Every command takes --json,
// which prints the API's own JSON answer instead of a table. The exit
// code is 0 on success, 1 when the API refused or failed (or drain timed
// out), and 2 on a usage error.
//
// # Embedding
//
// Run is the whole CLI as a function, so an app can ship it as a
// subcommand of its own binary (with its own default URL and auth), and
// cmd/workflow is the stand-alone binary.
package cli
