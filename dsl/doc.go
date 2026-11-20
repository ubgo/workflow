// Package dsl reads workflow definitions written as data (JSON) and turns
// them into workflow.Workflow values at run time, so the shape of a
// workflow (its steps, their order, waits, sleeps, retries, timeouts,
// concurrency, cron) can change without a new build of the program.
//
// The data names code; it never contains code. Every step that runs
// something names a handler, and the program registers each handler under
// its name in a Registry before it builds definitions. The Registry is the
// security boundary: a definition can only call handlers the program
// registered, so whoever writes definitions (an operator, an admin form, a
// database row) can rearrange and retune the program's steps but never
// run code the program did not ship.
//
// # Use
//
//	reg := dsl.NewRegistry()
//	_ = reg.Register("reserve", reserve) // a workflow.Handler
//	_ = reg.Register("ship", ship)
//	wf, err := dsl.Load(data, reg) // Parse then Build
//	if err != nil { ... }           // wraps workflow.ErrInvalid
//	err = client.Register(wf)       // like any workflow written in Go
//
// Parse checks a document's shape alone (no Registry), Build resolves the
// names and runs workflow.Validate, and Marshal writes a Definition back,
// so an admin tool can read, edit and store definitions. Every refusal
// wraps workflow.ErrInvalid; one tied to a place in the document is a
// *FieldError whose Path reads like the document ("steps[2].handler").
//
// # Where definitions live
//
// A definition is plain bytes: a file in the repository, a row in a
// database, an entry in a workflow.DataStore, an object in a bucket. A
// program loads its definitions at start-up and registers them on the
// client like any workflow. A deploy-less reload is the same call on new
// bytes: build a fresh client (or restart workers) with the new
// workflows. Runs already in flight follow the rules of Workflow.Version:
// bump "version" when a change would break them.
//
// # YAML
//
// YAML is a superset of JSON. The package reads JSON only, so the core
// module keeps to the standard library; an app that keeps definitions in
// YAML converts them to JSON with the YAML library it already uses and
// passes the result to Parse or Load. The field names are the same in
// both.
//
// # What has no data form
//
// Fields that are Go functions or expressions (Workflow.ScheduleWhen,
// RetryPolicy.NoRetryOn and DelayFor, KeyExpr, OnTimeout) and rate limits
// have no field in a Definition. A per-run key comes from a job label
// instead ("key_label" on a wait or on concurrency). A workflow that needs
// more is written in Go, or built from a Definition and then edited in Go
// before Register.
//
// # State machines
//
// MachineDefinition is a workflow.DefineMachine statechart as data:
// ParseMachine, BuildMachine (guards and actions by name from a
// MachineRegistry[D], typed on the app's data type), LoadMachine and
// MarshalMachine, with MachineEnv carrying the store, the timer client and
// the clock.
//
// Nothing in the package is global: no package-level registry, no cache,
// no init side effects.
package dsl
