// Package regional joins one workflow client per region into one client
// that sends each job to the region a Router picks, so an app keeps EU
// customers' runs, payloads and results in an EU backend and US ones in a
// US backend while its code talks to one workflow.Client.
//
//	c, err := regional.New(map[string]workflow.Client{"eu": euClient, "us": usClient},
//		regional.ByLabel("region", "eu"))
//	c.Register(wf) // registers on every region
//	c.Start(ctx)   // starts every region
//	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "invoice", Labels: map[string]string{"region": "us"}})
//	st, err := c.Status(ctx, ref) // asks the region holding the run
//
// # Routing
//
// A Router answers one question: which region takes this job. ByLabel
// reads a job label, with an optional fallback region; RouterFunc turns
// a function into a Router for any other rule (by tenant, by a payload
// field, by a lookup in the app's own customer table). A region the
// router names that the client was not built with is refused with an
// error matching ErrUnknownRegion and workflow.ErrInvalid, never sent to
// a default: a typo in a region label must not move a customer's data to
// another region. Enqueue never moves a job to another region when its
// region fails; that would be failover, and it would break the residency
// the router exists for (put a failover client inside one region
// instead). A chained job (Job.Chain) is enqueued by the region whose run
// finished it, so a chain stays in the region of its first job.
//
// # Run references
//
// Every RunRef this client returns, and every RunEvent, Tick, StepInfo
// and LifecycleEvent it passes on, names its region: RunID and
// ScheduledID carry the region name and a colon ("eu:") in front of the
// driver's own ID, which is kept unchanged after it, so Status, Cancel
// and every run operation reach the region holding the run. A ref without
// a region prefix, or naming a region this client does not have, is
// workflow.ErrNotFound: unlike failover, there is no backend to try first,
// and asking every region would let a driver ID that exists in two
// regions act on the wrong run. Region names are 1 to 64 letters, digits,
// ".", "_" or "-", so the first colon always ends the region.
//
// # Capabilities
//
// Capabilities is the intersection of every region's, so Start refuses a
// workflow some region could not run, and the answer does not depend on
// where the router sends a job. CapRunFinished also needs every region to
// implement workflow.RunNotifier. CapClosure and CapWorkerScale are never
// declared: an in-process closure carries no job for the router to read,
// and a worker count belongs to one region's workers (scale each region's
// client directly).
//
// # Schedules
//
// Client.Schedule asks the Router too, with a Job made of the schedule's
// Workflow, Input, Labels and Tenant, so a schedule lives in the region
// its runs would be sent to: ByLabel reads the schedule's label. The
// returned ScheduleID carries the region prefix. A workflow's declared
// Cron has no job to route; it fires in the region WithScheduleRegion
// names, and the other regions' copies carry no Cron, so a tick never
// fires twice. With more than one region and no WithScheduleRegion, a
// workflow with Cron is refused by Register.
//
// # Signals
//
// Signal goes to every region by default, since a run waiting for the
// event may sit in any of them; when one region fails, the others have
// already taken the event and the error names the failing region.
// WithSignalRouter picks the regions an event goes to instead, for an app
// whose event payloads must stay in one region too.
//
// # Managing runs
//
// The client implements the optional run and schedule interfaces, each
// needing its capability, and its interface, in every region, and refused
// with an UnsupportedError otherwise, never answered from some regions
// alone. Runs merges every region's feed newest first with prefixed refs
// and a cursor that records each feed's position; Retry, Delete,
// ForceComplete, ForceFail, Reschedule, CancelStep, Result and
// TriggerSchedule go to the region the ref or ID names; CancelAll, Pause,
// Resume and Drain go to every region (Paused lists the union);
// PauseSchedule goes to the region the ID names, or every region for All.
package regional
