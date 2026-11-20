// Package schedexport writes a workflow.Schedule as files another
// scheduler runs: a Kubernetes batch/v1 CronJob manifest (CronJob), a
// systemd .timer and .service pair (SystemdTimer), a launchd property list
// (Launchd) and a Windows Task Scheduler task (TaskScheduler). Each OS
// format also has a NextFire form, a one-shot file for the schedule's next
// fire time on an injected clock: the handoff a desktop app makes before
// it quits, so the OS starts it again when the next tick is due.
//
// The export covers when a schedule fires (Cron, Location, Seconds, the
// misfire and overlap policies and, where a format has them, Active and a
// year field). What runs is Target.Command, usually the app's own binary
// with arguments naming the schedule; Workflow, Input, Labels, Tenant and
// KeepRuns describe the run the app enqueues and are the command's to
// carry, so they are not written into the files.
//
// Every field is mapped honestly: a schedule the format cannot express
// exactly (a day rule it reads differently, a misfire policy it has no
// switch for, an overlap policy it cannot honour, a seconds field it has
// no place for) is refused with a *NotExpressibleError matching
// ErrNotExpressible, never written with a part silently dropped. Invalid
// input (no command, a name the format refuses, a control character)
// matches workflow.ErrInvalid.
//
// The package writes bytes and never touches the system: installing the
// files (kubectl apply, systemctl enable --now, launchctl bootstrap,
// Register-ScheduledTask) stays the app's job. It imports only the
// standard library and the core, like the rest of the core module.
package schedexport
