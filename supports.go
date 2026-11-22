package workflow

// Op names an optional operation that carries no capability bit, so a
// caller can ask whether a client really does it before offering it (an
// admin button), instead of finding out from the call's refusal.
type Op string

// The operations Supports answers for.
const (
	OpReschedule Op = "reschedule" // RunRescheduler
	//ds:def id=opeditrun-9u2hhcey owner=@khanakia stability=stable
	OpEditRun        Op = "edit-run"        // RunEditor
	OpResetRun       Op = "reset-run"       // RunResetter
	OpResetSucceeded Op = "reset-succeeded" // RunResetterWith (ResetOptions.Succeeded)
	OpForkRun        Op = "fork-run"        // RunForker
	//ds:def id=opsignalwaiting-z676jv7w owner=@khanakia stability=stable
	OpSignalWaiting Op = "signal-waiting" // TransientSignaller
	//ds:def id=opcancelstep-x8n8q838 owner=@khanakia stability=stable
	OpCancelStep Op = "cancel-step" // StepCanceller
	OpNodes      Op = "nodes"       // NodeLister
	//ds:def id=oplifecycle-mcqawhzd owner=@khanakia stability=stable
	OpLifecycle Op = "lifecycle" // LifecycleNotifier
	//ds:def id=oprunsignals-tmgz66m8 owner=@khanakia stability=stable
	OpRunSignals Op = "run-signals" // RunSignalReader
)

// OpProber is implemented by a client wrapper whose optional methods
// exist whether or not what it wraps can do them (Forward, failover): it
// answers for what is underneath.
//
//ds:def id=opprober-g847tnv9 owner=@khanakia stability=stable
type OpProber interface {
	Supports(op Op) bool
}

// Supports reports whether c really does op: its OpProber's answer when
// it has one (a wrapper), else whether c implements the operation's
// interface. An unknown op is false.
//
//ds:def id=supports-4xyjjwvz owner=@khanakia stability=stable
func Supports(c Client, op Op) bool {
	if p, ok := c.(OpProber); ok {
		return p.Supports(op)
	}
	switch op {
	case OpReschedule:
		_, ok := c.(RunRescheduler)
		return ok
	case OpCancelStep:
		_, ok := c.(StepCanceller)
		return ok
	case OpEditRun:
		_, ok := c.(RunEditor)
		return ok
	case OpResetRun:
		_, ok := c.(RunResetter)
		return ok
	case OpResetSucceeded:
		_, ok := c.(RunResetterWith)
		return ok
	case OpForkRun:
		_, ok := c.(RunForker)
		return ok
	case OpSignalWaiting:
		_, ok := c.(TransientSignaller)
		return ok
	case OpNodes:
		_, ok := c.(NodeLister)
		return ok
	case OpLifecycle:
		_, ok := c.(LifecycleNotifier)
		return ok
	case OpRunSignals:
		_, ok := c.(RunSignalReader)
		return ok
	}
	return false
}

// Supports answers for Inner (see OpProber), so a wrapper embedding
// Forward reports only what the wrapped client really does.
func (f Forward) Supports(op Op) bool { return Supports(f.Inner, op) }
