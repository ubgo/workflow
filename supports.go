package workflow

// Op names an optional operation that carries no capability bit, so a
// caller can ask whether a client really does it before offering it (an
// admin button), instead of finding out from the call's refusal.
type Op string

// The operations Supports answers for.
const (
	OpReschedule Op = "reschedule"  // RunRescheduler
	OpCancelStep Op = "cancel-step" // StepCanceller
	OpNodes      Op = "nodes"       // NodeLister
	OpLifecycle  Op = "lifecycle"   // LifecycleNotifier
)

// OpProber is implemented by a client wrapper whose optional methods
// exist whether or not what it wraps can do them (Forward, failover): it
// answers for what is underneath.
type OpProber interface {
	Supports(op Op) bool
}

// Supports reports whether c really does op: its OpProber's answer when
// it has one (a wrapper), else whether c implements the operation's
// interface. An unknown op is false.
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
	case OpNodes:
		_, ok := c.(NodeLister)
		return ok
	case OpLifecycle:
		_, ok := c.(LifecycleNotifier)
		return ok
	}
	return false
}

// Supports answers for Inner (see OpProber), so a wrapper embedding
// Forward reports only what the wrapped client really does.
func (f Forward) Supports(op Op) bool { return Supports(f.Inner, op) }
