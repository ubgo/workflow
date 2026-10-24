package workflow

import "context"

// Heartbeater receives a running attempt's heartbeats. A driver with
// CapHeartbeat sets one on the step's ctx (WithHeartbeater) before the
// Middleware chain runs: it resets the attempt's HeartbeatTimeout clock
// and, on a durable driver, extends the attempt's lease.
type Heartbeater interface {
	// Heartbeat records liveness. details is an optional progress marker
	// the driver may keep (nil is fine). An error means the beat was not
	// recorded (the backend is unreachable, or the attempt has lost its
	// lease and will be run elsewhere); a handler that gets
	// ErrCancelled should stop.
	Heartbeat(ctx context.Context, details []byte) error
}

// HeartbeatFunc adapts a function to Heartbeater.
type HeartbeatFunc func(ctx context.Context, details []byte) error

// Heartbeat calls f.
func (f HeartbeatFunc) Heartbeat(ctx context.Context, details []byte) error { return f(ctx, details) }

type heartbeaterKey struct{}

// WithHeartbeater returns a ctx carrying h. Drivers call it.
func WithHeartbeater(ctx context.Context, h Heartbeater) context.Context {
	return context.WithValue(ctx, heartbeaterKey{}, h)
}

// Heartbeat reports from inside a step that the attempt is alive, so a
// long job is not mistaken for a stuck one (Step.HeartbeatTimeout) and,
// on a durable driver, keeps its lease. It is cheap enough to call in a
// loop. Outside a step, or on a driver without CapHeartbeat, it returns
// nil and does nothing: liveness is a hint, and a portable handler must
// not fail because one driver has no use for it.
func Heartbeat(ctx context.Context, details []byte) error {
	h, ok := ctx.Value(heartbeaterKey{}).(Heartbeater)
	if !ok {
		return nil
	}
	return h.Heartbeat(ctx, details)
}
