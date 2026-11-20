package workflow

import (
	"errors"
	"fmt"
)

// ErrConstraint matches the error of a step that found its data breaking
// a rule the workflow relies on: an invariant that does not hold
// (Invariant), or a pre- or post-condition that failed
// (middleware.Check). Such an error is permanent: retrying the same input
// cannot make it hold, so the run fails at once and an operator sees
// which rule broke.
var ErrConstraint = errors.New("workflow: constraint violated")

// Invariant returns nil when cond holds, and otherwise a Permanent error
// wrapping ErrConstraint with the message format and args describe
// (fmt.Sprintf rules, %w included), so a step can state what must be true
// of its data and stop the run without spending retries when it is not:
//
//	if err := workflow.Invariant(order.Total >= 0, "order %s has a negative total", order.ID); err != nil {
//		return nil, err
//	}
//
// The error matches ErrConstraint and IsPermanent. It runs on every
// driver: a permanent error is part of the core contract.
func Invariant(cond bool, format string, args ...any) error {
	if cond {
		return nil
	}
	return Permanent(fmt.Errorf("%w: %w", ErrConstraint, fmt.Errorf(format, args...)))
}
