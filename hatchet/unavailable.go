package hatchet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ubgo/workflow"
)

// errOutageStatuses are the SDK's texts for the HTTP answers of a gateway
// or engine that is down or overloaded, as opposed to one refusing the
// request: the feature clients report every non-200 only as "got status
// N" (see errNotFoundStatus), so the text is the only signal.
var errOutageStatuses = []string{"got status 502", "got status 503", "got status 504"}

// engineErr wraps an error from an engine call as "hatchet driver: op:
// err" and marks it workflow.Unavailable when the engine could not be
// reached (isUnavailable), so a failover client or a caller can tell an
// outage from a refusal without parsing this driver's messages.
func engineErr(err error, format string, args ...any) error {
	wrapped := fmt.Errorf("hatchet driver: "+format+": %w", append(args, err)...)
	if isUnavailable(err) {
		return workflow.Unavailable(wrapped)
	}
	return wrapped
}

// isUnavailable reports whether err means the engine could not be reached
// or did not answer: a gRPC Unavailable status, a network error from the
// REST client (refused connection, DNS failure, transport timeout), or a
// 502, 503 or 504. The caller's own cancellation or deadline is never an
// outage: the engine was not given the time to answer, and treating it as
// one would make a failover client retry a request its caller abandoned.
func isUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := err.Error()
	for _, s := range errOutageStatuses {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
