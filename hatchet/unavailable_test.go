package hatchet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ubgo/workflow"
)

// TestEngineErrUnavailable pins which engine errors are marked
// workflow.ErrUnavailable: an unreachable engine (gRPC Unavailable, a
// refused or timed-out connection, a 502/503/504), and never a refusal
// (another gRPC code, a 404, a bad request) or the caller's own
// cancellation or deadline.
func TestEngineErrUnavailable(t *testing.T) {
	refused := &url.Error{Op: "Get", URL: "http://engine", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"grpc unavailable":      {status.Error(codes.Unavailable, "connection error"), true},
		"wrapped grpc":          {fmt.Errorf("trigger: %w", status.Error(codes.Unavailable, "x")), true},
		"connection refused":    {refused, true},
		"503":                   {errors.New("could not list runs: got status 503"), true},
		"504":                   {errors.New("got status 504"), true},
		"grpc invalid argument": {status.Error(codes.InvalidArgument, "bad"), false},
		"404":                   {errors.New("got status 404"), false},
		"plain error":           {errors.New("boom"), false},
		"caller cancelled":      {&url.Error{Op: "Get", URL: "http://engine", Err: context.Canceled}, false},
		"caller deadline":       {fmt.Errorf("x: %w", context.DeadlineExceeded), false},
	} {
		err := engineErr(tc.err, "op %s", "a")
		if got := workflow.IsUnavailable(err); got != tc.want {
			t.Errorf("%s: IsUnavailable = %v, want %v (err %v)", name, got, tc.want, err)
		}
		if !errors.Is(err, tc.err) {
			t.Errorf("%s: the engine error is not reachable through %v", name, err)
		}
		if want := "hatchet driver: op a: " + tc.err.Error(); err.Error() != want {
			t.Errorf("%s: message %q, want %q", name, err.Error(), want)
		}
	}
}
