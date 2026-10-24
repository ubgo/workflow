package workflow

import (
	"context"
	"os"
	"strconv"
	"time"
)

// DefaultNodeID is the node identity a driver uses when the app gives
// none: the host name and the process ID ("web-1:4182"), so two processes
// on one host are told apart. An app on a platform whose host names are
// meaningless (containers) passes its own through the driver's node-ID
// option.
func DefaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}

// NodeMode is what a node does.
type NodeMode string

// The node modes.
const (
	// NodeFull runs workers and the scheduler (the default).
	NodeFull NodeMode = "full"
	// NodeWorker runs workers only; another node fires schedules.
	NodeWorker NodeMode = "worker"
	// NodeScheduler fires schedules only, enqueueing the runs for worker
	// nodes (a separate scheduler process).
	NodeScheduler NodeMode = "scheduler"
	// NodeEnqueueOnly enqueues and answers queries, running nothing
	// (CapEnqueueOnly).
	NodeEnqueueOnly NodeMode = "enqueue-only"
)

// NodeInfo is one node as NodeLister reports it.
type NodeInfo struct {
	// ID is the node's identity (StepInfo.Node).
	ID string

	// Mode is what the node does.
	Mode NodeMode

	// Queues are the queues its workers serve; empty means every queue
	// (or none for NodeScheduler and NodeEnqueueOnly).
	Queues []string

	// Workers is its worker pool size.
	Workers int

	// Running is how many step attempts it was running at LastSeen.
	Running int

	// StartedAt is when its client started; LastSeen when it last
	// reported in, on the driver's clock.
	StartedAt, LastSeen time.Time
}

// NodeLister lists the nodes that are online, for an operator's view. It
// answers for itself: a client may implement it without CapNodeIdentity
// (an engine that lists its workers but keeps no per-attempt node), and
// callers check the interface, not the capability. Every client that
// declares CapNodeIdentity implements it. A node is
// online while it keeps reporting in; one that stops (a crash) drops out
// after the driver's expiry, which the driver documents.
type NodeLister interface {
	Nodes(ctx context.Context) ([]NodeInfo, error)
}

// HealthCheck is one check of a HealthReport.
type HealthCheck struct {
	// Name says what was checked ("database", "engine", "workers").
	Name string
	// OK is the check's verdict.
	OK bool
	// Detail says why it failed, or a short fact when it passed.
	Detail string
}

// HealthReport is a client's health: Live means the process should keep
// running (restart it when false); Ready means it can take work now
// (route traffic to it, or not, when false). A client is live but not
// ready before Start and while its backend is unreachable.
type HealthReport struct {
	Live, Ready bool
	Checks      []HealthCheck
}

// HealthChecker is an optional interface a client implements to report
// its health, for a liveness or readiness probe. Health must answer
// within ctx's deadline and must not block on a broken backend longer
// than that: a probe that hangs is worse than one that fails.
type HealthChecker interface {
	Health(ctx context.Context) HealthReport
}

// CheckHealth asks c for its health. A client without HealthChecker is
// reported live and ready, with one check saying it cannot tell: an app
// on such a driver needs its own probe.
func CheckHealth(ctx context.Context, c Client) HealthReport {
	if h, ok := c.(HealthChecker); ok {
		return h.Health(ctx)
	}
	return HealthReport{Live: true, Ready: true, Checks: []HealthCheck{{Name: "health", OK: true, Detail: "the driver reports no health"}}}
}
