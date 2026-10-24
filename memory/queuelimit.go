package memory

import "github.com/ubgo/workflow"

// queueFull reports whether queue q, with running attempts executing, has
// no room for another: it has hit its per-client Workers cap or its
// QueueConfig.Limit. This driver is one process, so the process is the
// whole cluster and Limit is a plain cap here (CapQueueLimitGlobal holds
// trivially). The slot is the queueExec count, released when the attempt
// returns, so a finished or panicked attempt never leaks one.
func queueFull(q workflow.QueueConfig, running int) bool {
	return (q.Workers > 0 && running >= q.Workers) || (q.Limit > 0 && running >= q.Limit)
}
