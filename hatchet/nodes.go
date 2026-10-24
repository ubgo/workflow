package hatchet

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.NodeLister     = (*Client)(nil)
	_ workflow.HealthChecker  = (*Client)(nil)
	_ workflow.PayloadLimiter = (*Client)(nil)
)

// Worker labels the driver sets on every engine worker it registers, so
// the engine's worker list and task events map back to a node. They use
// the driver's metadata prefix, which no app label may take.
const (
	workerLabelNode  = metadataPrefix + "node"
	workerLabelQueue = metadataPrefix + "queue"
)

// workerLabels are the engine labels of a worker of node serving queue
// ("" when the client has no Config.Queues and serves every queue).
func workerLabels(node, queue string) map[string]any {
	labels := map[string]any{workerLabelNode: node}
	if queue != "" {
		labels[workerLabelQueue] = queue
	}
	return labels
}

// workerLabel returns the value of label key on w, or "".
func workerLabel(w rest.Worker, key string) string {
	if w.Labels == nil {
		return ""
	}
	for _, l := range *w.Labels {
		if l.Key == key && l.Value != nil {
			return *l.Value
		}
	}
	return ""
}

// Nodes lists the nodes whose workers the engine reports ACTIVE
// (workflow.NodeLister): one NodeInfo per Config.NodeID, merging the
// node's workers (one per served queue). A worker of another app, or of
// a driver version that set no node label, is left out, as are
// enqueue-only clients, which register no worker. A node drops out once
// the engine marks its workers inactive, which it does when their
// heartbeats stop (a crash) or they stop. Workers is the sum of the
// workers' slots and Running the attempts the engine shows on them;
// StartedAt is when the first worker registered and LastSeen its newest
// heartbeat, both on the engine's clock. Mode is NodeFull: a worker
// client also enqueues and schedules, and the engine fires crons itself.
func (c *Client) Nodes(ctx context.Context) ([]workflow.NodeInfo, error) {
	sdk, err := c.ready()
	if err != nil {
		return nil, err
	}
	list, err := sdk.Workers().List(ctx)
	if err != nil {
		return nil, engineErr(err, "list workers")
	}
	if list.Rows == nil {
		return nil, nil
	}
	byNode := map[string]*workflow.NodeInfo{}
	for _, row := range *list.Rows {
		if row.Status == nil || *row.Status != rest.ACTIVE {
			continue
		}
		// The list leaves out labels and slots; the worker read has them.
		w, err := sdk.Workers().Get(ctx, row.Metadata.Id)
		if isNotFound(err) {
			continue // stopped between the list and the read
		}
		if err != nil {
			return nil, engineErr(err, "read worker")
		}
		node := workerLabel(*w, workerLabelNode)
		if node == "" {
			continue
		}
		n := byNode[node]
		if n == nil {
			n = &workflow.NodeInfo{ID: node, Mode: workflow.NodeFull}
			byNode[node] = n
		}
		mergeWorker(n, *w)
	}
	out := make([]workflow.NodeInfo, 0, len(byNode))
	for _, n := range byNode {
		sort.Strings(n.Queues)
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// mergeWorker adds engine worker w to node n.
func mergeWorker(n *workflow.NodeInfo, w rest.Worker) {
	if q := workerLabel(w, workerLabelQueue); q != "" && !slices.Contains(n.Queues, q) {
		n.Queues = append(n.Queues, q)
	}
	if w.SlotConfig != nil {
		for _, sc := range *w.SlotConfig {
			n.Workers += sc.Limit
			if sc.Available != nil {
				n.Running += max(sc.Limit-*sc.Available, 0)
			}
		}
	}
	if created := w.Metadata.CreatedAt; n.StartedAt.IsZero() || created.Before(n.StartedAt) {
		n.StartedAt = created
	}
	if w.LastHeartbeatAt != nil && w.LastHeartbeatAt.After(n.LastSeen) {
		n.LastSeen = *w.LastHeartbeatAt
	}
}

// Health reports the client's health (workflow.HealthChecker). Live is
// false only once a recycle (Config.Recycle) has stopped the client,
// since the process should then be replaced; a requested stop leaves it
// live. Ready needs the client started and not stopped, the engine
// answering (its version call, within ctx), and, for a worker client,
// every worker it registered listed ACTIVE by the engine; an enqueue-only
// client checks the engine alone.
func (c *Client) Health(ctx context.Context) workflow.HealthReport {
	c.mu.Lock()
	started, drained, recycled := c.started, c.drained, c.stopCause != nil
	workerIDs := slices.Clone(c.workerIDs)
	enqueueOnly := c.cfg.EnqueueOnly
	c.mu.Unlock()
	rep := workflow.HealthReport{Live: !recycled}
	state := workflow.HealthCheck{Name: "client", OK: started && !drained, Detail: "started"}
	switch {
	case recycled:
		state.Detail = "recycled; start a fresh process"
	case drained:
		state.Detail = "stopped"
	case !started:
		state.Detail = "not started"
	}
	rep.Checks = append(rep.Checks, state)
	if !state.OK {
		return rep
	}
	sdk, err := c.ready()
	if err != nil {
		rep.Checks = append(rep.Checks, workflow.HealthCheck{Name: "engine", Detail: err.Error()})
		return rep
	}
	engine := c.checkEngineReachable(ctx, sdk)
	rep.Checks = append(rep.Checks, engine)
	rep.Ready = engine.OK
	if !enqueueOnly && engine.OK {
		workers := c.checkWorkers(ctx, sdk, workerIDs)
		rep.Checks = append(rep.Checks, workers)
		rep.Ready = workers.OK
	}
	return rep
}

// checkEngineReachable asks the engine for its version within ctx.
func (c *Client) checkEngineReachable(ctx context.Context, sdk *hatchetsdk.Client) workflow.HealthCheck {
	version, err := sdk.GetEngineVersion(ctx)
	if err != nil {
		return workflow.HealthCheck{Name: "engine", Detail: engineErr(err, "engine version").Error()}
	}
	return workflow.HealthCheck{Name: "engine", OK: true, Detail: "version " + version}
}

// checkWorkers reports whether the engine lists every worker in ids as
// ACTIVE.
func (c *Client) checkWorkers(ctx context.Context, sdk *hatchetsdk.Client, ids []string) workflow.HealthCheck {
	list, err := sdk.Workers().List(ctx)
	if err != nil {
		return workflow.HealthCheck{Name: "workers", Detail: engineErr(err, "list workers").Error()}
	}
	active := map[string]bool{}
	if list.Rows != nil {
		for _, w := range *list.Rows {
			active[w.Metadata.Id] = w.Status != nil && *w.Status == rest.ACTIVE
		}
	}
	for _, id := range ids {
		if !active[id] {
			return workflow.HealthCheck{Name: "workers", Detail: "worker " + id + " is not active on the engine"}
		}
	}
	return workflow.HealthCheck{Name: "workers", OK: true, Detail: fmt.Sprintf("%d active", len(ids))}
}

// DefaultPayloadLimit is Config.PayloadLimit's default: 1.5 MiB. The
// engine and the SDK carry a run's input in one gRPC message of at most 4
// MiB by default (SERVER_GRPC_MAX_MSG_SIZE), and the driver stores a JSON
// object payload twice, as fields and as base64 (encodeInput), about 2.34
// times its size; 1.5 MiB leaves room for that and the run metadata.
const DefaultPayloadLimit = 3 << 19

// payloadLimitSource names the cap in a *workflow.PayloadTooLargeError.
const payloadLimitSource = "hatchet driver (Config.PayloadLimit)"

// PayloadLimit is the largest Job.Payload Enqueue accepts, in bytes
// (workflow.PayloadLimiter): Config.PayloadLimit.
func (c *Client) PayloadLimit() int { return c.cfg.PayloadLimit }
