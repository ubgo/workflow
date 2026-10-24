package hatchet

import (
	"fmt"

	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

// workerPool is one Hatchet worker Start runs: its name prefix, slots and
// the workflows it registers.
type workerPool struct {
	name      string
	queue     string // the served queue; "" when Config.Queues is empty (every queue)
	slots     int
	workflows []hatchetsdk.WorkflowBase
}

// checkQueues refuses a Config.Queues list Hatchet cannot honour: an
// invalid list (workflow.ValidateQueues), a non-zero Limit
// (CapQueueLimitGlobal) and any non-zero Priority or Weight, since each queue is its own Hatchet worker with its own slots:
// the engine cannot make one worker's runs wait for another's, and no
// worker picks between queues, so there is no pick to weight.
func checkQueues(qs []workflow.QueueConfig) error {
	if err := workflow.ValidateQueues(qs); err != nil {
		return fmt.Errorf("hatchet driver: Queues: %w", err)
	}
	// Limit: Hatchet has slots per worker and a concurrency expression per
	// workflow, but nothing that counts the in-flight steps of a whole
	// queue (a set of workflows) across workers, so the driver does not
	// declare CapQueueLimitGlobal and refuses a Limit rather than enforce
	// only a per-worker cap under the name of a cluster one.
	if err := workflow.CheckQueueLimits(caps, qs); err != nil {
		return err
	}
	for _, q := range qs {
		if q.Priority != 0 {
			return workflow.Unsupported("QueueConfig.Priority", "queue "+q.Name+": each queue is its own Hatchet worker, and the engine cannot hold one worker's runs back for another's")
		}
		if q.Weight != 0 {
			return workflow.Unsupported("QueueConfig.Weight", "queue "+q.Name+": each queue is its own Hatchet worker with its own slots, so no worker picks between queues and there is no pick to weight")
		}
	}
	return nil
}

// workerPools splits the declared workflows into the workers Start runs.
// Without cfg.Queues it is one worker for everything, as before queues
// existed. With them it is one worker per listed queue that has
// workflows, named WorkerName-queue; a workflow of an unlisted queue gets
// no worker here. The batch coordinator, when on, joins the first pool
// (or a pool of its own when there is none), so it always has a worker.
func workerPools(cfg Config, order []string, specs map[string]workflow.Workflow, declared map[string]*hatchetsdk.Workflow, coordinator *hatchetsdk.Workflow) []workerPool {
	var pools []workerPool
	if len(cfg.Queues) == 0 {
		p := workerPool{name: cfg.WorkerName, slots: cfg.Slots}
		for _, name := range order {
			p.workflows = append(p.workflows, declared[name])
		}
		if len(p.workflows) > 0 {
			pools = append(pools, p)
		}
	} else {
		for _, q := range cfg.Queues {
			p := workerPool{name: cfg.WorkerName + "-" + q.Name, queue: q.Name, slots: q.Workers}
			if p.slots == 0 {
				p.slots = cfg.Slots
			}
			for _, name := range order {
				if workflow.EffectiveQueue(specs[name], workflow.Job{}) == q.Name {
					p.workflows = append(p.workflows, declared[name])
				}
			}
			if len(p.workflows) > 0 {
				pools = append(pools, p)
			}
		}
	}
	if coordinator != nil {
		if len(pools) == 0 {
			pools = append(pools, workerPool{name: cfg.WorkerName, slots: cfg.Slots})
		}
		pools[0].workflows = append(pools[0].workflows, coordinator)
	}
	return pools
}
