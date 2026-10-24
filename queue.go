package workflow

import "fmt"

// DefaultQueue is the queue of a run whose Job.Queue and Workflow.Queue
// are both empty. Naming it lets a QueueConfig give the default queue its
// own worker limit and priority.
const DefaultQueue = "default"

// maxQueueName bounds a queue name so every engine (a worker name, a SQL
// index column) accepts it.
const maxQueueName = 64

// EffectiveQueue is the queue a run of wf enqueued as job runs on:
// job.Queue, else wf.Queue, else DefaultQueue. Every driver calls it, so
// no two drivers resolve a queue differently.
func EffectiveQueue(wf Workflow, job Job) string {
	switch {
	case job.Queue != "":
		return job.Queue
	case wf.Queue != "":
		return wf.Queue
	}
	return DefaultQueue
}

// QueueConfig is how one client's workers serve one named queue. Drivers
// take a list of them through their own option (memory.WithQueues and its
// siblings); the type is shared so the meaning is the same everywhere.
//
// A client given no QueueConfig serves every queue from one pool, as
// before queues existed. A client given some serves only those queues: on
// a shared store the runs of other queues wait for a client that serves
// them, which is how separate worker processes per queue are deployed.
type QueueConfig struct {
	// Name is the queue; it must pass ValidateQueueName.
	Name string

	// Workers caps how many step attempts of this queue execute at once
	// on this client. Zero means no per-queue cap: only the driver's pool
	// size limits it.
	Workers int

	// Limit caps the in-flight step attempts of this queue across EVERY
	// client sharing the store, not only this one: Workers is per client
	// (per node), Limit is the queue's whole-cluster ceiling. Zero means no
	// cluster cap. It needs CapQueueLimitGlobal; a driver without it
	// refuses a non-zero Limit at Start (CheckQueueLimits). An attempt
	// holds its slot from claim to outcome, and a slot whose holder died is
	// freed when the attempt's lease expires, never leaked. Every client
	// that serves the queue should declare the same Limit: a client that
	// declares none does not enforce it (it still counts toward it).
	Limit int

	// Priority orders queues when a worker is free: the ready attempts of
	// a higher queue are always taken before a lower one's. Equal priorities fall back to Job.Priority,
	// then arrival. A driver that cannot order queues refuses a non-zero
	// Priority at Start.
	Priority int

	// Weight shares a worker between queues of equal Priority: when
	// several of them have ready work, a free worker picks one at random
	// in proportion to the weights (PickWeighted), so a queue of weight 3
	// gets about three times the turns of a queue of weight 1 and none
	// starves. Zero means strict order within the priority: Job.Priority,
	// then arrival, as before. A queue with weight among queues without
	// counts as weight 1. A driver that cannot weight queues refuses a
	// non-zero Weight at Start.
	Weight int
}

// PickWeighted returns the index of the queue a weighted pick chooses
// from weights (each >= 1 here; a zero or negative weight counts as 1)
// given r, a uniform value in [0, 1). Every driver that weights queues
// calls it with its injectable random source, so all of them choose the
// same way and a test can pin the draw. It returns -1 for no weights.
func PickWeighted(weights []int, r float64) int {
	if len(weights) == 0 {
		return -1
	}
	total := 0
	for _, w := range weights {
		total += max(w, 1)
	}
	target := int(r * float64(total))
	for i, w := range weights {
		target -= max(w, 1)
		if target < 0 {
			return i
		}
	}
	return len(weights) - 1
}

// ValidateQueueName checks a queue name: 1 to 64 characters from
// letters, digits and ".", "_", ":", "-". Errors wrap ErrInvalid.
func ValidateQueueName(name string) error {
	if name == "" {
		return invalidf("queue name is empty")
	}
	if len(name) > maxQueueName {
		return invalidf("queue name %q is longer than %d characters", name, maxQueueName)
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == ':' || r == '-'
		if !ok {
			return invalidf("queue name %q has the character %q; use letters, digits and . _ : -", name, r)
		}
	}
	return nil
}

// ValidateQueues checks a QueueConfig list: valid, unique names and
// Workers not negative. Errors wrap ErrInvalid. Drivers call it from their
// option or from Start.
func ValidateQueues(qs []QueueConfig) error {
	seen := make(map[string]bool, len(qs))
	for i, q := range qs {
		if err := ValidateQueueName(q.Name); err != nil {
			return fmt.Errorf("queues[%d]: %w", i, err)
		}
		if seen[q.Name] {
			return invalidf("queue %q is configured twice", q.Name)
		}
		seen[q.Name] = true
		if q.Workers < 0 {
			return invalidf("queue %q: Workers is negative", q.Name)
		}
		if q.Limit < 0 {
			return invalidf("queue %q: Limit is negative", q.Name)
		}
		if q.Weight < 0 {
			return invalidf("queue %q: Weight is negative", q.Name)
		}
	}
	return nil
}

// CheckQueueLimits refuses a QueueConfig list that sets Limit when have
// lacks CapQueueLimitGlobal, so a driver that cannot count attempts across
// clients says so at Start instead of silently enforcing only a per-client
// cap. Drivers call it next to ValidateQueues.
func CheckQueueLimits(have Caps, qs []QueueConfig) error {
	if have.Has(CapQueueLimitGlobal) {
		return nil
	}
	for _, q := range qs {
		if q.Limit != 0 {
			return &UnsupportedError{Field: "QueueConfig.Limit", Missing: CapQueueLimitGlobal,
				Reason: "queue " + q.Name + ": this driver cannot cap a queue across clients"}
		}
	}
	return nil
}
