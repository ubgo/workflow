package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
)

// Labels EnqueuePartitions writes on every run it starts, so a run
// listing (PartitionStatus) finds the runs of a partition set and tells
// its partitions apart. They are reserved like LabelParentRun:
// EnqueuePartitions sets them itself, overwriting any value a job
// carried, and enqueues under AllowReservedLabels for these two keys
// only, so a public Enqueue cannot forge a partition run by accident or
// from untrusted input and skew a set's status. Like every reserved label
// they are a guard, not a sandbox (see AllowReservedLabels).
const (
	// LabelPartitionSet is the partition set's name (PartitionSet.Name).
	//ds:def id=labelpartitionset-umkwv73t owner=@khanakia stability=stable
	LabelPartitionSet = LabelPrefixInternal + "partition-set"

	// LabelPartitionKey is the run's partition key (a day, a tenant).
	//ds:def id=labelpartitionkey-ysc6y8xw owner=@khanakia stability=stable
	LabelPartitionKey = LabelPrefixInternal + "partition-key"
)

// PartitionSet is one logical job split into partitions: a run of a
// workflow per partition key, such as one per day of a backfill or one
// per tenant, each with its own status (WP-16). The set's Name ties the
// runs together, so it must be stable across deploys and unique among
// the sets on one client: it is part of every run's idempotency key and
// the label PartitionStatus filters on.
//
//ds:def id=partitionset-7944khcx owner=@khanakia stability=stable
type PartitionSet struct {
	// Name identifies the set ("orders-backfill-2026-11", "nightly-sync").
	Name string

	// Workflow is the workflow every partition runs. Ignored when Job is
	// set and returns a job naming its own workflow.
	Workflow string

	// Job, when set, builds the job for one key (a payload of the app's
	// own shape, a queue, a tenant, a priority). Reserved label keys in it
	// are refused (ErrInvalid). EnqueuePartitions still adds its labels
	// and, when the job has none, its idempotency key. Nil means
	// Job{Workflow: Workflow, Payload: the key as a JSON string}.
	Job func(key string) (Job, error)

	// AllowDuplicates lets EnqueuePartitions run on a client without
	// CapIdempotencyKey; on it, enqueueing a set again starts a second
	// run of a partition whose first run is unfinished.
	AllowDuplicates bool
}

// PartitionRun is the latest run of one partition, as PartitionStatus
// reports it.
//
//ds:def id=partitionrun-czz4h728 owner=@khanakia stability=stable
type PartitionRun struct {
	// Key is the partition key.
	Key string

	// Run is the partition's newest run: its RunRef, Status and Error
	// are the partition's state.
	Run RunInfo
}

// PartitionKey returns the idempotency key EnqueuePartitions gives the
// run of key in a set of workflow: "partition/<workflow>/<set>/<key>",
// each part path-escaped so a "/" inside a name cannot make two
// partitions share a key. Enqueueing a set again therefore never starts a
// partition whose run is still unfinished, while a partition whose run
// has ended (failed, or succeeded and asked for again) starts afresh.
//
//ds:def id=partitionkey-84t3xsxm owner=@khanakia stability=stable
func PartitionKey(workflow, set, key string) string {
	return "partition/" + url.PathEscape(workflow) + "/" + url.PathEscape(set) + "/" + url.PathEscape(key)
}

// EnqueuePartitions starts one run per key of set on client and returns
// their RunRefs in the order of keys. Each run carries LabelPartitionSet
// and LabelPartitionKey and, unless set.Job gave it one, the idempotency
// key PartitionKey(workflow, set.Name, key), so running the same backfill
// again (after a crash, a timeout, or on purpose) returns the unfinished
// partitions' runs instead of starting them twice.
//
// Every key is tried even when one fails; the error joins each failure,
// naming its key, and that key's RunRef is zero, so the caller enqueues
// the set again and only the partitions that failed to start are
// started. A set without a Name, a key that is empty or repeated, and
// a set with neither Workflow nor Job are refused with ErrInvalid before
// anything is enqueued. A client without CapIdempotencyKey is refused
// with an UnsupportedError unless set.AllowDuplicates is true.
//
//ds:def id=enqueuepartitions-h5gbqfcb owner=@khanakia stability=stable
func EnqueuePartitions(ctx context.Context, client Client, set PartitionSet, keys []string) ([]RunRef, error) {
	if err := checkPartitionSet(client, set); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k == "" {
			return nil, invalidf("partition set %q: empty key", set.Name)
		}
		if seen[k] {
			return nil, invalidf("partition set %q: key %q appears twice", set.Name, k)
		}
		seen[k] = true
	}
	allow := AllowReservedLabels(ctx, LabelPartitionSet, LabelPartitionKey)
	refs := make([]RunRef, len(keys))
	var errs []error
	for i, k := range keys {
		ref, err := enqueuePartition(allow, client, set, k)
		if err != nil {
			errs = append(errs, fmt.Errorf("workflow: partition set %q: key %q: %w", set.Name, k, err))
			continue
		}
		refs[i] = ref
	}
	return refs, errors.Join(errs...)
}

// checkPartitionSet refuses a set or client EnqueuePartitions cannot use.
//
//ds:def id=checkpartitionset-psguru6z owner=@khanakia stability=stable
func checkPartitionSet(client Client, set PartitionSet) error {
	switch {
	case client == nil:
		return invalidf("partition set %q: client is nil", set.Name)
	case set.Name == "":
		return invalidf("partition set has no Name")
	case set.Workflow == "" && set.Job == nil:
		return invalidf("partition set %q needs a Workflow or a Job", set.Name)
	}
	if !set.AllowDuplicates && !client.Capabilities().Has(CapIdempotencyKey) {
		return &UnsupportedError{Field: "EnqueuePartitions", Missing: CapIdempotencyKey,
			Reason: "enqueueing a set again would start an unfinished partition twice; set PartitionSet.AllowDuplicates to accept that"}
	}
	return nil
}

// enqueuePartition builds the job for one key and enqueues it under the
// ctx that allows the partition labels.
//
//ds:def id=enqueuepartition-z9fhqc5s owner=@khanakia stability=stable
func enqueuePartition(ctx context.Context, client Client, set PartitionSet, key string) (RunRef, error) {
	var job Job
	if set.Job != nil {
		var err error
		if job, err = set.Job(key); err != nil {
			return RunRef{}, err
		}
		if err := ValidateLabels(job.Labels); err != nil {
			return RunRef{}, err
		}
		if job.Workflow == "" {
			job.Workflow = set.Workflow
		}
	} else {
		payload, err := json.Marshal(key)
		if err != nil {
			return RunRef{}, err
		}
		job = Job{Workflow: set.Workflow, Payload: payload}
	}
	job.Labels = maps.Clone(job.Labels)
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[LabelPartitionSet] = set.Name
	job.Labels[LabelPartitionKey] = key
	if job.IdempotencyKey == "" {
		job.IdempotencyKey = PartitionKey(job.Workflow, set.Name, key)
	}
	return client.Enqueue(ctx, job)
}

// PartitionStatus lists the latest run of every partition of the set
// named set, sorted by key: one view of a backfill or of a per-tenant
// job. It reads every page of client's run listing filtered by
// LabelPartitionSet, so it needs CapRunQuery and a client implementing
// RunQuerier; without them it returns an UnsupportedError. A partition
// whose runs a driver's retention already removed is missing from the
// result, as is one never enqueued: compare the keys with the ones you
// expect to find the gaps.
//
//ds:def id=partitionstatus-6rn6e7q5 owner=@khanakia stability=stable
func PartitionStatus(ctx context.Context, client Client, set string) ([]PartitionRun, error) {
	if client == nil {
		return nil, invalidf("partition status %q: client is nil", set)
	}
	if set == "" {
		return nil, invalidf("partition status: empty set name")
	}
	q, ok := client.(RunQuerier)
	if !ok || !client.Capabilities().Has(CapRunQuery) {
		return nil, &UnsupportedError{Field: "PartitionStatus", Missing: CapRunQuery,
			Reason: "the status of a partition set is read from the run listing"}
	}
	latest := map[string]RunInfo{}
	query := RunQuery{Labels: map[string]string{LabelPartitionSet: set}, Limit: MaxRunsLimit}
	for {
		page, err := q.Runs(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("workflow: partition status %q: %w", set, err)
		}
		for _, r := range page.Runs {
			k, ok := r.Labels[LabelPartitionKey]
			if !ok {
				continue
			}
			// Pages run newest first, so the first run seen for a
			// key is its latest.
			if _, seen := latest[k]; !seen {
				latest[k] = r
			}
		}
		if page.Next == "" {
			break
		}
		query.Cursor = page.Next
	}
	out := make([]PartitionRun, 0, len(latest))
	for _, k := range slices.Sorted(maps.Keys(latest)) {
		out = append(out, PartitionRun{Key: k, Run: latest[k]})
	}
	return out, nil
}

// RetryFailedPartitions starts a new run for every partition of set whose
// latest run ended StatusFailed, and returns those keys, sorted, with
// their new RunRefs in the same order. Partitions that succeeded, are
// still running, were cancelled or were discarded are left alone: a
// cancel or a discard is a decision, not a failure. The new run is a
// fresh run of the whole workflow, enqueued by EnqueuePartitions with the
// same idempotency key (free again, since the failed run ended), so it
// works on every driver; to resume a failed run from its failed step on a
// driver with CapRetryRun, call RunRetrier.Retry with the RunRef that
// PartitionStatus reports instead. It needs what PartitionStatus and
// EnqueuePartitions need.
//
//ds:def id=retryfailedpartitions-wumd58gv owner=@khanakia stability=stable
func RetryFailedPartitions(ctx context.Context, client Client, set PartitionSet) ([]string, []RunRef, error) {
	if err := checkPartitionSet(client, set); err != nil {
		return nil, nil, err
	}
	runs, err := PartitionStatus(ctx, client, set.Name)
	if err != nil {
		return nil, nil, err
	}
	var keys []string
	for _, p := range runs {
		if p.Run.Status == StatusFailed {
			keys = append(keys, p.Key)
		}
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}
	refs, err := EnqueuePartitions(ctx, client, set, keys)
	return keys, refs, err
}

// PartitionOf returns the partition set and key of the run a step
// belongs to, read from the run's labels (StepInfo.Labels), and false
// when ctx is not a step's or its run was not started by
// EnqueuePartitions. A step whose set builds its own payload (Job) reads
// its key here.
//
//ds:def id=partitionof-zxdukbq3 owner=@khanakia stability=stable
func PartitionOf(ctx context.Context) (set, key string, ok bool) {
	info, found := StepInfoFrom(ctx)
	if !found {
		return "", "", false
	}
	set, okSet := info.Labels[LabelPartitionSet]
	key, okKey := info.Labels[LabelPartitionKey]
	if !okSet || !okKey {
		return "", "", false
	}
	return set, key, true
}
