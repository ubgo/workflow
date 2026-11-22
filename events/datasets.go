package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// EventDatasetUpdated is the Message.Name of the event Datasets.Updated
// publishes on its bus. The message's Key is the dataset's name, its ID
// is DatasetEventID(dataset, version) and its payload a DatasetUpdate in
// JSON, so an ordinary Subscription (with a Filter on Key) or a run
// waiting under WithSignal for that key reacts to one dataset.
//
//ds:def id=events-eventdatasetupdated-7wensnha owner=@khanakia stability=stable
const EventDatasetUpdated = "dataset.updated"

// LabelDatasetTrigger is the DatasetTrigger that started a run. It is
// reserved like the bus's own labels: Datasets sets it itself and enqueues
// under workflow.AllowReservedLabels for this key only, so a public
// Enqueue cannot pass a run off as a trigger's. Like every reserved label
// it is a guard, not a sandbox.
//
//ds:def id=events-labeldatasettrigger-z5uqjykw owner=@khanakia stability=stable
const LabelDatasetTrigger = workflow.LabelPrefixInternal + "dataset-trigger"

// DatasetUpdate is the payload of an EventDatasetUpdated message.
//
//ds:def id=events-datasetupdate-2vu56pzk owner=@khanakia stability=stable
type DatasetUpdate struct {
	Dataset string            `json:"dataset"`
	Version string            `json:"version"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// DatasetEventID is the ID of the event that announces version of
// dataset: both parts query-escaped and joined by "@", so a name with an
// "@" in it cannot make two versions share an ID. Because the ID is a
// function of the version, a step that is retried and announces the
// same version again is deduplicated by the bus.
//
//ds:def id=events-dataseteventid-pfnngt9c owner=@khanakia stability=stable
func DatasetEventID(dataset, version string) string {
	return url.QueryEscape(dataset) + "@" + url.QueryEscape(version)
}

// DatasetTrigger starts a run of Workflow when the datasets it watches
// have new versions (WP-14): with All false, when any one of Datasets has
// a version the trigger has not used yet; with All true, once every one
// of Datasets has a version it has not used yet (each updated since the
// trigger's last run), so a report that joins two tables waits for both.
// A run consumes the versions it was started for, recorded in the
// LineageStore, and a version already consumed never starts the trigger
// again, however often it is announced.
//
//ds:def id=events-datasettrigger-acgw9uw2 owner=@khanakia stability=stable
type DatasetTrigger struct {
	// Name identifies the trigger; unique within a Datasets and stable
	// across deploys, because its consumed versions and every run's
	// idempotency key are kept under it.
	Name string

	// Datasets are the dataset names the trigger watches.
	Datasets []string

	// All makes the trigger wait for every dataset instead of any one.
	All bool

	// Workflow is the workflow to run. Ignored when Job is set and
	// returns a job naming its own workflow.
	Workflow string

	// Job, when set, builds the job from the versions the run consumes
	// (dataset name to version). Reserved label keys in it are refused
	// (ErrInvalid). Datasets still adds LabelDatasetTrigger and, when the
	// job has none, its idempotency key. Nil means Job{Workflow: Workflow,
	// Payload: the versions as a JSON object}.
	Job func(versions map[string]string) (workflow.Job, error)
}

// TriggerRun is what one trigger did with an update.
//
//ds:def id=events-triggerrun-3yc7fzhc owner=@khanakia stability=stable
type TriggerRun struct {
	// Trigger is the trigger's name.
	Trigger string

	// Versions are the dataset versions the run was started for.
	Versions map[string]string

	// Ref is the run; with an unfinished run already started for the
	// same versions, that run.
	Ref workflow.RunRef

	// Err is why the run could not be started or its consumption not
	// recorded; announce the version again to retry.
	Err error
}

// UpdateResult is the outcome of Datasets.Updated.
//
//ds:def id=events-updateresult-35fe4gpa owner=@khanakia stability=stable
type UpdateResult struct {
	// Lineage is the produced record Updated wrote.
	Lineage Lineage

	// Event is the bus's outcome of publishing EventDatasetUpdated.
	Event Published

	// Triggered has one entry per trigger the update started (or found
	// already started), in the order the triggers were added. A trigger
	// still waiting for another dataset, or whose versions were already
	// consumed, has none.
	Triggered []TriggerRun
}

// DatasetOption configures a Datasets.
type DatasetOption func(*Datasets)

// WithLineage sets where lineage and the triggers' consumed versions are
// kept. The default is a new MemoryLineage, which only one process sees;
// processes sharing triggers need one shared store: sqlstore.LineageStore
// (SQLite, Postgres, MySQL; migrated by its own Migrate) or
// redis.LineageStore, or an implementation of their own checked with
// workflowtest.LineageStoreContract.
//
//ds:def id=events-withlineage-yazww7nk owner=@khanakia stability=stable
func WithLineage(store LineageStore) DatasetOption { return func(d *Datasets) { d.store = store } }

// WithDatasetClock sets the clock that stamps lineage records; the default
// is clock.Real. Tests pass a clock.Fake.
//
//ds:def id=events-withdatasetclock-vr6j2czg owner=@khanakia stability=stable
func WithDatasetClock(c clock.Clock) DatasetOption { return func(d *Datasets) { d.clock = c } }

// Datasets is data-aware scheduling on top of a Bus: code that writes a
// dataset announces the new version with Updated, and triggers start the
// workflows that read it (WP-14), while every version is recorded with
// the run and step that produced it and the runs that consumed it
// (ET-05). It is safe for concurrent use; Trigger may be called at any
// time.
//
//ds:def id=events-datasets-5pnedqtp owner=@khanakia stability=stable
type Datasets struct {
	bus   *Bus
	store LineageStore
	clock clock.Clock

	mu       sync.RWMutex
	triggers []DatasetTrigger
}

// NewDatasets returns a Datasets that publishes on bus and starts trigger
// runs on the bus's client, so a client the bus accepted (with
// CapIdempotencyKey, or AllowDuplicates) is what deduplicates them.
//
//ds:def id=events-newdatasets-9xyde79u owner=@khanakia stability=stable
func NewDatasets(bus *Bus, opts ...DatasetOption) (*Datasets, error) {
	if bus == nil {
		return nil, fmt.Errorf("%w: events: datasets: bus is nil", workflow.ErrInvalid)
	}
	d := &Datasets{bus: bus}
	for _, o := range opts {
		o(d)
	}
	if d.store == nil {
		d.store = NewMemoryLineage()
	}
	if d.clock == nil {
		d.clock = clock.Real{}
	}
	return d, nil
}

// Lineage returns the store Datasets records into, for Producers,
// Consumers, Outputs and Inputs.
//
//ds:def id=events-datasets-lineage-tjknab2w owner=@khanakia stability=stable
func (d *Datasets) Lineage() LineageStore { return d.store }

// Trigger adds t. It returns an error wrapping ErrInvalid when t has no
// Name, reuses one, watches no dataset or an empty or repeated name, or
// has neither Workflow nor Job.
//
//ds:def id=events-datasets-trigger-a9cqrc2m owner=@khanakia stability=stable
func (d *Datasets) Trigger(t DatasetTrigger) error {
	if t.Name == "" {
		return fmt.Errorf("%w: events: dataset trigger needs a Name", workflow.ErrInvalid)
	}
	if len(t.Datasets) == 0 {
		return fmt.Errorf("%w: events: dataset trigger %q watches no dataset", workflow.ErrInvalid, t.Name)
	}
	for i, ds := range t.Datasets {
		if ds == "" || slices.Contains(t.Datasets[:i], ds) {
			return fmt.Errorf("%w: events: dataset trigger %q: dataset %q is empty or repeated", workflow.ErrInvalid, t.Name, ds)
		}
	}
	if t.Workflow == "" && t.Job == nil {
		return fmt.Errorf("%w: events: dataset trigger %q needs a Workflow or a Job", workflow.ErrInvalid, t.Name)
	}
	t.Datasets = slices.Clone(t.Datasets)
	d.mu.Lock()
	defer d.mu.Unlock()
	if slices.ContainsFunc(d.triggers, func(x DatasetTrigger) bool { return x.Name == t.Name }) {
		return fmt.Errorf("%w: events: dataset trigger %q already exists", workflow.ErrInvalid, t.Name)
	}
	d.triggers = append(d.triggers, t)
	return nil
}

// Updated announces that dataset now has version, with optional meta
// (row count, location). It records the version as produced, by the run
// and step of ctx when called from a step (workflow.StepInfoFrom), then
// publishes EventDatasetUpdated on the bus, then starts every trigger
// watching dataset whose condition now holds.
//
// Call it again with the same dataset and version after any error: the
// produced record is not duplicated, the bus deduplicates by
// DatasetEventID, and a trigger whose run started already starts nothing
// (its consumption is recorded, or its idempotency key finds the
// unfinished run). A dataset or version that is empty is refused with
// ErrInvalid; a failure to record the version returns at once, before
// anything is published. Publish and trigger failures are joined, and
// each is in the result too.
//
//ds:def id=events-datasets-updated-tuf5ktvy owner=@khanakia stability=stable
func (d *Datasets) Updated(ctx context.Context, dataset, version string, meta map[string]string) (UpdateResult, error) {
	if dataset == "" || version == "" {
		return UpdateResult{}, fmt.Errorf("%w: events: dataset update needs a dataset and a version", workflow.ErrInvalid)
	}
	rec := Lineage{Kind: LineageProduced, Dataset: dataset, Version: version, Meta: maps.Clone(meta), At: d.clock.Now()}
	if info, ok := workflow.StepInfoFrom(ctx); ok {
		rec.Run, rec.Workflow, rec.Step = info.RunID, info.Workflow, info.Step
	}
	if err := d.store.Record(ctx, rec); err != nil {
		return UpdateResult{}, fmt.Errorf("events: record dataset %q version %q: %w", dataset, version, err)
	}
	out := UpdateResult{Lineage: rec}
	var errs []error

	payload, err := json.Marshal(DatasetUpdate{Dataset: dataset, Version: version, Meta: meta})
	if err != nil {
		return out, fmt.Errorf("events: dataset %q update: %w", dataset, err)
	}
	out.Event, err = d.bus.Publish(ctx, Message{ID: DatasetEventID(dataset, version), Name: EventDatasetUpdated, Key: dataset, Payload: payload})
	if err != nil {
		errs = append(errs, err)
	}

	d.mu.RLock()
	triggers := slices.Clone(d.triggers)
	d.mu.RUnlock()
	for _, t := range triggers {
		if !slices.Contains(t.Datasets, dataset) {
			continue
		}
		versions, fire, err := d.due(ctx, t, dataset)
		if err == nil && !fire {
			continue
		}
		run := TriggerRun{Trigger: t.Name, Versions: versions}
		if err == nil {
			run.Ref, err = d.start(ctx, t, versions)
		}
		if err != nil {
			run.Err = fmt.Errorf("events: dataset trigger %q: %w", t.Name, err)
			errs = append(errs, run.Err)
		}
		out.Triggered = append(out.Triggered, run)
	}
	return out, errors.Join(errs...)
}

// due decides whether t fires after dataset changed, and with which
// versions. The decision is a function of the store alone (each
// dataset's latest produced version against the version t last
// consumed), never of state held in this process, so a retry, a restart
// or another process sharing the store decides the same way.
//
//ds:def id=events-datasets-due-dfvb6fqg owner=@khanakia stability=stable
func (d *Datasets) due(ctx context.Context, t DatasetTrigger, dataset string) (map[string]string, bool, error) {
	watch := []string{dataset}
	if t.All {
		watch = t.Datasets
	}
	versions := make(map[string]string, len(watch))
	for _, ds := range watch {
		latest, err := d.store.Find(ctx, LineageQuery{Kind: LineageProduced, Dataset: ds, Limit: 1})
		if err != nil {
			return nil, false, err
		}
		if len(latest) == 0 {
			return nil, false, nil // all-of: this dataset has no version yet
		}
		used, err := d.store.Find(ctx, LineageQuery{Kind: LineageConsumed, Trigger: t.Name, Dataset: ds, Limit: 1})
		if err != nil {
			return nil, false, err
		}
		if len(used) > 0 && used[0].Version == latest[0].Version {
			return nil, false, nil // not updated since t's last run
		}
		versions[ds] = latest[0].Version
	}
	return versions, true, nil
}

// start enqueues t's run for versions and records what it consumed. The
// idempotency key is a function of the trigger and the versions, so two
// processes deciding the same fire, or a retry after the enqueue
// succeeded but the record failed, find the one unfinished run.
//
//ds:def id=events-datasets-start-cf4nhsuf owner=@khanakia stability=stable
func (d *Datasets) start(ctx context.Context, t DatasetTrigger, versions map[string]string) (workflow.RunRef, error) {
	var job workflow.Job
	if t.Job != nil {
		var err error
		if job, err = t.Job(maps.Clone(versions)); err != nil {
			return workflow.RunRef{}, err
		}
		if err := workflow.ValidateLabels(job.Labels); err != nil {
			return workflow.RunRef{}, err
		}
		if job.Workflow == "" {
			job.Workflow = t.Workflow
		}
	} else {
		payload, err := json.Marshal(versions)
		if err != nil {
			return workflow.RunRef{}, err
		}
		job = workflow.Job{Workflow: t.Workflow, Payload: payload}
	}
	job.Labels = maps.Clone(job.Labels)
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[LabelDatasetTrigger] = t.Name
	if job.IdempotencyKey == "" {
		job.IdempotencyKey = triggerKey(t.Name, versions)
	}
	ref, err := d.bus.client.Enqueue(workflow.AllowReservedLabels(ctx, LabelDatasetTrigger), job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	now := d.clock.Now()
	for _, ds := range slices.Sorted(maps.Keys(versions)) {
		err := d.store.Record(ctx, Lineage{Kind: LineageConsumed, Dataset: ds, Version: versions[ds],
			Run: ref.RunID, Workflow: job.Workflow, Trigger: t.Name, At: now})
		if err != nil {
			return ref, fmt.Errorf("record consumed %q version %q: %w", ds, versions[ds], err)
		}
	}
	return ref, nil
}

// triggerKey is "dataset/<trigger>/<hash>", where hash covers every
// dataset and version the run consumes, in sorted order.
//
//ds:def id=events-triggerkey-jwjjtq7x owner=@khanakia stability=stable
func triggerKey(trigger string, versions map[string]string) string {
	var b strings.Builder
	for _, ds := range slices.Sorted(maps.Keys(versions)) {
		b.WriteString(url.QueryEscape(ds))
		b.WriteByte('@')
		b.WriteString(url.QueryEscape(versions[ds]))
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "dataset/" + trigger + "/" + hex.EncodeToString(sum[:16])
}
