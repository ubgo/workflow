package workflow

import "maps"

// Trigger source: what started a run (a schedule, an operator, a parent
// run, an app's webhook handler). It is plain run metadata kept in two
// reserved labels, so every driver stores and returns it with no change,
// and a run recorder or an admin view can filter "fired at 2 am" apart from
// "an operator pressed Run now" or "a Slack message asked for it".
//
// Why labels and not a ctx value: metadata a caller must remember to put
// on a ctx gets forgotten, and the run then silently lacks it. A label
// travels on the Job, which every driver stores, and RequireSource (in the
// middleware package) can refuse a job that has none.
//
// Why reserved yet app-settable: the keys sit under LabelPrefixInternal so
// no app label collides with them, but unlike the other reserved keys an
// app is expected to set them (WithSource), so CheckJobLabels and
// ValidateLabels let these two through when their values are valid. A
// source is provenance for people and filters, never an authorization
// input: anything that can enqueue can claim any source.
const (
	// LabelSource holds the run's Source.
	//
	//ds:def id=labelsource-m6wjahhr owner=@khanakia stability=stable
	LabelSource = LabelPrefixInternal + "source"

	// LabelSourceRef holds a free-form reference within the source: a
	// webhook delivery ID, a chat thread, the user who pressed a button.
	//
	//ds:def id=labelsourceref-qfv7bbhp owner=@khanakia stability=stable
	LabelSourceRef = LabelPrefixInternal + "source-ref"
)

// Bounds of a source and its reference, so a label value stays an index
// key, not a payload.
const (
	// MaxSourceLen is the longest Source, in bytes.
	//
	//ds:def id=maxsourcelen-sx2rxxmg owner=@khanakia stability=stable
	MaxSourceLen = 64

	// MaxSourceRefLen is the longest source reference, in bytes.
	//
	//ds:def id=maxsourcereflen-gygg9xx4 owner=@khanakia stability=stable
	MaxSourceRefLen = 256
)

// Source names what started a run. The library derives the sources below
// from labels it already writes (SourceOf); an app names its own ("api",
// "webhook", "slack", "admin") with WithSource. A Source passes
// ValidateSource: lower-case letters, digits and . _ - only.
//
//ds:def id=source-ehzk2bkd owner=@khanakia stability=stable
type Source string

// The sources SourceOf derives from the library's own labels, and the one
// a run recorder gives a run it found on the engine but never saw enqueued.
const (
	// SourceSchedule: a schedule or a declared cron fired the run
	// (LabelSchedule).
	//
	//ds:def id=sourceschedule-q6meeq7a owner=@khanakia stability=stable
	SourceSchedule Source = "schedule"

	// SourceScheduleManual: ScheduleTriggerer started a schedule's run by
	// hand (LabelTrigger = LabelTriggerManual).
	//
	//ds:def id=sourceschedulemanual-rjzgkf9k owner=@khanakia stability=stable
	SourceScheduleManual Source = "schedule-manual"

	// SourceChild: a step spawned the run (LabelParentRun).
	//
	//ds:def id=sourcechild-yqm4kgby owner=@khanakia stability=stable
	SourceChild Source = "child"

	// SourceBatch: the run is a member of a batch (batch.LabelBatch).
	//
	//ds:def id=sourcebatch-3pgw8v9a owner=@khanakia stability=stable
	SourceBatch Source = "batch"

	// SourceEvent: an events.Bus subscription started the run
	// (events.LabelEvent).
	//
	//ds:def id=sourceevent-4czra9fx owner=@khanakia stability=stable
	SourceEvent Source = "event"

	// SourceExternal: a run recorder found the run on the engine without
	// having seen it enqueued (an engine UI, its CLI, a crash window).
	//
	//ds:def id=sourceexternal-pq4saxv5 owner=@khanakia stability=stable
	SourceExternal Source = "external"
)

// derivedSources is the canonical list of library sources, for docs,
// filters and tests that iterate them.
var derivedSources = [...]Source{SourceSchedule, SourceScheduleManual, SourceChild, SourceBatch, SourceEvent, SourceExternal}

// LibrarySources returns every source the library itself names, as a fresh
// slice. App sources are not in it.
//
//ds:def id=librarysources-crdb5g3y owner=@khanakia stability=stable
func LibrarySources() []Source { return append([]Source(nil), derivedSources[:]...) }

// The label keys SourceOf reads that are owned by packages the core cannot
// import (batch and events import the core). Their owners keep the same
// strings, and a test in each package pins the match.
const (
	labelBatchID = LabelPrefixBatch + "id"
	labelEvent   = LabelPrefixInternal + "event"
)

// ValidateSource checks s: non-empty, at most MaxSourceLen bytes, only
// lower-case letters, digits and . _ - (so it is safe as a filter value, a
// metric label and a URL segment). Errors wrap ErrInvalid.
//
//ds:def id=validatesource-73nzwq5k owner=@khanakia stability=stable
func ValidateSource(s Source) error {
	if s == "" {
		return invalidf("source is empty")
	}
	if len(s) > MaxSourceLen {
		return invalidf("source %q is longer than %d bytes", s, MaxSourceLen)
	}
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !ok {
			return invalidf("source %q has the character %q; use lower-case letters, digits and . _ -", s, r)
		}
	}
	return nil
}

// validateSourceRef checks a source reference's length; any text is
// allowed, because a reference is an outside system's ID.
func validateSourceRef(ref string) error {
	if len(ref) > MaxSourceRefLen {
		return invalidf("source ref is %d bytes, more than %d", len(ref), MaxSourceRefLen)
	}
	return nil
}

// WithSource returns a copy of job whose labels carry source and ref (ref
// may be empty), leaving the caller's Labels map unchanged. It panics on
// nothing: an invalid source is caught by CheckJobLabels when the job is
// enqueued, and ValidateSource tells a caller earlier.
//
//	job = workflow.WithSource(job, "webhook", deliveryID)
//
//ds:def id=withsource-g5xwt2m5 owner=@khanakia stability=stable
func WithSource(job Job, source Source, ref string) Job {
	labels := make(map[string]string, len(job.Labels)+2)
	maps.Copy(labels, job.Labels)
	labels[LabelSource] = string(source)
	if ref == "" {
		delete(labels, LabelSourceRef)
	} else {
		labels[LabelSourceRef] = ref
	}
	job.Labels = labels
	return job
}

// SourceOf returns the source of a run from its labels, and the source
// reference when there is one. An explicit LabelSource wins. Otherwise it
// derives one from the labels the library stamps, most specific first:
// a manual trigger of a schedule, a schedule fire, a spawned child, a batch
// member, an event subscription. For a derived source the reference is the
// natural ID (the schedule, the parent run, the batch, the event name).
// It returns "" when nothing says where the run came from; a caller picks
// its own default (a run recorder's default source).
//
//ds:def id=sourceof-7mrryr2k owner=@khanakia stability=stable
func SourceOf(labels map[string]string) (source Source, ref string) {
	if s, ok := labels[LabelSource]; ok && s != "" {
		return Source(s), labels[LabelSourceRef]
	}
	if labels[LabelTrigger] == LabelTriggerManual {
		return SourceScheduleManual, labels[LabelSchedule]
	}
	if id, ok := labels[LabelSchedule]; ok {
		return SourceSchedule, id
	}
	if id, ok := labels[LabelParentRun]; ok {
		return SourceChild, id
	}
	if id, ok := labels[labelBatchID]; ok {
		return SourceBatch, id
	}
	if name, ok := labels[labelEvent]; ok {
		return SourceEvent, name
	}
	return "", ""
}

// appSettableReserved lists the reserved keys an app may set itself, each
// with the check its value must pass. CheckJobLabels and ValidateLabels
// read it; every other reserved key stays owner-only.
var appSettableReserved = map[string]func(string) error{
	LabelSource:    func(v string) error { return ValidateSource(Source(v)) },
	LabelSourceRef: validateSourceRef,
}

// checkAppReserved reports whether key is an app-settable reserved key,
// and the error its value fails with (nil when valid).
func checkAppReserved(key, value string) (settable bool, err error) {
	check, ok := appSettableReserved[key]
	if !ok {
		return false, nil
	}
	// check's error already wraps ErrInvalid and names the value.
	return true, check(value)
}
