package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"time"
)

// DefaultMapInFlight is how many child runs MapRuns keeps running at once
// when MapRunsOptions.InFlight is zero.
//
//ds:def id=defaultmapinflight-nngs8xvm owner=@khanakia stability=stable
const DefaultMapInFlight = 10

// DefaultMapKeepFailed is how many failed children MapRunsResult lists
// when MapRunsOptions.KeepFailed is zero.
//
//ds:def id=defaultmapkeepfailed-pszz2thj owner=@khanakia stability=stable
const DefaultMapKeepFailed = 100

// MapUnlimitedFailures as MapRunsOptions.MaxFailures tolerates any number
// of failed items.
//
//ds:def id=mapunlimitedfailures-s5r92k9m owner=@khanakia stability=stable
const MapUnlimitedFailures = -1

// MapPage is one page of items a MapRuns source returns.
type MapPage[T any] struct {
	// Items are the items of this page, in order. A page may be empty.
	Items []T

	// Next is the cursor of the page after this one.
	Next string

	// Done is true on the last page: no page follows.
	Done bool
}

// MapRunsOptions tunes MapRuns.
//
//ds:def id=maprunsoptions-mg733853 owner=@khanakia stability=stable
type MapRunsOptions struct {
	// InFlight is how many child runs are kept running at once; zero
	// means DefaultMapInFlight.
	InFlight int

	// MaxFailures is how many items may fail before the map fails; zero
	// tolerates none, MapUnlimitedFailures any number.
	MaxFailures int

	// KeepFailed bounds MapRunsResult.Failed; zero means
	// DefaultMapKeepFailed.
	KeepFailed int

	// Poll is the snooze between looks; zero means DefaultChildPoll.
	Poll time.Duration

	// Parent is the client the calling step's workflow runs on (it must
	// honour Snooze); nil means client.
	Parent Client
}

// MapRunsResult counts what a finished map did.
//
//ds:def id=maprunsresult-jus3jnwu owner=@khanakia stability=stable
type MapRunsResult struct {
	// Started, Succeeded and Failed count child runs; Failed counts every
	// end other than success.
	Started, Succeeded, Failed int

	// FailedRefs are the first KeepFailed failed children with their
	// index, for a report or a retry.
	FailedRefs []MapFailure
}

// MapFailure is one item whose child run did not succeed.
type MapFailure struct {
	Index  int    `json:"index"`
	Ref    RunRef `json:"ref"`
	Status Status `json:"status"`
}

// MapRunsError is the Permanent error of a map that passed MaxFailures;
// Result is how far it got.
//
//ds:def id=maprunserror-6agjduvk owner=@khanakia stability=stable
type MapRunsError struct {
	Name   string
	Result MapRunsResult
}

// Error names the map and its counts.
func (e *MapRunsError) Error() string {
	return fmt.Sprintf("workflow: map %q: %d of %d started items failed", e.Name, e.Result.Failed, e.Result.Started)
}

// mapState is a map's progress, journalled after every change.
type mapState struct {
	Cursor    string            `json:"cursor"`
	Done      bool              `json:"done"`
	Pending   []json.RawMessage `json:"pending,omitempty"`
	Next      int               `json:"next"`
	InFlight  []mapChild        `json:"in_flight,omitempty"`
	Succeeded int               `json:"succeeded"`
	Failed    int               `json:"failed"`
	FailedAt  []MapFailure      `json:"failed_at,omitempty"`
	Finished  bool              `json:"finished"`
}

type mapChild struct {
	Index int    `json:"index"`
	Ref   RunRef `json:"ref"`
}

// MapRuns runs one child run per item of a source too big to hold in
// memory, keeping at most InFlight of them running, and tolerates up to
// MaxFailures failed items (WP-05, distributed map: a catalogue of a
// million products, each synced by its own run with its own retries).
// page returns the page of items at a cursor ("" for the first); job
// turns an item, with its index across the whole map, into the Job of its
// child run.
//
// It waits without holding a worker: each call looks at the children in
// flight, starts new ones up to InFlight, journals its progress, and
// returns Snooze(Poll) until every item has run, so the parent's driver
// needs CapSnooze. Progress is journalled after every child it starts
// (the Journal plugin is needed), and each child has an idempotency key
// made from the run, the step, name and the item's index (CapIdempotency
// Key is needed), so a crash between an enqueue and the save starts no
// item twice while its run is unfinished. page must return the same items
// for the same cursor, since a look after a crash may ask again. Items are
// kept in the journal between a page fetch and their start, as JSON, so
// keep a page small.
//
// Children carry LabelParentRun and LabelParentStep, so CancelChildren
// stops a map from outside. When failures pass MaxFailures the children
// still running are cancelled (on a client with CapCancel) and the map
// fails with a Permanent *MapRunsError.
//
//ds:def id=mapruns-etngp9jc owner=@khanakia stability=stable
func MapRuns[T any](ctx context.Context, client Client, name string,
	page func(ctx context.Context, cursor string) (MapPage[T], error),
	job func(index int, item T) (Job, error), opts MapRunsOptions) (MapRunsResult, error) {
	if client == nil || page == nil || job == nil || name == "" {
		return MapRunsResult{}, Permanent(invalidf("MapRuns needs a client, a name, a page source and a job builder"))
	}
	parent := opts.Parent
	if parent == nil {
		parent = client
	}
	if !parent.Capabilities().Has(CapSnooze) {
		return MapRunsResult{}, Permanent(&UnsupportedError{Field: "MapRuns", Missing: CapSnooze,
			Reason: "the parent's driver would spend an attempt on every look at unfinished children"})
	}
	if !client.Capabilities().Has(CapIdempotencyKey) {
		return MapRunsResult{}, Permanent(&UnsupportedError{Field: "MapRuns", Missing: CapIdempotencyKey,
			Reason: "a look after a crash could start an item twice"})
	}
	info, ok := StepInfoFrom(ctx)
	if !ok {
		return MapRunsResult{}, Permanent(invalidf("map %q: not called from a step", name))
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return MapRunsResult{}, err
	}
	inFlight := opts.InFlight
	if inFlight <= 0 {
		inFlight = DefaultMapInFlight
	}
	keepFailed := opts.KeepFailed
	if keepFailed <= 0 {
		keepFailed = DefaultMapKeepFailed
	}
	key := j.helperKey("map", name)
	var st mapState
	if raw, ok, err := j.store.Load(ctx, j.run, key); err != nil {
		return MapRunsResult{}, fmt.Errorf("workflow: map %q: load: %w", name, err)
	} else if ok {
		if err := json.Unmarshal(raw, &st); err != nil {
			return MapRunsResult{}, Permanent(fmt.Errorf("workflow: map %q: stored progress: %w", name, err))
		}
	}
	save := func() error {
		raw, err := encodeJSON("map "+name, st)
		if err != nil {
			return err
		}
		if err := j.store.Save(ctx, j.run, key, raw); err != nil {
			return fmt.Errorf("workflow: map %q: save: %w", name, err)
		}
		return nil
	}
	result := func() MapRunsResult {
		return MapRunsResult{Started: st.Next, Succeeded: st.Succeeded, Failed: st.Failed, FailedRefs: st.FailedAt}
	}
	if st.Finished {
		return result(), nil
	}

	// Retire the children that ended since the last look.
	still := st.InFlight[:0:0]
	for _, ch := range st.InFlight {
		s, err := client.Status(ctx, ch.Ref)
		if err != nil {
			return MapRunsResult{}, fmt.Errorf("workflow: map %q: item %d: %w", name, ch.Index, err)
		}
		switch {
		case s == StatusSucceeded:
			st.Succeeded++
		case s.IsTerminal():
			st.Failed++
			if len(st.FailedAt) < keepFailed {
				st.FailedAt = append(st.FailedAt, MapFailure{Index: ch.Index, Ref: ch.Ref, Status: s})
			}
		default:
			still = append(still, ch)
		}
	}
	st.InFlight = still
	if opts.MaxFailures != MapUnlimitedFailures && st.Failed > opts.MaxFailures {
		if client.Capabilities().Has(CapCancel) {
			for _, ch := range st.InFlight {
				if err := client.Cancel(ctx, ch.Ref); err != nil {
					return MapRunsResult{}, fmt.Errorf("workflow: map %q: cancel item %d: %w", name, ch.Index, err)
				}
			}
		}
		st.InFlight = nil
		if err := save(); err != nil {
			return MapRunsResult{}, err
		}
		return MapRunsResult{}, Permanent(&MapRunsError{Name: name, Result: result()})
	}

	// Start children up to InFlight, fetching pages as needed.
	for len(st.InFlight) < inFlight {
		if len(st.Pending) == 0 {
			if st.Done {
				break
			}
			p, err := page(ctx, st.Cursor)
			if err != nil {
				return MapRunsResult{}, fmt.Errorf("workflow: map %q: page %q: %w", name, st.Cursor, err)
			}
			for _, it := range p.Items {
				raw, err := encodeJSON("map "+name+" item", it)
				if err != nil {
					return MapRunsResult{}, err
				}
				st.Pending = append(st.Pending, raw)
			}
			st.Cursor, st.Done = p.Next, p.Done
			if err := save(); err != nil {
				return MapRunsResult{}, err
			}
			if len(p.Items) == 0 && !p.Done {
				break // an empty page that is not the last: look again later
			}
			continue
		}
		var item T
		if err := json.Unmarshal(st.Pending[0], &item); err != nil {
			return MapRunsResult{}, Permanent(fmt.Errorf("workflow: map %q: stored item: %w", name, err))
		}
		idx := st.Next
		jb, err := job(idx, item)
		if err != nil {
			return MapRunsResult{}, fmt.Errorf("workflow: map %q: job for item %d: %w", name, idx, err)
		}
		jb.Labels = maps.Clone(jb.Labels)
		if jb.Labels == nil {
			jb.Labels = map[string]string{}
		}
		jb.Labels[LabelParentRun] = info.RunID.String()
		jb.Labels[LabelParentStep] = info.Step
		if jb.IdempotencyKey == "" {
			jb.IdempotencyKey = "map/" + info.RunID.String() + "/" + key + "/" + strconv.Itoa(idx)
		}
		ref, err := client.Enqueue(AllowReservedLabels(ctx, LabelParentRun, LabelParentStep), jb)
		if err != nil {
			return MapRunsResult{}, fmt.Errorf("workflow: map %q: start item %d: %w", name, idx, err)
		}
		st.Pending = st.Pending[1:]
		st.Next++
		st.InFlight = append(st.InFlight, mapChild{Index: idx, Ref: ref})
		if err := save(); err != nil {
			return MapRunsResult{}, err
		}
	}
	if st.Done && len(st.Pending) == 0 && len(st.InFlight) == 0 {
		st.Finished = true
		if err := save(); err != nil {
			return MapRunsResult{}, err
		}
		return result(), nil
	}
	poll := opts.Poll
	if poll <= 0 {
		poll = DefaultChildPoll
	}
	return MapRunsResult{}, Snooze(poll)
}
