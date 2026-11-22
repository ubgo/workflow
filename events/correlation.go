package events

import (
	"context"
	"fmt"
	"maps"

	"github.com/ubgo/workflow"
)

// Choreography labels (XM-08, WP-28). Under WithCorrelation every run
// Publish starts carries LabelCorrelation, and LabelCausation when the
// message was published from a step of a run an event started. Like the
// bus's other labels they sit under the reserved internal prefix: Publish
// refuses them in Message.Labels and in the labels a Subscription.Job
// returns, sets them itself, and enqueues under
// workflow.AllowReservedLabels for exactly these keys, so a public
// Enqueue cannot forge them by accident or from untrusted input.
const (
	// LabelCorrelation names the choreography a run belongs to: every run
	// started, directly or through a chain of events published from steps,
	// by one first event carries the same value. Trail lists them.
	//ds:def id=events-labelcorrelation-2f3h8pg4 owner=@khanakia stability=stable
	LabelCorrelation = workflow.LabelPrefixInternal + "correlation"

	// LabelCausation is the ID of the event that started the run which
	// published this run's event: the edge from one hop of a
	// choreography to the next. Absent on runs of the first event.
	//ds:def id=events-labelcausation-sk55czmu owner=@khanakia stability=stable
	LabelCausation = workflow.LabelPrefixInternal + "causation"
)

// WithCorrelation makes the bus stamp every run it starts (Publish, a
// divert, Replay) with LabelCorrelation and, when known, LabelCausation,
// so a choreography (services that only react to events and publish their
// own, compensation events included, with no orchestrator) can be listed
// as one unit with Trail. Off by default; without it no label is added
// and a message carrying Correlation or Causation is refused.
//
// Publish fills the message's empty Correlation and Causation before it
// logs or delivers it, from the StepInfo on ctx: called from a step whose
// run carries LabelCorrelation, the message inherits that value, and the
// run's LabelEventID becomes its Causation. Outside such a step,
// Correlation is the message's own ID (the one given or the one the bus
// made), so the first event of a chain names the chain. Because the filled
// fields are logged (WithLog), a Replay keeps each message in its chain.
//
//ds:def id=events-withcorrelation-tnjs2y7e owner=@khanakia stability=stable
func WithCorrelation() Option { return func(b *Bus) { b.correlate = true } }

// resolveChain fills m's empty Correlation and Causation as
// WithCorrelation describes. m.ID is already set.
//
//ds:def id=events-resolvechain-eg8jtcb6 owner=@khanakia stability=stable
func resolveChain(ctx context.Context, m Message) Message {
	info, inStep := workflow.StepInfoFrom(ctx)
	if m.Correlation == "" {
		if inStep && info.Labels[LabelCorrelation] != "" {
			m.Correlation = info.Labels[LabelCorrelation]
		} else {
			m.Correlation = m.ID
		}
	}
	if m.Causation == "" && inStep {
		m.Causation = info.Labels[LabelEventID]
	}
	return m
}

// checkChain refuses Correlation or Causation on a bus without
// WithCorrelation, where they would be dropped without a trace.
//
//ds:def id=events-checkchain-vhtvgshe owner=@khanakia stability=stable
func (b *Bus) checkChain(m Message) error {
	if !b.correlate && (m.Correlation != "" || m.Causation != "") {
		return fmt.Errorf("%w: events: message %q sets Correlation or Causation on a bus without WithCorrelation", workflow.ErrInvalid, m.Name)
	}
	return nil
}

// chainLabels adds the choreography labels for m to labels and returns
// the reserved keys the Enqueue must allow. A message without a
// Correlation (logged before WithCorrelation was set, then replayed) is
// its own chain.
//
//ds:def id=events-bus-chainlabels-t4yjtmj3 owner=@khanakia stability=stable
func (b *Bus) chainLabels(labels map[string]string, m Message) []string {
	if !b.correlate {
		return nil
	}
	corr := m.Correlation
	if corr == "" {
		corr = m.ID
	}
	labels[LabelCorrelation] = corr
	if m.Causation != "" {
		labels[LabelCausation] = m.Causation
	}
	return []string{LabelCorrelation, LabelCausation}
}

// Trail lists every run of one choreography: the runs whose
// LabelCorrelation is correlation, through client's workflow.RunQuerier,
// following RunPage.Next until the last page, in the order the querier
// returns them (newest enqueue first). q narrows the listing (Workflow,
// Statuses, From and To, more Labels); its Labels are copied, never
// changed, and LabelCorrelation in them is replaced by correlation. Limit
// is the page size and Cursor, when set, the page to start from.
//
// It refuses with a *workflow.UnsupportedError naming CapRunQuery a client
// that does not implement workflow.RunQuerier or does not declare
// CapRunQuery (the memory, sqlstore, Redis and Hatchet drivers declare it),
// and with ErrInvalid an empty correlation. Errors of
// the querier are returned with the runs listed so far.
//
// The runs carry the label only when the bus that started them had
// WithCorrelation; the causation labels say which event led to which run,
// so the trail is a tree, not just a list.
//
//ds:def id=events-trail-pyxgkv3b owner=@khanakia stability=stable
func Trail(ctx context.Context, client workflow.Client, correlation string, q workflow.RunQuery) ([]workflow.RunInfo, error) {
	if correlation == "" {
		return nil, fmt.Errorf("%w: events: Trail needs a correlation", workflow.ErrInvalid)
	}
	if client == nil {
		return nil, fmt.Errorf("%w: events: client is nil", workflow.ErrInvalid)
	}
	rq, ok := client.(workflow.RunQuerier)
	if !ok || !client.Capabilities().Has(workflow.CapRunQuery) {
		return nil, &workflow.UnsupportedError{Field: "events.Trail", Missing: workflow.CapRunQuery,
			Reason: "listing a choreography's runs needs run queries"}
	}
	labels := maps.Clone(q.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[LabelCorrelation] = correlation
	q.Labels = labels
	var out []workflow.RunInfo
	for {
		page, err := rq.Runs(ctx, q)
		if err != nil {
			return out, fmt.Errorf("events: trail %q: %w", correlation, err)
		}
		out = append(out, page.Runs...)
		if page.Next == "" {
			return out, nil
		}
		q.Cursor = page.Next
	}
}
