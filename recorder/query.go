package recorder

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
)

// QueryCaps is what a Store's Runs and Records honour of a
// workflow.RunQuery: label prefixes, tenant and due-time filters on top of
// the basic ones. Stores pass it to workflow.CheckRunQuery (through
// ValidateQuery), and a caller that wants to know before it asks, an admin
// view, can read it.
const QueryCaps = workflow.CapRunQuery | workflow.CapLabelPrefix | workflow.CapTenant | workflow.CapDueQuery

// MaxSearchLen is the longest Query.Search, in bytes.
const MaxSearchLen = 256

// Query is a workflow.RunQuery plus the filters only a recorder can answer,
// because only it knows a run's source and outcome.
type Query struct {
	workflow.RunQuery

	// Sources matches runs whose Source is any of these; empty matches
	// any.
	Sources []workflow.Source

	// Outcomes matches runs whose Outcome is any of these; empty matches
	// any. An unfinished run with no outcome set matches none.
	Outcomes []workflow.Outcome

	// Search matches runs where it is a case-insensitive substring of the
	// RunID, the ScheduledID, the workflow name or the SourceRef; empty
	// matches any. It is a lookup box for an operator, not a full-text
	// index: a store may scan to answer it.
	Search string

	// OnlyStale matches only runs the Reconciler flagged stale.
	OnlyStale bool
}

// ValidateQuery checks q: workflow.ValidateRunQuery and
// workflow.CheckRunQuery against QueryCaps for the embedded query, known
// sources and outcomes, and Search within MaxSearchLen. Errors wrap
// workflow.ErrInvalid or workflow.ErrUnsupported.
func ValidateQuery(q Query) error {
	if err := workflow.ValidateRunQuery(q.RunQuery); err != nil {
		return err
	}
	if err := workflow.CheckRunQuery(QueryCaps, q.RunQuery); err != nil {
		return err
	}
	for _, s := range q.Sources {
		if err := workflow.ValidateSource(s); err != nil {
			return fmt.Errorf("recorder query: %w", err)
		}
	}
	for _, o := range q.Outcomes {
		if !o.Valid() {
			return fmt.Errorf("%w: recorder query: unknown outcome %q", workflow.ErrInvalid, o)
		}
	}
	if len(q.Search) > MaxSearchLen {
		return fmt.Errorf("%w: recorder query: Search is %d bytes, more than %d", workflow.ErrInvalid, len(q.Search), MaxSearchLen)
	}
	return nil
}

// Match reports whether r passes q's filters (everything but paging). The
// in-memory store filters with it; a SQL store builds the same filters as
// a WHERE clause.
func (q Query) Match(r RunRecord) bool {
	if !q.RunQuery.Match(r.RunInfo) {
		return false
	}
	if len(q.Sources) > 0 && !slices.Contains(q.Sources, r.Source) {
		return false
	}
	if len(q.Outcomes) > 0 && !slices.Contains(q.Outcomes, r.Outcome) {
		return false
	}
	if q.OnlyStale && !r.Stale {
		return false
	}
	if q.Search != "" {
		needle := strings.ToLower(q.Search)
		hay := []string{string(r.Ref.RunID), string(r.Ref.ScheduledID), r.Workflow, r.SourceRef}
		if !slices.ContainsFunc(hay, func(h string) bool { return strings.Contains(strings.ToLower(h), needle) }) {
			return false
		}
	}
	return true
}

// Cursor is the position after one record in the listing order (newest
// enqueue first, Key descending): the next page starts after it.
type Cursor struct {
	EnqueuedAt time.Time
	Key        string
}

// cursorSep separates the cursor's two parts; a record Key never holds it
// before its prefix ends, and the time part is a plain integer.
const cursorSep = "|"

// EncodeCursor makes the opaque cursor string the bundled stores hand out
// as a page's Next.
func EncodeCursor(c Cursor) string {
	raw := strconv.FormatInt(c.EnqueuedAt.UnixNano(), 10) + cursorSep + c.Key
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor reads a cursor EncodeCursor made. Anything else is an error
// wrapping workflow.ErrInvalid, as RunQuerier documents for a cursor the
// store did not issue.
func DecodeCursor(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: recorder: bad cursor", workflow.ErrInvalid)
	}
	ts, key, ok := strings.Cut(string(raw), cursorSep)
	if !ok || key == "" {
		return Cursor{}, fmt.Errorf("%w: recorder: bad cursor", workflow.ErrInvalid)
	}
	ns, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: recorder: bad cursor", workflow.ErrInvalid)
	}
	return Cursor{EnqueuedAt: time.Unix(0, ns).UTC(), Key: key}, nil
}

// After reports whether r sorts after c in the listing order, so it
// belongs on the page that c continues.
func (c Cursor) After(r RunRecord) bool {
	if cmp := r.EnqueuedAt.Compare(c.EnqueuedAt); cmp != 0 {
		return cmp < 0
	}
	return r.Key() < c.Key
}

// SortRecords orders records the way Runs and Records list them: newest
// enqueue first, ties broken by Key descending.
func SortRecords(rs []RunRecord) {
	slices.SortFunc(rs, func(a, b RunRecord) int {
		if c := b.EnqueuedAt.Compare(a.EnqueuedAt); c != 0 {
			return c
		}
		return strings.Compare(b.Key(), a.Key())
	})
}

// InfoPage converts a page of records to the RunPage a RunQuerier returns.
func InfoPage(p RecordPage) workflow.RunPage {
	out := workflow.RunPage{Next: p.Next, Runs: make([]workflow.RunInfo, len(p.Runs))}
	for i, r := range p.Runs {
		out.Runs[i] = r.RunInfo
	}
	return out
}
