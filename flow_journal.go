package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
)

// ErrNoJournal means Memo (or a helper built on it: MapEach, Loop, Spawn,
// RunData) was called from a ctx that carries no journal: the step's
// client was not built with the Journal plugin or the WithJournal
// middleware, or the call is not inside a step. It is returned marked
// Permanent, because retrying the step cannot wire a journal in.
var ErrNoJournal = errors.New("workflow: no journal on this context (use the Journal plugin)")

// JournalStore keeps the results of memoised work inside a run (XM-02), so
// a step that is retried, or a run retried with RunRetrier (which keeps
// its RunRef), skips the work that already succeeded and gets its stored
// result back instead of doing it twice.
//
// Entries are keyed by run and by a key Memo builds from the step name and
// the name the caller gave. A store must be safe for concurrent use, must
// return a copy (or a value it never changes again) from Load, and Save
// of an existing key replaces it. MemoryJournal is the in-process
// default; a durable store (SQL, Redis) makes the memo survive a crash of
// the process, which an in-process journal cannot.
type JournalStore interface {
	// Load returns the value saved under key for run, and false when
	// there is none.
	Load(ctx context.Context, run RunID, key string) (val []byte, ok bool, err error)

	// Save stores val under key for run.
	Save(ctx context.Context, run RunID, key string, val []byte) error

	// Forget removes every entry of run. Forgetting a run with no
	// entries is not an error.
	Forget(ctx context.Context, run RunID) error
}

// MemoryJournal is an in-process JournalStore: entries live in this
// process only and are lost when it exits, so the memo holds across step
// retries and RunRetrier retries in one process, not across a crash.
// The zero value is ready to use.
type MemoryJournal struct {
	mu   sync.Mutex
	runs map[RunID]map[string][]byte
}

// NewMemoryJournal returns an empty MemoryJournal.
func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{} }

// Load returns a copy of the value saved under key for run.
func (m *MemoryJournal) Load(_ context.Context, run RunID, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.runs[run][key]
	return slices.Clone(v), ok, nil
}

// Save stores a copy of val under key for run.
func (m *MemoryJournal) Save(_ context.Context, run RunID, key string, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runs == nil {
		m.runs = map[RunID]map[string][]byte{}
	}
	if m.runs[run] == nil {
		m.runs[run] = map[string][]byte{}
	}
	m.runs[run][key] = slices.Clone(val)
	return nil
}

// Forget removes every entry of run.
func (m *MemoryJournal) Forget(_ context.Context, run RunID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runs, run)
	return nil
}

// Runs returns the runs that hold at least one entry, sorted, for tests
// and for checking that finished runs were forgotten.
func (m *MemoryJournal) Runs() []RunID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.runs))
}

// journalScope is what WithJournal puts on a step's ctx: the store, the
// attempt's run and step, and the names Memo has used in this attempt, so
// a name used twice in one attempt is refused instead of silently
// returning the first call's result to the second.
type journalScope struct {
	store JournalStore
	run   RunID
	step  string

	mu   sync.Mutex
	used map[string]struct{}
}

type journalKey struct{}

// WithJournal returns step middleware that puts store on every step's
// ctx, for Memo and the helpers built on it. A ctx without StepInfo (a
// handler called outside a driver) passes through unchanged, so Memo
// there returns ErrNoJournal. The Journal plugin bundles this with the
// cleanup of finished runs; use WithJournal alone only when something
// else forgets runs.
func WithJournal(store JournalStore) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			info, ok := StepInfoFrom(ctx)
			if !ok || store == nil {
				return next(ctx, in)
			}
			return next(context.WithValue(ctx, journalKey{}, &journalScope{
				store: store, run: info.RunID, step: info.Step, used: map[string]struct{}{},
			}), in)
		}
	}
}

// JournalOption configures the Journal plugin.
type JournalOption func(*journalPlugin)

// JournalKeepFailed keeps the journal of a run that failed, so a later
// RunRetrier retry of that run resumes from its memoised work instead of
// redoing it. The app then forgets those runs itself (JournalStore.Forget,
// or the store's own retention). By default every finished run is
// forgotten, failed ones included, so an in-process journal never grows
// without bound.
func JournalKeepFailed() JournalOption { return func(p *journalPlugin) { p.keepFailed = true } }

type journalPlugin struct {
	store      JournalStore
	keepFailed bool
}

// Journal returns the plugin that turns on step memoisation: it puts
// store on every step's ctx (WithJournal) and forgets a run's entries
// when the run finishes. Pass it to Use before Register and Start; it
// needs a client with RunNotifier, as every run-finished plugin does.
func Journal(store JournalStore, opts ...JournalOption) Plugin {
	p := &journalPlugin{store: store}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Name names the plugin in refusals.
func (p *journalPlugin) Name() string { return "journal" }

// StepMiddleware puts the store on every step's ctx.
func (p *journalPlugin) StepMiddleware() []Middleware { return []Middleware{WithJournal(p.store)} }

// RunFinishedObservers forgets a finished run's entries. A Forget error
// is dropped: observers have no caller to report to, and a leftover entry
// only costs space, never a wrong result (a RunID is never reused).
func (p *journalPlugin) RunFinishedObservers() []RunFinishedFunc {
	return []RunFinishedFunc{func(ctx context.Context, ev RunEvent) {
		if ev.Ref.RunID.IsZero() || (p.keepFailed && ev.Status == StatusFailed) {
			return
		}
		_ = p.store.Forget(ctx, ev.Ref.RunID) // see the doc comment
	}}
}

// journalFrom returns the scope WithJournal set, or a Permanent
// ErrNoJournal.
func journalFrom(ctx context.Context) (*journalScope, error) {
	j, ok := ctx.Value(journalKey{}).(*journalScope)
	if !ok {
		return nil, Permanent(ErrNoJournal)
	}
	return j, nil
}

// claim records that name was used in this attempt and builds its key.
// The key carries the step name's length, so step "a/b" with name "c"
// and step "a" with name "b/c" can never share an entry.
func (j *journalScope) claim(name string) (string, error) {
	if name == "" {
		return "", Permanent(invalidf("memo name is empty"))
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, dup := j.used[name]; dup {
		return "", Permanent(invalidf("memo name %q is used twice in step %q; give each call its own name", name, j.step))
	}
	j.used[name] = struct{}{}
	return strconv.Itoa(len(j.step)) + ":" + j.step + "/" + name, nil
}

// Memo runs fn once per run under name and returns its result; on any
// later attempt of the same step of the same run (a step retry, or a run
// retried with RunRetrier) it returns the stored result without calling
// fn. This is step memoisation as a journal: code re-runs from the top
// and each named piece of work returns what it returned the first time.
//
// Rules a caller keeps:
//   - Names are stable across attempts and unique within one attempt of a
//     step; a name used twice in one attempt is refused (Permanent,
//     ErrInvalid), because the second call would silently get the first
//     call's result.
//   - An error from fn is returned and nothing is stored, so the next
//     attempt calls fn again.
//   - The result is stored as JSON. A stored value that no longer decodes
//     into T (the type changed between deploys) is a Permanent error.
//   - The guarantee is at least once: if the process dies after fn
//     returns and before Save, or Save fails, fn runs again on the next
//     attempt. Work that must not repeat also carries an idempotency key
//     the remote side honours.
//
// Memo needs the Journal plugin (or WithJournal) on the client, and
// returns a Permanent ErrNoJournal without it. Memo is safe to call from
// several goroutines of one step with different names.
func Memo[T any](ctx context.Context, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	j, err := journalFrom(ctx)
	if err != nil {
		return zero, err
	}
	key, err := j.claim(name)
	if err != nil {
		return zero, err
	}
	return memoKey(ctx, j, key, name, fn)
}

// memoKey is Memo after the name is claimed.
func memoKey[T any](ctx context.Context, j *journalScope, key, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	raw, ok, err := j.store.Load(ctx, j.run, key)
	if err != nil {
		return zero, fmt.Errorf("workflow: memo %q: load: %w", name, err)
	}
	if ok {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			return zero, Permanent(fmt.Errorf("workflow: memo %q: stored result does not decode: %w", name, err))
		}
		return v, nil
	}
	v, err := fn(ctx)
	if err != nil {
		return zero, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return zero, Permanent(fmt.Errorf("workflow: memo %q: encode result: %w", name, err))
	}
	if err := j.store.Save(ctx, j.run, key, b); err != nil {
		return zero, fmt.Errorf("workflow: memo %q: save: %w", name, err)
	}
	return v, nil
}
