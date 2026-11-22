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

// Scoped data (WX-01): values shared beyond one run. A run's own values
// live in the step journal (SetRunData, RunData); the scopes here outlive
// every run and are shared by many:
//
//   - ScopeFolder: a named group of runs (a customer, an import batch, a
//     project) that every run naming the folder reads and writes;
//   - ScopeWorkflow: every run of one workflow;
//   - ScopeGlobal: every run of every workflow.
//
// The data sits in a DataStore the app picks: MemoryData in one process,
// sqlstore.DataStore or redis.DataStore for a fleet. A step reaches it
// through the ctx (the Data plugin or WithData), so the same handler runs
// on any driver, Hatchet included, because the store is independent of
// the driver running the step.

// DataScopeKind names a scope's kind. The values are stored by durable
// stores, so they never change.
type DataScopeKind string

const (
	// ScopeFolder is a named group of runs (Folder).
	//ds:def id=scopefolder-f2h5p7uh owner=@khanakia stability=stable
	ScopeFolder DataScopeKind = "folder"
	// ScopeWorkflow is every run of one workflow (WorkflowScope).
	//ds:def id=scopeworkflow-sgm5gs92 owner=@khanakia stability=stable
	ScopeWorkflow DataScopeKind = "workflow"
	// ScopeGlobal is every run of every workflow (GlobalScope).
	//ds:def id=scopeglobal-8thshyka owner=@khanakia stability=stable
	ScopeGlobal DataScopeKind = "global"
)

// DataScope says which runs share a value: a kind and, for a folder or a
// workflow, its name. Build one with Folder, WorkflowScope or
// GlobalScope.
type DataScope struct {
	Kind DataScopeKind
	// Name is the folder's or the workflow's name; empty for ScopeGlobal.
	// An empty name on ScopeWorkflow means the workflow of the running
	// step (StepInfo.Workflow), resolved when the value is read or
	// written.
	Name string
}

// Folder is the scope of the runs that name folder: data kept for a
// group of runs the app defines (a tenant, an import, a project).
//
//ds:def id=folder-bacd2p59 owner=@khanakia stability=stable
func Folder(folder string) DataScope { return DataScope{Kind: ScopeFolder, Name: folder} }

// WorkflowScope is the scope of every run of the workflow named name;
// with an empty name, the workflow of the running step.
//
//ds:def id=workflowscope-7wc874f8 owner=@khanakia stability=stable
func WorkflowScope(name string) DataScope { return DataScope{Kind: ScopeWorkflow, Name: name} }

// GlobalScope is the scope of every run of every workflow.
//
//ds:def id=globalscope-9wdg3q2c owner=@khanakia stability=stable
func GlobalScope() DataScope { return DataScope{Kind: ScopeGlobal} }

// Validate reports a scope a store cannot key: an unknown kind, a folder
// without a name, or a global scope with one. An empty workflow name is
// valid here and resolved from the step by the helpers; a store is never
// handed one.
func (s DataScope) Validate() error {
	switch s.Kind {
	case ScopeFolder:
		if s.Name == "" {
			return invalidf("data scope: a folder needs a name")
		}
	case ScopeWorkflow:
	case ScopeGlobal:
		if s.Name != "" {
			return invalidf("data scope: the global scope has no name (got %q)", s.Name)
		}
	default:
		return invalidf("data scope: unknown kind %q", s.Kind)
	}
	return nil
}

// Key is the one string a store keys an entry by: the kind, the scope
// name's length, the scope name and the value's name. The length makes it
// unambiguous, so folder "a/b" with name "c" and folder "a" with name
// "b/c" never share an entry. Durable stores keep it, so its form never
// changes.
//
//ds:def id=datascope-key-uy73eutg owner=@khanakia stability=stable
func (s DataScope) Key(name string) string {
	return string(s.Kind) + ":" + strconv.Itoa(len(s.Name)) + ":" + s.Name + "/" + name
}

// String is the scope for messages: "global", "folder acme".
func (s DataScope) String() string {
	if s.Name == "" {
		return string(s.Kind)
	}
	return string(s.Kind) + " " + s.Name
}

// DataVersion is an entry's version in a DataStore: it grows on every
// write, so a write can be made conditional on what was read. Zero means
// no entry.
type DataVersion int64

// AnyVersion, passed to DataStore.Put, writes whatever version the entry
// has, or creates it.
const AnyVersion DataVersion = -1

// ErrDataConflict is returned by DataStore.Put when the entry's version
// is not the one asked for (another writer got there first), and by
// UpdateData when it kept losing that race. It is not Permanent: a retry
// reads the new value and tries again.
//
//ds:def id=errdataconflict-vv8g67fu owner=@khanakia stability=stable
var ErrDataConflict = errors.New("workflow: scoped data changed since it was read")

// ErrNoDataStore means a scoped data helper (GetData, SetData, UpdateData,
// DeleteData) ran on a ctx without a DataStore: the client was not built
// with the Data plugin or WithData middleware, or the call is outside a
// step and the ctx was not given one with ContextWithData. It is returned
// marked Permanent, because a retry cannot wire a store in.
//
//ds:def id=errnodatastore-j5bgw3ba owner=@khanakia stability=stable
var ErrNoDataStore = errors.New("workflow: no data store on this context (use the Data plugin)")

// DataStore keeps scoped values (WX-01): entries keyed by scope and name,
// each with a version that grows on every write, so writers sharing a
// value across runs and processes can update it without losing a write.
//
// A store must be safe for concurrent use; Get must return a copy (or a
// value it never changes again); Put must check match and write in one
// atomic step, as seen by every client of the store. Scopes handed to a
// store are valid and never a ScopeWorkflow without a name.
//
//ds:def id=datastore-kcjg5n7e owner=@khanakia stability=stable
type DataStore interface {
	// Get returns the value under (scope, name), its version (above
	// zero) and true; or false when there is none.
	Get(ctx context.Context, scope DataScope, name string) (val []byte, ver DataVersion, ok bool, err error)

	// Put stores val under (scope, name) when the entry's current
	// version is match (0: no entry yet; AnyVersion: whatever is there)
	// and returns the new version; otherwise it writes nothing and
	// returns ErrDataConflict.
	Put(ctx context.Context, scope DataScope, name string, val []byte, match DataVersion) (DataVersion, error)

	// Delete removes the entry under (scope, name). Deleting an entry
	// that is not there is not an error.
	Delete(ctx context.Context, scope DataScope, name string) error
}

// MemoryData is an in-process DataStore: entries live in this process and
// are lost when it exits, so the scopes are shared by the runs of one
// process only. The zero value is ready to use.
//
//ds:def id=memorydata-gahgvwnz owner=@khanakia stability=stable
type MemoryData struct {
	mu      sync.Mutex
	entries map[string]memoryDataEntry
}

type memoryDataEntry struct {
	val []byte
	ver DataVersion
}

var _ DataStore = (*MemoryData)(nil)

// NewMemoryData returns an empty MemoryData.
//
//ds:def id=newmemorydata-4h6jkcnw owner=@khanakia stability=stable
func NewMemoryData() *MemoryData { return &MemoryData{} }

// Get returns a copy of the value under (scope, name).
func (m *MemoryData) Get(_ context.Context, scope DataScope, name string) ([]byte, DataVersion, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[scope.Key(name)]
	if !ok {
		return nil, 0, false, nil
	}
	return slices.Clone(e.val), e.ver, true, nil
}

// Put stores a copy of val when the version matches.
func (m *MemoryData) Put(_ context.Context, scope DataScope, name string, val []byte, match DataVersion) (DataVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[string]memoryDataEntry{}
	}
	k := scope.Key(name)
	cur := m.entries[k].ver
	if match != AnyVersion && match != cur {
		return 0, fmt.Errorf("%w: %s %q is at version %d, not %d", ErrDataConflict, scope, name, cur, match)
	}
	if val == nil {
		val = []byte{}
	}
	m.entries[k] = memoryDataEntry{val: slices.Clone(val), ver: cur + 1}
	return cur + 1, nil
}

// Delete removes the entry under (scope, name).
func (m *MemoryData) Delete(_ context.Context, scope DataScope, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, scope.Key(name))
	return nil
}

// Keys returns every entry's key (DataScope.Key), sorted, for tests.
func (m *MemoryData) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.entries))
}

type dataStoreKey struct{}

// ContextWithData returns a ctx carrying store, for the scoped data
// helpers outside a step (an HTTP handler seeding a folder, a test).
// Inside steps the Data plugin or WithData does this.
//
//ds:def id=contextwithdata-b23ne8fk owner=@khanakia stability=stable
func ContextWithData(ctx context.Context, store DataStore) context.Context {
	return context.WithValue(ctx, dataStoreKey{}, store)
}

// WithData returns step middleware that puts store on every step's ctx,
// for GetData, SetData, UpdateData and DeleteData. The Data plugin
// bundles it; scoped data needs no run-end cleanup, so the plugin needs
// no RunNotifier and works on every driver.
func WithData(store DataStore) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			if store == nil {
				return next(ctx, in)
			}
			return next(ContextWithData(ctx, store), in)
		}
	}
}

type dataPlugin struct{ store DataStore }

// Data returns the plugin that gives steps scoped data (WithData). Pass it
// to Use before Register.
//
//ds:def id=data-26rctv2b owner=@khanakia stability=stable
func Data(store DataStore) Plugin { return dataPlugin{store: store} }

// Name names the plugin in refusals.
func (dataPlugin) Name() string { return "data" }

// StepMiddleware puts the store on every step's ctx.
func (p dataPlugin) StepMiddleware() []Middleware { return []Middleware{WithData(p.store)} }

// dataFrom returns the ctx's store and scope resolved for it: a
// ScopeWorkflow without a name takes the running step's workflow.
func dataFrom(ctx context.Context, scope DataScope, name string) (DataStore, DataScope, error) {
	store, _ := ctx.Value(dataStoreKey{}).(DataStore)
	if store == nil {
		return nil, scope, Permanent(ErrNoDataStore)
	}
	if err := scope.Validate(); err != nil {
		return nil, scope, Permanent(err)
	}
	if name == "" {
		return nil, scope, Permanent(invalidf("scoped data name is empty"))
	}
	if scope.Kind == ScopeWorkflow && scope.Name == "" {
		info, ok := StepInfoFrom(ctx)
		if !ok || info.Workflow == "" {
			return nil, scope, Permanent(invalidf("data scope: WorkflowScope(\"\") outside a step; name the workflow"))
		}
		scope.Name = info.Workflow
	}
	return store, scope, nil
}

// GetData returns the value stored under name in scope, and false when
// there is none. A stored value that does not decode into T is a
// Permanent error.
//
//ds:def id=getdata-dkufhpgf owner=@khanakia stability=stable
func GetData[T any](ctx context.Context, scope DataScope, name string) (T, bool, error) {
	var zero T
	store, scope, err := dataFrom(ctx, scope, name)
	if err != nil {
		return zero, false, err
	}
	raw, _, ok, err := store.Get(ctx, scope, name)
	if err != nil {
		return zero, false, fmt.Errorf("workflow: %s data %q: get: %w", scope, name, err)
	}
	if !ok {
		return zero, false, nil
	}
	v, err := decodeData[T](scope, name, raw)
	return v, err == nil, err
}

// SetData stores v (as JSON) under name in scope, replacing what was
// there whatever it was. When the new value depends on the old one (a
// counter, a list), use UpdateData, which never loses a concurrent write.
//
//ds:def id=setdata-q8jtqwxf owner=@khanakia stability=stable
func SetData(ctx context.Context, scope DataScope, name string, v any) error {
	store, scope, err := dataFrom(ctx, scope, name)
	if err != nil {
		return err
	}
	b, err := encodeJSON(scope.String()+" data "+name, v)
	if err != nil {
		return err
	}
	if _, err := store.Put(ctx, scope, name, b, AnyVersion); err != nil {
		return fmt.Errorf("workflow: %s data %q: put: %w", scope, name, err)
	}
	return nil
}

// DeleteData removes the value under name in scope.
//
//ds:def id=deletedata-6kzsuvhn owner=@khanakia stability=stable
func DeleteData(ctx context.Context, scope DataScope, name string) error {
	store, scope, err := dataFrom(ctx, scope, name)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, scope, name); err != nil {
		return fmt.Errorf("workflow: %s data %q: delete: %w", scope, name, err)
	}
	return nil
}

// UpdateDataAttempts is how many times UpdateData reads and writes before
// it gives up on a value other writers keep changing.
//
//ds:def id=updatedataattempts-3544vz5p owner=@khanakia stability=stable
const UpdateDataAttempts = 32

// UpdateData reads the value under name in scope, passes it to fn (ok is
// false when there is none) and stores what fn returns, only if nobody
// wrote the value in between; otherwise it reads again and calls fn
// again, so fn must have no side effects. It returns the stored value.
// An error from fn is returned as is and nothing is stored. After
// UpdateDataAttempts lost races it returns ErrDataConflict, which is not
// Permanent, so the step's retry policy tries again later.
//
//ds:def id=updatedata-zc9hugjv owner=@khanakia stability=stable
func UpdateData[T any](ctx context.Context, scope DataScope, name string, fn func(cur T, ok bool) (T, error)) (T, error) {
	var zero T
	store, scope, err := dataFrom(ctx, scope, name)
	if err != nil {
		return zero, err
	}
	for range UpdateDataAttempts {
		raw, ver, ok, err := store.Get(ctx, scope, name)
		if err != nil {
			return zero, fmt.Errorf("workflow: %s data %q: get: %w", scope, name, err)
		}
		var cur T
		if ok {
			if cur, err = decodeData[T](scope, name, raw); err != nil {
				return zero, err
			}
		}
		next, err := fn(cur, ok)
		if err != nil {
			return zero, err
		}
		b, err := encodeJSON(scope.String()+" data "+name, next)
		if err != nil {
			return zero, err
		}
		if !ok {
			ver = 0
		}
		_, err = store.Put(ctx, scope, name, b, ver)
		if err == nil {
			return next, nil
		}
		if !errors.Is(err, ErrDataConflict) {
			return zero, fmt.Errorf("workflow: %s data %q: put: %w", scope, name, err)
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
	}
	return zero, fmt.Errorf("workflow: %s data %q: %w after %d attempts", scope, name, ErrDataConflict, UpdateDataAttempts)
}

func decodeData[T any](scope DataScope, name string, raw []byte) (T, error) {
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, Permanent(fmt.Errorf("workflow: %s data %q: stored value does not decode: %w", scope, name, err))
	}
	return v, nil
}
