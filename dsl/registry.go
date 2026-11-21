package dsl

import (
	"slices"
	"sync"

	"github.com/ubgo/workflow"
)

// Registry maps handler names to the workflow.Handler code they stand for.
// It is the security boundary of definitions as data: a definition can
// only name handlers the program registered, so whoever can write a
// definition (an operator, an admin form, a database row) can rearrange
// and retune the program's steps but never run code the program did not
// ship.
//
// The zero value is an empty registry ready to use; NewRegistry returns
// one too. There is no package-level registry: each program builds its
// own and passes it to Build or Load, so two clients in one process (or
// two tests) never share names by accident. It is safe for concurrent
// use, so a reload may Build while another goroutine registers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]workflow.Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{} }

// Register adds h under name. An empty name, a nil handler and a name
// already registered are refused with an error wrapping
// workflow.ErrInvalid: a second registration under one name is almost
// always two features colliding, and replacing the first would change
// what running definitions do without a word.
func (r *Registry) Register(name string, h workflow.Handler) error {
	if name == "" {
		return fieldErr("", "a handler name is empty")
	}
	if h == nil {
		return fieldErr("", "handler %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[name]; dup {
		return fieldErr("", "handler %q is already registered", name)
	}
	if r.handlers == nil {
		r.handlers = make(map[string]workflow.Handler)
	}
	r.handlers[name] = h
	return nil
}

// Handler returns the handler registered under name, and false when none
// is. A nil Registry has none.
func (r *Registry) Handler(name string) (workflow.Handler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

// Names returns the registered names, sorted. A nil Registry has none.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for n := range r.handlers {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}
