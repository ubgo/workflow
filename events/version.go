package events

import (
	"fmt"
	"maps"

	"github.com/ubgo/workflow"
)

// versionPolicy is one event's current payload version and the
// upcasters that reach it.
type versionPolicy struct {
	current int
	ups     map[int]workflow.Upcaster
}

// WithUpcasters gives event a versioned payload (MQ-10): a message of
// that event carries its payload's version in Message.Version, and
// Publish (and Replay) bring a message below current up to current with
// workflow.Upcast before any subscription's Filter or Job, any cancel
// subscription, any divert or any signal sees it. ups[n] turns version n
// into n+1, as for workflow.DefineVersioned, so the same upcasters serve
// a workflow's payload and an event's.
//
// Refusals: Publish refuses a message whose Version is above current,
// published by newer code than this process runs, with ErrInvalid before
// anything is logged or started, so the publisher (or a retry after a
// deploy) sends it again; a gap in ups or an upcaster's error fails the
// Publish after the message is logged, also with ErrInvalid, so a
// deploy that fixes the upcaster can Replay it. New refuses, with
// ErrInvalid, an empty event, a negative current, an upcaster keyed
// outside [0, current) or nil, and a second WithUpcasters for one event.
//
// The run that a subscription starts gets the upcast payload; the bus
// does not stamp workflow.LabelPayloadVersion on it, since a workflow's
// payload version is the workflow's own (middleware.PayloadVersion).
func WithUpcasters(event string, current int, ups map[int]workflow.Upcaster) Option {
	return func(b *Bus) {
		if b.optErr != nil {
			return
		}
		switch {
		case event == "":
			b.optErr = fmt.Errorf("%w: events: WithUpcasters needs an event name", workflow.ErrInvalid)
			return
		case current < 0:
			b.optErr = fmt.Errorf("%w: events: WithUpcasters(%q): current version %d is negative", workflow.ErrInvalid, event, current)
			return
		}
		if _, dup := b.versions[event]; dup {
			b.optErr = fmt.Errorf("%w: events: WithUpcasters(%q) given twice", workflow.ErrInvalid, event)
			return
		}
		for v, up := range ups {
			if v < 0 || v >= current || up == nil {
				b.optErr = fmt.Errorf("%w: events: WithUpcasters(%q): upcaster for version %d is nil or outside [0, %d)", workflow.ErrInvalid, event, v, current)
				return
			}
		}
		if b.versions == nil {
			b.versions = map[string]versionPolicy{}
		}
		b.versions[event] = versionPolicy{current: current, ups: maps.Clone(ups)}
	}
}

// CurrentVersion returns the current payload version of event and
// whether the bus has upcasters for it (WithUpcasters).
func (b *Bus) CurrentVersion(event string) (int, bool) {
	p, ok := b.versions[event]
	return p.current, ok
}

// checkVersion refuses a negative version, and a version newer than the
// event's current one. It runs before the log, so a refused message is
// never logged.
func (b *Bus) checkVersion(m Message) error {
	if m.Version < 0 {
		return fmt.Errorf("%w: events: message %q has negative version %d", workflow.ErrInvalid, m.Name, m.Version)
	}
	p, ok := b.versions[m.Name]
	if ok && m.Version > p.current {
		return fmt.Errorf("%w: events: message %q has version %d, newer than this code's %d", workflow.ErrInvalid, m.Name, m.Version, p.current)
	}
	return nil
}

// upcast brings m's payload to its event's current version and sets
// m.Version to it. On error it returns m unchanged and an error wrapping
// ErrInvalid.
func (b *Bus) upcast(m Message) (Message, error) {
	if err := b.checkVersion(m); err != nil {
		return m, err
	}
	p, ok := b.versions[m.Name]
	if !ok || m.Version == p.current {
		return m, nil
	}
	out, err := workflow.Upcast(m.Payload, m.Version, p.current, p.ups)
	if err != nil {
		return m, fmt.Errorf("%w: events: message %q (ID %q): %w", workflow.ErrInvalid, m.Name, m.ID, err)
	}
	m.Payload = out
	m.Version = p.current
	return m, nil
}
