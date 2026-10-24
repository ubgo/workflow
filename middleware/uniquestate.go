package middleware

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ubgo/workflow"
)

// UniqueHolders is an optional interface of a UniqueStore that also
// remembers which run holds each key, so UniqueWith can look the holder
// up: to decide by the holder's status (UniqueConfig.States) or to return
// it (UniqueConfig.ReturnExisting). MemoryUnique implements it, and so
// should a shared store (sqlstore.UniqueStore).
type UniqueHolders interface {
	// SetHolder records ref as the run of key while owner holds it; a key
	// held by another owner, or free, is left alone.
	SetHolder(ctx context.Context, key, owner string, ref workflow.RunRef) error
	// Holder reports the owner and run of a held, unexpired key whose run
	// was recorded; ok is false otherwise.
	Holder(ctx context.Context, key string) (owner string, ref workflow.RunRef, ok bool, err error)
}

// UniqueConfig configures UniqueWith.
type UniqueConfig struct {
	// By derives the key; "" passes the job through. Required.
	By func(workflow.Job) string

	// Store holds the locks. Required. States and ReturnExisting need it
	// to implement UniqueHolders.
	Store UniqueStore

	// TTL bounds every lock, whatever the holder's state. Required.
	TTL time.Duration

	// States are the holder statuses that make a new job a duplicate. A
	// held key whose run is in none of them (or is gone) is taken over by
	// the new job: States{StatusScheduled, StatusQueued} lets a job in
	// once the holder started; adding StatusSucceeded keeps a key closed
	// after success until TTL. Empty means every status counts (plain
	// Unique). Needs Client to look the holder up.
	States []workflow.Status

	// Client answers Status for the holder's run; required with States.
	Client workflow.Client

	// ReturnExisting answers a duplicate with the holder's RunRef and a
	// nil error, marking the EnqueueOutcome a duplicate
	// (workflow.MarkDuplicate), instead of an error wrapping ErrDuplicate.
	// When the holder's run is not known yet (a race with its enqueue),
	// it still returns ErrDuplicate.
	ReturnExisting bool
}

// UniqueWith is Unique with a choice of which holder states count
// (States) and of answering a duplicate with the existing run
// (ReturnExisting). It refuses a config it cannot honour with ErrInvalid:
// a missing By, Store or TTL, States without a Client, or States or
// ReturnExisting on a store that does not implement UniqueHolders.
func UniqueWith(cfg UniqueConfig) (workflow.EnqueueMiddleware, error) {
	if cfg.By == nil || cfg.Store == nil || cfg.TTL <= 0 {
		return nil, fmt.Errorf("%w: UniqueWith needs By, Store and a positive TTL", workflow.ErrInvalid)
	}
	for _, s := range cfg.States {
		if !s.Valid() {
			return nil, fmt.Errorf("%w: UniqueWith: unknown status %q", workflow.ErrInvalid, s)
		}
	}
	if len(cfg.States) > 0 && cfg.Client == nil {
		return nil, fmt.Errorf("%w: UniqueWith: States needs a Client to look the holder up", workflow.ErrInvalid)
	}
	holders, ok := cfg.Store.(UniqueHolders)
	track := len(cfg.States) > 0 || cfg.ReturnExisting
	if track && !ok {
		return nil, fmt.Errorf("%w: UniqueWith: States and ReturnExisting need a store implementing UniqueHolders", workflow.ErrInvalid)
	}
	u := uniqueWith{cfg: cfg, holders: holders, track: track}
	return u.middleware, nil
}

type uniqueWith struct {
	cfg     UniqueConfig
	holders UniqueHolders
	track   bool
}

func (u uniqueWith) middleware(next workflow.EnqueueFunc) workflow.EnqueueFunc {
	return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
		if err := refuseInternalLabels("UniqueWith", job.Labels, ClaimCheckLabel); err != nil {
			return workflow.RunRef{}, err
		}
		key := u.cfg.By(job)
		if key == "" {
			return next(ctx, job)
		}
		owner, err := newOwnerToken()
		if err != nil {
			return workflow.RunRef{}, err
		}
		ok, err := u.acquire(ctx, key, owner)
		if err != nil {
			return workflow.RunRef{}, err
		}
		if !ok {
			return u.duplicate(ctx, key)
		}
		job.Labels = stampUnique(job.Labels, key, owner)
		ref, err := next(workflow.AllowReservedLabels(ctx, UniqueKeyLabel, UniqueOwnerLabel), job)
		if err != nil {
			_ = u.cfg.Store.Release(context.WithoutCancel(ctx), key, owner)
			return workflow.RunRef{}, err
		}
		if u.track {
			// A failed record only costs the lookup: the lock still holds,
			// and a later duplicate gets ErrDuplicate instead of the ref.
			_ = u.holders.SetHolder(context.WithoutCancel(ctx), key, owner, ref)
		}
		return ref, nil
	}
}

// acquire takes key, taking it over from a holder whose run is in none of
// States.
func (u uniqueWith) acquire(ctx context.Context, key, owner string) (bool, error) {
	ok, err := u.cfg.Store.Acquire(ctx, key, owner, u.cfg.TTL)
	if err != nil || ok || len(u.cfg.States) == 0 {
		return ok, wrapAcquire(err)
	}
	holder, ref, found, err := u.holders.Holder(ctx, key)
	if err != nil || !found {
		return false, wrapAcquire(err)
	}
	st, err := u.cfg.Client.Status(ctx, ref)
	switch {
	case errors.Is(err, workflow.ErrNotFound):
		st = "" // a forgotten holder holds nothing
	case err != nil:
		return false, fmt.Errorf("workflow/middleware: unique holder status: %w", err)
	}
	if slices.Contains(u.cfg.States, st) {
		return false, nil
	}
	if err := u.cfg.Store.Release(ctx, key, holder); err != nil {
		return false, fmt.Errorf("workflow/middleware: unique takeover: %w", err)
	}
	ok, err = u.cfg.Store.Acquire(ctx, key, owner, u.cfg.TTL)
	return ok, wrapAcquire(err)
}

// duplicate answers an enqueue whose key is held.
func (u uniqueWith) duplicate(ctx context.Context, key string) (workflow.RunRef, error) {
	if u.cfg.ReturnExisting {
		_, ref, found, err := u.holders.Holder(ctx, key)
		if err != nil {
			return workflow.RunRef{}, wrapAcquire(err)
		}
		if found {
			workflow.MarkDuplicate(ctx)
			return ref, nil
		}
	}
	return workflow.RunRef{}, fmt.Errorf("%w: unique key %q is held", workflow.ErrDuplicate, key)
}

func wrapAcquire(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("workflow/middleware: unique acquire: %w", err)
}
