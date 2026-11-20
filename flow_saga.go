package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// ErrSagaAborted is returned by Saga.Do once the saga started compensating
// (in this attempt or an earlier one): no new action runs after an abort.
var ErrSagaAborted = errors.New("workflow: saga aborted")

// ErrCompensated matches the error Saga.Abort ends with after every
// compensation ran: the step's work was rolled back, so the error is
// Permanent and the step is not retried.
var ErrCompensated = errors.New("workflow: saga compensated")

// Saga is a sequence of actions inside one step, each with a compensation
// that undoes it (CM-12, WX-16): when a later action fails, Abort runs the
// compensations of the actions that finished, newest first. Every action
// and every compensation is memoised through the journal (Memo), so a
// retried step never repeats an action that finished, never runs an
// action after the saga aborted, and never runs a finished compensation
// twice: a compensation that fails is retried on the step's next attempt,
// from where it stopped.
//
// It needs the Journal plugin (as Memo does) and runs on every driver; a
// durable JournalStore makes it survive a crash. Write the step so it
// calls Do for the same actions in the same order on every attempt (names
// are stable), and return Abort's error when an action fails:
//
//	saga := workflow.NewSaga("trip")
//	if err := saga.Do(ctx, "flight", bookFlight, cancelFlight); err != nil {
//		return nil, saga.Abort(ctx, err)
//	}
//
// A Saga value belongs to one attempt of one step; build a new one in each
// call of the handler.
type Saga struct {
	name string

	mu   sync.Mutex
	done []sagaAction
}

// sagaAction is a finished action and its compensation.
type sagaAction struct {
	name string
	undo func(ctx context.Context) error
}

// sagaAbortData is the run data name that marks an aborted saga; it
// keeps the cause, so a later attempt reports it.
func sagaAbortData(saga string) string { return "ubgo.workflow/saga/" + saga + "/aborted" }

// NewSaga returns a saga named name, unique within the step.
func NewSaga(name string) *Saga { return &Saga{name: name} }

// Do runs do once per run under action (memoised) and remembers undo as
// its compensation. It returns do's error. Once the saga has been aborted
// it calls nothing: an action that had finished returns nil again, and
// the first one that had not returns ErrSagaAborted (Permanent), so the
// step reaches its Abort call with every finished action known. A nil undo means the
// action needs no compensation.
func (s *Saga) Do(ctx context.Context, action string, do, undo func(ctx context.Context) error) error {
	if s.name == "" || action == "" || do == nil {
		return Permanent(invalidf("saga: a name, an action name and a do function are required"))
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return err
	}
	key, err := j.claim("saga/" + s.name + "/do/" + action)
	if err != nil {
		return err
	}
	_, aborted, err := RunData[string](ctx, sagaAbortData(s.name))
	if err != nil {
		return err
	}
	if aborted {
		// An action that finished before the abort reports success again
		// (as Memo would), so the step's code walks on to the first
		// action that never ran, which is refused; by then every finished
		// action is remembered and Abort can finish the compensation an
		// earlier attempt began.
		_, ok, err := j.store.Load(ctx, j.run, key)
		if err != nil {
			return fmt.Errorf("workflow: saga %q: load %q: %w", s.name, action, err)
		}
		if ok {
			s.remember(action, undo)
			return nil
		}
		return Permanent(fmt.Errorf("%w: saga %q, action %q not started", ErrSagaAborted, s.name, action))
	}
	if _, err := memoKey(ctx, j, key, action, func(ctx context.Context) (bool, error) {
		return true, do(ctx)
	}); err != nil {
		return err
	}
	s.remember(action, undo)
	return nil
}

func (s *Saga) remember(action string, undo func(ctx context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = append(s.done, sagaAction{name: action, undo: undo})
}

// Done returns the names of the actions that finished, in order.
func (s *Saga) Done() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.done))
	for i, a := range s.done {
		out[i] = a.name
	}
	return out
}

// Abort marks the saga aborted, then runs the compensation of every
// finished action, newest first, each memoised. When one fails it returns
// that error (not Permanent), so the step is retried and the compensation
// resumes on the next attempt. When all ran it returns a Permanent error
// matching ErrCompensated and cause. The cause given on the first abort is
// the one kept; a later attempt's cause (usually ErrSagaAborted) is
// replaced by it.
func (s *Saga) Abort(ctx context.Context, cause error) error {
	if cause == nil {
		cause = errors.New("aborted")
	}
	msg, aborted, err := RunData[string](ctx, sagaAbortData(s.name))
	if err != nil {
		return err
	}
	if !aborted {
		msg = cause.Error()
		if err := SetRunData(ctx, sagaAbortData(s.name), msg); err != nil {
			return err
		}
	}
	s.mu.Lock()
	done := slices.Clone(s.done)
	s.mu.Unlock()
	for i := len(done) - 1; i >= 0; i-- {
		a := done[i]
		if a.undo == nil {
			continue
		}
		if _, err := Memo(ctx, "saga/"+s.name+"/undo/"+a.name, func(ctx context.Context) (bool, error) {
			return true, a.undo(ctx)
		}); err != nil {
			return fmt.Errorf("workflow: saga %q: compensate %q: %w", s.name, a.name, err)
		}
	}
	return Permanent(fmt.Errorf("%w: saga %q: %s", ErrCompensated, s.name, msg))
}
