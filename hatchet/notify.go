package hatchet

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
)

// notify.go implements workflow.RunNotifier by polling the engine, and
// deliberately does not claim CapRunFinished.
//
// Why polling: Hatchet has no run-finished callback, and code inside the
// worker (a final task, OnFailure) cannot keep the contract. A final task
// runs before the engine marks the run succeeded, so an observer calling
// Status there would see "running", and no worker code runs at all for a
// run that is cancelled, dropped by a concurrency strategy, or deleted
// before it fired.
//
// Why best-effort: the engine lists runs by creation time only and this
// driver keeps no state outside the process, so a restarted client can
// only replay the ends of runs created within Config.RunFinishedCatchUp
// before it started; anything older that finished while no client was
// polling is not delivered. Every process with an observer polls on its
// own, so with several processes each one delivers every run (at least
// once, as the contract allows).
//
// Which clock: the list windows (since, the catch-up, the window an
// unfinished run is watched for) are compared with the engine's run
// creation times, so they use the wall clock the engine shares. What is
// local to this process uses Config.Clock: the poll ticker, when a run was
// delivered (the dedup map and its pruning), and FinishedAt of a delayed
// start cancelled before it fired, which has no engine timestamp.

// Defaults for the notifier's Config fields.
const (
	// DefaultRunFinishedPoll is how often the notifier asks the engine for
	// run ends.
	DefaultRunFinishedPoll = time.Second
	// DefaultRunFinishedWindow is how old an unfinished run may be and
	// still be watched: runs created earlier are not tracked.
	DefaultRunFinishedWindow = 24 * time.Hour
)

// listPage is the notifier's run-list page size.
const listPage = int64(200)

// listOverlap is how far each poll's terminal scan reaches back before the
// previous poll, to absorb the engine's read lag (a run created just
// before a poll may be listed only after it): twice Config.VisibilityLag.
func listOverlap(lag time.Duration) time.Duration { return 2 * lag }

var (
	_ workflow.RunNotifier = (*Client)(nil)

	terminalStatuses = []rest.V1TaskStatus{rest.V1TaskStatusCOMPLETED, rest.V1TaskStatusFAILED, rest.V1TaskStatusCANCELLED}
	liveStatuses     = []rest.V1TaskStatus{rest.V1TaskStatusQUEUED, rest.V1TaskStatusRUNNING}
)

// OnRunFinished registers fn to observe the end of every run of a
// registered workflow, best-effort (see notify.go). It returns
// ErrAlreadyStarted after Start.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return workflow.ErrAlreadyStarted
	}
	if c.cfg.EnqueueOnly {
		return workflow.Unsupported("Config.EnqueueOnly", "an enqueue-only client runs no run-finished poller; observe run ends on a worker process")
	}
	if fn != nil {
		c.observers = append(c.observers, fn)
	}
	return nil
}

// notifier is the polling loop's state; it lives only while Start's ctx
// does.
type notifier struct {
	c         *Client
	ctx       context.Context
	observers []workflow.RunFinishedFunc

	mu        sync.Mutex
	tracked   map[string]bool      // unfinished runs seen in a poll
	delivered map[string]time.Time // deliveryKey -> when delivered, kept for the window
	since     time.Time            // creation time the next terminal scan starts from
}

// startNotifier begins polling when observers are registered. Called from
// Start with c.mu held.
func (c *Client) startNotifier(ctx context.Context) {
	if len(c.observers) == 0 {
		return
	}
	n := &notifier{
		c:         c,
		ctx:       ctx,
		observers: append([]workflow.RunFinishedFunc(nil), c.observers...),
		tracked:   map[string]bool{},
		delivered: map[string]time.Time{},
		since:     time.Now().Add(-c.cfg.RunFinishedCatchUp - listOverlap(c.cfg.VisibilityLag)),
	}
	c.notify = n
	go n.loop()
}

func (n *notifier) loop() {
	for {
		n.poll()
		if !sleepCtx(n.ctx, n.c.cfg.Clock, n.c.cfg.RunFinishedPoll) {
			return
		}
	}
}

// sleepCtx waits d on clk and reports true, or false as soon as ctx ends.
func sleepCtx(ctx context.Context, clk clock.Clock, d time.Duration) bool {
	t := clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C():
		return true
	}
}

// poll does one round: track unfinished runs, deliver the ones that ended
// since the last round, and deliver runs created and finished between
// rounds. Errors are logged and leave the state as it was, so the next
// round retries.
func (n *notifier) poll() {
	now := time.Now() // wall clock: compared with engine creation times
	live, err := n.list(liveStatuses, now.Add(-n.c.cfg.RunFinishedWindow))
	if err != nil {
		n.c.cfg.Logger.ErrorContext(n.ctx, "hatchet driver: run-finished poll: list live runs", "error", err)
		return
	}
	stillLive := map[string]bool{}
	for _, r := range live {
		if n.ours(r.AdditionalMetadata) {
			stillLive[r.WorkflowRunExternalId.String()] = true
		}
	}
	n.mu.Lock()
	var ended []string
	for id := range n.tracked {
		if !stillLive[id] {
			ended = append(ended, id)
		}
	}
	for id := range stillLive {
		n.tracked[id] = true
	}
	since := n.since
	n.mu.Unlock()

	for _, id := range ended {
		d, err := n.c.sdk.Runs().Get(n.ctx, id)
		if isNotFound(err) {
			n.untrack(id)
			continue
		}
		if err != nil || d == nil {
			if err == nil {
				err = errors.New("engine returned no run")
			}
			n.c.cfg.Logger.ErrorContext(n.ctx, "hatchet driver: run-finished poll: read ended run", "run", id, "error", err)
			continue
		}
		st, err := n.c.endStatus(d)
		if err != nil {
			continue
		}
		if ev, ok := n.event(id, st, d.Run.AdditionalMetadata, d.Run.ErrorMessage, d.Run.FinishedAt); ok {
			n.deliver(id, n.c.withFailure(ev, d))
		}
	}

	done, err := n.list(terminalStatuses, since)
	if err != nil {
		n.c.cfg.Logger.ErrorContext(n.ctx, "hatchet driver: run-finished poll: list finished runs", "error", err)
		return
	}
	for _, r := range done {
		id := r.WorkflowRunExternalId.String()
		if r.Status == rest.V1TaskStatusFAILED {
			// A FAILED row may be the moment before a retry: read the
			// run and let endStatus judge it (failed.go), once per end.
			if n.seen(id, r.FinishedAt) || !n.ours(r.AdditionalMetadata) {
				continue
			}
			st, d, err := n.c.readEndStatus(n.ctx, n.c.sdk, id)
			if err != nil {
				n.c.cfg.Logger.ErrorContext(n.ctx, "hatchet driver: run-finished poll: read failed run", "run", id, "error", err)
				continue
			}
			if ev, ok := n.event(id, st, d.Run.AdditionalMetadata, d.Run.ErrorMessage, d.Run.FinishedAt); ok {
				n.deliver(id, n.c.withFailure(ev, d))
			}
			continue
		}
		st, err := runStatus(r.Status)
		if err != nil {
			continue
		}
		if ev, ok := n.event(id, st, r.AdditionalMetadata, r.ErrorMessage, r.FinishedAt); ok {
			n.deliver(id, ev)
		}
	}
	local := n.c.cfg.Clock.Now()
	n.mu.Lock()
	n.since = now.Add(-listOverlap(n.c.cfg.VisibilityLag))
	for id, at := range n.delivered {
		if local.Sub(at) > n.c.cfg.RunFinishedWindow {
			delete(n.delivered, id)
		}
	}
	n.mu.Unlock()
}

// list returns the runs created since since in the given statuses.
func (n *notifier) list(statuses []rest.V1TaskStatus, since time.Time) ([]rest.V1TaskSummary, error) {
	return n.c.listRuns(n.ctx, rest.V1WorkflowRunListParams{Since: since, Statuses: &statuses})
}

// ours reports whether a run belongs to a workflow registered on this
// client (the name Enqueue stamps).
func (n *notifier) ours(md *map[string]any) bool {
	name := metadataString(md, metaWorkflow)
	n.c.mu.Lock()
	defer n.c.mu.Unlock()
	_, ok := n.c.specs[name]
	return name != "" && ok
}

func (n *notifier) untrack(id string) {
	n.mu.Lock()
	delete(n.tracked, id)
	n.mu.Unlock()
}

// event builds the RunEvent for a terminal run of ours; ok is false for a
// live run or another app's run. st is the run's status as endStatus
// judged it (a FAILED engine status only reaches here through endStatus).
func (n *notifier) event(id string, st workflow.Status, md *map[string]any, errMsg *string, finished *time.Time) (workflow.RunEvent, bool) {
	if !st.IsTerminal() || !n.ours(md) {
		return workflow.RunEvent{}, false
	}
	labels := metadataStrings(md)
	ev := workflow.RunEvent{
		Ref:      workflow.RunRef{RunID: workflow.RunID(id), ScheduledID: scheduledIDFromMetadata(labels)},
		Workflow: labels[metaWorkflow],
		Status:   st,
		Err:      runError(st, errMsg),
		Labels:   jobLabels(labels),
		Tenant:   labels[metaTenant],
	}
	if finished != nil {
		ev.FinishedAt = *finished
	}
	return ev, true
}

// runError rebuilds a run's error from the engine's message: ErrCancelled
// for a cancelled run, the stored message for a failed one.
func runError(st workflow.Status, msg *string) error {
	switch st {
	case workflow.StatusCancelled:
		return workflow.ErrCancelled
	case workflow.StatusFailed:
		if msg != nil && *msg != "" {
			return errors.New(stripFinal(*msg))
		}
		return errors.New("hatchet driver: run failed")
	}
	return nil
}

// withFailure completes the event of a failed run from its details d:
// Step names the failed step, and Err is wrapped in a *workflow.RunError
// naming the run, workflow, step and attempt (DX-15), so an observer that
// only logs the error still knows which run it was. Other events are
// returned unchanged.
func (c *Client) withFailure(ev workflow.RunEvent, d *rest.V1WorkflowRunDetails) workflow.RunEvent {
	if ev.Status != workflow.StatusFailed {
		return ev
	}
	step, attempt := c.failedAttempt(ev.Workflow, d)
	ev.Step = step
	ev.Err = &workflow.RunError{RunID: ev.Ref.RunID, Workflow: ev.Workflow, Step: step, Attempt: attempt, Err: ev.Err}
	return ev
}

// deliver hands ev to every observer once per process and end (the
// engine may list a run twice across the overlap). A run that ends again
// after Retry has a new FinishedAt, so it is delivered again.
func (n *notifier) deliver(id string, ev workflow.RunEvent) {
	key := deliveryKey(id, ev.FinishedAt)
	n.mu.Lock()
	delete(n.tracked, id)
	if _, seen := n.delivered[key]; seen {
		n.mu.Unlock()
		return
	}
	n.delivered[key] = n.c.cfg.Clock.Now()
	n.mu.Unlock()
	n.call(ev)
}

// seen reports whether the end of run id that finished at finished was
// delivered already.
func (n *notifier) seen(id string, finished *time.Time) bool {
	var at time.Time
	if finished != nil {
		at = *finished
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.delivered[deliveryKey(id, at)]
	return ok
}

// deliveryKey tells one end of a run from another: the run ID, plus the
// engine's finish time when it has one.
func deliveryKey(id string, finished time.Time) string {
	if finished.IsZero() {
		return id
	}
	return id + "@" + strconv.FormatInt(finished.UnixNano(), 10)
}

// call runs the observers in registration order, each behind a recover so
// a panicking observer never stops the others or the loop; the panic is
// logged.
func (n *notifier) call(ev workflow.RunEvent) {
	for _, fn := range n.observers {
		func() {
			defer func() {
				if p := recover(); p != nil {
					n.c.cfg.Logger.ErrorContext(n.ctx, "hatchet driver: run-finished observer panicked", "run", ev.Ref.RunID.String(), "workflow", ev.Workflow, "panic", p)
				}
			}()
			fn(n.ctx, ev)
		}()
	}
}

// cancelledBeforeFire reports a delayed start Cancel deleted before it
// fired. No engine run exists for it, so only the process that cancelled
// it can report it.
func (c *Client) cancelledBeforeFire(ref workflow.RunRef, sched *rest.ScheduledWorkflows) {
	c.mu.Lock()
	n := c.notify
	c.mu.Unlock()
	if n == nil {
		return
	}
	labels := metadataStrings(sched.AdditionalMetadata)
	n.call(workflow.RunEvent{
		Ref:        workflow.RunRef{ScheduledID: ref.ScheduledID},
		Workflow:   labels[metaWorkflow],
		Status:     workflow.StatusCancelled,
		Err:        workflow.ErrCancelled,
		Labels:     jobLabels(labels),
		Tenant:     labels[metaTenant],
		FinishedAt: c.cfg.Clock.Now(),
	})
}

// metadataStrings flattens REST metadata to the string map runs carry.
func metadataStrings(md *map[string]any) map[string]string {
	out := map[string]string{}
	if md == nil {
		return out
	}
	for k, v := range *md {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// metadataString reads one string key from REST metadata.
func metadataString(md *map[string]any, key string) string {
	if md == nil {
		return ""
	}
	s, _ := (*md)[key].(string)
	return s
}
