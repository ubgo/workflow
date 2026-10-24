package hatchet

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// batchListLag widens each batch's run search before its CreatedAt, for
// clock skew between this process and the engine.
const batchListLag = time.Minute

// BatchSource returns a batch.Source that learns a batch's job ends from
// the engine's run list: one Runs().List call per unfinished batch each
// interval, filtered by the batch's batch.LabelBatch label and the
// terminal statuses, instead of batch.PollSource's one Client.Status call
// per unfinished job. Pass it to batch.WithCompletionSource, with
// batch.HookSource() for speed when wanted.
//
// store must be the Store the Manager uses: the source reads the
// unfinished batches from it. A job whose ref holds only a ScheduledID (a
// delayed start that has not fired) has no engine run to list, so the
// source asks Client.Status for that one job; that is how a delayed start
// cancelled before it fired is found. A List error is logged and retried
// on the next tick, never guessed into a status. Each tick also calls
// Manager.RetryCallbacks, as batch.PollSource does.
//
// A nil log means Config.Logger. Ticks come from Config.Clock, so a test
// on a fake clock drives them with Advance. Its Attach fails with
// workflow.ErrInvalid for an interval that is not positive.
func (c *Client) BatchSource(store batch.Store, interval time.Duration, log *slog.Logger) batch.Source {
	if log == nil {
		log = c.cfg.Logger
	}
	return &batchSource{c: c, store: store, interval: interval, log: log}
}

type batchSource struct {
	c        *Client
	store    batch.Store
	interval time.Duration
	log      *slog.Logger
}

func (s *batchSource) Attach(ctx context.Context, m *batch.Manager) error {
	if s.interval <= 0 {
		return fmt.Errorf("%w: hatchet BatchSource interval must be positive, got %s", workflow.ErrInvalid, s.interval)
	}
	if s.store == nil {
		return fmt.Errorf("%w: hatchet BatchSource needs the Manager's Store", workflow.ErrInvalid)
	}
	go s.loop(ctx, m)
	return nil
}

// loop polls the engine every interval of Config.Clock, as the run-end
// notifier does.
func (s *batchSource) loop(ctx context.Context, m *batch.Manager) {
	for {
		if !sleepCtx(ctx, s.c.cfg.Clock, s.interval) {
			return
		}
		s.poll(ctx, m)
	}
}

// poll is one tick: every unfinished batch with a job still open.
func (s *batchSource) poll(ctx context.Context, m *batch.Manager) {
	if _, err := s.c.ready(); err != nil {
		return
	}
	open, err := s.store.Unfinished(ctx)
	if err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: list unfinished batches", "error", err)
		return
	}
	for _, st := range open {
		s.pollBatch(ctx, m, st)
	}
	// As batch.PollSource does: a callback whose enqueue failed is retried
	// here, since a finished batch gets no further job end to retry it.
	if err := m.RetryCallbacks(ctx); err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: retry callbacks", "error", err)
	}
	// A dispatch whose process died part-way is finished here, once its
	// lease has ended (batch.Manager.ResumeDispatches).
	if err := m.ResumeDispatches(ctx); err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: resume dispatches", "error", err)
	}
}

func (s *batchSource) pollBatch(ctx context.Context, m *batch.Manager, st batch.State) {
	if st.Coordinated {
		s.pollCoordinated(ctx, m, st)
		return
	}
	listed := false
	for i, j := range st.Jobs {
		if j.Status.IsTerminal() || j.Ref.IsZero() {
			continue
		}
		if j.Ref.RunID == "" {
			// A delayed start not yet seen as a run: only Status knows it.
			s.status(ctx, m, st, i, j.Ref)
			continue
		}
		if !listed {
			listed = true
			s.list(ctx, m, st)
		}
	}
}

// FindsJobsByLabel marks the source as a batch.LabelSource: it learns job
// ends from the runs' labels, so a coordinated batch, whose children have
// no refs, can use it.
func (s *batchSource) FindsJobsByLabel() {}

// pollCoordinated is one tick for a batch started through the coordinator
// (Config.BatchCoordinator), whose jobs have no refs: it reports the ended
// children by label; while the batch is cancelled it cancels the group
// again, so a child started late by an in-flight trigger is still
// cancelled; and once the coordinator has ended and the run list has
// caught up, it records every job the engine never listed (the
// coordinator was cancelled, or ran out of retries) as cancelled.
func (s *batchSource) pollCoordinated(ctx context.Context, m *batch.Manager, st batch.State) {
	if !st.Dispatched {
		return // the dispatch, or its resume, is still in flight
	}
	s.list(ctx, m, st)
	if st.Cancelled {
		if err := s.c.CancelGroup(ctx, st.ID.String(), st.Parent, st.CreatedAt); err != nil {
			s.log.ErrorContext(ctx, "hatchet batch source: cancel group", "batch", st.ID.String(), "error", err)
		}
	}
	if st.Parent.IsZero() {
		return // a spawn that failed: Dispatch recorded every job already
	}
	ended, err := s.c.parentEnded(ctx, st.Parent)
	if err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: coordinator status", "batch", st.ID.String(), "error", err)
		return
	}
	// Wall clock: ended is the engine's timestamp.
	if !coordinatorSettled(ended, time.Now(), s.c.cfg.VisibilityLag) {
		return
	}
	found, err := s.c.FindBatchRuns(ctx, st.ID, st.CreatedAt)
	if err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: find children", "batch", st.ID.String(), "error", err)
		return
	}
	missing := unstartedChildren(st, found)
	for _, i := range missing {
		if err := m.RunFinished(ctx, jobEndLabels(st, i), workflow.StatusCancelled); err != nil {
			s.log.ErrorContext(ctx, "hatchet batch source: record unstarted child", "batch", st.ID.String(), "job", i, "error", err)
		}
	}
	if len(missing) > 0 {
		s.log.WarnContext(ctx, "hatchet batch source: coordinator ended without starting these jobs; recorded as cancelled", "batch", st.ID.String(), "jobs", missing)
	}
}

// coordinatorSettled reports whether a coordinator that ended at ended
// (zero while it runs) ended long enough before now for the run list to
// show every child it started (coordinatorSettle of lag). Only then may
// the jobs it does not list be taken as never started.
func coordinatorSettled(ended, now time.Time, lag time.Duration) bool {
	return !ended.IsZero() && now.Sub(ended) >= coordinatorSettle(lag)
}

// unstartedChildren returns, in index order, the jobs of a coordinated
// batch that have no recorded status and that found (FindBatchRuns, after
// the coordinator settled) does not list: the children the coordinator
// never started, which pollCoordinated records as cancelled.
func unstartedChildren(st batch.State, found map[int]workflow.RunRef) []int {
	var out []int
	for i, j := range st.Jobs {
		if _, ok := found[i]; ok || j.Status.IsTerminal() {
			continue
		}
		out = append(out, i)
	}
	return out
}

// parentEnded returns when the coordinator run finished, or the zero time
// while it has not (or the engine gives no finish time yet).
func (c *Client) parentEnded(ctx context.Context, parent workflow.RunRef) (time.Time, error) {
	sdk, err := c.ready()
	if err != nil {
		return time.Time{}, err
	}
	status, d, err := c.readEndStatus(ctx, sdk, parent.RunID.String())
	if err != nil {
		return time.Time{}, engineErr(err, "get coordinator run")
	}
	if !status.IsTerminal() || d.Run.FinishedAt == nil {
		return time.Time{}, nil
	}
	return *d.Run.FinishedAt, nil
}

// list reports every ended run labelled with the batch's ID.
func (s *batchSource) list(ctx context.Context, m *batch.Manager, st batch.State) {
	rows, err := s.c.listRuns(ctx, rest.V1WorkflowRunListParams{
		Since:              st.CreatedAt.Add(-batchListLag),
		Statuses:           &terminalStatuses,
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + st.ID.String()},
	})
	if err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: list runs", "batch", st.ID.String(), "error", err)
		return
	}
	for _, r := range rows {
		status, err := runStatus(r.Status)
		if err != nil || !status.IsTerminal() {
			continue
		}
		if r.Status == rest.V1TaskStatusFAILED {
			// A FAILED row may be the moment before a retry: endStatus
			// judges it (failed.go). A job already recorded needs no
			// second look.
			if jobRecorded(st, r.AdditionalMetadata) {
				continue
			}
			if status, err = s.c.settledStatus(ctx, r.WorkflowRunExternalId.String()); err != nil {
				s.log.ErrorContext(ctx, "hatchet batch source: read failed run", "batch", st.ID.String(), "error", err)
				continue
			}
			if !status.IsTerminal() {
				continue
			}
		}
		if err := m.RunFinished(ctx, jobLabels(metadataStrings(r.AdditionalMetadata)), status); err != nil {
			s.log.ErrorContext(ctx, "hatchet batch source: record job end", "batch", st.ID.String(), "error", err)
		}
	}
}

// status asks Client.Status for one job and reports it when terminal.
func (s *batchSource) status(ctx context.Context, m *batch.Manager, st batch.State, idx int, ref workflow.RunRef) {
	status, err := s.c.Status(ctx, ref)
	if err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: run status", "batch", st.ID.String(), "job", idx, "error", err)
		return
	}
	if err := m.RunFinished(ctx, jobEndLabels(st, idx), status); err != nil {
		s.log.ErrorContext(ctx, "hatchet batch source: record job end", "batch", st.ID.String(), "job", idx, "error", err)
	}
}

// jobEndLabels are the labels a job end is reported with: the batch, the
// index and the job's current attempt (batch.JobLabels), so a report for
// a job Manager.RetryFailed restarted counts against the new run, not the
// one it replaced.
func jobEndLabels(st batch.State, idx int) map[string]string {
	return batch.JobLabels(st.ID, idx, st.Jobs[idx].Attempt)
}

var (
	_ batch.RunFinder   = (*Client)(nil)
	_ batch.LabelSource = (*batchSource)(nil)
)

// FindBatchRuns implements batch.RunFinder: one paged run-list call,
// filtered by the batch's batch.LabelBatch label and no status, so a
// resumed dispatch learns which jobs the dead process started (the runs
// of a bulk trigger whose refs were never saved). A delayed start that
// has not fired is a schedule, not a run, and is not listed; the batch
// package never restarts such a job on this answer alone.
func (c *Client) FindBatchRuns(ctx context.Context, id batch.ID, since time.Time) (map[int]workflow.RunRef, error) {
	noPayloads := false
	rows, err := c.listRuns(ctx, rest.V1WorkflowRunListParams{
		Since:              since.Add(-batchListLag),
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + id.String()},
		IncludePayloads:    &noPayloads,
	})
	if err != nil {
		return nil, err
	}
	out := map[int]workflow.RunRef{}
	for _, r := range rows {
		md := metadataStrings(r.AdditionalMetadata)
		if md[batch.LabelBatch] != id.String() {
			continue
		}
		idx, err := strconv.Atoi(md[batch.LabelIndex])
		if err != nil {
			continue
		}
		out[idx] = workflow.RunRef{RunID: workflow.RunID(r.WorkflowRunExternalId.String())}
	}
	return out, nil
}

// listRuns pages through the runs matching params (Limit and Offset are
// set here).
func (c *Client) listRuns(ctx context.Context, params rest.V1WorkflowRunListParams) ([]rest.V1TaskSummary, error) {
	sdk, err := c.ready()
	if err != nil {
		return nil, err
	}
	var out []rest.V1TaskSummary
	for offset := int64(0); ; offset += listPage {
		limit, off := listPage, offset
		params.Limit, params.Offset = &limit, &off
		page, err := sdk.Runs().List(ctx, params)
		if err != nil {
			return nil, engineErr(err, "list runs")
		}
		out = append(out, page.Rows...)
		if int64(len(page.Rows)) < listPage {
			return out, nil
		}
	}
}

// jobRecorded reports whether the batch job a run's labels name already
// has a terminal status in st.
func jobRecorded(st batch.State, md *map[string]any) bool {
	idx, err := strconv.Atoi(metadataString(md, batch.LabelIndex))
	return err == nil && idx >= 0 && idx < len(st.Jobs) && st.Jobs[idx].Status.IsTerminal()
}

// settledStatus reads run id and judges it with endStatus (failed.go).
func (c *Client) settledStatus(ctx context.Context, id string) (workflow.Status, error) {
	sdk, err := c.ready()
	if err != nil {
		return "", err
	}
	st, _, err := c.readEndStatus(ctx, sdk, id)
	if err != nil {
		return "", engineErr(err, "read run")
	}
	return st, nil
}
