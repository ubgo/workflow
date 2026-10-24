package hatchet

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

var (
	_ workflow.RunQuerier    = (*Client)(nil)
	_ workflow.BulkCanceller = (*Client)(nil)
	_ workflow.RunRetrier    = (*Client)(nil)
)

const (
	// cursorPrefix versions the opaque RunPage.Next this driver issues.
	cursorPrefix = "h1:"
	// detailFetches caps the run-detail reads Runs makes at once.
	detailFetches = 8
	// cancelChunk caps the run IDs of one bulk cancel call.
	cancelChunk = 500
)

// listEpoch is the Since of a run query with no From: the run list needs a
// lower bound, and the engine keeps runs only for its retention anyway.
var listEpoch = time.Unix(0, 0).UTC()

// Runs lists runs (workflow.RunQuerier), newest first. It merges two
// engine listings: runs (immediate ones, and delayed starts that fired)
// and scheduled runs that have not fired yet (StatusScheduled). Each run
// is read in full (one detail read per run listed), so the page carries
// the failed step, the attempt count and the waiting status exactly.
//
// On Hatchet, EnqueuedAt is when the engine created the run: for a delayed
// start that fired, its fire time; for one that has not, when it was
// scheduled. From and To filter on that time. Priority is Job.Priority as
// Enqueue stamped it (zero for runs enqueued before the driver stamped
// it). The cursor is an offset into each listing, so runs created while a
// caller pages can make a later page repeat a run of an earlier one. Runs
// of workflows that are not registered on this client are listed too when
// Enqueue of this driver created them (the workflow name is stamped on
// the run), so any process can query.
func (c *Client) Runs(ctx context.Context, q workflow.RunQuery) (workflow.RunPage, error) {
	if err := workflow.ValidateRunQuery(q); err != nil {
		return workflow.RunPage{}, err
	}
	// Label prefixes are not claimed (no CapLabelPrefix): refused, never ignored.
	if err := workflow.CheckRunQuery(c.capabilities(), q); err != nil {
		return workflow.RunPage{}, err
	}
	if err := checkRunStates(q); err != nil {
		return workflow.RunPage{}, err
	}
	sdk, err := c.ready()
	if err != nil {
		return workflow.RunPage{}, err
	}
	if !q.Ref.RunID.IsZero() || !q.Ref.ScheduledID.IsZero() {
		return c.runByRef(ctx, sdk, q)
	}
	offRuns, offSched, err := parseCursor(q.Cursor)
	if err != nil {
		return workflow.RunPage{}, err
	}
	limit := int64(q.PageLimit())
	wantRuns, wantSched, statuses := listedStatuses(q.Statuses)
	filter := metadataFilter(q)

	var runs []rest.V1TaskSummary
	if wantRuns {
		since := listEpoch
		if !q.From.IsZero() {
			since = q.From
		}
		params := rest.V1WorkflowRunListParams{Since: since, Limit: &limit, Offset: &offRuns, Statuses: statuses}
		if !q.To.IsZero() {
			to := q.To
			params.Until = &to
		}
		if len(filter) > 0 {
			and := rest.AND
			params.AdditionalMetadata, params.AdditionalMetadataOperator = &filter, &and
		}
		page, err := sdk.Runs().List(ctx, params)
		if err != nil {
			return workflow.RunPage{}, engineErr(err, "list runs")
		}
		runs = page.Rows
	}
	var scheds []rest.ScheduledWorkflows
	if wantSched {
		field, dir := rest.ScheduledWorkflowsOrderByFieldCreatedAt, rest.WorkflowRunOrderByDirectionDESC
		params := rest.WorkflowScheduledListParams{Limit: &limit, Offset: &offSched, OrderByField: &field, OrderByDirection: &dir}
		if len(filter) > 0 {
			params.AdditionalMetadata = &filter
		}
		page, err := sdk.Schedules().List(ctx, params)
		if err != nil {
			return workflow.RunPage{}, engineErr(err, "list scheduled runs")
		}
		if page.Rows != nil {
			scheds = *page.Rows
		}
	}

	// Merge both listings newest first, consuming at most limit rows, so
	// the cursor can say how far into each one this page reached.
	type pick struct {
		run   *rest.V1TaskSummary
		sched *rest.ScheduledWorkflows
	}
	var picks []pick
	i, j := 0, 0
	for len(picks) < int(limit) && (i < len(runs) || j < len(scheds)) {
		switch {
		case j >= len(scheds) || (i < len(runs) && !runs[i].Metadata.CreatedAt.Before(scheds[j].Metadata.CreatedAt)):
			picks = append(picks, pick{run: &runs[i]})
			i++
		default:
			picks = append(picks, pick{sched: &scheds[j]})
			j++
		}
	}

	infos := make([]*workflow.RunInfo, len(picks))
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	sem := make(chan struct{}, detailFetches)
	for k, p := range picks {
		if p.sched != nil {
			if info, ok := scheduledRunInfo(*p.sched); ok {
				infos[k] = &info
			}
			continue
		}
		id := p.run.WorkflowRunExternalId.String()
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := sdk.Runs().Get(ctx, id)
			if isNotFound(err) || (err == nil && d == nil) {
				return
			}
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = engineErr(err, "read run")
				}
				mu.Unlock()
				return
			}
			if info, ok := c.runInfo(id, d); ok {
				infos[k] = &info
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return workflow.RunPage{}, firstErr
	}

	var page workflow.RunPage
	for _, info := range infos {
		if info != nil && q.Match(*info) {
			page.Runs = append(page.Runs, *info)
		}
	}
	if i < len(runs) || j < len(scheds) || int64(len(runs)) == limit || int64(len(scheds)) == limit {
		page.Next = formatCursor(offRuns+int64(i), offSched+int64(j))
	}
	return page, nil
}

// runByRef answers a query with Ref set by reading that one run, instead
// of paging the listings for it: a RunID is read directly, a ScheduledID
// is looked up as Status and Cancel do (the unfired delayed start, or the
// run it fired). The rest of q filters it; the page has no Next.
func (c *Client) runByRef(ctx context.Context, sdk *hatchetsdk.Client, q workflow.RunQuery) (workflow.RunPage, error) {
	if q.Cursor != "" {
		if _, _, err := parseCursor(q.Cursor); err != nil {
			return workflow.RunPage{}, err
		}
		return workflow.RunPage{}, nil // a single run fits on the first page
	}
	runID, state, err := c.resolve(ctx, sdk, q.Ref)
	if errors.Is(err, workflow.ErrNotFound) {
		return workflow.RunPage{}, nil
	}
	if err != nil {
		return workflow.RunPage{}, err
	}
	var info workflow.RunInfo
	var ok bool
	switch state {
	case statePending:
		sid, _ := parseScheduledID(q.Ref.ScheduledID)
		sched, err := findSchedule(ctx, sdk, sid.token)
		if err != nil || sched == nil {
			return workflow.RunPage{}, err
		}
		info, ok = scheduledRunInfo(*sched)
	case stateRun:
		d, err := runDetailsLagged(ctx, sdk, runID, c.cfg.VisibilityLag)
		if isNotFound(err) {
			return workflow.RunPage{}, nil
		}
		if err != nil {
			return workflow.RunPage{}, engineErr(err, "read run")
		}
		info, ok = c.runInfo(runID, d)
	}
	if !ok || !q.Match(info) {
		return workflow.RunPage{}, nil
	}
	return workflow.RunPage{Runs: []workflow.RunInfo{info}}, nil
}

// checkRunStates refuses a filter on StatusRetrying or StatusDiscarded.
// The engine has neither: a run in retry backoff lists as QUEUED and a
// discarded run as FAILED, so answering such a filter with an empty page
// would tell an operator "none" while many exist.
func checkRunStates(q workflow.RunQuery) error {
	for _, s := range q.Statuses {
		if s == workflow.StatusRetrying || s == workflow.StatusDiscarded {
			return unsupported("RunQuery.Statuses", workflow.CapRunStates, "Hatchet has no "+string(s)+" status: a run in retry backoff reads queued and a discarded run reads failed")
		}
	}
	return nil
}

// listedStatuses maps a query's statuses to the two engine listings:
// whether runs are listed (and with which engine statuses; nil means all)
// and whether unfired scheduled runs are.
func listedStatuses(want []workflow.Status) (runs, sched bool, statuses *[]rest.V1TaskStatus) {
	if len(want) == 0 {
		return true, true, nil
	}
	var out []rest.V1TaskStatus
	add := func(s rest.V1TaskStatus) {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	for _, s := range want {
		switch s {
		case workflow.StatusScheduled:
			sched = true
		case workflow.StatusQueued:
			add(rest.V1TaskStatusQUEUED)
		case workflow.StatusRunning, workflow.StatusWaiting:
			// The engine has no waiting status: a run parked on a wait
			// or sleep step is RUNNING (or QUEUED for a sleep), and Runs
			// tells it apart from the run's tasks.
			add(rest.V1TaskStatusRUNNING)
			add(rest.V1TaskStatusQUEUED)
		case workflow.StatusSucceeded:
			add(rest.V1TaskStatusCOMPLETED)
		case workflow.StatusFailed:
			add(rest.V1TaskStatusFAILED)
		case workflow.StatusCancelled:
			add(rest.V1TaskStatusCANCELLED)
		}
	}
	if len(out) == 0 {
		return false, sched, nil
	}
	return true, sched, &out
}

// metadataFilter is the engine-side metadata filter of q: its labels,
// workflow and queue as "key:value" pairs. A key holding ':' cannot be
// expressed (the engine splits on the first one) and is left to the
// exact filter Runs applies afterwards.
func metadataFilter(q workflow.RunQuery) []string {
	var out []string
	add := func(k, v string) {
		if !strings.Contains(k, ":") {
			out = append(out, k+":"+v)
		}
	}
	for k, v := range q.Labels {
		add(k, v)
	}
	if q.Workflow != "" {
		add(metaWorkflow, q.Workflow)
	}
	if q.Tenant != "" {
		add(metaTenant, q.Tenant)
	}
	// The queue is not filtered on the engine: runs enqueued before the
	// driver stamped queues have none and are in DefaultQueue.
	slices.Sort(out)
	return out
}

// runInfo builds the RunInfo of run id from its details; ok is false for
// a run this driver did not enqueue.
func (c *Client) runInfo(id string, d *rest.V1WorkflowRunDetails) (workflow.RunInfo, bool) {
	md := metadataStrings(d.Run.AdditionalMetadata)
	name := md[metaWorkflow]
	if name == "" {
		return workflow.RunInfo{}, false
	}
	st, err := c.endStatus(d)
	if err != nil {
		return workflow.RunInfo{}, false
	}
	if !st.IsTerminal() && c.suspended(d) {
		st = workflow.StatusWaiting
	}
	c.mu.Lock()
	spec, registered := c.specs[name]
	c.mu.Unlock()
	names := make(map[openapi_types.UUID]string, len(d.Shape))
	for _, sh := range d.Shape {
		names[sh.TaskExternalId] = sh.TaskName
	}
	isStep := func(task string) bool {
		if !registered {
			return task != ""
		}
		return slices.ContainsFunc(spec.Steps, func(s workflow.Step) bool { return s.Name == task })
	}
	info := workflow.RunInfo{
		Ref:      workflow.RunRef{RunID: workflow.RunID(id), ScheduledID: scheduledIDFromMetadata(md)},
		Workflow: name,
		Queue:    runQueue(md),
		Status:   st,
		Labels:   jobLabels(md),
		Priority: runPriorityLabel(md),
	}
	if d.Run.ErrorMessage != nil {
		info.Error = stripFinal(*d.Run.ErrorMessage)
	}
	info.Tenant = md[metaTenant]
	info.RunAt = runAtOf(md)
	if payload, err := decodeEnvelope(runInput(d.Run.Input), payloadField); err == nil {
		info.Payload = payload
	}
	// The run's insert time, which the run listing orders by (CreatedAt
	// is its first task event, a little later).
	info.EnqueuedAt = d.Run.Metadata.CreatedAt
	if d.Run.StartedAt != nil {
		info.StartedAt = *d.Run.StartedAt
	}
	if d.Run.FinishedAt != nil && st.IsTerminal() {
		info.FinishedAt = *d.Run.FinishedAt
	}
	for _, t := range d.Tasks {
		if isStep(names[t.TaskExternalId]) {
			info.Attempts += taskAttempts(t)
		}
	}
	if st == workflow.StatusFailed {
		info.FailedStep = c.failedStep(name, d)
	}
	if st == workflow.StatusSucceeded {
		if out, ok := c.runOutput(name, d); ok {
			info.Output = out
		}
	}
	return info, true
}

// failedAttempt is failedStep plus the attempts that step made (zero when
// no failed step is shown).
func (c *Client) failedAttempt(name string, d *rest.V1WorkflowRunDetails) (string, int) {
	step := c.failedStep(name, d)
	if step == "" {
		return "", 0
	}
	names := make(map[openapi_types.UUID]string, len(d.Shape))
	for _, sh := range d.Shape {
		names[sh.TaskExternalId] = sh.TaskName
	}
	for _, t := range d.Tasks {
		if names[t.TaskExternalId] == step && t.Status == rest.V1TaskStatusFAILED {
			return step, taskAttempts(t)
		}
	}
	return step, 0
}

// failedStep is the first step task of run d (of workflow name) that the
// engine shows FAILED, or empty; the on-failure task is not a step. For a
// workflow not registered here, any named task counts.
func (c *Client) failedStep(name string, d *rest.V1WorkflowRunDetails) string {
	c.mu.Lock()
	spec, registered := c.specs[name]
	c.mu.Unlock()
	names := make(map[openapi_types.UUID]string, len(d.Shape))
	for _, sh := range d.Shape {
		names[sh.TaskExternalId] = sh.TaskName
	}
	for _, t := range d.Tasks {
		task := names[t.TaskExternalId]
		if t.Status != rest.V1TaskStatusFAILED || task == "" {
			continue
		}
		if !registered || slices.ContainsFunc(spec.Steps, func(s workflow.Step) bool { return s.Name == task }) {
			return task
		}
	}
	return ""
}

// runInput is the trigger input inside a run's stored input: the engine
// keeps it under "input", beside the parents' outputs and trigger data
// (checked on hatchet-lite v0.110.5).
func runInput(doc map[string]any) any {
	if in, ok := doc["input"].(map[string]any); ok {
		return in
	}
	return doc
}

// taskAttempts is how many attempts one task has made: none while it is
// still queued for its first, else its retry count plus one.
func taskAttempts(t rest.V1TaskSummary) int {
	retries := 0
	if t.RetryCount != nil {
		retries = *t.RetryCount
	}
	if t.Status == rest.V1TaskStatusQUEUED && retries == 0 && t.StartedAt == nil {
		return 0
	}
	return retries + 1
}

// scheduledRunInfo builds the RunInfo of a delayed start that has not
// fired; ok is false for a scheduled run this driver did not create or
// one that already fired (its run is in the run listing).
func scheduledRunInfo(s rest.ScheduledWorkflows) (workflow.RunInfo, bool) {
	md := metadataStrings(s.AdditionalMetadata)
	sid := scheduledIDFromMetadata(md)
	if md[metaWorkflow] == "" || sid.IsZero() || s.WorkflowRunId != nil {
		return workflow.RunInfo{}, false
	}
	info := workflow.RunInfo{
		Ref:        workflow.RunRef{ScheduledID: sid},
		Workflow:   md[metaWorkflow],
		Queue:      runQueue(md),
		Status:     workflow.StatusScheduled,
		Labels:     jobLabels(md),
		Priority:   runPriorityLabel(md),
		EnqueuedAt: s.Metadata.CreatedAt,
		Tenant:     md[metaTenant],
		RunAt:      s.TriggerAt,
	}
	if s.Input != nil {
		if payload, err := decodeEnvelope(map[string]any(*s.Input), payloadField); err == nil {
			info.Payload = payload
		}
	}
	return info, true
}

// runQueue is the queue Enqueue stamped, or DefaultQueue for a run
// enqueued before the driver stamped queues.
func runQueue(md map[string]string) string {
	if q := md[metaQueue]; q != "" {
		return q
	}
	return workflow.DefaultQueue
}

// runPriorityLabel is the Job.Priority Enqueue stamped (zero when none).
func runPriorityLabel(md map[string]string) int {
	p, _ := strconv.Atoi(md[metaPriority])
	return p
}

// formatCursor encodes the offsets into the two listings.
func formatCursor(runs, sched int64) string {
	raw := strconv.FormatInt(runs, 10) + ":" + strconv.FormatInt(sched, 10)
	return cursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// parseCursor decodes a cursor formatCursor issued; empty is the first
// page. Anything else wraps ErrInvalid.
func parseCursor(cursor string) (runs, sched int64, err error) {
	if cursor == "" {
		return 0, 0, nil
	}
	bad := fmt.Errorf("%w: run query cursor %q was not issued by the Hatchet driver", workflow.ErrInvalid, cursor)
	enc, ok := strings.CutPrefix(cursor, cursorPrefix)
	if !ok {
		return 0, 0, bad
	}
	raw, derr := base64.RawURLEncoding.DecodeString(enc)
	if derr != nil {
		return 0, 0, bad
	}
	a, b, ok := strings.Cut(string(raw), ":")
	if !ok {
		return 0, 0, bad
	}
	runs, err1 := strconv.ParseInt(a, 10, 64)
	sched, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || runs < 0 || sched < 0 {
		return 0, 0, bad
	}
	return runs, sched, nil
}

// CancelAll cancels every run matching q (workflow.BulkCanceller): by
// default the delayed starts that have not fired and the queued runs. It
// lists every match first and then cancels, so cancelling cannot shift
// the listing under it. Delayed starts are cancelled one by one as Cancel
// does (each reported to run-finished observers by this process); runs in
// bulk cancel calls of up to 500. It returns how many it asked the engine
// to cancel; a run that finished between the listing and the cancel is
// counted but left as it ended.
func (c *Client) CancelAll(ctx context.Context, q workflow.RunQuery) (int, error) {
	if err := workflow.ValidateCancelQuery(q); err != nil {
		return 0, err
	}
	// Label prefixes are not claimed (no CapLabelPrefix): refused, never ignored.
	if err := workflow.CheckRunQuery(c.capabilities(), q); err != nil {
		return 0, err
	}
	if err := checkRunStates(q); err != nil {
		return 0, err
	}
	sdk, err := c.ready()
	if err != nil {
		return 0, err
	}
	q.Statuses = workflow.CancelStatuses(q)
	q.Limit = workflow.MaxRunsLimit
	var scheduled []workflow.RunRef
	var ids []openapi_types.UUID
	for {
		page, err := c.Runs(ctx, q)
		if err != nil {
			return 0, err
		}
		for _, r := range page.Runs {
			if r.Ref.RunID.IsZero() {
				scheduled = append(scheduled, r.Ref)
				continue
			}
			if id, err := uuid.Parse(r.Ref.RunID.String()); err == nil {
				ids = append(ids, id)
			}
		}
		if page.Next == "" {
			break
		}
		q.Cursor = page.Next
	}
	n := 0
	for _, ref := range scheduled {
		if err := c.Cancel(ctx, ref); err != nil {
			return n, err
		}
		n++
	}
	for chunk := range slices.Chunk(ids, cancelChunk) {
		if _, err := sdk.Runs().Cancel(ctx, rest.V1CancelTaskRequest{ExternalIds: &chunk}); err != nil {
			return n, engineErr(err, "cancel runs")
		}
		n += len(chunk)
	}
	return n, nil
}

// Retry re-runs a failed run from the steps that failed
// (workflow.RunRetrier), through Hatchet's replay of those tasks: the
// engine replays a task and every task after it, and keeps the output of
// the steps that succeeded, so they do not run again. Every failed or
// cancelled step task of the run is replayed, which covers a branch the
// failure cancelled. The run keeps its RunID.
//
// The idempotency key: Retry refuses with ErrDuplicate when another
// unfinished run carries the same caller key. The engine does not take
// the key again for a replayed run, so an Enqueue with that key while the
// retried run is unfinished starts a second run; this is a known gap of
// the Hatchet driver.
func (c *Client) Retry(ctx context.Context, ref workflow.RunRef) (workflow.RunRef, error) {
	sdk, err := c.ready()
	if err != nil {
		return workflow.RunRef{}, err
	}
	runID, state, err := c.resolve(ctx, sdk, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if state != stateRun {
		return workflow.RunRef{}, fmt.Errorf("%w: run %v has not run, so it has not failed", workflow.ErrWrongState, ref)
	}
	d, err := runDetailsLagged(ctx, sdk, runID, c.cfg.VisibilityLag)
	if isNotFound(err) {
		return workflow.RunRef{}, fmt.Errorf("%w: run %q", workflow.ErrNotFound, runID)
	}
	if err != nil {
		return workflow.RunRef{}, engineErr(err, "read run")
	}
	// endStatus, not the raw status: a run read FAILED a moment before its
	// own retry must not be replayed on top of that retry.
	if st, _ := c.endStatus(d); st != workflow.StatusFailed {
		return workflow.RunRef{}, fmt.Errorf("%w: run %q is %s, not failed", workflow.ErrWrongState, runID, st)
	}
	md := metadataStrings(d.Run.AdditionalMetadata)
	if err := c.checkKeyFree(ctx, sdk, md[metaIdempotencyKey], runID); err != nil {
		return workflow.RunRef{}, err
	}
	tasks := replayTasks(d)
	if len(tasks) == 0 {
		return workflow.RunRef{}, fmt.Errorf("%w: run %q has no failed step to retry", workflow.ErrWrongState, runID)
	}
	if _, err := sdk.Runs().Replay(ctx, rest.V1ReplayTaskRequest{ExternalIds: &tasks}); err != nil {
		return workflow.RunRef{}, engineErr(err, "replay run")
	}
	// The replay is applied asynchronously: wait until the engine no
	// longer reports the run failed, so a Status right after Retry sees it
	// queued or running.
	deadline := time.Now().Add(2 * c.cfg.VisibilityLag)
	for {
		d, err := sdk.Runs().Get(ctx, runID)
		if err == nil && d != nil && d.Run.Status != rest.V1TaskStatusFAILED {
			break
		}
		if time.Now().After(deadline) {
			return workflow.RunRef{}, fmt.Errorf("hatchet driver: replay of run %q not applied within %s", runID, 2*c.cfg.VisibilityLag)
		}
		select {
		case <-ctx.Done():
			return workflow.RunRef{}, ctx.Err()
		case <-time.After(registerPoll):
		}
	}
	return workflow.RunRef{RunID: workflow.RunID(runID), ScheduledID: scheduledIDFromMetadata(md)}, nil
}

// replayTasks is the external IDs of the run's step tasks that failed or
// were cancelled.
func replayTasks(d *rest.V1WorkflowRunDetails) []openapi_types.UUID {
	var out []openapi_types.UUID
	for _, t := range d.Tasks {
		if t.Status == rest.V1TaskStatusFAILED || t.Status == rest.V1TaskStatusCANCELLED {
			out = append(out, t.TaskExternalId)
		}
	}
	return out
}

// checkKeyFree returns ErrDuplicate when a run other than self holds key
// and is unfinished. A per-run key (no caller key) is never shared.
func (c *Client) checkKeyFree(ctx context.Context, sdk *hatchetsdk.Client, key, self string) error {
	if key == "" || !strings.Contains(key, idemKeySep+idemKindCaller+idemKeySep) {
		return nil
	}
	filter := []string{metaIdempotencyKey + ":" + key}
	live := []rest.V1TaskStatus{rest.V1TaskStatusQUEUED, rest.V1TaskStatusRUNNING}
	since := time.Now().Add(-c.cfg.IdempotencyMaxHold)
	rows, err := c.listRuns(ctx, rest.V1WorkflowRunListParams{Since: since, Statuses: &live, AdditionalMetadata: &filter})
	if err != nil {
		return err
	}
	for _, r := range rows {
		if id := r.WorkflowRunExternalId.String(); id != self && metadataString(r.AdditionalMetadata, metaIdempotencyKey) == key {
			return fmt.Errorf("%w: run %s holds the key", workflow.ErrDuplicate, id)
		}
	}
	return nil
}
