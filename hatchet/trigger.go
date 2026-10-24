package hatchet

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"github.com/hatchet-dev/hatchet/pkg/constants"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// The metadata the engine stamps on every run a cron trigger fires
// (internal/services/ticker/cron_v1.go in v0.107.1), beside the trigger's
// own metadata.
var (
	metaCronExpression  = constants.CronExpressionKey.String()
	metaCronName        = constants.CronNameKey.String()
	metaCronScheduledAt = constants.CronScheduledAtKey.String()
)

// DefaultScheduleHistoryWindow is how far back Schedules looks for a
// trigger's last fire when Config.ScheduleHistoryWindow is zero. The
// engine lists runs only from a start time, so a trigger whose last fire
// is older than the window reads as never fired; a week covers every
// cron that fires at least weekly.
const DefaultScheduleHistoryWindow = 7 * 24 * time.Hour

// overlapReason is why OverlapSkip is refused.
const overlapReason = "the Hatchet engine fires a cron tick without asking any worker first and has no setting that skips a tick while the previous run is unfinished"

var _ workflow.ScheduleTriggerer = (*Client)(nil)

// TriggerSchedule starts a run of the cron trigger id now
// (workflow.ScheduleTriggerer): the trigger's workflow, with the
// trigger's input and labels plus workflow.TriggeredLabelsAt, through the
// same path as Enqueue. A declared trigger (Workflow.Cron) has an engine
// ID too, as Schedules lists it, and is triggered the same way. It works
// on a disabled trigger and leaves the next cron fire unchanged. The
// overlap policy needs no check: OverlapSkip is refused at Register and
// Schedule. An unknown ID, or one of another namespace, returns
// workflow.ErrNotFound.
func (c *Client) TriggerSchedule(ctx context.Context, id workflow.ScheduleID) (workflow.RunRef, error) {
	sdk, err := c.ready()
	if err != nil {
		return workflow.RunRef{}, err
	}
	notFound := fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	rows, err := c.cronRows(ctx, sdk, id)
	if err != nil {
		return workflow.RunRef{}, err
	}
	if len(rows) == 0 {
		return workflow.RunRef{}, notFound
	}
	row := &rows[0]
	name, ok := c.localName(row.WorkflowName)
	if !ok {
		return workflow.RunRef{}, notFound
	}
	var input any
	if row.Input != nil {
		input = map[string]any(*row.Input)
	}
	payload, err := decodeEnvelope(input, payloadField)
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("hatchet driver: trigger schedule %s: read its input: %w", id, err)
	}
	// Labelled with the ID Schedules lists, whichever ID the caller
	// passed, so the run is found as the schedule's Last, and with the
	// trigger time as its tick (workflow.CapTickTime).
	labels := workflow.TriggeredLabelsAt(jobLabels(metadataStrings(row.AdditionalMetadata)), c.scheduleIDOf(*row), cronName(*row), c.cfg.Clock.Now())
	return c.enqueue(ctx, workflow.Job{Workflow: name, Payload: payload, Labels: labels, Tenant: metadataString(row.AdditionalMetadata, metaTenant)})
}

// cronName is the name a trigger is listed under: its own name for one
// Schedule made, its expression for a declared one.
func cronName(row rest.CronWorkflows) string {
	if row.Method != rest.CronWorkflowsMethodAPI || row.Name == nil {
		return row.Cron
	}
	return *row.Name
}

// scheduleLast finds the last fire of the trigger row: the newest of the
// newest run the engine fired for it (found by the cron metadata the
// engine stamps) and the newest run TriggerSchedule started for it (found
// by workflow.LabelSchedule), within Config.ScheduleHistoryWindow. Status
// is judged as Status judges it. It costs two run-list calls and one run
// read per trigger.
func (c *Client) scheduleLast(ctx context.Context, sdk *hatchetsdk.Client, row rest.CronWorkflows) (workflow.ScheduleLast, error) {
	since := time.Now().Add(-c.cfg.ScheduleHistoryWindow)
	var filter string
	if row.Method == rest.CronWorkflowsMethodAPI && row.Name != nil {
		filter = metaCronName + ":" + *row.Name
	} else {
		filter = metaCronExpression + ":" + row.Cron
	}
	tick, err := c.newestRun(ctx, sdk, row, since, filter, func(md map[string]string) bool {
		// A declared trigger is found by its expression, which a named
		// trigger of the same workflow may share: only unnamed fires count.
		if row.Method != rest.CronWorkflowsMethodAPI || row.Name == nil {
			_, named := md[metaCronName]
			return md[metaCronExpression] == row.Cron && !named
		}
		return md[metaCronName] == *row.Name
	})
	if err != nil {
		return workflow.ScheduleLast{}, err
	}
	listed := c.scheduleIDOf(row).String()
	manual, err := c.newestRun(ctx, sdk, row, since, workflow.LabelSchedule+":"+listed, func(md map[string]string) bool {
		return md[workflow.LabelSchedule] == listed
	})
	if err != nil {
		return workflow.ScheduleLast{}, err
	}
	last := tick
	if manual.Run.RunID != "" && manual.Fire.After(tick.Fire) {
		last = manual
	}
	if last.Run.RunID == "" {
		return workflow.ScheduleLast{}, nil
	}
	st, err := c.settledStatus(ctx, last.Run.RunID.String())
	if err != nil && !isNotFound(err) {
		return workflow.ScheduleLast{}, err
	}
	if err == nil {
		last.Status = st
	}
	return last, nil
}

// newestRun returns the newest run of row's workflow carrying the
// metadata pair filter and accepted by keep, as a ScheduleLast without
// Status. The engine lists runs newest first, so one page is enough
// unless keep rejects a row.
func (c *Client) newestRun(ctx context.Context, sdk *hatchetsdk.Client, row rest.CronWorkflows, since time.Time, filter string, keep func(map[string]string) bool) (workflow.ScheduleLast, error) {
	params := rest.V1WorkflowRunListParams{
		Since:              since,
		AdditionalMetadata: &[]string{filter},
		IncludePayloads:    new(bool),
	}
	if wfID, perr := uuid.Parse(row.WorkflowId); perr == nil {
		params.WorkflowIds = &[]openapi_types.UUID{openapi_types.UUID(wfID)}
	}
	for offset := int64(0); ; offset += listPage {
		limit, off := listPage, offset
		params.Limit, params.Offset = &limit, &off
		page, err := sdk.Runs().List(ctx, params)
		if err != nil {
			return workflow.ScheduleLast{}, engineErr(err, "list schedule runs")
		}
		rows := slices.Clone(page.Rows)
		slices.SortStableFunc(rows, func(a, b rest.V1TaskSummary) int { return b.CreatedAt.Compare(a.CreatedAt) })
		for _, r := range rows {
			md := metadataStrings(r.AdditionalMetadata)
			if !keep(md) {
				continue
			}
			return workflow.ScheduleLast{
				Fire: fireTime(md, r.CreatedAt),
				Run:  workflow.RunRef{RunID: workflow.RunID(r.WorkflowRunExternalId.String())},
			}, nil
		}
		if int64(len(page.Rows)) < listPage {
			return workflow.ScheduleLast{}, nil
		}
	}
}

// fireTime is when a fire happened: the tick time the engine stamps on a
// cron run, the enqueue time the driver stamps on a triggered one, else
// the run's creation.
func fireTime(md map[string]string, created time.Time) time.Time {
	if t, err := time.Parse(time.RFC3339, md[metaCronScheduledAt]); err == nil {
		return t
	}
	if ms, err := strconv.ParseInt(md[metaEnqueuedAt], 10, 64); err == nil {
		return time.UnixMilli(ms)
	}
	return created
}
