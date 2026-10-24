package hatchet

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
)

var _ workflow.ScheduleLister = (*Client)(nil)

// Schedules lists the engine's cron triggers (workflow.ScheduleLister):
// the declared ones (Workflow.Cron, Declared true, named by their
// expression) and the ones Schedule made, of every workflow in this
// client's namespace, whichever process declared or made them. A trigger
// made in Hatchet's dashboard or by another client of the namespace is
// listed too: the engine does not record who made a trigger. ID is the
// named schedule's ID for a trigger made by a named Schedule (stable
// across replacements), else the engine's trigger ID; a declared trigger
// gets a new one when its workflow is declared again with a changed
// definition. Location is
// always UTC, the only zone Hatchet runs crons in; Next is computed with
// the core cron parser on Config.Clock. A trigger disabled in the
// dashboard is listed with Paused true and no Next. Last
// (workflow.CapScheduleHistory) is read from the engine's run list, not
// stored by the driver: the newest run the engine fired for the trigger
// or TriggerSchedule started for it within Config.ScheduleHistoryWindow,
// with its status as Status reads it; this costs two run-list calls and
// one run read per trigger.
func (c *Client) Schedules(ctx context.Context) ([]workflow.ScheduleInfo, error) {
	sdk, err := c.ready()
	if err != nil {
		return nil, err
	}
	rows, err := c.listCrons(ctx, sdk, rest.CronWorkflowListParams{})
	if err != nil {
		return nil, err
	}
	now := c.cfg.Clock.Now()
	var out []workflow.ScheduleInfo
	for _, row := range rows {
		name, ok := c.localName(row.WorkflowName)
		if !ok {
			continue
		}
		info := workflow.ScheduleInfo{
			ID:       c.scheduleIDOf(row),
			Name:     row.Cron,
			Workflow: name,
			Cron:     row.Cron,
			Location: time.UTC,
			Declared: row.Method != rest.CronWorkflowsMethodAPI,
			Paused:   !row.Enabled,
			Labels:   jobLabels(metadataStrings(row.AdditionalMetadata)),
			Tenant:   metadataString(row.AdditionalMetadata, metaTenant),
		}
		if !info.Declared && row.Name != nil {
			info.Name = *row.Name
		}
		// The engine tells a seconds expression by its six fields.
		info.Seconds = len(strings.Fields(row.Cron)) == cronSecondsFields
		if sched, err := workflow.ParseCron(row.Cron, info.Seconds, nil); err == nil && !info.Paused {
			info.Next = sched.Next(now, time.UTC)
		}
		if info.Last, err = c.scheduleLast(ctx, sdk, row); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b workflow.ScheduleInfo) int {
		return cmp.Or(cmp.Compare(a.Workflow, b.Workflow), cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// localName is the workflow name behind an engine-side name, and false
// for a workflow of another namespace.
func (c *Client) localName(engine string) (string, bool) {
	if c.cfg.Namespace == "" {
		return engine, true
	}
	return strings.CutPrefix(engine, engineName(c.cfg.Namespace, ""))
}

// cronSecondsFields is the field count of an expression with seconds.
const cronSecondsFields = 6

// cronListPage is the page size of a cron trigger listing.
const cronListPage int64 = 200

// replacedCron is what replaceNamedCron removed.
type replacedCron struct {
	// disabled is true when a removed trigger was disabled, so the
	// replacement is disabled too.
	disabled bool
}

// replaceNamedCron makes s.Name name at most one cron trigger of s's
// workflow. It reports found true when a trigger of that name already
// holds s's definition, or deletes every trigger of that name whose
// definition differs and reports found false, so the caller creates the
// replacement.
func (c *Client) replaceNamedCron(ctx context.Context, sdk *hatchetsdk.Client, s workflow.Schedule) (replacedCron, bool, error) {
	var out replacedCron
	rows, err := c.listCrons(ctx, sdk, rest.CronWorkflowListParams{CronName: &s.Name})
	if err != nil {
		return out, false, err
	}
	found := false
	for _, row := range rows {
		if row.WorkflowName != engineName(c.cfg.Namespace, s.Workflow) || row.Name == nil || *row.Name != s.Name {
			continue
		}
		if !found && sameCron(row, s) {
			found = true
			continue
		}
		out.disabled = out.disabled || !row.Enabled
		if err := c.deleteCron(ctx, sdk, row.Metadata.Id); err != nil {
			return out, false, err
		}
	}
	return out, found, nil
}

// namedIDPrefix starts the ScheduleID of a named schedule. Hatchet cannot
// change a trigger's expression, input or labels in place, so a changed
// named schedule becomes a new engine trigger; its ScheduleID names the
// (workflow, name) pair instead of the engine's trigger ID, so it stays
// the same across replacements, as on every other driver.
const namedIDPrefix = "named:"

// metaNamed marks a trigger Schedule made for a named Schedule, so
// Schedules lists it under its named ID. A trigger made before the marker
// existed fails sameCron and is replaced by the next Schedule call.
const metaNamed = metadataPrefix + "named"

// namedSep separates the workflow and the name inside a named ID; neither
// can hold it.
const namedSep = "\x00"

// namedScheduleID is the ScheduleID of the named schedule name of
// workflow wf.
func namedScheduleID(wf, name string) workflow.ScheduleID {
	return workflow.ScheduleID(namedIDPrefix + base64.RawURLEncoding.EncodeToString([]byte(wf+namedSep+name)))
}

// parseNamedID returns the workflow and name a named ScheduleID holds.
func parseNamedID(id workflow.ScheduleID) (wf, name string, ok bool) {
	enc, ok := strings.CutPrefix(id.String(), namedIDPrefix)
	if !ok {
		return "", "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return "", "", false
	}
	wf, name, ok = strings.Cut(string(raw), namedSep)
	return wf, name, ok && wf != "" && name != ""
}

// scheduleIDOf is the ScheduleID row is listed under: the named ID for a
// trigger made for a named Schedule, else the engine's trigger ID.
func (c *Client) scheduleIDOf(row rest.CronWorkflows) workflow.ScheduleID {
	if row.Method == rest.CronWorkflowsMethodAPI && row.Name != nil && metadataString(row.AdditionalMetadata, metaNamed) != "" {
		if wf, ok := c.localName(row.WorkflowName); ok {
			return namedScheduleID(wf, *row.Name)
		}
	}
	return workflow.ScheduleID(row.Metadata.Id)
}

// cronRows returns the triggers behind id: those of the name for a named
// ID (normally one), the one trigger for an engine ID, none for an
// unknown or malformed ID.
func (c *Client) cronRows(ctx context.Context, sdk *hatchetsdk.Client, id workflow.ScheduleID) ([]rest.CronWorkflows, error) {
	if wf, name, ok := parseNamedID(id); ok {
		rows, err := c.listCrons(ctx, sdk, rest.CronWorkflowListParams{CronName: &name})
		if err != nil {
			return nil, err
		}
		var out []rest.CronWorkflows
		for _, row := range rows {
			if row.WorkflowName == engineName(c.cfg.Namespace, wf) && row.Name != nil && *row.Name == name {
				out = append(out, row)
			}
		}
		return out, nil
	}
	if _, err := uuid.Parse(id.String()); err != nil {
		return nil, nil
	}
	row, err := sdk.Crons().Get(ctx, id.String())
	if err != nil && !isNotFound(err) {
		return nil, engineErr(err, "read cron %s", id)
	}
	if row == nil {
		return nil, nil
	}
	return []rest.CronWorkflows{*row}, nil
}

// sameCron reports whether row already holds s's expression, input and
// labels.
func sameCron(row rest.CronWorkflows, s workflow.Schedule) bool {
	if row.Cron != s.Cron {
		return false
	}
	var input any
	if row.Input != nil {
		input = map[string]any(*row.Input)
	}
	payload, err := decodeEnvelope(input, payloadField)
	if err != nil || !bytes.Equal(payload, s.Input) {
		return false
	}
	if metadataString(row.AdditionalMetadata, metaNamed) == "" || metadataString(row.AdditionalMetadata, metaTenant) != s.Tenant {
		return false
	}
	labels := s.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return maps.Equal(jobLabels(metadataStrings(row.AdditionalMetadata)), labels)
}

// listCrons pages through the cron triggers matching params (Limit and
// Offset are set here).
func (c *Client) listCrons(ctx context.Context, sdk *hatchetsdk.Client, params rest.CronWorkflowListParams) ([]rest.CronWorkflows, error) {
	var out []rest.CronWorkflows
	for offset := int64(0); ; offset += cronListPage {
		limit, off := cronListPage, offset
		params.Limit, params.Offset = &limit, &off
		page, err := sdk.Crons().List(ctx, params)
		if err != nil {
			return nil, engineErr(err, "list crons")
		}
		if page.Rows == nil {
			return out, nil
		}
		out = append(out, *page.Rows...)
		if int64(len(*page.Rows)) < cronListPage {
			return out, nil
		}
	}
}
