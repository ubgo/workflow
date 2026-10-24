package hatchet

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/ubgo/workflow"
)

// API is the engine's REST API for the calls SDK v0.107.1 has no method
// for and whose REST client it does not expose: workflow pause and resume,
// and enabling or disabling a cron trigger. Set Config.API to turn on
// CapPauseWorkflow and CapSchedulePause; without it both are refused.
// Build one with NewAPI, or set Client and TenantID yourself (for a
// custom HTTP client or proxy).
type API struct {
	// Client is the engine's REST client, authenticated as the SDK
	// client is (a bearer token for the same tenant).
	Client *rest.ClientWithResponses
	// TenantID is the tenant the token belongs to.
	TenantID string
}

// NewAPI builds an API from an engine API token, the value of
// HATCHET_CLIENT_TOKEN. The tenant comes from the token's sub claim and,
// when serverURL is empty, the REST address from its server_url claim, as
// the SDK itself does; pass serverURL when the engine is reached at
// another address (HATCHET_CLIENT_SERVER_URL). The token is only decoded,
// not verified: the engine verifies it on every call.
func NewAPI(token, serverURL string) (*API, error) {
	claims, err := tokenClaims(token)
	if err != nil {
		return nil, err
	}
	if serverURL == "" {
		serverURL = claims.ServerURL
	}
	if serverURL == "" {
		return nil, fmt.Errorf("%w: hatchet driver: NewAPI: no server URL given and none in the token", workflow.ErrInvalid)
	}
	if _, err := uuid.Parse(claims.Sub); err != nil {
		return nil, fmt.Errorf("%w: hatchet driver: NewAPI: the token names no tenant", workflow.ErrInvalid)
	}
	auth := func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
	client, err := rest.NewClientWithResponses(serverURL, rest.WithRequestEditorFn(auth))
	if err != nil {
		return nil, fmt.Errorf("hatchet driver: NewAPI: %w", err)
	}
	return &API{Client: client, TenantID: claims.Sub}, nil
}

// jwtClaims are the token claims NewAPI reads.
type jwtClaims struct {
	Sub       string `json:"sub"`
	ServerURL string `json:"server_url"`
}

// tokenClaims decodes the payload of a JWT without verifying it.
func tokenClaims(token string) (jwtClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return jwtClaims{}, fmt.Errorf("%w: hatchet driver: NewAPI: the token is not a JWT", workflow.ErrInvalid)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jwtClaims{}, fmt.Errorf("%w: hatchet driver: NewAPI: token payload: %v", workflow.ErrInvalid, err)
	}
	var c jwtClaims
	if err := json.Unmarshal(raw, &c); err != nil {
		return jwtClaims{}, fmt.Errorf("%w: hatchet driver: NewAPI: token payload: %v", workflow.ErrInvalid, err)
	}
	return c, nil
}

// pauseQueueTTL is how long the engine holds the queued runs of a paused
// workflow before it CANCELS them. The engine requires a TTL with every
// pause, and an empty or unparsable one silently becomes five minutes
// (convert_duration_to_interval in the engine's schema), which would turn
// a pause into a delayed cancel. The workflow.Pauser contract is "hold,
// never cancel", so the driver sends a hundred years, in the "<n>y" form
// the engine parses exactly (up to 8 digits, then d, w or y).
const pauseQueueTTL = "100y"

// pauseRequest is the engine's pause request: hold queued runs for ttl,
// and queue (never drop) the cron and scheduled runs that come due while
// paused, so timers keep running and what they make ready waits.
func pauseRequest(ttl string) (rest.WorkflowUpdateRequest, error) {
	var p rest.PauseWorkflowRequest
	err := p.FromPauseWorkflowRequestPause(rest.PauseWorkflowRequestPause{
		Action:                                  rest.Pause,
		PausedWorkflowQueueTTL:                  ttl,
		PausedWorkflowCronRunQueueBehavior:      rest.QUEUE,
		PausedWorkflowScheduledRunQueueBehavior: rest.QUEUE,
	})
	return rest.WorkflowUpdateRequest{Pause: &p}, err
}

// unpauseRequest is the engine's resume request.
func unpauseRequest() (rest.WorkflowUpdateRequest, error) {
	var p rest.PauseWorkflowRequest
	err := p.FromPauseWorkflowRequestUnpause(rest.PauseWorkflowRequestUnpause{Action: rest.Unpause})
	return rest.WorkflowUpdateRequest{Pause: &p}, err
}

// capabilities is what this client declares: the fixed set, plus pause
// when Config.API is set.
func (c *Client) capabilities() workflow.Caps {
	if c.cfg.API != nil {
		return caps | workflow.CapPauseWorkflow | workflow.CapSchedulePause
	}
	return caps
}

var (
	_ workflow.Pauser         = (*Client)(nil)
	_ workflow.SchedulePauser = (*Client)(nil)
)

// errPauseAll is why PauseAll and PauseSchedule(All) are refused.
const errPauseAll = "the Hatchet engine stores a pause per workflow (and per cron trigger), with no pause of everything that would also hold workflows and triggers made later; pause each one"

// Pause holds the runs of one workflow (workflow.Pauser) through the
// engine's workflow pause: queued steps move aside and are not assigned,
// running steps finish, Enqueue still works, and crons and delayed starts
// that come due are queued and held too. The pause is stored on the
// engine, so every worker obeys it. Needs Config.API. PauseQueue and
// PauseAll are refused: a queue is not a thing the engine pauses, and
// the engine has no pause of everything (see errPauseAll).
func (c *Client) Pause(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(c.capabilities(), t); err != nil {
		return err
	}
	if t.All {
		return unsupported("PauseTarget.All", 0, errPauseAll)
	}
	req, err := pauseRequest(pauseQueueTTL)
	if err != nil {
		return fmt.Errorf("hatchet driver: pause request: %w", err)
	}
	return c.updateWorkflow(ctx, t.Workflow, req)
}

// Resume releases a paused workflow (workflow.Pauser); the engine queues
// its held steps again. Resume(PauseAll()) resumes every paused workflow
// of this client's namespace. Resuming a workflow that is not paused is a
// no-op.
func (c *Client) Resume(ctx context.Context, t workflow.PauseTarget) error {
	if err := workflow.CheckPause(c.capabilities(), t); err != nil {
		return err
	}
	req, err := unpauseRequest()
	if err != nil {
		return fmt.Errorf("hatchet driver: resume request: %w", err)
	}
	if !t.All {
		return c.updateWorkflow(ctx, t.Workflow, req)
	}
	paused, err := c.Paused(ctx)
	if err != nil {
		return err
	}
	for _, p := range paused {
		if err := c.updateWorkflow(ctx, p.Workflow, req); err != nil {
			return err
		}
	}
	return nil
}

// Paused lists the paused workflows of this client's namespace, by name
// (workflow.Pauser). It reads the engine, so it sees pauses made by any
// process or in the dashboard.
func (c *Client) Paused(ctx context.Context) ([]workflow.PauseTarget, error) {
	api, err := c.api()
	if err != nil {
		return nil, err
	}
	tenant, _ := uuid.Parse(api.TenantID)
	var out []workflow.PauseTarget
	for offset := 0; ; offset += int(cronListPage) {
		limit, off := int(cronListPage), offset
		resp, err := api.Client.WorkflowListWithResponse(ctx, tenant, &rest.WorkflowListParams{Limit: &limit, Offset: &off})
		if err != nil {
			return nil, engineErr(err, "list workflows")
		}
		if resp.JSON200 == nil {
			return nil, apiErr(resp.StatusCode(), resp.Body, "list workflows")
		}
		rows := []rest.Workflow{}
		if resp.JSON200.Rows != nil {
			rows = *resp.JSON200.Rows
		}
		for _, w := range rows {
			if w.IsPaused == nil || !*w.IsPaused {
				continue
			}
			if name, ok := c.localName(w.Name); ok {
				out = append(out, workflow.PauseWorkflow(name))
			}
		}
		if len(rows) < limit {
			break
		}
	}
	slices.SortFunc(out, func(a, b workflow.PauseTarget) int { return cmp.Compare(a.Workflow, b.Workflow) })
	return out, nil
}

// updateWorkflow sends req for the workflow known locally as name.
func (c *Client) updateWorkflow(ctx context.Context, name string, req rest.WorkflowUpdateRequest) error {
	api, err := c.api()
	if err != nil {
		return err
	}
	id, err := c.workflowID(ctx, api, engineName(c.cfg.Namespace, name))
	if err != nil {
		return err
	}
	resp, err := api.Client.WorkflowUpdateWithResponse(ctx, id, req)
	if err != nil {
		return engineErr(err, "update workflow %q", name)
	}
	if resp.JSON200 == nil {
		return apiErr(resp.StatusCode(), resp.Body, "update workflow "+name)
	}
	return nil
}

// workflowID finds the engine ID of the workflow named exactly name. The
// engine's name filter matches by substring (the SDK's Workflows().Get
// takes its first row, so pausing "report" could pause "report_daily";
// seen live), so every row is compared by its full name.
func (c *Client) workflowID(ctx context.Context, api *API, name string) (openapi_types.UUID, error) {
	tenant, _ := uuid.Parse(api.TenantID)
	for offset := 0; ; offset += int(cronListPage) {
		limit, off := int(cronListPage), offset
		resp, err := api.Client.WorkflowListWithResponse(ctx, tenant, &rest.WorkflowListParams{Name: &name, Limit: &limit, Offset: &off})
		if err != nil {
			return openapi_types.UUID{}, engineErr(err, "find workflow %q", name)
		}
		if resp.JSON200 == nil {
			return openapi_types.UUID{}, apiErr(resp.StatusCode(), resp.Body, "find workflow "+name)
		}
		var rows []rest.Workflow
		if resp.JSON200.Rows != nil {
			rows = *resp.JSON200.Rows
		}
		for _, w := range rows {
			if w.Name == name {
				id, err := uuid.Parse(w.Metadata.Id)
				if err != nil {
					return openapi_types.UUID{}, fmt.Errorf("hatchet driver: workflow %q has id %q: %w", name, w.Metadata.Id, err)
				}
				return id, nil
			}
		}
		if len(rows) < limit {
			return openapi_types.UUID{}, fmt.Errorf("%w: workflow %q is not on the engine", workflow.ErrNotFound, name)
		}
	}
}

// PauseSchedule disables one cron trigger (workflow.SchedulePauser);
// declared crons included. Ticks that fall due while it is disabled are
// never fired, whatever the misfire policy: the engine does not catch up
// (Hatchet accepts no misfire policy). PauseSchedule(All) is refused (see
// errPauseAll). An unknown ID returns ErrNotFound. Needs Config.API.
func (c *Client) PauseSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	if err := c.checkSchedulePause(t); err != nil {
		return err
	}
	if t.All {
		return unsupported("ScheduleTarget.All", 0, errPauseAll)
	}
	return c.setCronEnabled(ctx, t.ID, false)
}

// ResumeSchedule enables a cron trigger again; ResumeSchedule(All)
// enables every disabled trigger of this client's namespace. It fires
// from the next tick on.
func (c *Client) ResumeSchedule(ctx context.Context, t workflow.ScheduleTarget) error {
	if err := c.checkSchedulePause(t); err != nil {
		return err
	}
	if !t.All {
		return c.setCronEnabled(ctx, t.ID, true)
	}
	list, err := c.Schedules(ctx)
	if err != nil {
		return err
	}
	for _, s := range list {
		if s.Paused {
			if err := c.setCronEnabled(ctx, s.ID, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSchedulePause validates t and refuses without Config.API.
func (c *Client) checkSchedulePause(t workflow.ScheduleTarget) error {
	if err := workflow.ValidateScheduleTarget(t); err != nil {
		return err
	}
	if !c.capabilities().Has(workflow.CapSchedulePause) {
		return unsupported("SchedulePause", workflow.CapSchedulePause, "set Config.API: SDK v0.107.1 has no call to enable or disable a cron trigger")
	}
	return nil
}

// setCronEnabled enables or disables the trigger id.
func (c *Client) setCronEnabled(ctx context.Context, id workflow.ScheduleID, enabled bool) error {
	api, err := c.api()
	if err != nil {
		return err
	}
	// The engine answers an update of an unknown trigger with a 500
	// (seen live), so its existence is checked first.
	sdk, err := c.ready()
	if err != nil {
		return err
	}
	rows, err := c.cronRows(ctx, sdk, id)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	}
	for _, row := range rows {
		if err := c.updateCronEnabled(ctx, api, row.Metadata.Id, enabled); err != nil {
			return err
		}
	}
	return nil
}

// updateCronEnabled enables or disables the trigger with engine ID
// cronID.
func (c *Client) updateCronEnabled(ctx context.Context, api *API, cronID string, enabled bool) error {
	id, perr := uuid.Parse(cronID)
	if perr != nil {
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, cronID)
	}
	tenant, _ := uuid.Parse(api.TenantID)
	resp, err := api.Client.WorkflowCronUpdateWithResponse(ctx, tenant, openapi_types.UUID(id), rest.UpdateCronWorkflowTriggerRequest{Enabled: &enabled})
	if err != nil {
		return engineErr(err, "update cron %s", id)
	}
	switch code := resp.StatusCode(); {
	case code == http.StatusNotFound:
		return fmt.Errorf("%w: schedule %q", workflow.ErrNotFound, id)
	case code >= 300:
		return apiErr(code, resp.Body, "update cron "+id.String())
	}
	return nil
}

// api returns Config.API once Start has run, or the refusal when it is
// not set.
func (c *Client) api() (*API, error) {
	if c.cfg.API == nil {
		return nil, unsupported("Config.API", workflow.CapPauseWorkflow|workflow.CapSchedulePause, "pause needs the engine's REST API, which SDK v0.107.1 does not expose")
	}
	if _, err := c.ready(); err != nil {
		return nil, err
	}
	return c.cfg.API, nil
}

// apiErr is the error of a REST call that answered without the expected
// body: an outage status matches workflow.ErrUnavailable.
func apiErr(code int, body []byte, op string) error {
	err := fmt.Errorf("hatchet driver: %s: engine answered %d: %s", op, code, strings.TrimSpace(string(body)))
	if code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout {
		return workflow.Unavailable(err)
	}
	return err
}
