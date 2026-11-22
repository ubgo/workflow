package redis

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

var _ workflow.RunEditor = (*Client)(nil)

// editScript changes a run's payload and priority while it is scheduled.
// ARGV base, id, then field and value pairs. Returns 'ok', 'notfound' or
// the run's status when it is not scheduled.
var editScript = goredis.NewScript(luaPrelude + `
local key = rk(ARGV[1], ARGV[2])
local st = redis.call('HGET', key, 'status')
if not st then return 'notfound' end
if st ~= 'scheduled' then return st end
for i = 3, #ARGV, 2 do redis.call('HSET', key, ARGV[i], ARGV[i + 1]) end
return 'ok'
`)

// EditRun changes a scheduled run that has not started: payload, priority
// and start (workflow.RunEditor). The fields change in one script that
// checks the run is still scheduled, so an edit racing the run's due time
// either lands before it or returns ErrWrongState; the run's concurrency
// and rate keys are computed from the payload when it is admitted, as for
// every run. A new start goes through Reschedule.
//
//ds:def id=redis-client-editrun-fwewy3st owner=@khanakia stability=stable
func (c *Client) EditRun(ctx context.Context, ref workflow.RunRef, edit workflow.JobEdit) (workflow.RunRef, error) {
	if err := workflow.CheckJobEdit(edit); err != nil {
		return workflow.RunRef{}, err
	}
	id, err := runID(ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	r, err := c.load(ctx, id, ref)
	if err != nil {
		return workflow.RunRef{}, err
	}
	var args []any
	if edit.Payload != nil {
		if err := workflow.CheckPayload(workflow.Job{Workflow: r.wf, Payload: *edit.Payload}, c.PayloadLimit(), "redis.WithPayloadLimit"); err != nil {
			return workflow.RunRef{}, err
		}
		args = append(args, "payload", string(*edit.Payload))
	}
	if edit.Priority != nil {
		args = append(args, "prio", strconv.Itoa(*edit.Priority))
	}
	out := workflow.RunRef{ScheduledID: workflow.ScheduledID(r.sid)}
	if len(args) > 0 {
		res, err := editScript.Run(ctx, c.rdb, c.keys(), append([]any{c.base, id}, args...)...).Text()
		if err != nil {
			return workflow.RunRef{}, fmt.Errorf("redis: edit run: %w", classify(err))
		}
		switch res {
		case "ok":
		case "notfound":
			return workflow.RunRef{}, fmt.Errorf("%w: run %+v", workflow.ErrNotFound, ref)
		default:
			return workflow.RunRef{}, fmt.Errorf("%w: run %+v is %s, not scheduled", workflow.ErrWrongState, ref, res)
		}
	}
	if edit.RunAt != nil {
		return c.Reschedule(ctx, ref, *edit.RunAt)
	}
	return out, nil
}
