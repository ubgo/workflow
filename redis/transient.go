package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

var _ workflow.TransientSignaller = (*Client)(nil)

// SignalWaiting resumes the runs whose wait step is suspended on ev's name
// and key now, and keeps ev for no one else (workflow.TransientSignaller):
// it is signalScript without the writes that keep the event, in one
// script, so a run that begins its wait a moment later never sees ev.
func (c *Client) SignalWaiting(ctx context.Context, ev workflow.Event) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := c.usable(); err != nil {
		return 0, err
	}
	n, err := transientSignalScript.Run(ctx, c.rdb, c.keys(), c.base, sigKey(ev.Name, ev.Key), string(ev.Payload), c.clk.Now().UnixMilli()).Int()
	if err != nil {
		return 0, fmt.Errorf("redis: transient signal: %w", classify(err))
	}
	return n, nil
}

// transientSignalScript resumes the runs waiting on a signal key now,
// keeping no copy of the event. ARGV base, signal key, payload, now (ms).
// The sequence number still comes from sigseq, so the consumed marker of
// each resumed run is unique. Returns how many runs it resumed.
var transientSignalScript = goredis.NewScript(luaPrelude + `
local b, sk, payload = ARGV[1], ARGV[2], ARGV[3]
readyNow = tonumber(ARGV[4])
local n = redis.call('INCR', b .. ':sigseq')
local resumed = 0
for _, m in ipairs(redis.call('SMEMBERS', b .. ':waiters:' .. sk)) do
  local nl = string.find(m, '\n', 1, true)
  local id, step = string.sub(m, 1, nl - 1), string.sub(m, nl + 1)
  local key = rk(b, id)
  local h = redis.call('HMGET', key, 'status', 'owner', 'w:' .. step)
  if not h[1] or terminal(h[1]) or h[3] ~= sk then
    redis.call('SREM', b .. ':waiters:' .. sk, m)
  else
    redis.call('HSET', key, 'wv:' .. step, payload, 'c:' .. n, '1')
    redis.call('HDEL', key, 'w:' .. step)
    redis.call('SREM', b .. ':waiters:' .. sk, m)
    if h[2] == '' then
      redis.call('ZREM', b .. ':due', id)
      redis.call('HSET', key, 'status', 'queued')
      enready(b, id)
    else
      redis.call('HSET', key, 'sigpend', '1')
    end
    resumed = resumed + 1
  end
end
if resumed > 0 then redis.call('PUBLISH', b .. ':wake', 'e') end
return resumed
`)
