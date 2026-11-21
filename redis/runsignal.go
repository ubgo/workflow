package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
)

var _ workflow.RunSignalReader = (*Client)(nil)

// PendingSignals lists the signals addressed to run (workflow.RunKey)
// with one of names that the run may still take: kept under the signal
// key of the name and the run's address, above the run's signal floor,
// without the run's consumed marker, oldest first
// (workflow.RunSignalReader). The ID is "<sequence>/<name>", so
// TakeSignal finds the signal key without a scan.
func (c *Client) PendingSignals(ctx context.Context, run workflow.RunID, names []string, limit int) ([]workflow.RunSignal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.usable(); err != nil {
		return nil, err
	}
	if len(names) == 0 || run.IsZero() {
		return nil, nil
	}
	args := []any{c.base, string(run), limit}
	for _, n := range names {
		args = append(args, sigKey(n, workflow.RunKey(run)), n)
	}
	res, err := pendingSignalsScript.Run(ctx, c.rdb, c.keys(), args...).Slice()
	if err != nil {
		return nil, fmt.Errorf("redis: pending signals: %w", classify(err))
	}
	out := make([]workflow.RunSignal, 0, len(res)/3)
	for i := 0; i+2 < len(res); i += 3 {
		name := str(res[i+1])
		out = append(out, workflow.RunSignal{ID: str(res[i]) + "/" + name, Name: name, Payload: []byte(str(res[i+2]))})
	}
	return out, nil
}

// TakeSignal sets the run's consumed marker for the signal, the marker a
// wait step taking it sets, in one script that first checks the signal is
// kept under the run's address and above its floor
// (workflow.RunSignalReader).
func (c *Client) TakeSignal(ctx context.Context, run workflow.RunID, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := c.usable(); err != nil {
		return false, err
	}
	seq, name, ok := strings.Cut(id, "/")
	if _, err := strconv.ParseUint(seq, 10, 64); !ok || err != nil || name == "" {
		return false, fmt.Errorf("%w: signal %q", workflow.ErrNotFound, id)
	}
	res, err := takeSignalScript.Run(ctx, c.rdb, c.keys(), c.base, string(run), seq, sigKey(name, workflow.RunKey(run))).Text()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return false, fmt.Errorf("%w: signal %q for run %s", workflow.ErrNotFound, id, run)
		}
		return false, fmt.Errorf("redis: take signal: %w", classify(err))
	}
	switch res {
	case "took":
		return true, nil
	case "had":
		return false, nil
	}
	return false, fmt.Errorf("%w: signal %q for run %s", workflow.ErrNotFound, id, run)
}

// pendingSignalsScript lists a run's untaken addressed signals. ARGV
// base, run id, limit (0: none), then pairs of signal key and name.
// Returns a flat list of sequence, name, payload, oldest first; nothing
// for an unknown or finished run.
var pendingSignalsScript = goredis.NewScript(luaPrelude + `
local b, id, limit = ARGV[1], ARGV[2], tonumber(ARGV[3])
local key = rk(b, id)
local st = redis.call('HGET', key, 'status')
if not st or terminal(st) then return {} end
local sf = redis.call('HGET', key, 'sf') or '0'
local found = {}
for i = 4, #ARGV, 2 do
  local sk, name = ARGV[i], ARGV[i + 1]
  for _, n in ipairs(redis.call('ZRANGEBYSCORE', b .. ':sigs:' .. sk, '(' .. sf, '+inf')) do
    if not redis.call('HGET', key, 'c:' .. n) then
      found[#found + 1] = {tonumber(n), name, redis.call('HGET', b .. ':sigdata:' .. sk, n) or ''}
    end
  end
end
table.sort(found, function(x, y) return x[1] < y[1] end)
local out = {}
for i, f in ipairs(found) do
  if limit > 0 and i > limit then break end
  out[#out + 1] = tostring(f[1])
  out[#out + 1] = f[2]
  out[#out + 1] = f[3]
end
return out
`)

// takeSignalScript marks a run's addressed signal taken. ARGV base, run
// id, sequence, signal key. Returns 'took', 'had' (already taken) or
// 'none' (no such signal for this run, or the run is unknown or ended).
var takeSignalScript = goredis.NewScript(luaPrelude + `
local b, id, n, sk = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local key = rk(b, id)
local st = redis.call('HGET', key, 'status')
if not st or terminal(st) then return 'none' end
if tonumber(n) <= tonumber(redis.call('HGET', key, 'sf') or '0') then return 'none' end
if not redis.call('ZSCORE', b .. ':sigs:' .. sk, n) then return 'none' end
if redis.call('HSETNX', key, 'c:' .. n, '1') == 1 then return 'took' end
return 'had'
`)
