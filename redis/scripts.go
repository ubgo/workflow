package redis

import goredis "github.com/redis/go-redis/v9"

// Every state change is one Lua script, so Redis applies it atomically:
// no other client sees a run half claimed, half cancelled or half
// finished. Each script takes the key base (the hash-tagged prefix) as
// ARGV[1] and builds its keys from it, and gets KEYS[1] = <base>:runs only
// so a cluster client routes it to the slot every key of the base shares.
//
// Writes a worker makes about a run it holds (save, release, finish,
// renew, wait) are fenced: they compare the run's owner field with the
// claim token the worker got, so a worker whose lease expired and whose
// run was claimed again writes nothing. That fence is what makes a run's
// state changes happen once per lease.
//
// A run hash holds, besides its job and progress: q (its queue, set once
// the run is admitted), jq (Job.Queue as enqueued), adm ('1' once
// admitted: a run of a workflow with a concurrency or rate policy, or of
// a workflow the enqueuing client did not register, waits in
// {base}:pending:<workflow> until a worker that registered it admits it),
// ck (the concurrency bucket key holding it), chain (the rest of its job
// chain), sf (the signal floor: only signals numbered above it may resume
// its waits), w:<step> and wd:<step> (a registered wait and its deadline),
// wv:<step> (the payload of the signal that resolved it), c:<signal> (a
// signal it took), sigpend (a signal resolved a wait while the run was
// held) and forced (ForceFail ended it).

// luaPrelude holds the helpers every script shares. create writes one new
// run from createArity ARGV values starting at index i (see newRun in
// enqueue.go for their order).
const luaPrelude = `
local function rk(b, id) return b .. ':run:' .. id end
local function terminal(st)
  return st == 'succeeded' or st == 'failed' or st == 'cancelled' or st == 'discarded'
end
local function release_idem(b, id, f)
  if f and f ~= '' and redis.call('HGET', b .. ':idem', f) == id then
    redis.call('HDEL', b .. ':idem', f)
  end
end
local function readykey(b, wf, q) return b .. ':ready:' .. q .. '/' .. wf end
-- sincekey is the companion of a ready set: the same runs scored by when
-- they became claimable (ms on the scheduling clock), for QueueLoads.
local function sincekey(b, wf, q) return b .. ':since:' .. q .. '/' .. wf end
-- readyNow is when the running script makes runs claimable; every script
-- that calls enready sets it first.
local readyNow = 0
-- enready puts an unfinished run nobody holds where it waits for a
-- worker: the ready set of its queue once admitted, else the pending set
-- of its workflow, which a worker that registered the workflow admits.
local function enready(b, id)
  local h = redis.call('HMGET', rk(b, id), 'wf', 'q', 'prio', 'adm', 'seq')
  local score = -tonumber(h[3])
  -- a FIFO workflow (Concurrency.FIFO) orders by enqueue order alone
  if redis.call('HEXISTS', b .. ':fifo', h[1]) == 1 then score = tonumber(h[5] or '0') end
  if h[4] == '1' then
    redis.call('ZADD', readykey(b, h[1], h[2]), score, id)
    redis.call('ZADD', sincekey(b, h[1], h[2]), 'NX', readyNow, id)
    redis.call('SADD', b .. ':wfq:' .. h[1], h[2])
  else
    redis.call('ZADD', b .. ':pending:' .. h[1], score, id)
  end
end
-- ended is what every terminal transition shares: the run leaves every
-- index that hands out work, its idempotency key, concurrency slot and
-- waits are released, its end is recorded for run-finished observers, and
-- the retention clock starts.
local function ended(b, id, retention)
  local key = rk(b, id)
  local h = redis.call('HMGET', key, 'wf', 'q', 'idem', 'ck')
  redis.call('ZREM', b .. ':due', id)
  redis.call('ZREM', readykey(b, h[1], h[2] or ''), id)
  redis.call('ZREM', sincekey(b, h[1], h[2] or ''), id)
  redis.call('ZREM', b .. ':pending:' .. h[1], id)
  redis.call('ZREM', b .. ':leases', id)
  redis.call('ZREM', b .. ':floors', id)
  redis.call('ZREM', b .. ':rto', id)
  release_idem(b, id, h[3])
  if h[4] and h[4] ~= '' then redis.call('ZREM', h[4], id) end
  for _, f in ipairs(redis.call('HKEYS', key)) do
    if string.sub(f, 1, 2) == 'w:' then
      local sk = redis.call('HGET', key, f)
      redis.call('SREM', b .. ':waiters:' .. sk, id .. '\n' .. string.sub(f, 3))
      redis.call('HDEL', key, f)
    end
  end
  redis.call('HDEL', key, 'sigpend')
  redis.call('ZADD', b .. ':ended:' .. h[1], 0, id)
  if tonumber(retention) > 0 then redis.call('PEXPIRE', key, retention) end
end
-- cancel_run ends an unfinished run cancelled and tells its holder.
local function cancel_run(b, id, fin, err, retention)
  redis.call('HSET', rk(b, id), 'status', 'cancelled', 'finished', fin, 'err', err, 'owner', '')
  ended(b, id, retention)
  redis.call('PUBLISH', b .. ':wake', 'c:' .. id)
end
-- overlapped returns the schedule's last run, its status and its
-- ScheduledID when the schedule's overlap policy is 'skip' and that run
-- is unfinished; nothing otherwise (a run Redis no longer has does not
-- block).
local function overlapped(b, sk)
  if redis.call('HGET', sk, 'overlap') ~= 'skip' then return nil end
  local lr = redis.call('HGET', sk, 'lrun')
  if not lr or lr == '' then return nil end
  local h = redis.call('HMGET', rk(b, lr), 'status', 'sid')
  if h[1] and not terminal(h[1]) then return lr, h[1], h[2] or '' end
  return nil
end
local function create(b, a, i)
  local id, wf = a[i], a[i+1]
  local due, now = tonumber(a[i+8]), tonumber(a[i+11])
  readyNow = now
  local status = 'queued'
  if due > now then status = 'scheduled' end
  local sf = redis.call('GET', b .. ':sigseq') or '0'
  local seq = redis.call('INCR', b .. ':seq')
  redis.call('HSET', rk(b, id), 'wf', wf, 'payload', a[i+2], 'labels', a[i+3],
    'prio', a[i+4], 'idem', a[i+5], 'enq', a[i+6], 'sid', a[i+9], 'retry', a[i+10],
    'status', status, 'steps', '{}', 'attempts', '0', 'reclaims', '0', 'owner', '',
    'q', a[i+12], 'jq', a[i+13], 'adm', a[i+14], 'chain', a[i+15], 'sf', sf,
    'tn', a[i+16], 'runat', a[i+17], 'rto', a[i+18], 'seq', seq)
  if tonumber(a[i+18]) > 0 then redis.call('ZADD', b .. ':rto', a[i+18], id) end
  redis.call('ZADD', b .. ':runs', a[i+7], id)
  redis.call('ZADD', b .. ':floors', sf, id)
  if status == 'scheduled' then
    redis.call('ZADD', b .. ':due', due, id)
  else
    enready(b, id)
  end
end
-- create_idem creates the run unless idempotency field f is held by an
-- unfinished run, whose id, status and ScheduledID it returns instead.
local function create_idem(b, f, a, i)
  if f ~= '' then
    local cur = redis.call('HGET', b .. ':idem', f)
    if cur then
      local h = redis.call('HMGET', rk(b, cur), 'status', 'sid')
      if h[1] and not terminal(h[1]) then return {'dup', cur, h[1], h[2] or ''} end
    end
    redis.call('HSET', b .. ':idem', f, a[i])
  end
  create(b, a, i)
  return nil
end
`

// createArity is how many ARGV values create reads per run.
const createArity = 19

// enqueueScript: ARGV base, idem field (empty for none), then the create
// values. With an idempotency field held by an unfinished run it creates
// nothing and returns {'dup', id, status, sid}; otherwise {'new'}.
var enqueueScript = goredis.NewScript(luaPrelude + `
local b, f = ARGV[1], ARGV[2]
local dup = create_idem(b, f, ARGV, 3)
if dup then return dup end
redis.call('PUBLISH', b .. ':wake', 'e')
return {'new'}
`)

// claimScript moves due runs to where they wait for a worker, hands back
// runs whose lease expired, then claims the best ready run this client
// may take: the highest queue priority, then the highest Job.Priority,
// then the oldest ID (IDs start with their creation time). Paused
// workflows and queues are passed over. ARGV base, now (scheduling clock,
// ms), lease now (ms), lease until (ms), token, started (ns), queue mode
// ('all': every queue; 'some': only the listed ones), n, then n triples of
// queue, priority and rank (among queues of equal priority the lower rank
// goes first: the client's weighted draw; equal ranks fall back to
// Job.Priority and age), then the served workflow names. It returns false
// when nothing is ready, else {id, HGETALL of the run}.
var claimScript = goredis.NewScript(luaPrelude + `
local b, now, lnow, luntil, tok, started = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
readyNow = tonumber(now)
for _, id in ipairs(redis.call('ZRANGEBYSCORE', b .. ':due', '-inf', now, 'LIMIT', 0, 100)) do
  redis.call('ZREM', b .. ':due', id)
  local h = redis.call('HMGET', rk(b, id), 'status', 'owner')
  if h[1] and not terminal(h[1]) and h[2] == '' then
    redis.call('HSET', rk(b, id), 'status', 'queued')
    enready(b, id)
  end
end
for _, id in ipairs(redis.call('ZRANGEBYSCORE', b .. ':leases', '-inf', '(' .. lnow, 'LIMIT', 0, 100)) do
  redis.call('ZREM', b .. ':leases', id)
  local h = redis.call('HMGET', rk(b, id), 'status', 'owner')
  if h[1] == 'running' and h[2] and h[2] ~= '' then
    redis.call('HSET', rk(b, id), 'owner', '', 'status', 'queued')
    redis.call('HINCRBY', rk(b, id), 'reclaims', 1)
    enready(b, id)
  end
end
local paused = {}
local pl = redis.call('HGETALL', b .. ':paused')
for i = 1, #pl, 2 do paused[pl[i]] = true end
if paused['all'] then return false end
local mode, n = ARGV[7], tonumber(ARGV[8])
local qprio, qrank = {}, {}
for i = 0, n - 1 do
  qprio[ARGV[9 + 3 * i]] = tonumber(ARGV[10 + 3 * i])
  qrank[ARGV[9 + 3 * i]] = tonumber(ARGV[11 + 3 * i])
end
local first = 9 + 3 * n
local qof
for _ = 1, 100 do
  local best, bestKey, bp, br, bs
  for i = first, #ARGV do
    local wf = ARGV[i]
    if not paused['w:' .. wf] then
      for _, q in ipairs(redis.call('SMEMBERS', b .. ':wfq:' .. wf)) do
        if (mode == 'all' or qprio[q]) and not paused['q:' .. q] then
          local key = readykey(b, wf, q)
          local r = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
          if r[1] then qof = qof or {} qof[key] = {wf, q} end
          if r[1] then
            local p, rank, s = qprio[q] or 0, qrank[q] or 0, tonumber(r[2])
            if not best or p > bp or (p == bp and (rank < br or (rank == br and
                (s < bs or (s == bs and r[1] < best))))) then
              best, bestKey, bp, br, bs = r[1], key, p, rank, s
            end
          end
        end
      end
    end
  end
  if not best then return false end
  redis.call('ZREM', bestKey, best)
  redis.call('ZREM', sincekey(b, qof[bestKey][1], qof[bestKey][2]), best)
  local key = rk(b, best)
  local h = redis.call('HMGET', key, 'status', 'owner')
  if h[1] and not terminal(h[1]) and h[2] == '' then
    redis.call('HSET', key, 'owner', tok, 'status', 'running', 'lease', luntil)
    if not redis.call('HGET', key, 'started') then redis.call('HSET', key, 'started', started) end
    redis.call('HDEL', key, 'sigpend')
    redis.call('ZADD', b .. ':leases', luntil, best)
    return {best, redis.call('HGETALL', key)}
  end
end
return false
`)

// renewScript extends a held lease. ARGV base, id, token, until (ms). It
// returns 'ok', the run's terminal status when it ended meanwhile (a
// Cancel), or 'lost' when the token no longer owns the run.
var renewScript = goredis.NewScript(luaPrelude + `
local b, id, tok, luntil = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local h = redis.call('HMGET', rk(b, id), 'owner', 'status')
if h[1] ~= tok then return 'lost' end
if terminal(h[2]) then return h[2] end
redis.call('HSET', rk(b, id), 'lease', luntil)
redis.call('ZADD', b .. ':leases', luntil, id)
return 'ok'
`)

// saveScript records a held run's progress. ARGV base, id, token, steps
// JSON, attempts, progress ('1' resets the reclaim count: a step finished,
// so the run is not killing its workers), error history JSON, chain JSON.
// Returns 1, or 0 when fenced off.
var saveScript = goredis.NewScript(luaPrelude + `
local b, id, tok = ARGV[1], ARGV[2], ARGV[3]
local h = redis.call('HMGET', rk(b, id), 'owner', 'status')
if h[1] ~= tok or terminal(h[2]) then return 0 end
redis.call('HSET', rk(b, id), 'steps', ARGV[4], 'attempts', ARGV[5], 'errs', ARGV[7], 'chain', ARGV[8])
if ARGV[6] == '1' then redis.call('HSET', rk(b, id), 'reclaims', '0') end
return 1
`)

// releaseScript hands a held run back: ready now (due 0), at due (ms on
// the scheduling clock), or parked until a signal resumes it (due -1),
// with status queued, waiting or retrying. A wait a signal resolved while
// the run was held makes it ready now whatever due says. ARGV base, id,
// token, steps JSON, attempts, due, status, error history JSON, chain
// JSON, now (ms). Returns 1, or 0 when fenced.
var releaseScript = goredis.NewScript(luaPrelude + `
local b, id, tok, due = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[6])
readyNow = tonumber(ARGV[10])
local key = rk(b, id)
local h = redis.call('HMGET', key, 'owner', 'status', 'sigpend')
if h[1] ~= tok or terminal(h[2]) then return 0 end
local status = ARGV[7]
if h[3] == '1' then due, status = 0, 'queued' end
redis.call('HSET', key, 'owner', '', 'status', status, 'steps', ARGV[4], 'attempts', ARGV[5], 'errs', ARGV[8], 'chain', ARGV[9])
redis.call('HDEL', key, 'sigpend')
redis.call('ZREM', b .. ':leases', id)
if due > 0 then
  redis.call('ZADD', b .. ':due', due, id)
elseif due == 0 then
  enready(b, id)
  redis.call('PUBLISH', b .. ':wake', 'e')
end
return 1
`)

// finishScript ends a held run. ARGV base, id, token, status, finished
// (ns), error, failed step, steps JSON, attempts, retention (ms, 0 keeps
// it), error history JSON, output (a succeeded run's result), then for a
// succeeded run with a chain the next job's idempotency field and
// createArity values, created in the same script. Returns 1, or 0 when
// fenced.
var finishScript = goredis.NewScript(luaPrelude + `
local b, id, tok = ARGV[1], ARGV[2], ARGV[3]
local key = rk(b, id)
local h = redis.call('HMGET', key, 'owner', 'status')
if h[1] ~= tok or terminal(h[2]) then return 0 end
redis.call('HSET', key, 'status', ARGV[4], 'finished', ARGV[5], 'err', ARGV[6],
  'fstep', ARGV[7], 'steps', ARGV[8], 'attempts', ARGV[9], 'owner', '', 'errs', ARGV[11], 'out', ARGV[12])
ended(b, id, ARGV[10])
if #ARGV > 13 then
  create_idem(b, ARGV[13], ARGV, 14)
  redis.call('HSET', key, 'chain', '')
  redis.call('PUBLISH', b .. ':wake', 'e')
end
return 1
`)

// cancelScript cancels an unfinished run, held or not, when its status is
// one of the listed ones (all unfinished statuses when none is listed).
// ARGV base, id, finished (ns), error, retention (ms), then the statuses.
// Returns 'ok', 'done' (already terminal), 'skip' (a status not listed)
// or 'notfound', and publishes the cancel so the holder stops its step at
// once.
var cancelScript = goredis.NewScript(luaPrelude + `
local b, id = ARGV[1], ARGV[2]
local st = redis.call('HGET', rk(b, id), 'status')
if not st then return 'notfound' end
if terminal(st) then return 'done' end
if #ARGV > 5 then
  local ok = false
  for i = 6, #ARGV do if ARGV[i] == st then ok = true end end
  if not ok then return 'skip' end
end
cancel_run(b, id, ARGV[3], ARGV[4], ARGV[5])
return 'ok'
`)

// deleteScript forgets a terminal run. ARGV base, id. Returns 'ok',
// 'notfound' or the run's unfinished status.
var deleteScript = goredis.NewScript(luaPrelude + `
local b, id = ARGV[1], ARGV[2]
local h = redis.call('HMGET', rk(b, id), 'status', 'wf')
if not h[1] then
  redis.call('ZREM', b .. ':runs', id)
  return 'notfound'
end
if not terminal(h[1]) then return h[1] end
redis.call('DEL', rk(b, id))
redis.call('ZREM', b .. ':runs', id)
redis.call('ZREM', b .. ':ended:' .. h[2], id)
return 'ok'
`)

// fireScript fires one schedule's due ticks once across every client: it
// moves the schedule's last catch-up instant from the value the caller
// read and, only when that still matches, writes the next fire time and
// creates the tick runs in the same script. Under overlap 'skip' it
// creates nothing while the run of the schedule's last fire is unfinished,
// and of several due ticks it creates only the first, so the decision and
// the fire cannot interleave with another client's. ARGV base, schedule
// id, expected last (ms), new last (ms), new next (ms), run count, then
// per run its tick time (ms) and createArity values. Returns {1, runs
// created} when this caller moved the schedule (the rest of the ticks
// were skipped for overlap), {0, 0} when another client got there first or
// the schedule is gone.
var fireScript = goredis.NewScript(luaPrelude + `
local b, sid = ARGV[1], ARGV[2]
local sk = b .. ':sched:' .. sid
local last = redis.call('HGET', sk, 'last')
if last ~= ARGV[3] then return {0, 0} end
redis.call('HSET', sk, 'last', ARGV[4], 'next', ARGV[5])
redis.call('ZADD', b .. ':scheds', ARGV[5], sid)
local n = tonumber(ARGV[6])
if n == 0 or overlapped(b, sk) then return {1, 0} end
local skip = redis.call('HGET', sk, 'overlap') == 'skip'
local keep = tonumber(redis.call('HGET', sk, 'keep') or '0') or 0
local made, id, at = 0, nil, nil
for j = 0, n - 1 do
  if skip and made == 1 then break end
  local i = 8 + j * 20
  create(b, ARGV, i)
  id, at = ARGV[i], ARGV[i - 1]
  if keep > 0 then redis.call('ZADD', b .. ':sruns:' .. sid, ARGV[i + 7], id) end
  made = made + 1
end
redis.call('HSET', sk, 'lfire', at, 'lrun', id)
redis.call('PUBLISH', b .. ':wake', 'e')
return {1, made}
`)

// schedPauseScript pauses or resumes a schedule, or all of them. ARGV
// base, schedule id, all ('1'), pause ('1'). Pausing all sets the global
// flag (read by every schedule, including later ones); resuming all clears
// it and every schedule's own pause. Returns 'ok' or 'notfound'.
var schedPauseScript = goredis.NewScript(luaPrelude + `
local b, id, all, pause = ARGV[1], ARGV[2], ARGV[3] == '1', ARGV[4] == '1'
if all then
  if pause then
    redis.call('SET', b .. ':schedpaused', '1')
  else
    redis.call('DEL', b .. ':schedpaused')
    for _, sid in ipairs(redis.call('ZRANGE', b .. ':scheds', 0, -1)) do
      redis.call('HDEL', b .. ':sched:' .. sid, 'paused')
    end
  end
  return 'ok'
end
local sk = b .. ':sched:' .. id
if redis.call('EXISTS', sk) == 0 then return 'notfound' end
if pause then redis.call('HSET', sk, 'paused', '1') else redis.call('HDEL', sk, 'paused') end
return 'ok'
`)

// triggerScript enqueues one run of a schedule now, as a manual trigger.
// ARGV base, schedule id, fire time (ms), then createArity values. It
// returns {'ok'}, {'notfound'}, or {'overlap', run id, status, sid} when
// the schedule's overlap policy is 'skip' and its last run is unfinished.
var triggerScript = goredis.NewScript(luaPrelude + `
local b, sid = ARGV[1], ARGV[2]
local sk = b .. ':sched:' .. sid
if redis.call('EXISTS', sk) == 0 then return {'notfound'} end
local lr, st, rsid = overlapped(b, sk)
if lr then return {'overlap', lr, st, rsid} end
create(b, ARGV, 4)
if (tonumber(redis.call('HGET', sk, 'keep') or '0') or 0) > 0 then
  redis.call('ZADD', b .. ':sruns:' .. sid, ARGV[11], ARGV[4])
end
redis.call('HSET', sk, 'lfire', ARGV[3], 'lrun', ARGV[4])
redis.call('PUBLISH', b .. ':wake', 'e')
return {'ok'}
`)

// scheduleScript upserts a schedule. ARGV base, candidate id, name field
// (workflow and name; empty for an unnamed schedule, which always uses
// the candidate id), spec (what decides the fire times), workflow, cron,
// location, input, labels, misfire JSON, last (ms), next (ms), display
// name, declared ('1' for a Workflow.Cron entry), overlap policy, seconds
// ('1' for six fields), Active period, runs to keep (0: all), windows JSON,
// tenant (Schedule.Tenant; empty for none and for a declared cron).
// A schedule
// whose spec is unchanged keeps its last and next fire times, so an app
// that declares its schedules at every boot neither skips nor repeats a
// tick; its input and labels are replaced. Returns the schedule id.
var scheduleScript = goredis.NewScript(luaPrelude + `
local b, id, f = ARGV[1], ARGV[2], ARGV[3]
if f ~= '' then
  local cur = redis.call('HGET', b .. ':schedname', f)
  if cur then id = cur else redis.call('HSET', b .. ':schedname', f, id) end
end
local sk = b .. ':sched:' .. id
redis.call('HSET', sk, 'name', f, 'wf', ARGV[5], 'cron', ARGV[6], 'loc', ARGV[7],
  'input', ARGV[8], 'labels', ARGV[9], 'misfire', ARGV[10], 'sname', ARGV[13],
  'declared', ARGV[14], 'overlap', ARGV[15], 'sec', ARGV[16], 'act', ARGV[17], 'keep', ARGV[18], 'win', ARGV[19], 'tn', ARGV[20])
if tonumber(ARGV[18]) > 0 then
  redis.call('SADD', b .. ':keepscheds', id)
else
  redis.call('SREM', b .. ':keepscheds', id)
end
if redis.call('HGET', sk, 'spec') ~= ARGV[4] or not redis.call('HGET', sk, 'next') then
  redis.call('HSET', sk, 'spec', ARGV[4], 'last', ARGV[11], 'next', ARGV[12])
  redis.call('ZADD', b .. ':scheds', ARGV[12], id)
end
return id
`)

// unscheduleScript removes a schedule and its name. ARGV base, id.
var unscheduleScript = goredis.NewScript(luaPrelude + `
local b, id = ARGV[1], ARGV[2]
local sk = b .. ':sched:' .. id
local f = redis.call('HGET', sk, 'name')
if f and f ~= '' and redis.call('HGET', b .. ':schedname', f) == id then
  redis.call('HDEL', b .. ':schedname', f)
end
redis.call('DEL', sk)
redis.call('ZREM', b .. ':scheds', id)
redis.call('SREM', b .. ':keepscheds', id)
redis.call('DEL', b .. ':sruns:' .. id)
return 1
`)

// forceScript ends a run from outside. ARGV base, id, mode ('complete' or
// 'fail'), finished (ns), error, retention (ms), then for 'complete' with
// a chain the next job's idempotency field and createArity values.
// 'complete' accepts an unfinished or failed run and ends it succeeded,
// handing its chain on; 'fail' accepts an unfinished run and ends it
// failed. Returns 'ok', 'notfound' or the run's status when the mode does
// not accept it. The holder is told through a published message and stops
// its steps; its later writes are fenced off because the run is terminal.
var forceScript = goredis.NewScript(luaPrelude + `
local b, id, mode = ARGV[1], ARGV[2], ARGV[3]
local key = rk(b, id)
local st = redis.call('HGET', key, 'status')
if not st then return 'notfound' end
if mode == 'complete' then
  if terminal(st) and st ~= 'failed' then return st end
  redis.call('HSET', key, 'status', 'succeeded', 'err', '', 'fstep', '', 'forced', '')
else
  if terminal(st) then return st end
  redis.call('HSET', key, 'status', 'failed', 'err', ARGV[5], 'forced', '1')
end
redis.call('HSET', key, 'finished', ARGV[4], 'owner', '')
if st == 'failed' then
  -- already ended: only the end is recorded again for observers
  redis.call('ZADD', b .. ':ended:' .. redis.call('HGET', key, 'wf'), 0, id)
else
  ended(b, id, ARGV[6])
end
if mode == 'complete' and #ARGV > 7 then
  create_idem(b, ARGV[7], ARGV, 8)
  redis.call('HSET', key, 'chain', '')
  redis.call('PUBLISH', b .. ':wake', 'e')
end
redis.call('PUBLISH', b .. ':wake', 'f:' .. id)
return 'ok'
`)

// waitScript begins, or finds, a held run's wait. ARGV base, id, token,
// step, signal key (event name and key), deadline (ns, 0 for none). A
// signal kept since the run was enqueued that the run has not taken
// resolves the wait at once. Otherwise the wait is registered (once: a
// second call returns the stored deadline). Returns {'got', payload},
// {'wait', deadline} or {'lost'} when the token no longer owns the run.
var waitScript = goredis.NewScript(luaPrelude + `
local b, id, tok, step, sk = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local key = rk(b, id)
if redis.call('HGET', key, 'owner') ~= tok then return {'lost'} end
local v = redis.call('HGET', key, 'wv:' .. step)
if v then return {'got', v} end
if redis.call('HGET', key, 'w:' .. step) then
  return {'wait', redis.call('HGET', key, 'wd:' .. step) or '0'}
end
local sf = redis.call('HGET', key, 'sf') or '0'
for _, n in ipairs(redis.call('ZRANGEBYSCORE', b .. ':sigs:' .. sk, '(' .. sf, '+inf')) do
  if not redis.call('HGET', key, 'c:' .. n) then
    local p = redis.call('HGET', b .. ':sigdata:' .. sk, n) or ''
    redis.call('HSET', key, 'c:' .. n, '1', 'wv:' .. step, p)
    return {'got', p}
  end
end
redis.call('HSET', key, 'w:' .. step, sk, 'wd:' .. step, ARGV[6])
redis.call('SADD', b .. ':waiters:' .. sk, id .. '\n' .. step)
return {'wait', ARGV[6]}
`)

// signalScript keeps a signal and resolves every registered wait on its
// name and key, one wait per run: the signal's payload is stored for the
// step and the run is made ready (or, when a client holds it, flagged so
// its hand-back makes it ready). ARGV base, signal key, payload, now (ms).
// Returns how many waits it resolved.
var signalScript = goredis.NewScript(luaPrelude + `
local b, sk, payload = ARGV[1], ARGV[2], ARGV[3]
readyNow = tonumber(ARGV[4])
local n = redis.call('INCR', b .. ':sigseq')
redis.call('ZADD', b .. ':sigs:' .. sk, n, n)
redis.call('HSET', b .. ':sigdata:' .. sk, n, payload)
redis.call('SADD', b .. ':sigkeys', sk)
local resumed = 0
for _, m in ipairs(redis.call('SMEMBERS', b .. ':waiters:' .. sk)) do
  local nl = string.find(m, '\n', 1, true)
  local id, step = string.sub(m, 1, nl - 1), string.sub(m, nl + 1)
  local key = rk(b, id)
  local h = redis.call('HMGET', key, 'status', 'owner', 'w:' .. step, 'c:' .. n)
  if not h[1] or terminal(h[1]) or h[3] ~= sk then
    redis.call('SREM', b .. ':waiters:' .. sk, m)
  elseif not h[4] then
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

// admitScript admits pending runs of one workflow in the order given
// (priority, then age), under its concurrency and rate policy, decided
// against the stored bucket members and rate windows in the same script,
// so two clients admitting at once never exceed a limit. ARGV base,
// workflow, now (ms, scheduling clock), finished (ns), retention (ms),
// max runs per bucket (0: no concurrency policy), strategy, number of rate
// limits L, FIFO ('1': a run held back holds back the later runs of its
// bucket), then L pairs of limit and window (ms), then per run: id, queue,
// bucket, run deadline (ms, 0 for none), then L rate keys. Returns the
// earliest time (ms) a run held back by a rate limit may start, or 0.
var admitScript = goredis.NewScript(luaPrelude + `
local b, wf, now, fin, ret = ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4], ARGV[5]
readyNow = now
local maxr, strat, nl, fifo = tonumber(ARGV[6]), ARGV[7], tonumber(ARGV[8]), ARGV[9] == '1'
local lim, per = {}, {}
for i = 1, nl do lim[i], per[i] = tonumber(ARGV[8 + 2 * i]), tonumber(ARGV[9 + 2 * i]) end
local first, width = 10 + 2 * nl, 4 + nl
local wake = 0
local held = {}
local pend = b .. ':pending:' .. wf
for j = first, #ARGV, width do
  local id, q, bucket, rto = ARGV[j], ARGV[j + 1], ARGV[j + 2], ARGV[j + 3]
  local st = redis.call('HGET', rk(b, id), 'status')
  if not redis.call('ZSCORE', pend, id) then
    -- admitted or ended meanwhile
  elseif not st or terminal(st) then
    redis.call('ZREM', pend, id)
  else
    local blocked = fifo and held[bucket] or false
    for i = 1, nl do
      local wk = b .. ':rate:' .. wf .. '\n' .. i .. '\n' .. ARGV[j + 3 + i]
      redis.call('ZREMRANGEBYSCORE', wk, '-inf', now - per[i])
      if redis.call('ZCARD', wk) >= lim[i] then
        blocked = true
        local o = redis.call('ZRANGE', wk, 0, 0, 'WITHSCORES')
        local at = tonumber(o[2]) + per[i]
        if wake == 0 or at < wake then wake = at end
      end
    end
    local ck = ''
    if not blocked and maxr > 0 then
      ck = b .. ':conc:' .. wf .. '\n' .. bucket
      if redis.call('ZCARD', ck) >= maxr then
        if strat == 'drop_newest' or strat == 'cancel_newest' then
          cancel_run(b, id, fin, 'redis: concurrency bucket full (' .. strat .. ')', ret)
          blocked = true
        elseif strat == 'cancel_in_progress' then
          while redis.call('ZCARD', ck) >= maxr do
            local old = redis.call('ZRANGE', ck, 0, 0)[1]
            local ost = redis.call('HGET', rk(b, old), 'status')
            if ost and not terminal(ost) then
              cancel_run(b, old, fin, 'redis: cancelled by a newer run (cancel_in_progress)', ret)
            end
            redis.call('ZREM', ck, old)
          end
        else
          blocked = true
        end
      end
    end
    if blocked then held[bucket] = true end
    if not blocked then
      for i = 1, nl do
        local wk = b .. ':rate:' .. wf .. '\n' .. i .. '\n' .. ARGV[j + 3 + i]
        redis.call('ZADD', wk, now, id)
        redis.call('PEXPIRE', wk, per[i] * 2 + 60000)
      end
      if ck ~= '' then redis.call('ZADD', ck, now, id) end
      redis.call('HSET', rk(b, id), 'q', q, 'adm', '1', 'ck', ck, 'rto', rto)
      if tonumber(rto) > 0 then redis.call('ZADD', b .. ':rto', rto, id) end
      redis.call('ZREM', pend, id)
      enready(b, id)
      redis.call('PUBLISH', b .. ':wake', 'e')
    end
  end
end
return wake
`)

// retryScript re-opens a failed run from its failed step. ARGV base, id,
// the run's finished value as read (so a run that changed since is not
// touched), the new steps JSON, now (ms). It takes the idempotency key
// again. Returns {'ok'}, {'notfound'}, {'state', status} or {'dup', id,
// status, sid}.
var retryScript = goredis.NewScript(luaPrelude + `
local b, id = ARGV[1], ARGV[2]
readyNow = tonumber(ARGV[5])
local key = rk(b, id)
local h = redis.call('HMGET', key, 'status', 'finished', 'idem', 'wf', 'sf')
if not h[1] then return {'notfound'} end
if h[1] ~= 'failed' or h[2] ~= ARGV[3] then return {'state', h[1]} end
if h[3] and h[3] ~= '' then
  local cur = redis.call('HGET', b .. ':idem', h[3])
  if cur and cur ~= id then
    local c = redis.call('HMGET', rk(b, cur), 'status', 'sid')
    if c[1] and not terminal(c[1]) then return {'dup', cur, c[1], c[2] or ''} end
  end
  redis.call('HSET', b .. ':idem', h[3], id)
end
redis.call('PERSIST', key)
redis.call('HSET', key, 'status', 'queued', 'err', '', 'fstep', '', 'finished', '', 'forced', '',
  'steps', ARGV[4], 'owner', '', 'reclaims', '0', 'adm', '0')
redis.call('HDEL', key, 'ck', 'sigpend', 'rto')
for _, f in ipairs(redis.call('HKEYS', key)) do
  if string.sub(f, 1, 3) == 'xs:' then redis.call('HDEL', key, f) end
end
redis.call('ZREM', b .. ':ended:' .. h[4], id)
redis.call('ZADD', b .. ':floors', h[5] or '0', id)
enready(b, id)
redis.call('PUBLISH', b .. ':wake', 'e')
return {'ok'}
`)

// deliverScript claims undelivered run ends of the given workflows for
// run-finished observers, under a lease, so one client delivers each end
// and a client that dies mid-delivery leaves it to another. An end whose
// run Redis no longer has is dropped. ARGV base, lease now (ms), lease
// until (ms), batch size, then workflow names. Returns a flat list of
// {workflow, id, HGETALL of the run}.
var deliverScript = goredis.NewScript(luaPrelude + `
local b, lnow, luntil, n = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
local out = {}
for i = 5, #ARGV do
  local ek = b .. ':ended:' .. ARGV[i]
  for _, id in ipairs(redis.call('ZRANGEBYSCORE', ek, '-inf', '(' .. lnow, 'LIMIT', 0, n)) do
    local h = redis.call('HGETALL', rk(b, id))
    if #h == 0 then
      redis.call('ZREM', ek, id)
    else
      redis.call('ZADD', ek, luntil, id)
      table.insert(out, ARGV[i])
      table.insert(out, id)
      table.insert(out, h)
    end
  end
end
return out
`)

// ackScript marks a run end delivered: it removes the entry only when it
// still carries the lease this client set, so an end recorded again
// meanwhile (a Retry that failed again) is delivered again. ARGV base,
// workflow, id, lease until (ms).
var ackScript = goredis.NewScript(luaPrelude + `
local ek = ARGV[1] .. ':ended:' .. ARGV[2]
if redis.call('ZSCORE', ek, ARGV[3]) == ARGV[4] then redis.call('ZREM', ek, ARGV[3]) end
return 1
`)

// housekeepScript drops what can never matter again: kept signals no
// unfinished run may take (each run takes only signals numbered above its
// floor), and recorded ends whose run Redis expired before any observer
// took them. ARGV base, then workflow names whose ends to check.
var housekeepScript = goredis.NewScript(luaPrelude + `
local b = ARGV[1]
local fl = redis.call('ZRANGE', b .. ':floors', 0, 0, 'WITHSCORES')
local floor = tonumber(redis.call('GET', b .. ':sigseq') or '0')
if fl[1] then floor = tonumber(fl[2]) end
for _, sk in ipairs(redis.call('SMEMBERS', b .. ':sigkeys')) do
  local zk = b .. ':sigs:' .. sk
  for _, n in ipairs(redis.call('ZRANGEBYSCORE', zk, '-inf', floor)) do
    redis.call('HDEL', b .. ':sigdata:' .. sk, n)
  end
  redis.call('ZREMRANGEBYSCORE', zk, '-inf', floor)
  if redis.call('ZCARD', zk) == 0 then
    redis.call('SREM', b .. ':sigkeys', sk)
    redis.call('DEL', b .. ':sigdata:' .. sk)
  end
end
for i = 2, #ARGV do
  local ek = b .. ':ended:' .. ARGV[i]
  for _, id in ipairs(redis.call('ZRANGE', ek, 0, 199)) do
    if redis.call('EXISTS', rk(b, id)) == 0 then redis.call('ZREM', ek, id) end
  end
end
return 1
`)

// rescheduleScript moves a delayed run that has not started. ARGV base,
// id, due (ms), RunAt (ns), now (ms). A due time not after now makes it
// ready at once. Returns 'ok', 'notfound' or the run's status when it is
// not scheduled.
var rescheduleScript = goredis.NewScript(luaPrelude + `
local b, id, due, now = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[5])
readyNow = now
local key = rk(b, id)
local st = redis.call('HGET', key, 'status')
if not st then return 'notfound' end
if st ~= 'scheduled' then return st end
redis.call('HSET', key, 'runat', ARGV[4])
if due > now then
  redis.call('ZADD', b .. ':due', due, id)
else
  redis.call('ZREM', b .. ':due', id)
  redis.call('HSET', key, 'status', 'queued')
  enready(b, id)
  redis.call('PUBLISH', b .. ':wake', 'e')
end
return 'ok'
`)

// fieldScript sets fields of a held run (progress, the nodes that ran
// it), fenced by the claim token. ARGV base, id, token, then field and
// value pairs. Returns 1, or 0 when fenced.
var fieldScript = goredis.NewScript(luaPrelude + `
local key = rk(ARGV[1], ARGV[2])
if redis.call('HGET', key, 'owner') ~= ARGV[3] then return 0 end
for i = 4, #ARGV, 2 do redis.call('HSET', key, ARGV[i], ARGV[i + 1]) end
return 1
`)

// keepScript forgets the finished runs of schedules with a KeepRuns limit
// beyond the newest N, as Delete would; unfinished runs are kept and not
// counted. ARGV base. Returns how many runs it forgot.
var keepScript = goredis.NewScript(luaPrelude + `
local b = ARGV[1]
local gone = 0
for _, sid in ipairs(redis.call('SMEMBERS', b .. ':keepscheds')) do
  local keep = tonumber(redis.call('HGET', b .. ':sched:' .. sid, 'keep') or '0') or 0
  local zk = b .. ':sruns:' .. sid
  if keep <= 0 then
    redis.call('SREM', b .. ':keepscheds', sid)
  else
    local kept = 0
    for _, id in ipairs(redis.call('ZREVRANGE', zk, 0, -1)) do
      local h = redis.call('HMGET', rk(b, id), 'status', 'wf')
      if not h[1] then
        redis.call('ZREM', zk, id)
      elseif terminal(h[1]) then
        kept = kept + 1
        if kept > keep then
          redis.call('DEL', rk(b, id))
          redis.call('ZREM', b .. ':runs', id)
          redis.call('ZREM', b .. ':ended:' .. h[2], id)
          redis.call('ZREM', zk, id)
          gone = gone + 1
        end
      end
    end
  end
end
return gone
`)

// cancelStepScript withdraws one step of an unfinished run: it flags the
// step (the holder, or the next one to claim the run, fails it unless an
// attempt running now returns an output) and makes a run nobody holds
// ready now so its step is failed at once. ARGV base, id, step, now (ms).
// Returns 'ok', 'notfound' or the run's terminal status.
var cancelStepScript = goredis.NewScript(luaPrelude + `
local b, id, step = ARGV[1], ARGV[2], ARGV[3]
readyNow = tonumber(ARGV[4])
local key = rk(b, id)
local h = redis.call('HMGET', key, 'status', 'owner', 'adm')
if not h[1] then return 'notfound' end
if terminal(h[1]) then return h[1] end
redis.call('HSET', key, 'xs:' .. step, '1')
if h[2] == '' and h[3] == '1' then
  redis.call('ZREM', b .. ':due', id)
  redis.call('HSET', key, 'status', 'queued')
  enready(b, id)
end
redis.call('PUBLISH', b .. ':wake', 'x:' .. id .. '\n' .. step)
return 'ok'
`)

// expireScript makes ready the runs past their Workflow.RunTimeout that
// no client holds, so a worker claims and fails them; a held run is told
// through a published message. ARGV base, now (ms). Returns how many it
// made ready.
var expireScript = goredis.NewScript(luaPrelude + `
local b, now = ARGV[1], ARGV[2]
readyNow = tonumber(now)
local n = 0
for _, id in ipairs(redis.call('ZRANGEBYSCORE', b .. ':rto', '-inf', now, 'LIMIT', 0, 100)) do
  redis.call('ZREM', b .. ':rto', id)
  local key = rk(b, id)
  local h = redis.call('HMGET', key, 'status', 'owner', 'adm')
  if h[1] and not terminal(h[1]) then
    if h[2] == '' then
      redis.call('ZREM', b .. ':due', id)
      redis.call('HSET', key, 'status', 'queued')
      enready(b, id)
      n = n + 1
    else
      redis.call('PUBLISH', b .. ':wake', 't:' .. id)
    end
  end
end
if n > 0 then redis.call('PUBLISH', b .. ':wake', 'e') end
return n
`)
