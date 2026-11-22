package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/events"
)

// LineageStore is an events.LineageStore in Redis: every lineage record is
// kept under the prefix, so events.Datasets in every process sharing the
// Redis sees every produced version and every version a trigger consumed,
// and the lineage queries (events.Producers, Consumers, Outputs, Inputs)
// answer for the whole fleet.
//
// Each record is a JSON field of one hash ({prefix}:lineage:recs) named by
// its order key: At as Unix seconds (mapped to unsigned so the order holds
// before 1970) and nanoseconds, then a sequence number from one counter,
// all zero-padded decimal, so the keys sort by At and ties by recording.
// The key goes into sorted sets, all at score 0 and so ordered by key:
// every record, by dataset, by kind and dataset, by run, by trigger, and by
// trigger and dataset. Find walks the most selective set that fits the
// query newest first (ZREVRANGEBYLEX) and filters what that set does not
// decide, so the queries events.Datasets makes (a dataset's latest
// produced version, a trigger's latest consumed version of a dataset) read
// one entry. Record is one script that first claims the record's identity
// (a SHA-256 of Kind, Dataset, Version, Run, Workflow, Step and Trigger) in
// {prefix}:lineage:ids, so a second Record of the same fact, from any
// process, is ignored and the first one's Meta and At stay. Records carry
// no expiry: lineage is history.
//
//ds:def id=redis-lineagestore-bzauvu45 owner=@khanakia stability=stable
type LineageStore struct {
	rdb  goredis.UniversalClient
	base string
}

var _ events.LineageStore = (*LineageStore)(nil)

// NewLineageStore returns a LineageStore over rdb under prefix.
//
//ds:def id=redis-newlineagestore-2gc6pxjc owner=@khanakia stability=stable
func NewLineageStore(rdb goredis.UniversalClient, prefix string) (*LineageStore, error) {
	base, err := storeBase(rdb, prefix)
	if err != nil {
		return nil, err
	}
	return &LineageStore{rdb: rdb, base: base}, nil
}

// LineageStore returns a LineageStore over this client's Redis and prefix.
func (c *Client) LineageStore() *LineageStore { return &LineageStore{rdb: c.rdb, base: c.base} }

// The lineage keys. Index keys that join two values length-prefix the
// first, so ("a:b", "c") and ("a", "b:c") never share a key.
func (s *LineageStore) key(part string) string { return s.base + ":lineage:" + part }
func (s *LineageStore) dsKey(ds string) string { return s.key("ds:" + ds) }
func (s *LineageStore) runKey(run workflow.RunID) string {
	return s.key("run:" + string(run))
}
func (s *LineageStore) trigKey(trig string) string { return s.key("trig:" + trig) }
func (s *LineageStore) kindDSKey(kind events.LineageKind, ds string) string {
	return s.key("kds:" + strconv.Itoa(len(kind)) + ":" + string(kind) + ":" + ds)
}
func (s *LineageStore) trigDSKey(trig, ds string) string {
	return s.key("tds:" + strconv.Itoa(len(trig)) + ":" + trig + ":" + ds)
}

// lineageRecord is a record as stored: Meta keeps nil apart from empty
// (null against {}).
type lineageRecord struct {
	Kind     events.LineageKind `json:"k"`
	Dataset  string             `json:"d"`
	Version  string             `json:"v"`
	Meta     map[string]string  `json:"m"`
	Run      workflow.RunID     `json:"r"`
	Workflow string             `json:"w"`
	Step     string             `json:"s"`
	Trigger  string             `json:"t"`
	Sec      int64              `json:"as"`
	Nsec     int                `json:"an"`
}

// lineageRecordScript records a fact once. KEYS ids, seq, recs, all,
// dataset, kind+dataset, run, trigger, trigger+dataset; ARGV identity,
// order prefix, record JSON, then '1' or '0' for whether the record has a
// dataset, a run and a trigger. Returns 1 when recorded, 0 when the fact
// was already there.
var lineageRecordScript = goredis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
local m = ARGV[2] .. string.format('%020d', redis.call('INCR', KEYS[2]))
redis.call('HSET', KEYS[1], ARGV[1], m)
redis.call('HSET', KEYS[3], m, ARGV[3])
redis.call('ZADD', KEYS[4], 0, m)
if ARGV[4] == '1' then
  redis.call('ZADD', KEYS[5], 0, m)
  redis.call('ZADD', KEYS[6], 0, m)
end
if ARGV[5] == '1' then redis.call('ZADD', KEYS[7], 0, m) end
if ARGV[6] == '1' then
  redis.call('ZADD', KEYS[8], 0, m)
  if ARGV[4] == '1' then redis.call('ZADD', KEYS[9], 0, m) end
end
return 1
`)

// lineageIdent is the hash of l's identifying fields, each
// length-prefixed so no two different records share an encoding.
func lineageIdent(l events.Lineage) string {
	h := sha256.New()
	for _, f := range []string{string(l.Kind), l.Dataset, l.Version, string(l.Run), l.Workflow, l.Step, l.Trigger} {
		h.Write([]byte(strconv.Itoa(len(f))))
		h.Write([]byte{':'})
		h.Write([]byte(f))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// orderPrefix is the At part of a record's order key: the seconds mapped
// onto uint64 so that negative ones sort first, then the nanoseconds.
func orderPrefix(at time.Time) string {
	return fmt.Sprintf("%020d%09d", uint64(at.Unix())^(1<<63), at.Nanosecond())
}

func flag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// Record implements events.LineageStore in one script (see LineageStore).
func (s *LineageStore) Record(ctx context.Context, l events.Lineage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rec, err := json.Marshal(lineageRecord{Kind: l.Kind, Dataset: l.Dataset, Version: l.Version, Meta: l.Meta, Run: l.Run,
		Workflow: l.Workflow, Step: l.Step, Trigger: l.Trigger, Sec: l.At.Unix(), Nsec: l.At.Nanosecond()})
	if err != nil {
		return fmt.Errorf("redis: lineage record: %w", err)
	}
	keys := []string{s.key("ids"), s.key("seq"), s.key("recs"), s.key("all"), s.dsKey(l.Dataset), s.kindDSKey(l.Kind, l.Dataset),
		s.runKey(l.Run), s.trigKey(l.Trigger), s.trigDSKey(l.Trigger, l.Dataset)}
	err = lineageRecordScript.Run(ctx, s.rdb, keys, lineageIdent(l), orderPrefix(l.At), rec,
		flag(l.Dataset != ""), flag(l.Run != ""), flag(l.Trigger != "")).Err()
	if err != nil {
		return fmt.Errorf("redis: lineage record: %w", classify(err))
	}
	return nil
}

// lineagePage is how many order keys Find reads per round trip.
const lineagePage = 256

// Find implements events.LineageStore: the matching records, latest At
// first, ties latest recorded first. A negative Limit is refused with
// workflow.ErrInvalid.
func (s *LineageStore) Find(ctx context.Context, q events.LineageQuery) ([]events.Lineage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.Limit < 0 {
		return nil, fmt.Errorf("%w: redis: lineage query Limit %d is negative", workflow.ErrInvalid, q.Limit)
	}
	// The most selective set the query names; the filters it does not
	// decide are checked on each record.
	var set string
	switch {
	case q.Run != "":
		set = s.runKey(q.Run)
	case q.Trigger != "" && q.Dataset != "":
		set = s.trigDSKey(q.Trigger, q.Dataset)
	case q.Trigger != "":
		set = s.trigKey(q.Trigger)
	case q.Dataset != "" && q.Kind != "":
		set = s.kindDSKey(q.Kind, q.Dataset)
	case q.Dataset != "":
		set = s.dsKey(q.Dataset)
	default:
		set = s.key("all")
	}
	var out []events.Lineage
	// Each page starts below the last key read, not at an offset, so a
	// record added meanwhile (always above: it is newer, or later
	// recorded) never shifts a record into a second page.
	for hi := "+"; ; {
		keys, err := s.rdb.ZRevRangeByLex(ctx, set, &goredis.ZRangeBy{Max: hi, Min: "-", Count: lineagePage}).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: lineage find: %w", classify(err))
		}
		if len(keys) == 0 {
			return out, nil
		}
		recs, err := s.rdb.HMGet(ctx, s.key("recs"), keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: lineage find: %w", classify(err))
		}
		for i, v := range recs {
			if v == nil {
				return nil, fmt.Errorf("redis: lineage find: record %s is missing", keys[i])
			}
			var r lineageRecord
			if err := json.Unmarshal([]byte(str(v)), &r); err != nil {
				return nil, fmt.Errorf("redis: lineage find: record %s: %w", keys[i], err)
			}
			l := events.Lineage{Kind: r.Kind, Dataset: r.Dataset, Version: r.Version, Meta: r.Meta, Run: r.Run,
				Workflow: r.Workflow, Step: r.Step, Trigger: r.Trigger, At: time.Unix(r.Sec, int64(r.Nsec)).UTC()}
			if (q.Kind != "" && l.Kind != q.Kind) || (q.Dataset != "" && l.Dataset != q.Dataset) ||
				(q.Run != "" && l.Run != q.Run) || (q.Trigger != "" && l.Trigger != q.Trigger) {
				continue
			}
			out = append(out, l)
			if q.Limit > 0 && len(out) == q.Limit {
				return out, nil
			}
		}
		if len(keys) < lineagePage {
			return out, nil
		}
		hi = "(" + keys[len(keys)-1]
	}
}
