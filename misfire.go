package workflow

import "time"

// TickSource lists a schedule's fire times. cron.Schedule satisfies it; the
// core names only the one method it needs, so it does not import a parser.
type TickSource interface {
	// Between returns the fire times t with from < t <= to, ascending, at
	// most limit of them.
	Between(from, to time.Time, loc *time.Location, limit int) []time.Time
}

// tickScanChunk is how many ticks Due asks a TickSource for at a time
// while looking for the newest ones, so a long outage of a frequent cron
// costs bounded memory however many ticks it missed.
const tickScanChunk = 1024

// Due returns the ticks of src in (last, now] that m fires, oldest first,
// in loc: the one rule every driver applies so the policies mean the same
// everywhere. last is when the scheduler last caught up (or started); now
// is the instant it woke.
//
//   - MisfireSkip: the newest tick, only when now is within MisfireGrace
//     of it; every older tick is dropped.
//   - MisfireFireOnce: the newest tick, unless Within is set and it is
//     more than Within old.
//   - MisfireFireAll: every tick no more than Within old (any age when
//     Within is zero), the newest Max of them.
//
// The zero policy is the driver's own default, so Due returns nil for it
// and the driver applies its documented behaviour instead.
func (m Misfire) Due(src TickSource, last, now time.Time, loc *time.Location) []time.Time {
	var limit time.Duration // the oldest a tick may be; zero means any age
	keep := 1
	switch m.Policy {
	case MisfireSkip:
		limit = MisfireGrace
	case MisfireFireOnce:
		limit = m.Within
	case MisfireFireAll:
		limit, keep = m.Within, m.Max
	default:
		return nil
	}
	if keep < 1 || !now.After(last) {
		return nil
	}
	from := last
	if limit > 0 {
		// Cron ticks have one-second resolution; start a second early and
		// filter exactly below, so a tick exactly limit old still counts.
		if earliest := now.Add(-limit - time.Second); earliest.After(from) {
			from = earliest
		}
	}
	ticks := newestTicks(src, from, now, loc, keep)
	if limit > 0 {
		kept := ticks[:0]
		for _, t := range ticks {
			if now.Sub(t) <= limit {
				kept = append(kept, t)
			}
		}
		ticks = kept
	}
	if len(ticks) == 0 {
		return nil
	}
	return ticks
}

// newestTicks returns the newest n ticks of src in (from, to], oldest
// first, scanning in chunks and keeping only n at a time.
func newestTicks(src TickSource, from, to time.Time, loc *time.Location, n int) []time.Time {
	var kept []time.Time
	for {
		chunk := src.Between(from, to, loc, tickScanChunk)
		kept = append(kept, chunk...)
		if len(kept) > n {
			kept = append(kept[:0], kept[len(kept)-n:]...)
		}
		if len(chunk) < tickScanChunk || !chunk[len(chunk)-1].After(from) {
			return kept // done, or a source that does not advance
		}
		from = chunk[len(chunk)-1]
	}
}
