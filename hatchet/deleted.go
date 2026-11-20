package hatchet

import "sync"

// deletedStartsMax bounds deletedStarts: the most recent tokens are kept,
// the oldest dropped. A dropped token costs only speed: its ScheduledID
// then reads cancelled after Config.VisibilityLag instead of at once.
const deletedStartsMax = 4096

// deletedStarts holds the tokens of the delayed starts this client's
// Cancel deleted while they were at least rescheduleGuard from due, which
// the engine cannot have fired. resolve calls such a ScheduledID cancelled
// at once; any other ScheduledID whose scheduled record is gone and whose
// run is not listed waits out the engine's read lag first, since the run
// it fired may not be listed yet. It lives in one process: another
// client's Status of the same ScheduledID still waits the lag. The zero
// value is ready to use.
type deletedStarts struct {
	mu   sync.Mutex
	set  map[string]struct{}
	ring []string // the tokens in set, oldest at next once full
	next int
}

// add records token, dropping the oldest once deletedStartsMax are held.
func (d *deletedStarts) add(token string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.set == nil {
		d.set = make(map[string]struct{})
		d.ring = make([]string, deletedStartsMax)
	}
	if _, ok := d.set[token]; ok {
		return
	}
	if old := d.ring[d.next]; old != "" {
		delete(d.set, old)
	}
	d.ring[d.next] = token
	d.set[token] = struct{}{}
	d.next = (d.next + 1) % deletedStartsMax
}

// has reports whether token was recorded by add and is still held.
func (d *deletedStarts) has(token string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.set[token]
	return ok
}
