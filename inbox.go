package workflow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ubgo/workflow/clock"
)

// Idempotent consumption (MQ-15, the consumer inbox of TX-07): a message
// that arrives more than once (an at-least-once broker redelivering, a
// webhook sender retrying, an outbox relay resending after a crash) is
// processed once. Job.IdempotencyKey already drops a duplicate enqueue
// while the driver keeps the run; the inbox records the message IDs a
// consumer has processed, for as long as the app says, independent of any
// driver, so the handler itself runs once per ID.
//
// The record is taken before the work and confirmed after it:
//
//  1. Claim reserves (consumer, ID) for a lease. A message already
//     processed is a duplicate; one another worker holds is busy.
//  2. The work runs.
//  3. Complete marks the ID processed and keeps it for Keep; on an error
//     Release frees it, so the redelivery runs the work again.
//
// A worker that dies between 1 and 3 leaves its claim to expire, after
// which a redelivery runs the work again: at least once, like Memo. Work
// that writes to the database the inbox lives in can be exactly once:
// the SQL driver's Inbox.MarkTx records the ID inside the app's own
// transaction, so the record and the writes commit or roll back together.

// InboxKey names one message for one consumer. Two consumers (an email
// sender and a search indexer) reading one message each process it once.
type InboxKey struct {
	// Consumer names the reader; required.
	Consumer string
	// ID is the message's ID as the sender gave it (a broker message ID,
	// a webhook delivery ID, an event ID); required.
	ID string
}

// String is the one string a store keys an entry by: the consumer's
// length, the consumer and the ID, so consumer "a/b" with ID "c" and
// consumer "a" with ID "b/c" never share an entry. Durable stores keep
// it, so its form never changes.
func (k InboxKey) String() string {
	return strconv.Itoa(len(k.Consumer)) + ":" + k.Consumer + "/" + k.ID
}

// validate refuses a key without a consumer or an ID.
func (k InboxKey) validate() error {
	if k.Consumer == "" || k.ID == "" {
		return invalidf("inbox key needs a Consumer and an ID (got %q, %q)", k.Consumer, k.ID)
	}
	return nil
}

// InboxState is what Claim found.
type InboxState int

const (
	// InboxClaimed means the caller now holds the message: process it,
	// then Complete or Release with the token.
	//ds:def id=inboxclaimed-pr3yk9gm owner=@khanakia stability=stable
	InboxClaimed InboxState = iota + 1
	// InboxDone means the message was processed already: drop it.
	//ds:def id=inboxdone-xysa8a3u owner=@khanakia stability=stable
	InboxDone
	// InboxBusy means another worker holds an unexpired claim: try again
	// after its lease (a broker nack, a 503 to a webhook sender).
	//ds:def id=inboxbusy-qh2qbbhu owner=@khanakia stability=stable
	InboxBusy
)

// String names the state.
func (s InboxState) String() string {
	switch s {
	case InboxClaimed:
		return "claimed"
	case InboxDone:
		return "done"
	case InboxBusy:
		return "busy"
	default:
		return "InboxState(" + strconv.Itoa(int(s)) + ")"
	}
}

// ErrInboxBusy is returned by InboxConsumer.Once when another worker
// holds the message. It is not Permanent: the message comes back later.
//
//ds:def id=errinboxbusy-qtu9arf9 owner=@khanakia stability=stable
var ErrInboxBusy = errors.New("workflow: message is being processed by another worker")

// ErrInboxLost is returned by Inbox.Complete when the claim's token no
// longer holds the entry: the lease expired and another worker took the
// message over (both ran the work), or the entry was released.
//
//ds:def id=errinboxlost-crwssvfu owner=@khanakia stability=stable
var ErrInboxLost = errors.New("workflow: inbox claim lost (its lease expired)")

// Inbox records which messages each consumer processed (MQ-15, TX-07).
// Every method takes now from the caller's clock; a store compares it
// with the times it stored and never reads its own clock for decisions,
// so an injected clock drives it (a store may still expire entries on its
// own clock as a cleanup net, well after they stop mattering).
//
// A store must be safe for concurrent use and decide Claim atomically for
// every client sharing it: of two workers claiming one key at once,
// exactly one gets InboxClaimed.
//
//ds:def id=inbox-kq8jz7jg owner=@khanakia stability=stable
type Inbox interface {
	// Claim takes key for the caller until now+lease and returns
	// InboxClaimed with a fresh token, unless the key was processed and
	// is still kept (InboxDone) or another claim holds it until after now
	// (InboxBusy). An expired claim or an expired done entry is taken
	// over.
	Claim(ctx context.Context, key InboxKey, lease time.Duration, now time.Time) (InboxState, string, error)

	// Complete marks key processed, kept until now+keep, when token still
	// holds it; otherwise it returns ErrInboxLost and changes nothing.
	Complete(ctx context.Context, key InboxKey, token string, keep time.Duration, now time.Time) error

	// Release frees key when token still holds it, so the next delivery
	// claims it. Releasing a claim that is gone is not an error.
	Release(ctx context.Context, key InboxKey, token string) error
}

// Default lease and keep of an InboxConsumer.
const (
	// DefaultInboxLease is how long a claim holds a message: longer than
	// the work takes, or another worker runs it too.
	//ds:def id=defaultinboxlease-s3zfehbn owner=@khanakia stability=stable
	DefaultInboxLease = 5 * time.Minute
	// DefaultInboxKeep is how long a processed ID is remembered: longer
	// than any redelivery (SQS keeps a message up to 14 days, Pub/Sub up
	// to 31 with retention on). Seven days covers most senders.
	//ds:def id=defaultinboxkeep-kzbwa4qr owner=@khanakia stability=stable
	DefaultInboxKeep = 7 * 24 * time.Hour
)

// InboxConsumer processes each message once per consumer name over an
// Inbox. The zero Lease and Keep take the defaults; a nil Clock is the
// real clock.
type InboxConsumer struct {
	Inbox    Inbox
	Consumer string
	Lease    time.Duration
	Keep     time.Duration
	Clock    clock.Clock
}

func (c InboxConsumer) now() time.Time {
	if c.Clock == nil {
		return clock.Real{}.Now()
	}
	return c.Clock.Now()
}

// Once runs fn for the message id unless this consumer processed it
// already, and reports whether fn ran. A duplicate returns false and nil.
// A message another worker holds returns false and ErrInboxBusy. fn's
// error is returned as is after the claim is released, so the next
// delivery runs fn again; after fn succeeds a failed Complete is returned
// too (the work is done, but a redelivery may run it again).
//
//ds:def id=inboxconsumer-once-mta79w6e owner=@khanakia stability=stable
func (c InboxConsumer) Once(ctx context.Context, id string, fn func(ctx context.Context) error) (bool, error) {
	if c.Inbox == nil {
		return false, invalidf("inbox consumer %q has no Inbox", c.Consumer)
	}
	key := InboxKey{Consumer: c.Consumer, ID: id}
	if err := key.validate(); err != nil {
		return false, err
	}
	lease, keep := c.Lease, c.Keep
	if lease <= 0 {
		lease = DefaultInboxLease
	}
	if keep <= 0 {
		keep = DefaultInboxKeep
	}
	state, token, err := c.Inbox.Claim(ctx, key, lease, c.now())
	if err != nil {
		return false, fmt.Errorf("workflow: inbox %s: claim: %w", key, err)
	}
	switch state {
	case InboxDone:
		return false, nil
	case InboxBusy:
		return false, fmt.Errorf("%w: %s", ErrInboxBusy, key)
	case InboxClaimed:
	default:
		return false, fmt.Errorf("workflow: inbox %s: claim returned %v", key, state)
	}
	if err := fn(ctx); err != nil {
		// A failed release only delays the redelivery until the lease
		// ends; fn's error is the one the caller acts on.
		_ = c.Inbox.Release(context.WithoutCancel(ctx), key, token)
		return true, err
	}
	if err := c.Inbox.Complete(context.WithoutCancel(ctx), key, token, keep, c.now()); err != nil {
		return true, fmt.Errorf("workflow: inbox %s: complete: %w", key, err)
	}
	return true, nil
}

// MemoryInbox is an in-process Inbox: entries live in this process and
// are lost when it exits, so it drops duplicates that reach one process.
// Prune removes entries whose time has passed. The zero value is ready.
//
//ds:def id=memoryinbox-c3zuf7tj owner=@khanakia stability=stable
type MemoryInbox struct {
	mu      sync.Mutex
	entries map[string]memoryInboxEntry
	seq     uint64
}

type memoryInboxEntry struct {
	done  bool
	token string
	until time.Time
}

var _ Inbox = (*MemoryInbox)(nil)

// NewMemoryInbox returns an empty MemoryInbox.
func NewMemoryInbox() *MemoryInbox { return &MemoryInbox{} }

// Claim implements Inbox.
func (m *MemoryInbox) Claim(_ context.Context, key InboxKey, lease time.Duration, now time.Time) (InboxState, string, error) {
	if err := key.validate(); err != nil {
		return 0, "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[string]memoryInboxEntry{}
	}
	k := key.String()
	if e, ok := m.entries[k]; ok && e.until.After(now) {
		if e.done {
			return InboxDone, "", nil
		}
		return InboxBusy, "", nil
	}
	m.seq++
	token := strconv.FormatUint(m.seq, 10)
	m.entries[k] = memoryInboxEntry{token: token, until: now.Add(lease)}
	return InboxClaimed, token, nil
}

// Complete implements Inbox.
func (m *MemoryInbox) Complete(_ context.Context, key InboxKey, token string, keep time.Duration, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key.String()
	e, ok := m.entries[k]
	if !ok || e.done || e.token != token {
		return fmt.Errorf("%w: %s", ErrInboxLost, key)
	}
	m.entries[k] = memoryInboxEntry{done: true, token: token, until: now.Add(keep)}
	return nil
}

// Release implements Inbox.
func (m *MemoryInbox) Release(_ context.Context, key InboxKey, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key.String()
	if e, ok := m.entries[k]; ok && !e.done && e.token == token {
		delete(m.entries, k)
	}
	return nil
}

// Prune removes every entry whose lease or keep ended at or before now
// and returns how many it removed.
func (m *MemoryInbox) Prune(now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, e := range m.entries {
		if !e.until.After(now) {
			delete(m.entries, k)
			n++
		}
	}
	return n
}

// Keys returns every entry's key (InboxKey.String), sorted, for tests.
func (m *MemoryInbox) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.entries))
}
