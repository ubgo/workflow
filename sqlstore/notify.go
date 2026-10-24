package sqlstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Notifier carries wake-ups between the clients of one database, so an
// idle worker on another machine starts a run as soon as it is enqueued
// instead of at its next poll. It is a latency tool only: every client still
// polls (WithPollInterval), so a lost, late or duplicated wake-up changes
// when work starts, never whether it runs.
//
// The sqlstore/pgnotify package implements it on Postgres LISTEN/NOTIFY.
// Any other bus (Redis pub/sub, NATS, an in-process fan-out in tests) fits
// the same two methods.
type Notifier interface {
	// Notify tells every client listening on channel that work may be due.
	// The client calls it from one background goroutine, coalescing
	// bursts, with a bounded context; an error is logged and dropped.
	Notify(ctx context.Context, channel string) error
	// Listen subscribes to channel and calls wake for every notification
	// until ctx ends, then returns. It may call wake spuriously (once after
	// subscribing is recommended, to cover the gap before it). Any other
	// return is a lost subscription: the client logs it and calls Listen
	// again after its poll interval (at most a second), polling meanwhile.
	Listen(ctx context.Context, channel string, wake func()) error
}

// WithNotifier wakes idle clients of the same tables on other machines
// when this client makes work due (an enqueue, a finished step, a retry, a
// resume), and wakes this client when another one does. Without it, which
// is the default, a client finds another node's work at its next poll.
// Clients over different table prefixes or schemas use different channels.
func WithNotifier(n Notifier) Option {
	return func(cl *Client) { cl.notifier = n }
}

// notifyTimeout bounds one Notify call, so a stuck bus cannot hold the
// sender goroutine past a few polls.
const notifyTimeout = 5 * time.Second

// listenRetry is the longest a lost subscription waits before Listen is
// called again; a shorter poll interval is used instead.
const listenRetry = time.Second

// maxChannelLen is the longest channel name Postgres accepts (NAMEDATALEN
// minus one); longer names are hashed down to fit.
const maxChannelLen = 63

// channelPrefix starts every wake-up channel name, so the library's
// channels are recognisable on a shared bus.
const channelPrefix = "ubgo_workflow_"

// notifyChannel is the channel the clients of these tables share: the
// schema and table prefix, which are what make two clients one cluster.
func (c *Client) notifyChannel() string {
	name := channelPrefix + c.prefix + "wake"
	if c.schema != "" {
		name = channelPrefix + c.schema + "_" + c.prefix + "wake"
	}
	if len(name) <= maxChannelLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return channelPrefix + hex.EncodeToString(sum[:16])
}

// signalFleet asks the sender goroutine to notify the other clients. It
// never blocks: a signal already pending covers this one too.
func (c *Client) signalFleet() {
	if c.notifier == nil {
		return
	}
	if c.enqueueOnly {
		// No sender goroutine runs here: notify before the enqueue returns.
		ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
		defer cancel()
		if err := c.notifier.Notify(ctx, c.notifyChannel()); err != nil {
			c.log.Warn("sqlstore: notify other clients", "error", err)
		}
		return
	}
	select {
	case c.fleet <- struct{}{}:
	default:
	}
}

// startNotifierLocked starts the sender and the listener when a Notifier
// is set. c.mu is held and c.ctx is set.
func (c *Client) startNotifierLocked() {
	if c.notifier == nil {
		return
	}
	ch := c.notifyChannel()
	c.wg.Add(2)
	go c.notifySender(ch)
	go c.notifyListener(ch)
}

// notifySender sends one Notify per pending signal until the client stops.
func (c *Client) notifySender(ch string) {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.fleet:
		}
		ctx, cancel := context.WithTimeout(c.ctx, notifyTimeout)
		err := c.notifier.Notify(ctx, ch)
		cancel()
		if err != nil && c.ctx.Err() == nil {
			c.log.Warn("sqlstore: notify other clients", "channel", ch, "error", err)
		}
	}
}

// notifyListener keeps a subscription open and wakes this client's loops
// on every notification. A lost subscription is retried after the poll
// interval or listenRetry, whichever is shorter; polling covers the gap.
func (c *Client) notifyListener(ch string) {
	defer c.wg.Done()
	for {
		err := c.notifier.Listen(c.ctx, ch, c.wakeLocal)
		if c.ctx.Err() != nil {
			return
		}
		c.log.Warn("sqlstore: listen for other clients", "channel", ch, "error", err)
		t := c.leaseClk.NewTimer(min(c.poll, listenRetry))
		select {
		case <-c.ctx.Done():
			t.Stop()
			return
		case <-t.C():
		}
	}
}
