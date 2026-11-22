package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/ubgo/workflow"
)

// DefaultWebhookMaxBody caps a webhook body when WebhookConfig.MaxBody is
// zero: 1 MiB.
//
//ds:def id=events-defaultwebhookmaxbody-kdm3hgb3 owner=@khanakia stability=stable
const DefaultWebhookMaxBody = 1 << 20

// WebhookConfig configures NewWebhook. Exactly one target is set: Bus with
// Event (publish the delivery as an event), or Client with Workflow
// (enqueue one run).
//
//ds:def id=events-webhookconfig-7atgkns7 owner=@khanakia stability=stable
type WebhookConfig struct {
	// Verifier authenticates each request. Required unless Unsigned.
	Verifier Verifier

	// Unsigned accepts requests without verifying them. It must be set
	// on purpose: a nil Verifier without it is refused, so an endpoint is
	// never left open by omission.
	Unsigned bool

	// Bus and Event publish each delivery as Message{Name: Event}.
	Bus   *Bus
	Event string

	// Client and Workflow enqueue each delivery as a run of Workflow.
	Client   workflow.Client
	Workflow string

	// ID returns the delivery's deduplication ID when the Verifier gives
	// none. Nil means the SHA-256 of the body in hex, so a sender's retry
	// of the same body starts nothing new.
	ID func(r *http.Request, body []byte) string

	// MaxBody caps the body in bytes; a larger one gets 413. Zero means
	// DefaultWebhookMaxBody.
	MaxBody int64

	// Labels, when set, adds labels to the run (or event) from the
	// request (a tenant from the path). It runs per request, so a reserved
	// key cannot be refused at NewWebhook: such a request gets 500 and a
	// log line naming the config error.
	Labels func(r *http.Request) map[string]string

	// Logger gets enqueue failures, which the response does not detail.
	// Nil discards them.
	Logger *slog.Logger

	// Inbox, when set, records each delivery's ID per consumer
	// (workflow.InboxConsumer.Once): a redelivery of an ID already
	// processed is answered 200 with nothing enqueued, for as long as the
	// inbox keeps it (DefaultInboxKeep, seven days), however long the
	// driver keeps the run its idempotency key points at; a delivery
	// another request is processing gets 503, so the sender retries. A
	// durable inbox (sqlstore.Inbox, redis.Inbox) drops duplicates that
	// reach any replica. Inbox.Inbox and Inbox.Consumer are required.
	Inbox *workflow.InboxConsumer
}

// webhook is the http.Handler NewWebhook returns.
type webhook struct {
	cfg WebhookConfig
}

// NewWebhook returns an http.Handler that turns a signed delivery into a
// run (EV-03): it accepts POST only (405), reads at most MaxBody bytes
// (413), verifies the signature (401, with no detail), enqueues with the
// idempotency key "webhook/<workflow>/<id>" or publishes with the
// delivery's ID, and answers 200 with {"id": "<id>"} as soon as the run is
// stored, before it runs. An enqueue that fails because the backend is
// unavailable gets 503 and any other failure 500, so the sender retries;
// the retry carries the same ID and starts nothing twice.
//
// It returns an error wrapping ErrInvalid when the config has no target,
// two targets, or no Verifier without Unsigned.
//
//ds:def id=events-newwebhook-gweyfedm owner=@khanakia stability=stable
func NewWebhook(cfg WebhookConfig) (http.Handler, error) {
	byBus, byClient := cfg.Bus != nil || cfg.Event != "", cfg.Client != nil || cfg.Workflow != ""
	switch {
	case byBus == byClient:
		return nil, fmt.Errorf("%w: webhook needs exactly one target: Bus and Event, or Client and Workflow", workflow.ErrInvalid)
	case byBus && (cfg.Bus == nil || cfg.Event == ""):
		return nil, fmt.Errorf("%w: webhook needs both Bus and Event", workflow.ErrInvalid)
	case byClient && (cfg.Client == nil || cfg.Workflow == ""):
		return nil, fmt.Errorf("%w: webhook needs both Client and Workflow", workflow.ErrInvalid)
	case cfg.Verifier == nil && !cfg.Unsigned:
		return nil, fmt.Errorf("%w: webhook has no Verifier; set Unsigned to accept unverified requests", workflow.ErrInvalid)
	case cfg.Inbox != nil && (cfg.Inbox.Inbox == nil || cfg.Inbox.Consumer == ""):
		return nil, fmt.Errorf("%w: webhook Inbox needs an Inbox and a Consumer", workflow.ErrInvalid)
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = DefaultWebhookMaxBody
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &webhook{cfg: cfg}, nil
}

// ServeHTTP handles one delivery.
//
//ds:def id=events-webhook-servehttp-vwuu8jtt owner=@khanakia stability=stable
func (h *webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.cfg.MaxBody+1))
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.cfg.MaxBody {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	var id string
	if h.cfg.Verifier != nil {
		if id, err = h.cfg.Verifier.Verify(r, body); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if id == "" {
		id = h.deliveryID(r, body)
	}
	var labels map[string]string
	if h.cfg.Labels != nil {
		labels = h.cfg.Labels(r)
		if err := workflow.ValidateLabels(labels); err != nil {
			// A configuration error, not an enqueue failure: the Labels
			// func returned a reserved key. It cannot be checked at
			// NewWebhook (the func depends on the request). 500 makes the
			// sender retry, which succeeds once the app is fixed.
			h.cfg.Logger.ErrorContext(r.Context(), "webhook: configuration error: the Labels func returned a reserved label key; nothing was enqueued", slog.Any("err", err))
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}
	if err := h.deliver(r.Context(), id, body, labels); err != nil {
		h.fail(r.Context(), w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"id": id}) // the response is already committed
}

//ds:def id=events-webhook-deliveryid-egxfcpng owner=@khanakia stability=stable
func (h *webhook) deliveryID(r *http.Request, body []byte) string {
	if h.cfg.ID != nil {
		if id := h.cfg.ID(r, body); id != "" {
			return id
		}
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// deliver enqueues the delivery, once per ID when an Inbox is set.
//
//ds:def id=events-webhook-deliver-e8x8hcsu owner=@khanakia stability=stable
func (h *webhook) deliver(ctx context.Context, id string, body []byte, labels map[string]string) error {
	if h.cfg.Inbox == nil {
		return h.enqueue(ctx, id, body, labels)
	}
	_, err := h.cfg.Inbox.Once(ctx, id, func(ctx context.Context) error { return h.enqueue(ctx, id, body, labels) })
	return err
}

//ds:def id=events-webhook-enqueue-kem7sxby owner=@khanakia stability=stable
func (h *webhook) enqueue(ctx context.Context, id string, body []byte, labels map[string]string) error {
	if h.cfg.Bus != nil {
		_, err := h.cfg.Bus.Publish(ctx, Message{ID: id, Name: h.cfg.Event, Payload: body, Labels: labels})
		return err
	}
	_, err := h.cfg.Client.Enqueue(ctx, workflow.Job{
		Workflow: h.cfg.Workflow, Payload: body, Labels: labels,
		IdempotencyKey: "webhook/" + h.cfg.Workflow + "/" + id,
	})
	return err
}

// fail logs err and answers 503 for an outage or a delivery another
// request is processing, 500 otherwise; the body never carries the error.
//
//ds:def id=events-webhook-fail-qxvpc8yk owner=@khanakia stability=stable
func (h *webhook) fail(ctx context.Context, w http.ResponseWriter, err error) {
	h.cfg.Logger.ErrorContext(ctx, "webhook: enqueue failed", slog.Any("err", err))
	if errors.Is(err, workflow.ErrUnavailable) || errors.Is(err, workflow.ErrInboxBusy) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "internal error", http.StatusInternalServerError)
}
