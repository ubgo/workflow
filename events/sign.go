package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow/clock"
)

// ErrSignature means a request's signature is missing, malformed, does
// not match, or (for a timestamped scheme) is too old or too new. The
// webhook handler answers it with 401 and never says which.
var ErrSignature = errors.New("events: signature does not verify")

// Verifier checks an inbound request's signature over its body (the raw
// bytes, read once by the handler). It returns the delivery ID the
// sender put on the request, or "" when the scheme has none, and an
// error matching ErrSignature when the request is not authentic.
type Verifier interface {
	Verify(r *http.Request, body []byte) (id string, err error)
}

// Encoding is how an HMAC scheme writes its digest.
type Encoding string

// The encodings.
const (
	EncodingHex    Encoding = "hex"
	EncodingBase64 Encoding = "base64"
)

// HMAC is a plain HMAC scheme: a header holding Prefix followed by the
// HMAC of the body, with no timestamp (the shape GitHub uses with
// X-Hub-Signature-256 and "sha256=", or Shopify with
// X-Shopify-Hmac-Sha256 and base64). It signs outgoing bodies (Sign) and
// verifies incoming ones (Verify). Several secrets verify, so a secret
// can be rotated without dropping deliveries; Sign uses the first.
//
// It has no timestamp, so it cannot refuse a replayed request; the
// webhook handler's deduplication by delivery ID is what makes a replay
// start nothing new. Prefer StandardWebhooks where the sender supports it.
type HMAC struct {
	// Header holds the signature. Required.
	Header string

	// Secrets are the keys; at least one.
	Secrets [][]byte

	// Prefix is written before the digest ("sha256="). Optional.
	Prefix string

	// Encoding of the digest. Empty means EncodingHex.
	Encoding Encoding

	// Hash builds the hash. Nil means SHA-256.
	Hash func() hash.Hash

	// IDHeader, when set, is the header carrying the delivery ID
	// ("X-GitHub-Delivery"), which Verify returns.
	IDHeader string
}

func (h HMAC) mac(secret, body []byte) []byte {
	hf := h.Hash
	if hf == nil {
		hf = sha256.New
	}
	m := hmac.New(hf, secret)
	m.Write(body) // hash.Hash.Write never returns an error
	return m.Sum(nil)
}

func (h HMAC) encode(sum []byte) string {
	if h.Encoding == EncodingBase64 {
		return base64.StdEncoding.EncodeToString(sum)
	}
	return hex.EncodeToString(sum)
}

func (h HMAC) decode(s string) ([]byte, error) {
	if h.Encoding == EncodingBase64 {
		return base64.StdEncoding.DecodeString(s)
	}
	return hex.DecodeString(s)
}

// Sign returns the header value for body, signed with the first secret.
func (h HMAC) Sign(body []byte) (string, error) {
	if len(h.Secrets) == 0 || len(h.Secrets[0]) == 0 {
		return "", fmt.Errorf("events: HMAC has no secret")
	}
	return h.Prefix + h.encode(h.mac(h.Secrets[0], body)), nil
}

// Verify checks the header against body with every secret, in constant
// time per secret.
func (h HMAC) Verify(r *http.Request, body []byte) (string, error) {
	if h.Header == "" || len(h.Secrets) == 0 {
		return "", fmt.Errorf("%w: HMAC verifier has no Header or Secrets", ErrSignature)
	}
	v, ok := strings.CutPrefix(r.Header.Get(h.Header), h.Prefix)
	if !ok || v == "" {
		return "", ErrSignature
	}
	got, err := h.decode(v)
	if err != nil {
		return "", ErrSignature
	}
	for _, s := range h.Secrets {
		if len(s) > 0 && hmac.Equal(got, h.mac(s, body)) {
			if h.IDHeader != "" {
				return r.Header.Get(h.IDHeader), nil
			}
			return "", nil
		}
	}
	return "", ErrSignature
}

// The Standard Webhooks headers.
const (
	HeaderWebhookID        = "webhook-id"
	HeaderWebhookTimestamp = "webhook-timestamp"
	HeaderWebhookSignature = "webhook-signature"
)

// StandardWebhooksTolerance is how far a delivery's timestamp may be from
// now, either way, by default.
const StandardWebhooksTolerance = 5 * time.Minute

// standardSecretPrefix starts an encoded Standard Webhooks secret.
const standardSecretPrefix = "whsec_"

// StandardWebhooks is the Standard Webhooks scheme (standardwebhooks.com),
// offered as one convention among others, not a default: HMAC-SHA256 over
// "<id>.<timestamp>.<body>", sent as "v1,<base64>" in webhook-signature
// beside webhook-id and webhook-timestamp. The timestamp lets Verify
// refuse a replayed request older than the tolerance. Build it with
// NewStandardWebhooks.
type StandardWebhooks struct {
	secrets   [][]byte
	tolerance time.Duration
	clk       clock.Clock
}

// NewStandardWebhooks parses secrets ("whsec_" followed by base64; the
// first signs, all verify) and returns the scheme. tolerance zero means
// StandardWebhooksTolerance; clk nil means clock.Real.
func NewStandardWebhooks(secrets []string, tolerance time.Duration, clk clock.Clock) (*StandardWebhooks, error) {
	if len(secrets) == 0 {
		return nil, errors.New("events: standard webhooks needs at least one secret")
	}
	s := &StandardWebhooks{tolerance: tolerance, clk: clk}
	if s.tolerance <= 0 {
		s.tolerance = StandardWebhooksTolerance
	}
	if s.clk == nil {
		s.clk = clock.Real{}
	}
	for i, raw := range secrets {
		key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(raw, standardSecretPrefix))
		if err != nil || len(key) == 0 {
			return nil, fmt.Errorf("events: standard webhooks secret %d is not %q followed by base64", i, standardSecretPrefix)
		}
		s.secrets = append(s.secrets, key)
	}
	return s, nil
}

func standardSign(key []byte, id string, ts int64, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + ".")) // hash.Hash.Write never returns an error
	m.Write(body)
	return base64.StdEncoding.EncodeToString(m.Sum(nil))
}

// Headers returns the three headers that sign body as delivery id at the
// clock's now, for an outgoing request.
func (s *StandardWebhooks) Headers(id string, body []byte) http.Header {
	ts := s.clk.Now().Unix()
	h := http.Header{}
	h.Set(HeaderWebhookID, id)
	h.Set(HeaderWebhookTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderWebhookSignature, "v1,"+standardSign(s.secrets[0], id, ts, body))
	return h
}

// Verify checks the headers against body: an ID, a timestamp within the
// tolerance of now, and a "v1," signature that matches under any secret.
// It returns the webhook-id.
func (s *StandardWebhooks) Verify(r *http.Request, body []byte) (string, error) {
	id := r.Header.Get(HeaderWebhookID)
	ts, err := strconv.ParseInt(r.Header.Get(HeaderWebhookTimestamp), 10, 64)
	if id == "" || err != nil {
		return "", ErrSignature
	}
	at := time.Unix(ts, 0)
	if d := s.clk.Now().Sub(at); d > s.tolerance || d < -s.tolerance {
		return "", ErrSignature
	}
	for _, sig := range strings.Fields(r.Header.Get(HeaderWebhookSignature)) {
		v, ok := strings.CutPrefix(sig, "v1,")
		if !ok {
			continue
		}
		for _, key := range s.secrets {
			if hmac.Equal([]byte(v), []byte(standardSign(key, id, ts, body))) {
				return id, nil
			}
		}
	}
	return "", ErrSignature
}
