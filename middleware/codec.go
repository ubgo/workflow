package middleware

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ubgo/workflow"
)

// ErrDecode is wrapped by the error DecodeStep returns when the input does
// not decode (a wrong key, tampered or truncated bytes, or a value that was
// never encoded). The error never carries the bytes themselves.
var ErrDecode = errors.New("workflow/middleware: payload does not decode")

// Codec turns plain bytes into the bytes an engine stores, and back. It is
// how payload encryption and compression plug in without a driver knowing.
// Decode(Encode(b)) must equal b. A Codec works on raw bytes; the
// middleware in this file wraps its output in a sealed envelope (see
// envelopePrefix) so the result survives every place a driver moves it.
type Codec interface {
	Encode(plain []byte) ([]byte, error)
	Decode(encoded []byte) ([]byte, error)
}

// envelopePrefix starts every sealed value. A sealed value is a JSON
// string, "wfenc1:" followed by the standard base64 of the Codec's output.
// It is a JSON string, not raw codec bytes, because a step with several
// parents receives workflow.StepInput's JSON object, which embeds a valid
// JSON output as is but turns any other output into a JSON string, and
// that conversion replaces invalid UTF-8, so raw ciphertext would be
// destroyed. As a JSON string the sealed output is embedded unchanged and
// can be opened per parent. The version in the prefix leaves room for a
// later format.
const envelopePrefix = "wfenc1:"

// seal encodes plain with c and wraps it in the envelope. Empty stays
// empty, so a job with no input needs no key to read.
func seal(c Codec, plain []byte) ([]byte, error) {
	if len(plain) == 0 {
		return plain, nil
	}
	enc, err := c.Encode(plain)
	if err != nil {
		return nil, err
	}
	// Marshalling a string cannot fail.
	return json.Marshal(envelopePrefix + base64.StdEncoding.EncodeToString(enc))
}

// openValue opens one sealed JSON value. ok is false when v is not an
// envelope at all, so the caller can tell "never sealed" from "sealed with
// another key".
func openValue(c Codec, v []byte) (plain []byte, ok bool, err error) {
	var s string
	if json.Unmarshal(v, &s) != nil || !strings.HasPrefix(s, envelopePrefix) {
		return nil, false, nil
	}
	enc, err := base64.StdEncoding.DecodeString(s[len(envelopePrefix):])
	if err != nil {
		return nil, true, err
	}
	plain, err = c.Decode(enc)
	return plain, true, err
}

// open turns a sealed step input back into what the step would have
// received without the codec, following the workflow.StepInput rule:
//
//   - empty: empty (an empty payload or output is never sealed);
//   - one sealed value (a job payload, a single parent's output, a sealed
//     Signal payload): its plain bytes;
//   - a JSON object (several parents): the same object, in the same key
//     order, with each sealed value replaced by its plain output embedded
//     the way StepInput embeds it, and null kept as null.
//
// Anything else, including a parent value that was never sealed, is
// refused: the codec is closed by default, so plaintext slipping past it
// (an unsealed Signal, a step without DecodeStep) fails loudly.
func open(c Codec, in []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(in)
	if len(trimmed) == 0 {
		return in, nil
	}
	if trimmed[0] != '{' {
		plain, ok, err := openValue(c, trimmed)
		switch {
		case err != nil:
			return nil, err
		case !ok:
			return nil, errNotSealed
		}
		return plain, nil
	}
	return openObject(c, trimmed)
}

var (
	errNotSealed       = errors.New("input was not sealed by this codec")
	errShortCiphertext = errors.New("ciphertext shorter than nonce and tag")
)

func openObject(c Codec, in []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(in))
	if _, err := dec.Token(); err != nil { // the opening '{'
		return nil, err
	}
	buf := []byte{'{'}
	for i := 0; dec.More(); i++ {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name, isString := tok.(string)
		if !isString {
			return nil, errNotSealed
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		if i > 0 {
			buf = append(buf, ',')
		}
		key, _ := json.Marshal(name) // marshalling a string cannot fail
		buf = append(buf, key...)
		buf = append(buf, ':')
		if string(raw) == "null" {
			buf = append(buf, "null"...)
			continue
		}
		plain, ok, err := openValue(c, raw)
		switch {
		case err != nil:
			return nil, fmt.Errorf("parent %q: %w", name, err)
		case !ok:
			return nil, fmt.Errorf("parent %q: %w", name, errNotSealed)
		}
		// The same embedding rule as workflow.StepInput, so the step sees
		// exactly the object it would see with no codec at all.
		switch {
		case len(plain) == 0:
			buf = append(buf, "null"...)
		case json.Valid(plain):
			buf = append(buf, plain...)
		default:
			s, _ := json.Marshal(string(plain)) // marshalling a string cannot fail
			buf = append(buf, s...)
		}
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		return nil, err
	}
	return append(buf, '}'), nil
}

// EncodeEnqueue seals Job.Payload, and the payload of every job in
// Job.Chain, before the job reaches the driver, so the engine only ever
// stores sealed bytes. Pair it with DecodeStep using
// the same Codec on the worker side. WithCodec does this and also seals
// Signal payloads and Schedule inputs; prefer it unless only Enqueue
// carries data.
func EncodeEnqueue(c Codec) workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			sealed, err := sealJob(c)(job)
			if err != nil {
				return workflow.RunRef{}, err
			}
			if sealed.Chain, err = mapChain(job.Chain, sealJob(c)); err != nil {
				return workflow.RunRef{}, err
			}
			return next(ctx, sealed)
		}
	}
}

// WithCodec returns a Client that seals every byte the app hands the
// driver: Job.Payload on Enqueue, Event.Payload on Signal (it becomes a
// wait step's output, which a child step decodes) and Schedule.Input on
// Schedule (each fired run's payload). The extra enqueue middleware, if
// any, runs after sealing, so it sees sealed payloads; put Unique or Route
// keyed on payload content in a WrapClient outside this one instead.
// Capabilities are c's own, and workflow.RunNotifier and the optional
// interfaces workflow.Forward covers are forwarded to c. Those pass bytes
// through as stored: a RunInfo.Payload from Runs is sealed, and opening
// it is the app's call (Codec.Decode).
func WithCodec(c workflow.Client, codec Codec, mws ...workflow.EnqueueMiddleware) workflow.Client {
	inner := WrapClient(c, append([]workflow.EnqueueMiddleware{EncodeEnqueue(codec)}, mws...)...)
	return &codecClient{Client: inner, Forward: workflow.Forward{Inner: inner}, codec: codec}
}

type codecClient struct {
	workflow.Client
	workflow.Forward
	codec Codec
}

func (cc *codecClient) OnRunFinished(fn workflow.RunFinishedFunc) error {
	return forwardRunFinished(cc.Client, fn)
}

func (cc *codecClient) Signal(ctx context.Context, ev workflow.Event) error {
	sealed, err := seal(cc.codec, ev.Payload)
	if err != nil {
		return fmt.Errorf("workflow/middleware: encode event payload: %w", err)
	}
	ev.Payload = sealed
	return cc.Client.Signal(ctx, ev)
}

func (cc *codecClient) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	sealed, err := seal(cc.codec, s.Input)
	if err != nil {
		return "", fmt.Errorf("workflow/middleware: encode schedule input: %w", err)
	}
	s.Input = sealed
	return cc.Client.Schedule(ctx, s)
}

// sealJob returns the function that seals one job's payload.
func sealJob(c Codec) func(workflow.Job) (workflow.Job, error) {
	return func(job workflow.Job) (workflow.Job, error) {
		sealed, err := seal(c, job.Payload)
		if err != nil {
			return job, fmt.Errorf("workflow/middleware: encode payload: %w", err)
		}
		job.Payload = sealed
		return job, nil
	}
}

// DecodeStep opens a step's input before the handler sees it and seals its
// output afterwards, and seals the payload of every job the step adds to
// its chain (workflow.PrependToChain, AppendToChain), so a child step's input (its parent's output) is
// sealed just like a job payload and every byte the engine stores stays
// sealed. It handles every input shape a driver builds with
// workflow.StepInput: the payload, one parent's output, and the JSON
// object of several parents, which it opens per parent. A wait step's
// output is the Signal payload, so Signal must go through WithCodec.
//
// Input that does not open, or that was never sealed, is a Permanent error
// wrapping ErrDecode: retrying the same bytes with the same key cannot
// succeed, and accepting plaintext would make the codec optional without
// anyone noticing.
func DecodeStep(c Codec) workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			plain, err := open(c, in)
			if err != nil {
				return nil, workflow.Permanent(fmt.Errorf("%w: %w", ErrDecode, err))
			}
			out, err := next(withMappedChain(ctx, sealJob(c)), plain)
			if err != nil {
				return out, err
			}
			sealed, err := seal(c, out)
			if err != nil {
				return nil, workflow.Permanent(fmt.Errorf("workflow/middleware: encode output: %w", err))
			}
			return sealed, nil
		}
	}
}

// Encrypt returns an AES-GCM Codec keyed by key, which must be 16, 24 or
// 32 bytes (AES-128, -192, -256). Each Encode draws a fresh random nonce
// and writes nonce || ciphertext || tag, so equal payloads never encrypt
// to equal bytes. Decode authenticates before returning anything, so a
// wrong key or a changed byte is an error, never garbage. The key belongs
// to the app.
//
// oldKeys rotate it: Encode always uses key, while Decode tries key first
// and then each old key in order, so payloads sealed before the rotation
// still open. The sealed format is the same for every key (no key ID is
// written), so rotating needs no change to stored runs; drop an old key
// once no run sealed with it can still be read. Every old key must be a
// valid AES key too.
//
//	codec, err := middleware.Encrypt(newKey, previousKey)
func Encrypt(key []byte, oldKeys ...[]byte) (Codec, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	g := gcmCodec{aead: aead}
	for i, k := range oldKeys {
		old, err := newGCM(k)
		if err != nil {
			return nil, fmt.Errorf("old key %d: %w", i, err)
		}
		g.old = append(g.old, old)
	}
	return g, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: Encrypt key must be 16, 24 or 32 bytes", workflow.ErrInvalid)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("workflow/middleware: gcm: %w", err)
	}
	return aead, nil
}

// gcmCodec seals with aead and opens with aead, then each of old (the
// decrypt-only keys of a rotation).
type gcmCodec struct {
	aead cipher.AEAD
	old  []cipher.AEAD
}

func (g gcmCodec) Encode(plain []byte) ([]byte, error) {
	nonce := make([]byte, g.aead.NonceSize(), g.aead.NonceSize()+len(plain)+g.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	return g.aead.Seal(nonce, nonce, plain, nil), nil
}

func (g gcmCodec) Decode(enc []byte) ([]byte, error) {
	plain, err := gcmOpen(g.aead, enc)
	for _, old := range g.old {
		if err == nil {
			break
		}
		plain, err = gcmOpen(old, enc)
	}
	return plain, err
}

// gcmOpen opens nonce || ciphertext || tag with aead. cipher's error is
// fixed text ("message authentication failed"); it never echoes the bytes.
func gcmOpen(aead cipher.AEAD, enc []byte) ([]byte, error) {
	n := aead.NonceSize()
	if len(enc) < n+aead.Overhead() {
		return nil, errShortCiphertext
	}
	return aead.Open(nil, enc[:n], enc[n:], nil)
}
