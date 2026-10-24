package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/ubgo/workflow"
)

// ErrBlobNotFound is what a BlobStore's Get returns for a key it does not
// hold. The claim check step middleware turns it into a Permanent error: a
// blob that is gone (expired, deleted by hand) does not come back on retry.
var ErrBlobNotFound = errors.New("workflow/middleware: blob not found")

// BlobStore holds the payloads a ClaimCheck moves out of the queue. It is
// the seam for S3, a filesystem or a database; adapters for those live in
// their own modules, MemoryBlobStore is the in-process one for tests.
//
// A store shared by every process that enqueues or runs steps is what
// makes the claim check work across processes: the step that resolves a
// reference may run on another machine than the enqueue that stored it.
type BlobStore interface {
	// Put stores data under key, replacing any blob already there.
	Put(ctx context.Context, key string, data []byte) error
	// Get returns the blob under key, or an error wrapping ErrBlobNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete removes the blob under key. Deleting a missing key is a
	// no-op, not an error, because run-finished events arrive at least
	// once.
	Delete(ctx context.Context, key string) error
}

// BlobPrefixDeleter is an optional upgrade of BlobStore: a store that can
// delete every key under a prefix (an S3 list-and-delete, a directory
// removal). With it, CleanupOnFinish removes a run's step outputs too;
// without it, only the run's payload is removed and step outputs are left
// to the store's own expiry. A store that cannot do it is never forced to
// pretend it can.
type BlobPrefixDeleter interface {
	// DeletePrefix removes every blob whose key starts with prefix.
	DeletePrefix(ctx context.Context, prefix string) error
}

// ClaimCheckLabel carries the run's claim check token, a random value
// ClaimCheck.Enqueue draws per job. Every blob of the run is stored under
// "<token>/", and the step middleware resolves a reference only when its
// key is under the token of the run it runs in. ClaimCheck owns the label:
// Enqueue refuses, with workflow.ErrInvalid, a job that arrives carrying it
// or any other label under workflow.LabelPrefixInternal except Unique's
// two (Unique may run before it), so a caller can neither choose a token
// nor point a run at another run's blobs. A job enqueued straight on
// the driver, bypassing ClaimCheck.Enqueue, must not carry it.
const ClaimCheckLabel = "ubgo.workflow/claim-check"

// claimRefPrefix starts every reference. A reference is a JSON string,
// "wfclaim1:" followed by the blob key, for the same reason a codec
// envelope is one: workflow.StepInput embeds a single parent's output that
// is valid JSON unchanged inside a several-parents object, so a reference
// survives that embedding and can be resolved per parent. The version in
// the prefix leaves room for a later format.
//
// The prefix alone does not make a value a reference: a payload or output
// that merely looks like one (an app storing the string "wfclaim1:x", or a
// hostile payload naming another run's key) must never be fetched. A value
// is resolved only when its key is under the token in ClaimCheckLabel of
// the run being executed. The token is drawn after the app built the
// payload, so app data cannot carry it by accident, and scoping keys by it
// means even a forged reference can reach only the run's own blobs. A
// label alone could not mark step outputs, which are produced after the
// labels are fixed; an envelope alone could be forged by any payload.
const claimRefPrefix = "wfclaim1:"

// Key segments under a run's token. The payload has a fixed name so
// CleanupOnFinish can delete it from the RunEvent alone; outputs get a
// fresh key per Put, because an attempt that succeeded may run again (a
// driver that lost the result) and must not overwrite a blob a child is
// reading.
const (
	claimKeySep        = "/"
	claimPayloadKey    = "payload"
	claimOutputKeyPart = "out"
)

// claimTokenBytes is the size of the default random key: 128 bits, so two
// runs never share a token and a token cannot be guessed.
const claimTokenBytes = 16

// ClaimCheck stores payloads larger than a threshold outside the queue and
// puts a small reference in their place (the claim check pattern), so a
// broker or engine with a payload cap (SQS, a Hatchet row) carries only
// the reference. Build it with NewClaimCheck, then wire all three halves:
//
//   - Enqueue, an enqueue middleware: stamps ClaimCheckLabel on every job
//     and moves a Job.Payload above the threshold into the store;
//   - Step, a step middleware: resolves references in a step's input
//     before the handler runs, and moves an output above the threshold
//     into the store so the child step's input is a reference too;
//   - CleanupOnFinish: deletes the run's blobs when the run finishes.
//
// Order with a Codec: claim check runs after encoding on the way in and
// before decoding on the way out, so it only ever sees sealed bytes and
// the store never holds plaintext. Concretely, pass Enqueue as an extra
// middleware of WithCodec (WithCodec(c, codec, cc.Enqueue()) seals first,
// then claims), and put Step outside DecodeStep (Chain(cc.Step(),
// DecodeStep(codec)): references are resolved before opening, and outputs
// are sealed before they are claimed).
//
// Not covered: Signal payloads and Schedule inputs stay inline (they are
// made outside any run, so there is no token to scope them by), and runs
// fired by a Schedule or a declared Cron carry no token, so their payload
// and outputs stay inline whatever their size.
type ClaimCheck struct {
	store     BlobStore
	threshold int
	newKey    func() (string, error)
}

// ClaimCheckOption configures a ClaimCheck.
type ClaimCheckOption func(*ClaimCheck)

// WithClaimKeys replaces the random key source. fn returns the run token
// and the per-output key segment, so it must return values that are unique
// and unguessable: a token another run can predict lets a forged reference
// in that run's payload reach its blobs. A value that is empty or holds
// "/" fails the enqueue or step with workflow.ErrInvalid, because "/"
// separates a token from its keys and would let one run's prefix cover
// another's. A nil fn keeps the default, 128 random bits in hex.
func WithClaimKeys(fn func() (string, error)) ClaimCheckOption {
	return func(cc *ClaimCheck) {
		if fn != nil {
			cc.newKey = fn
		}
	}
}

// NewClaimCheck returns a ClaimCheck storing in store any payload or step
// output longer than threshold bytes; one of exactly threshold bytes stays
// inline. A nil store or a threshold of zero or less is workflow.ErrInvalid.
func NewClaimCheck(store BlobStore, threshold int, opts ...ClaimCheckOption) (*ClaimCheck, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: NewClaimCheck needs a BlobStore", workflow.ErrInvalid)
	}
	if threshold <= 0 {
		return nil, fmt.Errorf("%w: NewClaimCheck needs a positive threshold", workflow.ErrInvalid)
	}
	cc := &ClaimCheck{store: store, threshold: threshold, newKey: randomClaimKey}
	for _, o := range opts {
		o(cc)
	}
	return cc, nil
}

// Enqueue returns the enqueue middleware. It stamps a fresh token in
// ClaimCheckLabel on every job, large or not, so the run's step outputs
// can be claimed too; a Payload above the threshold is stored under
// "<token>/payload" and replaced by its reference.
//
// When the driver refuses the enqueue, the blob is kept: an error does not
// prove the run was not created (a timeout after the engine stored it),
// and deleting the payload of a run that exists would fail it. A blob
// whose run never existed, or a job deduplicated by its IdempotencyKey
// onto an older run, is an orphan the store's own expiry must remove.
func (cc *ClaimCheck) Enqueue() workflow.EnqueueMiddleware {
	return func(next workflow.EnqueueFunc) workflow.EnqueueFunc {
		return func(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
			prepared, err := cc.prepare(ctx)(job)
			if err != nil {
				return workflow.RunRef{}, err
			}
			if prepared.Chain, err = mapChain(job.Chain, cc.prepare(ctx)); err != nil {
				return workflow.RunRef{}, err
			}
			return next(workflow.AllowReservedLabels(ctx, ClaimCheckLabel), prepared)
		}
	}
}

// prepare returns the function that gives one job its own claim token
// and claims its payload when it is over the threshold. Enqueue applies
// it to the job and to every job of its Job.Chain, each of which becomes
// a run of its own; a chain that ends early leaves the blobs of the jobs
// it never started, which CleanupOnFinish cannot see, so give a store
// holding chained payloads its own expiry.
func (cc *ClaimCheck) prepare(ctx context.Context) func(workflow.Job) (workflow.Job, error) {
	return func(job workflow.Job) (workflow.Job, error) {
		if err := refuseInternalLabels("ClaimCheck", job.Labels, UniqueKeyLabel, UniqueOwnerLabel); err != nil {
			return job, err
		}
		token, err := cc.key()
		if err != nil {
			return job, err
		}
		job.Labels = stampClaim(job.Labels, token)
		if len(job.Payload) > cc.threshold {
			ref, err := cc.claim(ctx, token+claimKeySep+claimPayloadKey, job.Payload)
			if err != nil {
				return job, fmt.Errorf("workflow/middleware: claim check payload: %w", err)
			}
			job.Payload = ref
		}
		return job, nil
	}
}

// Step returns the step middleware. Before the handler runs it resolves
// every reference in the input that belongs to the run (see
// claimRefPrefix): the payload, a single parent's output, or each parent's
// value in the several-parents object, which it rebuilds with the
// workflow.StepInput embedding rule so the handler sees exactly the bytes
// it would see without a claim check. Anything else, a lookalike included,
// passes through unchanged. After the handler, an output above the
// threshold is stored and replaced by its reference. Jobs the step adds to
// its chain (workflow.PrependToChain, AppendToChain) are prepared as
// Enqueue prepares a job: their own token, and a claimed payload when big.
//
// The blobs stay until the run finishes, so a retried attempt resolves
// the same references again. A missing blob is a Permanent error wrapping
// ErrBlobNotFound; any other store error, on Get or Put, fails the attempt
// and leaves the retry to the policy. A step outside a run stamped by
// Enqueue (no workflow.StepInfo, or no ClaimCheckLabel) runs untouched.
func (cc *ClaimCheck) Step() workflow.Middleware {
	return func(next workflow.Handler) workflow.Handler {
		return func(ctx context.Context, in []byte) ([]byte, error) {
			ctx = withMappedChain(ctx, cc.prepare(ctx))
			token := runClaimToken(ctx)
			if token == "" {
				return next(ctx, in)
			}
			resolved, err := cc.resolve(ctx, token, in)
			if err != nil {
				if errors.Is(err, ErrBlobNotFound) {
					return nil, workflow.Permanent(err)
				}
				return nil, err
			}
			out, err := next(ctx, resolved)
			if err != nil || len(out) <= cc.threshold {
				return out, err
			}
			seg, err := cc.key()
			if err != nil {
				return nil, err
			}
			ref, err := cc.claim(ctx, token+claimKeySep+claimOutputKeyPart+claimKeySep+seg, out)
			if err != nil {
				return nil, fmt.Errorf("workflow/middleware: claim check output: %w", err)
			}
			return ref, nil
		}
	}
}

// CleanupOnFinish deletes a run's blobs when the run reaches a terminal
// status, never before, so every retry still finds its payload. With a
// BlobPrefixDeleter store it deletes everything under the run's token;
// otherwise it deletes the payload and leaves step outputs to the store's
// expiry. c is the driver's client or any wrapper from this package (they
// forward workflow.RunNotifier), and this must be called before c.Start.
//
// A client that does not implement workflow.RunNotifier gets an
// UnsupportedError naming CapRunFinished: there, and for every event a
// best-effort driver loses, the store's own expiry is the only cleanup, so
// set one longer than the longest run. A run replayed by hand on an engine
// after it finished finds its blobs gone. Failed deletes are not reported
// (the observer has no caller); expiry catches them too.
func (cc *ClaimCheck) CleanupOnFinish(c workflow.Client) error {
	n, ok := c.(workflow.RunNotifier)
	if !ok {
		return &workflow.UnsupportedError{Field: "ClaimCheck.CleanupOnFinish", Missing: workflow.CapRunFinished, Reason: "the client does not implement workflow.RunNotifier"}
	}
	err := n.OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
		token := ev.Labels[ClaimCheckLabel]
		if validClaimKey(token) != nil {
			return
		}
		ctx = context.WithoutCancel(ctx)
		if pd, ok := cc.store.(BlobPrefixDeleter); ok {
			_ = pd.DeletePrefix(ctx, token+claimKeySep)
			return
		}
		_ = cc.store.Delete(ctx, token+claimKeySep+claimPayloadKey)
	})
	if err != nil {
		return fmt.Errorf("workflow/middleware: register claim check cleanup: %w", err)
	}
	return nil
}

// key draws a key from the configured source and checks it.
func (cc *ClaimCheck) key() (string, error) {
	k, err := cc.newKey()
	if err != nil {
		return "", fmt.Errorf("workflow/middleware: claim check key: %w", err)
	}
	if err := validClaimKey(k); err != nil {
		return "", err
	}
	return k, nil
}

// claim stores data under key and returns its reference.
func (cc *ClaimCheck) claim(ctx context.Context, key string, data []byte) ([]byte, error) {
	if err := cc.store.Put(ctx, key, data); err != nil {
		return nil, err
	}
	// Marshalling a string cannot fail.
	return json.Marshal(claimRefPrefix + key)
}

// resolve returns in with every reference of the run's token replaced by
// its blob, following the workflow.StepInput shapes: a single JSON string
// (the payload or one parent's output) or a JSON object of several parents.
// When nothing resolves it returns in itself, byte for byte.
func (cc *ClaimCheck) resolve(ctx context.Context, token string, in []byte) ([]byte, error) {
	if !bytes.Contains(in, []byte(claimRefPrefix)) {
		return in, nil
	}
	trimmed := bytes.TrimSpace(in)
	switch trimmed[0] {
	case '"':
		key, ok := parseClaimRef(trimmed, token)
		if !ok {
			return in, nil
		}
		return cc.fetch(ctx, key)
	case '{':
		return cc.resolveObject(ctx, token, in, trimmed)
	}
	return in, nil
}

// resolveObject resolves each top-level value of a several-parents object.
// Input that is not a well-formed object is not a StepInput object (a
// payload of the app's own), so it passes through.
func (cc *ClaimCheck) resolveObject(ctx context.Context, token string, in, obj []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	if _, err := dec.Token(); err != nil { // the opening '{'
		return in, nil
	}
	buf := []byte{'{'}
	resolved := false
	for i := 0; dec.More(); i++ {
		tok, err := dec.Token()
		if err != nil {
			return in, nil
		}
		name, isString := tok.(string)
		if !isString {
			return in, nil
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return in, nil
		}
		if i > 0 {
			buf = append(buf, ',')
		}
		k, _ := json.Marshal(name) // marshalling a string cannot fail
		buf = append(buf, k...)
		buf = append(buf, ':')
		key, ok := parseClaimRef(raw, token)
		if !ok {
			buf = append(buf, raw...)
			continue
		}
		blob, err := cc.fetch(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("parent %q: %w", name, err)
		}
		resolved = true
		// The same embedding rule as workflow.StepInput, so the step sees
		// exactly the object it would see with no claim check at all.
		switch {
		case len(blob) == 0:
			buf = append(buf, "null"...)
		case json.Valid(blob):
			buf = append(buf, blob...)
		default:
			s, _ := json.Marshal(string(blob)) // marshalling a string cannot fail
			buf = append(buf, s...)
		}
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		return in, nil
	}
	if !resolved {
		return in, nil
	}
	return append(buf, '}'), nil
}

func (cc *ClaimCheck) fetch(ctx context.Context, key string) ([]byte, error) {
	blob, err := cc.store.Get(ctx, key)
	if err != nil {
		// The key is ours (a token and a fixed or random segment), never
		// app data, so naming it in the error leaks nothing.
		return nil, fmt.Errorf("workflow/middleware: claim check get %q: %w", key, err)
	}
	return blob, nil
}

// parseClaimRef reports the blob key of v when v is a reference under
// token. A value with the prefix but another token, or none, is a
// lookalike and reports false, so it is passed through, never fetched.
func parseClaimRef(v []byte, token string) (string, bool) {
	var s string
	if json.Unmarshal(v, &s) != nil {
		return "", false
	}
	key, ok := strings.CutPrefix(s, claimRefPrefix)
	if !ok || !strings.HasPrefix(key, token+claimKeySep) {
		return "", false
	}
	return key, true
}

// runClaimToken is the token of the run ctx belongs to, or "" when the
// step is not in a run stamped by ClaimCheck.Enqueue.
func runClaimToken(ctx context.Context) string {
	info, ok := workflow.StepInfoFrom(ctx)
	if !ok {
		return ""
	}
	token := info.Labels[ClaimCheckLabel]
	if validClaimKey(token) != nil {
		return ""
	}
	return token
}

func validClaimKey(k string) error {
	if k == "" || strings.Contains(k, claimKeySep) {
		return fmt.Errorf("%w: claim check key must be non-empty and hold no %q", workflow.ErrInvalid, claimKeySep)
	}
	return nil
}

// stampClaim returns labels with ClaimCheckLabel set to token. It copies
// the map, so the caller's map is never mutated.
func stampClaim(labels map[string]string, token string) map[string]string {
	out := make(map[string]string, len(labels)+1)
	maps.Copy(out, labels)
	out[ClaimCheckLabel] = token
	return out
}

func randomClaimKey() (string, error) {
	b := make([]byte, claimTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// MemoryBlobStore is a BlobStore inside one process, for tests and a
// single server where every step runs in the enqueuing process. It has no
// expiry. Use NewMemoryBlobStore.
type MemoryBlobStore struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

// NewMemoryBlobStore returns an empty store.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{blobs: map[string][]byte{}}
}

// Put implements BlobStore. It keeps a copy, so the caller may reuse data.
func (m *MemoryBlobStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[key] = bytes.Clone(data)
	return nil
}

// Get implements BlobStore. It returns a copy.
func (m *MemoryBlobStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[key]
	if !ok {
		return nil, ErrBlobNotFound
	}
	return bytes.Clone(b), nil
}

// Delete implements BlobStore.
func (m *MemoryBlobStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, key)
	return nil
}

// DeletePrefix implements BlobPrefixDeleter.
func (m *MemoryBlobStore) DeletePrefix(_ context.Context, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.blobs {
		if strings.HasPrefix(k, prefix) {
			delete(m.blobs, k)
		}
	}
	return nil
}

// Len reports how many blobs the store holds, so a test can prove that a
// payload was moved out and that cleanup removed it.
func (m *MemoryBlobStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.blobs)
}

var (
	_ BlobStore         = (*MemoryBlobStore)(nil)
	_ BlobPrefixDeleter = (*MemoryBlobStore)(nil)
)
