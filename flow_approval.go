package workflow

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ApprovalEvent is the event name approvals travel under (DE-05).
const ApprovalEvent = "ubgo.workflow.approval"

// ErrApprovalToken matches a signed approval token that is malformed,
// forged or expired.
var ErrApprovalToken = errors.New("workflow: invalid approval token")

// Decision is a person's answer to an approval request; it is the
// approval step's output, read by the next step with DecisionOf.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by,omitempty"`
	Note     string `json:"note,omitempty"`
}

// ApprovalSpec describes an approval step: a run suspends there, holding
// no worker, until Approve or Reject is called with its key.
type ApprovalSpec struct {
	// Name is the step name.
	Name string
	// After lists the steps the approval waits for.
	After []string
	// Key or KeyFrom identify the request: the order ID, or a token the
	// app generated with NewApprovalToken and stored in the job (read it
	// back with KeyFrom). Exactly one is needed.
	Key     string
	KeyFrom ConcurrencyKey
	// Timeout bounds the wait; zero waits forever. A timeout fails the
	// run with ErrWaitTimeout (pair it with WithRules for an escalation
	// branch).
	Timeout time.Duration
}

// ApprovalStep returns the wait step of a human approval: Step.Wait on
// ApprovalEvent keyed by the request. It needs CapWaitForEvent (memory,
// sqlstore, Redis, Hatchet). Approve and Reject resume it; an answer sent
// before the run reaches the step is kept, as every Signal is.
func ApprovalStep(a ApprovalSpec) (Step, error) {
	if a.Name == "" {
		return Step{}, invalidf("approval step has no Name")
	}
	if (a.Key == "") == a.KeyFrom.IsZero() {
		return Step{}, invalidf("approval step %q needs exactly one of Key and KeyFrom", a.Name)
	}
	return Step{Name: a.Name, After: a.After, Wait: &WaitSpec{Event: ApprovalEvent, Key: a.Key, KeyFrom: a.KeyFrom, Timeout: a.Timeout}}, nil
}

// Approve resumes the runs waiting for approval key with an approval.
func Approve(ctx context.Context, c Client, key, by, note string) error {
	return answerApproval(ctx, c, key, Decision{Approved: true, By: by, Note: note})
}

// Reject resumes the runs waiting for approval key with a rejection.
func Reject(ctx context.Context, c Client, key, by, note string) error {
	return answerApproval(ctx, c, key, Decision{Approved: false, By: by, Note: note})
}

func answerApproval(ctx context.Context, c Client, key string, d Decision) error {
	if key == "" {
		return invalidf("approval key is empty")
	}
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return c.Signal(ctx, Event{Name: ApprovalEvent, Key: key, Payload: b})
}

// DecisionOf decodes the approval step's output, the input of the step
// after it.
func DecisionOf(in []byte) (Decision, error) {
	var d Decision
	if err := json.Unmarshal(in, &d); err != nil {
		return Decision{}, Permanent(fmt.Errorf("workflow: approval decision does not decode: %w", err))
	}
	return d, nil
}

// NewApprovalToken returns a fresh random key (128 bits, hex) for an
// approval request that has no natural ID.
func NewApprovalToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ApprovalSigner signs approval keys for links sent to people (an email's
// approve button), so the handler that receives the click can tell a real
// link from a forged one and an expired one, without storing anything.
// The token is "<key>.<unix expiry>.<HMAC-SHA256>" in URL-safe base64.
// Keys rotate: Sign uses Keys[0], Verify accepts any of Keys.
type ApprovalSigner struct {
	Keys [][]byte
}

// Sign returns a token for key that expires at expires.
func (s ApprovalSigner) Sign(key string, expires time.Time) (string, error) {
	if len(s.Keys) == 0 || len(s.Keys[0]) < 16 {
		return "", invalidf("approval signer needs a key of at least 16 bytes")
	}
	if key == "" || strings.Contains(key, ".") {
		return "", invalidf("approval key must be non-empty and contain no dot")
	}
	body := key + "." + strconv.FormatInt(expires.Unix(), 10)
	return base64.RawURLEncoding.EncodeToString([]byte(body)) + "." + approvalMAC(s.Keys[0], body), nil
}

// Verify checks token at now and returns the approval key it carries. A
// bad or expired token matches ErrApprovalToken.
func (s ApprovalSigner) Verify(token string, now time.Time) (string, error) {
	enc, mac, ok := strings.Cut(token, ".")
	if !ok {
		return "", ErrApprovalToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return "", ErrApprovalToken
	}
	body := string(raw)
	valid := false
	for _, k := range s.Keys {
		if hmac.Equal([]byte(approvalMAC(k, body)), []byte(mac)) {
			valid = true
		}
	}
	if !valid {
		return "", ErrApprovalToken
	}
	key, exp, ok := strings.Cut(body, ".")
	unix, err := strconv.ParseInt(exp, 10, 64)
	if !ok || err != nil || key == "" {
		return "", ErrApprovalToken
	}
	if !now.Before(time.Unix(unix, 0)) {
		return "", fmt.Errorf("%w: expired", ErrApprovalToken)
	}
	return key, nil
}

func approvalMAC(key []byte, body string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
