package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// ErrSecretInPayload means SecretGuard found what looks like a credential
// in a job's payload or labels. It wraps ErrInvalid, so errors.Is matches
// both. The error names where and which rule, never the value.
var ErrSecretInPayload = fmt.Errorf("%w: job carries a secret; pass a SecretRef instead", ErrInvalid)

// SecretFinding is one place a detector believes holds a credential.
type SecretFinding struct {
	// Where locates it: "payload" plus a JSON path ("payload.user.password"),
	// "label <key>", or "chain[2].payload...".
	Where string

	// Rule names the rule that matched ("key", "pem-private-key",
	// "aws-access-key", ...), so a false positive can be allowed by name.
	Rule string
}

// SecretDetector decides whether a job carries credentials (MS-05). The
// stock one is NewSecretDetector; an app with its own rules (a vault's
// token format, an entropy check) supplies its own.
type SecretDetector interface {
	Detect(job Job) []SecretFinding
}

// SecretRules tunes the stock detector.
type SecretRules struct {
	// AllowKeys are JSON object keys and label keys never reported by the
	// key rule, compared after the same folding the rule applies
	// (lower case, "_", "-" and "." removed), such as "pagetoken".
	AllowKeys []string

	// AllowRules are value rules turned off by name, such as "jwt" for
	// an app that passes signed links.
	AllowRules []string
}

// secretKeyExact are key names that are a credential on their own;
// secretKeyParts are fragments that make any key containing them one.
// Keys are folded first (foldKey).
var (
	secretKeyExact = []string{"token", "pass", "pwd", "auth", "apikey", "secret"}
	secretKeyParts = []string{"password", "passwd", "secret", "apikey", "privatekey", "accesstoken",
		"refreshtoken", "authtoken", "bearertoken", "clientsecret", "authorization", "credential", "sessiontoken"}
)

// secretValueRules match credential formats inside any string value.
var secretValueRules = []struct {
	name string
	re   *regexp.Regexp
}{
	{"pem-private-key", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"aws-access-key", regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`)},
	{"stripe-live-key", regexp.MustCompile(`\b(sk|rk)_live_[0-9A-Za-z]{10,}`)},
	{"github-token", regexp.MustCompile(`\b(gh[pousr]_[0-9A-Za-z]{30,}|github_pat_[0-9A-Za-z_]{30,})`)},
	{"slack-token", regexp.MustCompile(`\bxox[abprs]-[0-9A-Za-z-]{10,}`)},
	{"jwt", regexp.MustCompile(`\beyJ[0-9A-Za-z_-]{8,}\.eyJ[0-9A-Za-z_-]{8,}\.[0-9A-Za-z_-]{8,}`)},
}

// stockDetector is NewSecretDetector's detector.
type stockDetector struct {
	allowKeys  map[string]bool
	allowRules map[string]bool
}

// NewSecretDetector returns the stock detector. It reports a JSON object
// key or label key that names a credential ("password", "api_key",
// "client_secret", "token", ...) holding a non-empty string, and any
// string value matching a known credential format: a PEM private key, an
// AWS access key ID, a Stripe live key, a GitHub or Slack token, a JWT.
// A value that is an object (a SecretRef) is never reported. A payload
// that is not JSON is scanned whole by the value rules only.
func NewSecretDetector(r SecretRules) SecretDetector {
	d := stockDetector{allowKeys: map[string]bool{}, allowRules: map[string]bool{}}
	for _, k := range r.AllowKeys {
		d.allowKeys[foldKey(k)] = true
	}
	for _, n := range r.AllowRules {
		d.allowRules[n] = true
	}
	return d
}

// foldKey lowers k and drops "_", "-" and ".", so "API_Key", "api-key"
// and "apiKey" fold to one name.
func foldKey(k string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '_', '-', '.':
			return -1
		}
		return r
	}, strings.ToLower(k))
}

// Detect scans job, its labels and every chained job.
func (d stockDetector) Detect(job Job) []SecretFinding {
	return d.detect("", job)
}

func (d stockDetector) detect(prefix string, job Job) []SecretFinding {
	var out []SecretFinding
	var v any
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &v); err == nil {
			out = d.walk(out, prefix+"payload", v)
		} else {
			out = d.value(out, prefix+"payload", string(job.Payload))
		}
	}
	for _, k := range slices.Sorted(maps.Keys(job.Labels)) {
		out = d.keyed(out, prefix+"label "+k, k, job.Labels[k])
	}
	for i, c := range job.Chain {
		out = append(out, d.detect(prefix+"chain["+strconv.Itoa(i)+"].", c)...)
	}
	return out
}

func (d stockDetector) walk(out []SecretFinding, where string, v any) []SecretFinding {
	switch x := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(x)) {
			p := where + "." + k
			if s, ok := x[k].(string); ok {
				if k == secretRefKey {
					continue // a SecretRef's name, not a secret
				}
				out = d.keyed(out, p, k, s)
				continue
			}
			out = d.walk(out, p, x[k])
		}
	case []any:
		for i, e := range x {
			out = d.walk(out, where+"["+strconv.Itoa(i)+"]", e)
		}
	case string:
		out = d.value(out, where, x)
	}
	return out
}

// keyed applies the key rule, then the value rules, to one string held
// under key.
func (d stockDetector) keyed(out []SecretFinding, where, key, s string) []SecretFinding {
	if s != "" && d.secretKey(key) {
		return append(out, SecretFinding{Where: where, Rule: "key"})
	}
	return d.value(out, where, s)
}

func (d stockDetector) secretKey(key string) bool {
	k := foldKey(key)
	if d.allowKeys[k] {
		return false
	}
	if slices.Contains(secretKeyExact, k) {
		return true
	}
	for _, p := range secretKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

func (d stockDetector) value(out []SecretFinding, where, s string) []SecretFinding {
	for _, r := range secretValueRules {
		if !d.allowRules[r.name] && r.re.MatchString(s) {
			return append(out, SecretFinding{Where: where, Rule: r.name})
		}
	}
	return out
}

// SecretGuard returns enqueue middleware that refuses a job carrying
// credentials, with an error matching ErrSecretInPayload (and ErrInvalid)
// that lists where and which rule, never the value. A payload is stored
// with the run, shown in admin screens and logs, and kept by retention;
// a secret in it outlives its rotation. Pass a SecretRef naming the secret
// and resolve it inside the step instead. A nil detector means
// NewSecretDetector(SecretRules{}).
func SecretGuard(d SecretDetector) EnqueueMiddleware {
	if d == nil {
		d = NewSecretDetector(SecretRules{})
	}
	return func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, job Job) (RunRef, error) {
			if found := d.Detect(job); len(found) > 0 {
				parts := make([]string, len(found))
				for i, f := range found {
					parts[i] = f.Where + " (" + f.Rule + ")"
				}
				return RunRef{}, fmt.Errorf("%w: %s", ErrSecretInPayload, strings.Join(parts, ", "))
			}
			return next(ctx, job)
		}
	}
}

// SecretRef names a secret instead of carrying it: put it in a payload
// where the credential would go, and resolve it in the step with
// ResolveSecret. Its JSON form is {"secret_ref": "<name>"}, an object, so
// SecretGuard never reports it.
type SecretRef struct {
	Ref string `json:"secret_ref"`
}

// secretRefKey is SecretRef's JSON key, which the stock detector skips.
const secretRefKey = "secret_ref"

// SecretResolver turns a secret's name into its value at the moment a
// step needs it: a vault, a cloud secret manager, environment variables.
type SecretResolver interface {
	ResolveSecret(ctx context.Context, name string) ([]byte, error)
}

// ErrNoSecretRef means ResolveSecret got a SecretRef with no name.
var ErrNoSecretRef = errors.New("workflow: secret reference has no name")

// ResolveSecret returns the value ref names, from r. An empty ref is a
// Permanent ErrNoSecretRef; r's own errors are returned as they are, so a
// vault outage retries the step.
func ResolveSecret(ctx context.Context, r SecretResolver, ref SecretRef) ([]byte, error) {
	if ref.Ref == "" {
		return nil, Permanent(ErrNoSecretRef)
	}
	if r == nil {
		return nil, Permanent(invalidf("secret %q: resolver is nil", ref.Ref))
	}
	return r.ResolveSecret(ctx, ref.Ref)
}
