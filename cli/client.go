package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// maxResponse bounds how much of one response the CLI reads, so a
// misconfigured URL pointing at a large file cannot exhaust memory.
const maxResponse = 32 << 20

// APIError is an error answer from the admin API: its HTTP status and the
// code and message from its {"error":{...}} body.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("admin API: HTTP %d: %s", e.Status, e.Message)
	}
	return fmt.Sprintf("admin API: HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// api is the admin JSON API at one base URL, with the headers that
// authenticate the caller.
type api struct {
	base    string // scheme://host/path, no trailing slash
	headers http.Header
	hc      *http.Client
}

// newAPI parses base, which is where the app mounted admin.NewHandler
// (for example https://app.example.com/admin).
func newAPI(base string, headers http.Header, hc *http.Client) (*api, error) {
	if base == "" {
		return nil, errors.New("no admin URL: pass --url or set " + EnvURL)
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("admin URL %q: want http(s)://host/path", base)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("admin URL %q: give the mount point without a query or fragment", base)
	}
	return &api{base: strings.TrimSuffix(base, "/"), headers: headers, hc: hc}, nil
}

// call sends one request. path is relative to the base, already
// escaped, and may carry a query string; body, when not nil, is sent as
// JSON. The raw response body is returned on a 2xx answer; any other
// answer is an *APIError.
func (a *api) call(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+"/"+strings.TrimPrefix(path, "/"), rd)
	if err != nil {
		return nil, err
	}
	for k, vs := range a.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")
	if method != http.MethodGet {
		// The admin API refuses a write without this type (its CSRF
		// guard), so every write sends it, with an empty object when the
		// route takes no body.
		req.Header.Set("Content-Type", "application/json")
		if rd == nil {
			req.Body = io.NopCloser(strings.NewReader("{}"))
			req.ContentLength = 2
		}
	}
	res, err := a.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, maxResponse))
	if err != nil {
		return nil, err
	}
	if res.StatusCode/100 != 2 {
		e := &APIError{Status: res.StatusCode, Message: http.StatusText(res.StatusCode)}
		var eb struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		if json.Unmarshal(b, &eb) == nil && eb.Error.Code != "" {
			e.Code, e.Message = eb.Error.Code, eb.Error.Message
		}
		return nil, e
	}
	return b, nil
}

// get calls GET path and decodes the answer into v, returning the raw
// body too for --json.
func (a *api) get(ctx context.Context, path string, v any) ([]byte, error) {
	return a.do(ctx, http.MethodGet, path, nil, v)
}

// do calls method path with body and decodes the answer into v.
func (a *api) do(ctx context.Context, method, path string, body, v any) ([]byte, error) {
	b, err := a.call(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	if v != nil {
		if err := json.Unmarshal(b, v); err != nil {
			return nil, fmt.Errorf("admin API answered something that is not its JSON (is --url the admin mount?): %w", err)
		}
	}
	return b, nil
}
