// Command secret-guard shows SecretGuard, enqueue middleware that refuses
// a job carrying a credential in its payload or labels, and SecretRef,
// which names a secret in the payload so the step resolves it when it
// runs. It runs on the in-memory driver, so nothing needs to be installed.
//
// It is the program behind docs/reliability/secrets.md: the guide copies
// its regions (marked "snippet:") and task docs:snippets fails when they
// drift.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/middleware"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "secret-guard:", err)
		os.Exit(1)
	}
}

// vault is the app's secret store; here a map. Any SecretResolver works:
// a vault client, a cloud secret manager, environment variables.
type vault map[string]string

// ResolveSecret implements workflow.SecretResolver.
func (v vault) ResolveSecret(_ context.Context, name string) ([]byte, error) {
	s, ok := v[name]
	if !ok {
		return nil, workflow.Permanent(fmt.Errorf("secret %q not found", name))
	}
	return []byte(s), nil
}

// syncArgs is the payload of the sync job: the credential is a reference.
// snippet:payload
type syncArgs struct {
	Shop  string             `json:"shop"`
	Token workflow.SecretRef `json:"token"` // {"secret_ref": "shops/acme/token"}, never the token
}

// snippet:end

// run is main without the process exit, so the test drives the same code.
func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	secrets := vault{"shops/acme/token": "shpat_live_value"}
	mc := memory.New()
	// snippet:step
	err := mc.Register(workflow.Workflow{Name: "sync-shop", Steps: []workflow.Step{{Name: "sync",
		Run: workflow.Define(func(ctx context.Context, a syncArgs) (string, error) {
			token, err := workflow.ResolveSecret(ctx, secrets, a.Token) // read when the step runs
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("synced %s with a %d-byte token", a.Shop, len(token)), nil
		}),
	}}})
	// snippet:end
	if err != nil {
		return err
	}
	if err := mc.Start(ctx); err != nil {
		return err
	}

	// snippet:guard
	c := middleware.WrapClient(mc, workflow.SecretGuard(nil)) // nil: the stock detector
	// snippet:end

	fmt.Fprintln(out, "refused:")
	// snippet:refused
	_, err = c.Enqueue(ctx, workflow.Job{
		Workflow: "sync-shop",
		Payload:  []byte(`{"shop":"acme","api_key":"shpat_live_value","note":"-----BEGIN RSA PRIVATE KEY-----"}`),
	})
	fmt.Fprintln(out, " ", err)
	fmt.Fprintln(out, "  ErrSecretInPayload:", errors.Is(err, workflow.ErrSecretInPayload), "ErrInvalid:", errors.Is(err, workflow.ErrInvalid))
	// snippet:end
	_, err = c.Enqueue(ctx, workflow.Job{Workflow: "sync-shop", Labels: map[string]string{"auth_token": "abc"}})
	fmt.Fprintln(out, " ", err)

	fmt.Fprintln(out, "accepted:")
	// snippet:ref
	payload, err := json.Marshal(syncArgs{Shop: "acme", Token: workflow.SecretRef{Ref: "shops/acme/token"}})
	if err != nil {
		return err
	}
	ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-shop", Payload: payload})
	if err != nil {
		return err
	}
	// snippet:end
	fmt.Fprintf(out, "  payload %s\n", payload)
	res, err := workflow.AwaitResult(ctx, c, ref, workflow.AwaitOptions{Poll: 2 * time.Millisecond})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "  result %s\n", res)

	fmt.Fprintln(out, "rules:")
	// snippet:rules
	d := workflow.NewSecretDetector(workflow.SecretRules{
		AllowKeys:  []string{"page_token"}, // a pagination cursor, not a credential
		AllowRules: []string{"jwt"},        // this app passes signed links
	})
	for _, f := range d.Detect(workflow.Job{Payload: []byte(`{"page_token":"p2","password":"hunter2"}`)}) {
		fmt.Fprintf(out, "  %s (%s)\n", f.Where, f.Rule)
	}
	// snippet:end
	_, err = workflow.ResolveSecret(ctx, secrets, workflow.SecretRef{})
	fmt.Fprintf(out, "  empty SecretRef: ErrNoSecretRef=%v permanent=%v\n", errors.Is(err, workflow.ErrNoSecretRef), workflow.IsPermanent(err))
	return nil
}
