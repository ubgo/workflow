# secret-guard

`workflow.SecretGuard` refuses a job that carries a credential in its payload or labels, naming where and which rule matched but never the value. A `workflow.SecretRef` names the secret instead, and the step resolves it when it runs. Runs on the in-memory driver.

```sh
cd examples
go run ./secret-guard
```

```text
refused:
  workflow: invalid spec: job carries a secret; pass a SecretRef instead: payload.api_key (key), payload.note (pem-private-key)
  ErrSecretInPayload: true ErrInvalid: true
  workflow: invalid spec: job carries a secret; pass a SecretRef instead: label auth_token (key)
accepted:
  payload {"shop":"acme","token":{"secret_ref":"shops/acme/token"}}
  result "synced acme with a 16-byte token"
rules:
  payload.password (key)
  empty SecretRef: ErrNoSecretRef=true permanent=true
```

## The code

The guard on every enqueue:

<!-- snippet: examples/secret-guard/main.go#guard -->
```go
c := middleware.WrapClient(mc, workflow.SecretGuard(nil)) // nil: the stock detector
```

A payload with a key and a private key in it is refused:

<!-- snippet: examples/secret-guard/main.go#refused -->
```go
_, err = c.Enqueue(ctx, workflow.Job{
	Workflow: "sync-shop",
	Payload:  []byte(`{"shop":"acme","api_key":"shpat_live_value","note":"-----BEGIN RSA PRIVATE KEY-----"}`),
})
fmt.Fprintln(out, " ", err)
fmt.Fprintln(out, "  ErrSecretInPayload:", errors.Is(err, workflow.ErrSecretInPayload), "ErrInvalid:", errors.Is(err, workflow.ErrInvalid))
```

The payload names the secret:

<!-- snippet: examples/secret-guard/main.go#payload -->
```go
type syncArgs struct {
	Shop  string             `json:"shop"`
	Token workflow.SecretRef `json:"token"` // {"secret_ref": "shops/acme/token"}, never the token
}
```

<!-- snippet: examples/secret-guard/main.go#ref -->
```go
payload, err := json.Marshal(syncArgs{Shop: "acme", Token: workflow.SecretRef{Ref: "shops/acme/token"}})
if err != nil {
	return err
}
ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "sync-shop", Payload: payload})
if err != nil {
	return err
}
```

The step resolves it from the app's store:

<!-- snippet: examples/secret-guard/main.go#step -->
```go
err := mc.Register(workflow.Workflow{Name: "sync-shop", Steps: []workflow.Step{{Name: "sync",
	Run: workflow.Define(func(ctx context.Context, a syncArgs) (string, error) {
		token, err := workflow.ResolveSecret(ctx, secrets, a.Token) // read when the step runs
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("synced %s with a %d-byte token", a.Shop, len(token)), nil
	}),
}}})
```

The stock detector with an allowed key and a rule turned off:

<!-- snippet: examples/secret-guard/main.go#rules -->
```go
d := workflow.NewSecretDetector(workflow.SecretRules{
	AllowKeys:  []string{"page_token"}, // a pagination cursor, not a credential
	AllowRules: []string{"jwt"},        // this app passes signed links
})
for _, f := range d.Detect(workflow.Job{Payload: []byte(`{"page_token":"p2","password":"hunter2"}`)}) {
	fmt.Fprintf(out, "  %s (%s)\n", f.Where, f.Rule)
}
```

The guide that copies this program is [secrets in payloads](../../docs/reliability/secrets.md). `go test ./secret-guard` checks every line.
