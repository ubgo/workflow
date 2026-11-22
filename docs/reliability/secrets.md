# Secrets in payloads

A payload is stored with its run, shown on admin screens, written to logs and kept by retention, so a password or API token put in one outlives its rotation and leaks wherever runs are read. [`workflow.SecretGuard`](ds:block?id=secretguard-r5z473u8) is enqueue middleware that refuses a job carrying something that looks like a credential, naming where and which rule matched but never the value. A [`workflow.SecretRef`](ds:block?id=secretref-b6xpzmb7) names the secret instead, and the step resolves it from your secret store when it runs.

## Before you start

- Nothing driver-specific: the guard is enqueue middleware and runs on any client ([`SecretGuard`](ds:block?id=secretguard-r5z473u8)), and [`ResolveSecret`](ds:block?id=resolvesecret-bxbe3t42) runs inside any step.
- Enqueue middleware and `middleware.WrapClient` are explained on [enqueue middleware](../middleware/enqueue-middleware.md).
- The example is [`examples/secret-guard`](../../examples/secret-guard). Run it with `go run ./secret-guard` from `examples/`.

## Refuse credentials at enqueue

Wrap the client the app enqueues with. A nil detector means the stock one, `workflow.NewSecretDetector(workflow.SecretRules{})` ([`SecretGuard`](ds:block?id=secretguard-r5z473u8)):

<!-- snippet: examples/secret-guard/main.go#guard -->
```go
c := middleware.WrapClient(mc, workflow.SecretGuard(nil)) // nil: the stock detector
```

A job with a credential is refused with an error matching [`workflow.ErrSecretInPayload`](ds:block?id=errsecretinpayload-nzpdde34), which wraps `workflow.ErrInvalid`, so `errors.Is` matches both. The error lists each finding as where and which rule, never the value ([`TestSecretGuard`](ds:block?id=testsecretguard-2tfh9yq4)):

<!-- snippet: examples/secret-guard/main.go#refused -->
```go
_, err = c.Enqueue(ctx, workflow.Job{
	Workflow: "sync-shop",
	Payload:  []byte(`{"shop":"acme","api_key":"shpat_live_value","note":"-----BEGIN RSA PRIVATE KEY-----"}`),
})
fmt.Println(" ", err)
fmt.Println("  ErrSecretInPayload:", errors.Is(err, workflow.ErrSecretInPayload), "ErrInvalid:", errors.Is(err, workflow.ErrInvalid))
```

The [stock detector](ds:block?id=newsecretdetector-fxr7ftrt) scans the payload, the labels and every chained job (`Job.Chain`; [`detect`](ds:block?id=stockdetector-detect-bsxzsdv2)), and reports:

- **A key that names a credential** holding a non-empty string, in a JSON object or a label: `token`, `pass`, `pwd`, `auth`, `apikey` or `secret` exactly ([`secretKeyExact`](ds:block?id=secretkeyexact-e3nqvje3)), or any key containing `password`, `passwd`, `secret`, `apikey`, `privatekey`, `accesstoken`, `refreshtoken`, `authtoken`, `bearertoken`, `clientsecret`, `authorization`, `credential` or `sessiontoken` ([`secretKeyParts`](ds:block?id=secretkeyparts-6mvqvtx5)). Keys are compared in lower case with `_`, `-` and `.` removed ([`foldKey`](ds:block?id=foldkey-ff7k2uxa)), so `API_Key`, `api-key` and `apiKey` are one name. The rule is called `key`.
- **A value in a known credential format**, under any key: `pem-private-key`, `aws-access-key`, `stripe-live-key`, `github-token`, `slack-token` and `jwt` ([`secretValueRules`](ds:block?id=secretvaluerules-am67pun5)).

A payload that is not JSON is scanned whole by the value rules only ([`detect`](ds:block?id=stockdetector-detect-bsxzsdv2)). A value that is an object, such as a `SecretRef`, is never reported ([`walk`](ds:block?id=stockdetector-walk-6jccu3xm)). A finding's [`Where`](ds:block?id=secretfinding-pkmsg7tw) is `payload` plus a JSON path (`payload.user.password`), `label <key>`, or `chain[2].payload...`.

## Pass a SecretRef instead

[`workflow.SecretRef`](ds:block?id=secretref-b6xpzmb7) names a secret: its JSON form is `{"secret_ref": "<name>"}`. Put it where the credential would go:

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

The step turns the name into the value when it runs, through a [`workflow.SecretResolver`](ds:block?id=secretresolver-rygjyhdf) of yours (a vault, a cloud secret manager, environment variables):

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

[`workflow.ResolveSecret(ctx, resolver, ref)`](ds:block?id=resolvesecret-bxbe3t42) returns a permanent `workflow.ErrNoSecretRef` for a reference with no name ([`TestResolveSecret`](ds:block?id=testresolvesecret-cz6ravyv)), and a permanent `ErrInvalid` for a nil resolver. The resolver's own errors are returned as they are, so a vault outage retries the step. Because the value is read at run time, a rotated secret takes effect for every run not yet started, including retries.

## Tune the rules

[`workflow.SecretRules`](ds:block?id=secretrules-zqms9kak) turns off what is a false positive for your app: `AllowKeys` are keys never reported by the key rule, folded the same way, and `AllowRules` turns off value rules by name:

<!-- snippet: examples/secret-guard/main.go#rules -->
```go
d := workflow.NewSecretDetector(workflow.SecretRules{
	AllowKeys:  []string{"page_token"}, // a pagination cursor, not a credential
	AllowRules: []string{"jwt"},        // this app passes signed links
})
for _, f := range d.Detect(workflow.Job{Payload: []byte(`{"page_token":"p2","password":"hunter2"}`)}) {
	fmt.Printf("  %s (%s)\n", f.Where, f.Rule)
}
```

For rules of your own (a vault's token format, an entropy check), implement [`workflow.SecretDetector`](ds:block?id=secretdetector-fn6kdnj9), whose one method `Detect(job) []workflow.SecretFinding` returns where and which rule, and pass it to `SecretGuard`.

## Try it

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

## Pitfalls

- **It is a guard, not a proof.** A detector finds credentials by name and by format ([`secretKeyParts`](ds:block?id=secretkeyparts-6mvqvtx5), [`secretValueRules`](ds:block?id=secretvaluerules-am67pun5)); a token under an innocent key in an unknown format passes. Treat a refusal as a bug found, not a passing job as a clean one.
- **Only what passes through the wrapped client is checked.** Jobs enqueued on the inner client, through [`EnqueueTx`](ds:block?id=sql-client-enqueuetx-8tdebvcq) (which runs no enqueue middleware), or by another service skip the middleware. `outbox.Write` stores a job unchecked: the guard sees it only if the relay enqueues on a wrapped client, after the commit, and a refused entry is then retried until it goes dead ([`Relay.fail`](ds:block?id=outbox-relay-fail-5pyc97na)).
- **Encryption is the other half.** Data that must travel in the payload but must not be readable in the tables belongs in an [encrypted payload](../middleware/codecs.md#encrypt-payloads), not past the guard with an allow rule.
- **Never log a resolved secret** in the step: step output, errors and captured task output are stored with the run too.

## Related

- [Enqueue middleware](../middleware/enqueue-middleware.md), [Payload codecs](../middleware/codecs.md)
- [Claim check](claim-check.md) for payloads that are big rather than secret
- Example: [examples/secret-guard](../../examples/secret-guard/)
