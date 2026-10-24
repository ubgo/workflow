# encrypted-payloads

AES-GCM payload encryption on the in-memory driver: `middleware.Encrypt` builds the codec, `middleware.WithCodec` seals every payload before the driver sees it, and `middleware.DecodeStep` opens each step's input and seals its output, so a two-step workflow's intermediate output is sealed too. It then rotates the key (`Encrypt(newKey, oldKey)` opens both), and shows the two refusals: a plaintext payload, and a worker that dropped the old key too early. Both fail with a permanent error wrapping `middleware.ErrDecode`.

```sh
cd examples
go run ./encrypted-payloads
```

```text
driver stores a sealed payload: true, card visible: false
capture got auth-o-1
sealed with the new key: succeeded, decode error: false
capture got auth-o-2
sealed with the old key: succeeded, decode error: false
plaintext: failed, decode error: true
old key dropped: failed, decode error: true, permanent: true
```

The keys are fixed bytes so the program is self-contained; a real app loads them from its secret store. The guide that copies this program is [payload codecs](../../docs/middleware/codecs.md). `go test ./encrypted-payloads` runs it and checks every line.
