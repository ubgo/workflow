# worker-wakeup

Start a job on an idle worker as soon as another node enqueues it, instead of at the worker's next poll: a web client and a worker share one database and a `sqlstore.Notifier`. The worker polls only every five seconds, and the job still starts at once. Without `PG_DSN` it runs on a SQLite file with a small in-process notifier, which shows the seam any bus fits; with `PG_DSN` it uses `pgnotify`, Postgres LISTEN/NOTIFY.

```sh
cd examples
go run ./worker-wakeup
```

```text
started before the next poll (5s): true
```

On the host Postgres of the shared local stack, in a database you can create tables in (the program uses a fresh table prefix and drops its tables at the end):

```sh
PG_DSN="postgres://$USER@localhost:5432/ubgo_workflow_test?sslmode=disable" go run ./worker-wakeup
```

## The code

A notifier is two methods:

<!-- snippet: examples/worker-wakeup/main.go#notifier -->
```go
func (b *localBus) Notify(_ context.Context, channel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, wake := range b.subs[channel] {
		wake()
	}
	return nil
}

func (b *localBus) Listen(ctx context.Context, channel string, wake func()) error {
	b.mu.Lock()
	b.subs[channel] = append(b.subs[channel], wake)
	b.mu.Unlock()
	wake() // cover anything sent before the subscription
	<-ctx.Done()
	return nil
}
```

The worker:

<!-- snippet: examples/worker-wakeup/main.go#worker -->
```go
worker, err := sqlstore.New(db, dialect,
	sqlstore.WithTablePrefix(prefix),
	sqlstore.WithPollInterval(pollInterval), // the fallback; a lost wake-up only delays work
	sqlstore.WithNotifier(notifier),         // pgnotify.New(db) on Postgres
)
```

`main_test.go` runs the program and checks its output.

## Guides

- [The SQL driver › Wake workers at once](../../docs/drivers/sqlstore.md#wake-workers-at-once)
