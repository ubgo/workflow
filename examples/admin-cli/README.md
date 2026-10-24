# admin-cli

The `workflow` command line client against a real admin JSON API: an `httptest` server runs `admin.NewHandler` over a memory client behind a bearer token, and `cli.Run` sends it commands, a bulk retry previewed and done, the health report, the online nodes as JSON, a drain, and the exit codes of a refused token and of an unknown command. Nothing needs to be installed.

```sh
cd examples
go run ./admin-cli
```

```text
$ workflow retry --all --workflow send-email --dry-run
dry run: would retry 3 runs; nothing changed
  run-8
  run-5
  run-2
exit 0
$ workflow retry --all --workflow send-email
matched 3, retried 3, skipped 0 (changed meanwhile)
exit 0
$ workflow health
CHECK    OK   DETAIL
live     yes  
ready    yes  
workers  yes  0 of 4 busy
exit 0
$ workflow nodes --json
{
  "nodes": [
    {
      "id": "worker-1",
      "mode": "full",
      "queues": [],
      "workers": 4,
      "running": 0,
      "started_at": "2026-10-14T09:00:00Z",
      "last_seen": "2026-10-14T09:00:00Z"
    }
  ]
}
exit 0
$ workflow drain --wait 1m
drained after 0s
exit 0
$ workflow failures
workflow: admin API: HTTP 401 unauthorized: authentication required
exit 1
$ workflow retyr
workflow: unknown command "retyr"
run 'workflow help' for usage
exit 2
```

## The code

The CLI as a function, the way an app embeds it in its own binary:

<!-- snippet: examples/admin-cli/main.go#embed -->
```go
env := map[string]string{
	cli.EnvURL:   srv.URL + "/admin", // where the app mounted admin.NewHandler
	cli.EnvToken: opsToken,           // sent as a bearer token, never printed
}
workflowCmd := func(args ...string) int {
	return cli.Run(ctx, args, cli.Options{
		Stdout: out,
		Stderr: out,
		Getenv: func(k string) string { return env[k] },
		Name:   "workflow",
	})
}
```

`main_test.go` runs the program and compares its whole output with the block above.

## Guides

- [The workflow command](../../docs/admin/cli.md)
