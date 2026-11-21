# admin-screenshots

Captures the admin web UI screenshots of the run reset, fork and waiting views in `docs/assets/admin-ui` (`run-reset-light.png`, `run-reset-dark.png`, `run-fork-light.png`, `run-forked-light.png`, `run-waiting-light.png`), shown in [Admin web UI](../../docs/admin/ui.md). It serves the UI over an in-memory client with dummy data (`demo.go`: made-up tenants, invoice and order numbers, node `worker-1`), drives a headless Chrome through chromedp at 1360 x 900, and writes PNGs recompressed at the best level (about 60 KB each).

It is a module of its own, never released (`.volt.yml`), so chromedp never reaches a library module or the examples. It needs Chrome or Chromium on the machine; `-chrome` names another binary.

From the repository root:

```sh
task docs:screenshots
```

or directly:

```sh
cd scripts/admin-screenshots
go run . -out ../../docs/assets/admin-ui
```

`go test ./...` here checks the demo data and that the UI would show reset, fork and signal-waiting, without a browser.
