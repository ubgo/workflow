// Command admin-screenshots captures the admin web UI screenshots of the
// run reset, fork and waiting views shown in docs/admin/ui.md. It serves
// the UI over an in-memory client with dummy data (demo.go), drives a
// headless Chrome through chromedp, and writes compressed PNGs at the size
// of the other images in docs/assets/admin-ui (1360 x 900, light and
// dark).
//
// It is a module of its own so chromedp never reaches a library module or
// the examples module. Run it from the repository root:
//
//	task docs:screenshots
//
// or directly:
//
//	cd scripts/admin-screenshots && go run . -out ../../docs/assets/admin-ui
//
// It needs Chrome or Chromium on the machine (chromedp finds the usual
// install paths; -chrome names another binary).
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"image/png"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/chromedp"
)

// Viewport of every screenshot, matching the existing ones.
const (
	width  = 1360
	height = 900
)

func main() {
	out := flag.String("out", "../../docs/assets/admin-ui", "directory the PNGs are written to")
	chrome := flag.String("chrome", "", "path of the Chrome or Chromium binary (default: chromedp's search)")
	flag.Parse()
	if err := run(context.Background(), *out, *chrome); err != nil {
		fmt.Fprintln(os.Stderr, "admin-screenshots:", err)
		os.Exit(1)
	}
}

// shot is one screenshot: the page it opens, what it does there, and the
// colour scheme.
type shot struct {
	name    string // file name without .png
	dark    bool
	path    string // URL path and hash under the server
	actions []chromedp.Action
}

func run(ctx context.Context, outDir, chromePath string) error {
	mux, runs, stop, err := demo(ctx)
	if err != nil {
		return err
	}
	defer stop()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	opts := append(chromedp.DefaultExecAllocatorOptions[:], chromedp.WindowSize(width, height))
	if chromePath != "" {
		opts = append(opts, chromedp.ExecPath(chromePath))
	}
	actx, cancelAlloc := chromedp.NewExecAllocator(ctx, opts...)
	defer cancelAlloc()

	succeeded := "/admin/ui/#/run/" + string(runs.Succeeded.RunID)
	waiting := "/admin/ui/#/run/" + string(runs.Waiting.RunID)
	resetForm := []chromedp.Action{
		chromedp.SetValue(`input[name="redo-step"]`, "transform", chromedp.ByQuery),
		chromedp.Click(`input[name="redo-succeeded"]`, chromedp.ByQuery),
		// Reset is a danger button: the first click arms it ("Confirm reset?").
		chromedp.Click(`//button[normalize-space()="Reset"]`, chromedp.BySearch),
	}
	forkForm := []chromedp.Action{
		chromedp.SetValue(`input[name="redo-step"]`, "transform", chromedp.ByQuery),
		chromedp.SetValue(`input[name="fork-key"]`, "INV-1040-reupload", chromedp.ByQuery),
	}
	shots := []shot{
		{name: "run-reset-light", path: succeeded, actions: resetForm},
		{name: "run-reset-dark", dark: true, path: succeeded, actions: resetForm},
		{name: "run-fork-light", path: succeeded, actions: forkForm},
		{name: "run-forked-light", path: succeeded, actions: append(forkForm[:len(forkForm):len(forkForm)],
			chromedp.Click(`//button[normalize-space()="Fork"]`, chromedp.BySearch),
			// The fork opens the new run and says "Forked as <run>".
			chromedp.WaitVisible(`//*[@id="toast" and contains(., "Forked as")]`, chromedp.BySearch),
		)},
		{name: "run-waiting-light", path: waiting},
	}
	for _, s := range shots {
		png, err := capture(actx, srv.URL, s)
		if err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		file := filepath.Join(outDir, s.name+".png")
		if err := os.WriteFile(file, png, 0o644); err != nil {
			return err
		}
		fmt.Printf("%s (%d KB)\n", file, len(png)/1024)
	}
	return nil
}

// capture opens a fresh tab, logs in, opens the shot's page in its colour
// scheme, runs its actions and returns the viewport as a recompressed PNG.
func capture(actx context.Context, base string, s shot) ([]byte, error) {
	tctx, cancel := chromedp.NewContext(actx)
	defer cancel()
	tctx, cancelT := context.WithTimeout(tctx, 30*time.Second)
	defer cancelT()
	scheme := "light"
	if s.dark {
		scheme = "dark"
	}
	var raw []byte
	tasks := chromedp.Tasks{
		emulation.SetDeviceMetricsOverride(width, height, 1, false),
		emulation.SetEmulatedMedia().WithFeatures([]*emulation.MediaFeature{{Name: "prefers-color-scheme", Value: scheme}}),
		chromedp.Navigate(base + "/login"),
		chromedp.Navigate(base + s.path),
		chromedp.WaitVisible(`dl.kv`, chromedp.ByQuery),
	}
	tasks = append(tasks, s.actions...)
	tasks = append(tasks,
		// Let the toast and any re-render settle.
		chromedp.Sleep(400*time.Millisecond),
		chromedp.CaptureScreenshot(&raw),
	)
	if err := chromedp.Run(tctx, tasks); err != nil {
		return nil, err
	}
	return recompress(raw)
}

// recompress re-encodes a PNG at the best compression, so the images stay
// a few dozen KB each.
func recompress(raw []byte) ([]byte, error) {
	img, err := png.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestCompression}
	if err := enc.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
