package hatchet_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/hatchet"
	"github.com/ubgo/workflow/workflowtest"
)

// crashCase is the name fragment of the conformance case that abandons a
// client mid-step, and crashStepTimeout the execution timeout that lets
// the engine reclaim that step within the suite's wait.
const (
	crashCase        = "crash"
	crashStepTimeout = 15 * time.Second
)

// envToken names the setting that turns the live suite on. The SDK reads
// it, and HATCHET_CLIENT_HOST_PORT / HATCHET_CLIENT_TLS_STRATEGY, itself.
const envToken = "HATCHET_CLIENT_TOKEN"

// TestConformance runs the shared workflowtest suite against a real
// Hatchet engine (the shared local stack: REST on :7003, gRPC on :7004).
// It is skipped, with the reason, when no token is set, so `go test ./...`
// stays green on a machine without Hatchet; run it through `task
// test:live` in this module.
func TestConformance(t *testing.T) {
	if os.Getenv(envToken) == "" {
		t.Skipf("%s is not set: the Hatchet conformance run needs a live engine (task test:live)", envToken)
	}
	workflowtest.Run(t, workflowtest.Harness{
		New: func(t *testing.T) workflowtest.Instance {
			cfg := hatchet.Config{WorkerName: "wftest-hatchet"}
			if strings.Contains(t.Name(), crashCase) {
				// A step whose worker is gone is reclaimed by the engine
				// when the step's execution timeout expires (see doc.go);
				// a short one keeps the case inside Harness.Wait.
				cfg.StepTimeout = crashStepTimeout
			}
			newClient := func(*testing.T) workflow.Client { return hatchet.New(cfg) }
			return workflowtest.Instance{
				Client:  newClient(t),
				Restart: newClient,
				// Crash abandons the old client as a killed process
				// would: its Start ctx is not cancelled, so its worker
				// never deregisters and its stuck step is never released.
				Crash: newClient,
				// A web node's client: no worker, no Start; the started
				// Client above runs what it enqueues.
				EnqueueOnly: func(*testing.T) workflow.Client {
					return hatchet.New(hatchet.Config{EnqueueOnly: true})
				},
			}
		},
		KeyFromLabel: func(label string) workflow.ConcurrencyKey {
			return workflow.KeyExpr("additional_metadata[" + strconv.Quote(label) + "]")
		},
		// Hatchet rate limits take fixed windows only (second, minute,
		// hour, day, week); a minute also leaves room for the engine's
		// coarse refill, which starts the next run up to ~30s late.
		RateLimitPer: time.Minute,
		// Quiet doubles as the real-time timing slack: 3s covers the
		// dispatch jitter between the engine admitting a run and its
		// handler starting (about 1s seen live on rate-limited starts).
		Quiet:    3 * time.Second,
		RealCron: os.Getenv("WFTEST_REAL_CRON") != "",
		Prefix:   "wftest_hatchet",
	})
}
