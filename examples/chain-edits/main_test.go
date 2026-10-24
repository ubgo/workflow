package main

import (
	"context"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// want is the program's whole output, the one its README and guide show.
const want = `chain: import-file(a.csv) -> notify(imported)
edited while running (scan tried 2 times): import-files(inbox) -> import-files(inbox) -> import-file(b.csv) -> notify(imported) -> reindex(all)
a failed run ends the chain: charge(o-1) (failed)
AppendToChain outside a step: true, missing Chain
on hatchet: ErrUnsupported true (workflow: Job.Chain unsupported by this driver (missing Chain): job for workflow import-file)
`

// TestRun runs the program and checks every line it prints.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

func noop(context.Context, []byte) ([]byte, error) { return nil, nil }

// snippet:fake-chain
func TestImportEnqueuesItsChain(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	for _, name := range []string{"import-file", "notify"} {
		if err := fake.Register(workflow.Workflow{Name: name, Steps: []workflow.Step{{Name: "run", Run: noop}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}
	job := workflow.Job{Workflow: "import-file", Chain: []workflow.Job{{Workflow: "notify"}}}
	if _, err := fake.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	fake.AssertEnqueued(t, "import-file", workflowtest.HasChain("notify"))
}

// snippet:end

// snippet:runstep-chain
func TestScanEditsTheChain(t *testing.T) {
	scan := func(ctx context.Context, _ []byte) ([]byte, error) {
		if err := workflow.PrependToChain(ctx, workflow.Job{Workflow: "import-file"}); err != nil {
			return nil, err
		}
		return nil, workflow.AppendToChain(ctx, workflow.Job{Workflow: "reindex"})
	}
	out := workflowtest.RunStep(scan, nil, workflowtest.StepChain(workflow.Job{Workflow: "notify"}))
	out.AssertSucceeded(t)
	if got := strings.Join(out.ChainNames(), " -> "); got != "import-file -> notify -> reindex" {
		t.Errorf("chain after the step: %s", got)
	}
}

// snippet:end
