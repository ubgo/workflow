package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// TestThenSkipsRedo: a succeeded import starts index once; resetting the
// succeeded import (ResetOptions.Succeeded) and forking it do not start
// index again, since both carry LabelRedo; with ThenAfterRedo each redo
// starts it.
func TestThenSkipsRedo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []workflow.ThenOption
		after int // index runs after the first import, the reset and the fork
	}{
		{"default", nil, 1},
		{"after redo", []workflow.ThenOption{workflow.ThenAfterRedo()}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec thenRec
			then, err := workflow.Then([]workflow.ThenRule{{When: "import", Start: func(ev workflow.RunEvent) (workflow.Job, bool, error) {
				return workflow.Job{Workflow: "index"}, true, nil
			}}}, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			c := startThen(t, memory.New(), then, rec.wf("import", false), rec.wf("index", false))
			ctx := context.Background()
			ref, err := c.Enqueue(ctx, workflow.Job{Workflow: "import"})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "index run", func() bool { return len(rec.byWorkflow("index")) == 1 })
			if _, err := workflow.Await(ctx, c, ref, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
				t.Fatal(err)
			}
			again, err := c.(workflow.RunResetterWith).ResetRunWith(ctx, ref, "s", workflow.ResetOptions{Succeeded: true})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the reset import", func() bool { return len(rec.byWorkflow("import")) == 2 })
			if _, err := workflow.Await(ctx, c, again, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
				t.Fatal(err)
			}
			fork, err := c.(workflow.RunForker).ForkRun(ctx, ref, "s", workflow.ForkOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := workflow.Await(ctx, c, fork, workflow.AwaitOptions{Poll: time.Millisecond}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "index runs", func() bool { return len(rec.byWorkflow("index")) >= tc.after })
			time.Sleep(50 * time.Millisecond)
			if n := len(rec.byWorkflow("index")); n != tc.after {
				t.Fatalf("index ran %d times, want %d", n, tc.after)
			}
		})
	}
}
