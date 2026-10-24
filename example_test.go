package workflow_test

import (
	"context"
	"fmt"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// Example is the README's example, kept here so it is compiled and run.
func Example() {
	type greet struct{ Name string }
	done := make(chan string, 1)

	c := memory.New()
	err := c.Register(workflow.Workflow{
		Name: "greet",
		Steps: []workflow.Step{{
			Name: "say",
			Run: workflow.Define(func(ctx context.Context, in greet) (string, error) {
				done <- "hello " + in.Name
				return "ok", nil
			}),
		}},
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stops the workers
	if err := c.Start(ctx); err != nil {
		panic(err)
	}
	if _, err := c.Enqueue(ctx, workflow.Job{Workflow: "greet", Payload: []byte(`{"Name":"Ada"}`)}); err != nil {
		panic(err)
	}
	fmt.Println(<-done)
	// Output: hello Ada
}
