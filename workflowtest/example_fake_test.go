package workflowtest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/workflowtest"
)

// signUp is app code under test: it takes the Client interface, so a test
// hands it a Fake and production hands it a real driver.
func signUp(ctx context.Context, c workflow.Client, email string) error {
	_, err := c.Enqueue(ctx, workflow.Job{
		Workflow:       "welcome-email",
		Payload:        []byte(`{"email":"` + email + `"}`),
		IdempotencyKey: "welcome:" + email,
		Labels:         map[string]string{"source": "signup"},
	})
	return err
}

var welcome = workflow.Workflow{Name: "welcome-email", Steps: []workflow.Step{{
	Name: "send",
	Run:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
}}}

// TestSignUpEnqueuesWelcomeEmail is the shape of an app test on the Fake.
func TestSignUpEnqueuesWelcomeEmail(t *testing.T) {
	ctx := context.Background()
	fake := workflowtest.NewFake() // or NewFake(workflowtest.FakeLike(prodClient))
	if err := fake.Register(welcome); err != nil {
		t.Fatal(err)
	}
	if err := fake.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := signUp(ctx, fake, "a@example.com"); err != nil {
		t.Fatal(err)
	}
	if err := signUp(ctx, fake, "a@example.com"); err != nil { // a double submit
		t.Fatal(err)
	}

	fake.AssertEnqueued(t, "welcome-email",
		workflowtest.HasPayloadJSON(map[string]string{"email": "a@example.com"}),
		workflowtest.HasLabel("source", "signup"))
	fake.AssertEnqueuedTimes(t, "welcome-email", 1) // the key deduped the double submit
	fake.AssertNotEnqueued(t, "password-reset")
}

func ExampleFake() {
	ctx := context.Background()
	fake := workflowtest.NewFake()
	_ = fake.Register(welcome)
	_ = fake.Start(ctx)

	_ = signUp(ctx, fake, "a@example.com")
	_ = signUp(ctx, fake, "b@example.com")

	for _, j := range fake.Enqueued("welcome-email", workflowtest.HasLabel("source", "signup")) {
		fmt.Println(j.IdempotencyKey)
	}
	// Output:
	// welcome:a@example.com
	// welcome:b@example.com
}
