package workflow_test

import (
	"encoding/json"
	"testing"

	"github.com/ubgo/workflow"
)

func TestStepInput(t *testing.T) {
	outs := map[string][]byte{"a": []byte(`{"n":1}`), "b": []byte("plain text")}

	if got := workflow.StepInput([]byte("payload"), nil, outs); string(got) != "payload" {
		t.Fatalf("no parents: %s", got)
	}
	if got := workflow.StepInput([]byte("payload"), []string{"b"}, outs); string(got) != "plain text" {
		t.Fatalf("one parent: %s", got)
	}
	got := workflow.StepInput(nil, []string{"b", "a", "missing"}, outs)
	if string(got) != `{"b":"plain text","a":{"n":1},"missing":null}` {
		t.Fatalf("several parents: %s", got)
	}
	if !json.Valid(got) {
		t.Fatal("several parents: not valid JSON")
	}
}
