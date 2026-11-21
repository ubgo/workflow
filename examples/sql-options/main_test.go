package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun runs the program and checks every line the guide shows: the run
// enqueued in a transaction starts after Wake although the client polls
// hourly, each error lands in its class, the middleware wraps the step,
// Client.DB reads the runs table, and both stores work with no client.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := `run enqueued in a transaction ran before the next poll: true
sqlite duplicate: {UniqueViolation:true Retryable:false Unavailable:false}
proxy BUSY:      {UniqueViolation:false Retryable:true Unavailable:false}
proxy DOWN:      {UniqueViolation:false Retryable:false Unavailable:true}
other:           {UniqueViolation:false Retryable:false Unavailable:false}
middleware: send-receipt/send attempt 1
succeeded runs in wf_runs: 3
data store without a client: cursor 42 (found true, version 1)
journal store without a client: fetch "page-1" (found true)
`
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
