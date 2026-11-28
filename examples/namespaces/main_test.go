package main

import (
	"bytes"
	"context"
	"os"
	"testing"
)

// sqlOut is what the program prints on SQLite: the tables of each
// namespace, what each client sees, and the settings New refuses.
const sqlOut = `tables: 47 staging_*, 47 prod_*
staging worker: staging run scheduled, 1 schedule(s)
prod: staging run not found, 0 schedule(s)
bad prefix: invalid true
schema on SQLite: unsupported true
`

// TestRun checks every line the guide shows. Without REDIS_URL only the
// SQLite part runs; with it, the Redis part proves two key prefixes apart.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := sqlOut + "REDIS_URL is not set: the Redis part is skipped\n"
	if os.Getenv(envURL) != "" {
		want = sqlOut + "redis prod: staging run not found true\n"
	}
	if got := out.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}
