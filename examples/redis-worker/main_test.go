package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// offlineOut is what the program prints without a server: what the driver
// declares, and the closures it cannot store.
const offlineOut = `durable: true cron: true overlap skip: true
closures: false false
`

// TestRun checks every line the guide shows. Without REDIS_URL only the
// offline part runs; with it, the same key gives the same run, the retried
// step sends once with one error kept, and the triggered schedule's last
// run reads succeeded.
func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	want := offlineOut + "REDIS_URL is not set: the live run is skipped\n"
	if os.Getenv(envURL) != "" {
		want = offlineOut + `same key, same run: true
send-email succeeded after 2 attempts, sent 1 time(s), errors kept 1
schedule nightly: last run succeeded, next fire 02:00 UTC
`
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}
