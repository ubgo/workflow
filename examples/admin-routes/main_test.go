package main

import (
	"bytes"
	"context"
	"testing"
)

// TestRun checks every line the guide shows: one answer per admin route
// and one per error code.
func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != want {
		t.Errorf("output:\n%s\nwant:\n%s", got, want)
	}
}

// want is the program's whole output, as docs/admin/http-api.md shows it.
const want = `-- read
200 GET /capabilities  25 operations, output=true metrics=true
200 GET /runs?workflow=etl&count=1&limit=1  1 run on this page, total 2, pages 2
200 GET /runs?label=team=data*  2 runs with a team label starting data
200 GET /runs/run-3  etl failed, failed step transform
200 GET /failures  3 failed runs
200 GET /discarded  1 discarded run of cleanup
200 GET /runs/run-6/output  2 attempts captured, last: step transform, error "transform: bad date in row 7"
-- act on many runs
200 POST /runs/cancel  matched 4, done 0, dry run true
200 POST /runs/retry  matched 2, done 0, dry run true
200 POST /runs/delete  matched 1, done 1, dry run false, more match true
-- act on one run
200 POST /runs/run-12/retry  ref run-12
200 POST /runs/run-23/cancel  ok true
200 DELETE /runs/run-9  ok true
200 POST /runs/run-25/steps/await/cancel  ok true
200 POST /runs/run-3/reset  ref run-3
200 POST /runs/run-3/fork  ref run-36
200 POST /runs/run-27/complete  ok true
200 POST /signals/waiting  resumed 1
-- pauses and queues
200 POST /pause  ok true
200 GET /pauses  1 active, queue emails
200 POST /resume  ok true
200 GET /queues  2 queues: default workers 2, emails workers 1
-- schedules
200 GET /schedules  1 schedule, next fire 2026-10-15T02:00:00Z
200 GET /schedules/schedule-1  nightly-report "0 2 * * *" in UTC, paused false
200 POST /schedules/schedule-1/pause  ok true
200 POST /schedules/schedule-1/resume  ok true
200 POST /schedules/schedule-1/trigger  ref run-41
-- a delayed run
200 GET /runs/delayed-44?scheduled=1  scheduled, due 2026-10-14T10:00:00Z
200 POST /runs/delayed-44/reschedule?scheduled=1  ref delayed-44
200 POST /runs/delayed-44/edit?scheduled=1  ref delayed-44
200 POST /runs/delayed-44/fail?scheduled=1  ok true
-- charts, system and the UI
200 GET /metrics?from=2026-10-14T09:00:00Z&to=2026-10-14T09:10:00Z&step=5m  2 points, first: succeeded 8 failed 5 cancelled 1 discarded 1
200 GET /nodes  worker-1, 2 workers
200 GET /health  live true ready true, 1 checks
302 GET / -> ui/
302 GET /ui -> ui/
200 GET /ui/
-- error codes
400 POST /runs/delete  invalid: fix the request
401 GET /failures  unauthorized: log in again
403 POST /runs/run-6/retry  forbidden: ask for a role that allows it
404 GET /runs/no-such-run  not_found: refresh the list: the run or schedule is gone
409 POST /runs/run-3/retry  conflict: reload the run: its state changed
415 POST /runs/run-6/retry  unsupported_media_type: send Content-Type: application/json
501 GET /metrics  unsupported: hide the button: the server cannot do it
500 GET /runs/run-6/output  internal: try again later: the server logged the cause
`
