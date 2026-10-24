package hatchet

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// TestCoordinatorSwitch pins that the coordinator is opt-in: SpawnsGroups
// follows Config.BatchCoordinator, and SpawnGroup on a client without it
// refuses with ErrInvalid (no worker of that client runs coordinators),
// before any engine call.
func TestCoordinatorSwitch(t *testing.T) {
	off := New(Config{})
	if off.SpawnsGroups() {
		t.Fatal("SpawnsGroups is on without Config.BatchCoordinator")
	}
	if _, err := off.SpawnGroup(context.Background(), "g", []workflow.Job{{Workflow: "a"}}); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("SpawnGroup without the coordinator = %v, want ErrInvalid", err)
	}
	on := New(Config{BatchCoordinator: true})
	if !on.SpawnsGroups() {
		t.Fatal("SpawnsGroups is off with Config.BatchCoordinator")
	}
	if on.cfg.BatchCoordinatorTimeout != DefaultBatchCoordinatorTimeout {
		t.Fatalf("BatchCoordinatorTimeout = %s, want the default", on.cfg.BatchCoordinatorTimeout)
	}
	if _, err := on.SpawnGroup(context.Background(), "g", []workflow.Job{{Workflow: "a"}}); !errors.Is(err, workflow.ErrNotStarted) {
		t.Fatalf("SpawnGroup before Start = %v, want ErrNotStarted", err)
	}
}

// TestCoordinatorIdempotencyExpression pins the coordinator's CEL key: it
// is scoped to the coordinator and reads the batch label, so a second
// SpawnGroup for one batch collides with the first.
func TestCoordinatorIdempotencyExpression(t *testing.T) {
	expr := coordinatorIdempotencyExpression()
	for _, want := range []string{`"` + coordinatorWorkflow + idemKeySep + `"`, `additional_metadata["workflow.batch.id"]`} {
		if !strings.Contains(expr, want) {
			t.Fatalf("expression %s does not contain %s", expr, want)
		}
	}
}

// TestCoordinatorSettled pins when BatchSource may judge a coordinator's
// children: never while it runs, not within coordinatorSettle of its end
// (the run list may lag, by Config.VisibilityLag), and from then on.
func TestCoordinatorSettled(t *testing.T) {
	end := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	lag := 3 * time.Second
	settle := 2 * lag
	for _, tc := range []struct {
		name  string
		ended time.Time
		now   time.Time
		want  bool
	}{
		{"still running", time.Time{}, end.Add(time.Hour), false},
		{"just ended", end, end.Add(settle - time.Millisecond), false},
		{"settled", end, end.Add(settle), true},
	} {
		if got := coordinatorSettled(tc.ended, tc.now, lag); got != tc.want {
			t.Errorf("%s: coordinatorSettled = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestUnstartedChildren pins which jobs a settled coordinator never
// started: those the engine does not list and that have no status yet. A
// listed child, and a job whose end is already recorded (by the hook or
// an earlier tick), are never recorded as cancelled.
func TestUnstartedChildren(t *testing.T) {
	st := batch.State{Jobs: []batch.JobState{
		{},                                 // 0: listed
		{},                                 // 1: never started
		{Status: workflow.StatusSucceeded}, // 2: ended, not listed (aged out of the list)
		{},                                 // 3: never started
		{Status: workflow.StatusCancelled}, // 4: already recorded
	}}
	found := map[int]workflow.RunRef{0: {RunID: "c0"}}
	if got, want := unstartedChildren(st, found), []int{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unstartedChildren = %v, want %v", got, want)
	}
	all := map[int]workflow.RunRef{0: {RunID: "c0"}, 1: {RunID: "c1"}, 3: {RunID: "c3"}}
	if got := unstartedChildren(st, all); got != nil {
		t.Fatalf("unstartedChildren with every child listed = %v, want none", got)
	}
}
