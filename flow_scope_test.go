package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
	"github.com/ubgo/workflow/workflowtest"
)

func TestMemoryDataContract(t *testing.T) {
	s := workflow.NewMemoryData()
	workflowtest.DataStoreContract(t, s, s)
}

// TestScopedDataAcrossRuns: a folder value written by one run is read by
// another; the workflow scope resolves to the running workflow, so two
// workflows keep their own counters; the global scope is shared by both.
func TestScopedDataAcrossRuns(t *testing.T) {
	store := workflow.NewMemoryData()
	c, err := workflow.Use(memory.New(), workflow.Data(store))
	if err != nil {
		t.Fatal(err)
	}
	type seen struct{ Plan string }
	got := make(chan seen, 1)
	bump := func(ctx context.Context, _ []byte) ([]byte, error) {
		if _, err := workflow.UpdateData(ctx, workflow.WorkflowScope(""), "runs", func(n int, _ bool) (int, error) { return n + 1, nil }); err != nil {
			return nil, err
		}
		_, err := workflow.UpdateData(ctx, workflow.GlobalScope(), "runs", func(n int, _ bool) (int, error) { return n + 1, nil })
		return nil, err
	}
	wfs := []workflow.Workflow{
		{Name: "signup", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			if err := workflow.SetData(ctx, workflow.Folder("acme"), "plan", "pro"); err != nil {
				return nil, err
			}
			return bump(ctx, in)
		}}}},
		{Name: "invoice", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, in []byte) ([]byte, error) {
			plan, ok, err := workflow.GetData[string](ctx, workflow.Folder("acme"), "plan")
			if err != nil || !ok {
				return nil, workflow.Permanent(errors.New("plan missing"))
			}
			select {
			case got <- seen{Plan: plan}:
			default:
			}
			return bump(ctx, in)
		}}}},
	}
	for _, wf := range wfs {
		if err := c.Register(wf); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "signup"})); st != workflow.StatusSucceeded {
		t.Fatalf("signup = %v", st)
	}
	for range 2 {
		if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "invoice"})); st != workflow.StatusSucceeded {
			t.Fatalf("invoice = %v", st)
		}
	}
	if s := <-got; s.Plan != "pro" {
		t.Fatalf("invoice read plan %q", s.Plan)
	}
	// Outside a step the app reads the same store with ContextWithData
	// and names the workflow.
	rc := workflow.ContextWithData(context.Background(), store)
	for scope, want := range map[workflow.DataScope]int{
		workflow.WorkflowScope("signup"):  1,
		workflow.WorkflowScope("invoice"): 2,
		workflow.GlobalScope():            3,
	} {
		if n, ok, err := workflow.GetData[int](rc, scope, "runs"); !ok || err != nil || n != want {
			t.Errorf("%s runs = %d, %v, %v; want %d", scope, n, ok, err, want)
		}
	}
}

func TestScopedDataRefusals(t *testing.T) {
	ctx := context.Background()
	if err := workflow.SetData(ctx, workflow.GlobalScope(), "x", 1); !errors.Is(err, workflow.ErrNoDataStore) || !workflow.IsPermanent(err) {
		t.Fatalf("no store = %v; want a Permanent ErrNoDataStore", err)
	}
	dc := workflow.ContextWithData(ctx, workflow.NewMemoryData())
	for name, scope := range map[string]workflow.DataScope{
		"folder without a name":            workflow.Folder(""),
		"global with a name":               {Kind: workflow.ScopeGlobal, Name: "x"},
		"unknown kind":                     {Kind: "team", Name: "x"},
		"workflow scope outside a step":    workflow.WorkflowScope(""),
		"empty kind (zero value of scope)": {},
	} {
		if err := workflow.SetData(dc, scope, "x", 1); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
			t.Errorf("%s: %v; want a Permanent ErrInvalid", name, err)
		}
	}
	if err := workflow.SetData(dc, workflow.GlobalScope(), "", 1); !errors.Is(err, workflow.ErrInvalid) {
		t.Errorf("empty name: %v", err)
	}
	if err := workflow.SetData(dc, workflow.GlobalScope(), "s", "text"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := workflow.GetData[int](dc, workflow.GlobalScope(), "s"); !workflow.IsPermanent(err) {
		t.Errorf("a value of another type = %v; want Permanent", err)
	}
	boom := errors.New("boom")
	if _, err := workflow.UpdateData(dc, workflow.GlobalScope(), "n", func(int, bool) (int, error) { return 0, boom }); !errors.Is(err, boom) {
		t.Errorf("UpdateData fn error = %v", err)
	}
	if _, ok, _ := workflow.GetData[int](dc, workflow.GlobalScope(), "n"); ok {
		t.Error("UpdateData stored a value after fn failed")
	}
	if err := workflow.DeleteData(dc, workflow.GlobalScope(), "s"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := workflow.GetData[string](dc, workflow.GlobalScope(), "s"); ok {
		t.Error("DeleteData kept the value")
	}
}

// conflictStore always refuses a conditional Put, as a store under
// constant contention would.
type conflictStore struct{ *workflow.MemoryData }

func (c conflictStore) Put(ctx context.Context, s workflow.DataScope, n string, v []byte, match workflow.DataVersion) (workflow.DataVersion, error) {
	if match != workflow.AnyVersion {
		return 0, workflow.ErrDataConflict
	}
	return c.MemoryData.Put(ctx, s, n, v, match)
}

func TestUpdateDataGivesUpRetryably(t *testing.T) {
	dc := workflow.ContextWithData(context.Background(), conflictStore{workflow.NewMemoryData()})
	calls := 0
	_, err := workflow.UpdateData(dc, workflow.GlobalScope(), "n", func(n int, _ bool) (int, error) { calls++; return n + 1, nil })
	if !errors.Is(err, workflow.ErrDataConflict) || workflow.IsPermanent(err) {
		t.Fatalf("err = %v; want a retryable ErrDataConflict", err)
	}
	if calls != workflow.UpdateDataAttempts {
		t.Fatalf("fn called %d times, want %d", calls, workflow.UpdateDataAttempts)
	}
}

func TestDataScopeKeyIsUnambiguous(t *testing.T) {
	a := workflow.Folder("a/b").Key("c")
	b := workflow.Folder("a").Key("b/c")
	if a == b {
		t.Fatalf("keys collide: %q", a)
	}
	if got := workflow.Folder("acme").Key("plan"); got != "folder:4:acme/plan" {
		t.Fatalf("Key = %q; the stored form changed", got)
	}
}
