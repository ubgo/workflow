package workflow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

// flowClient returns a started memory client with the Journal plugin on
// store, after registering wfs through it.
func flowClient(t *testing.T, store workflow.JournalStore, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	return flowClientOpts(t, store, nil, wfs...)
}

func flowClientOpts(t *testing.T, store workflow.JournalStore, opts []workflow.JournalOption, wfs ...workflow.Workflow) workflow.Client {
	t.Helper()
	c, err := workflow.Use(memory.New(), workflow.Journal(store, opts...))
	if err != nil {
		t.Fatal(err)
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
	return c
}

// waitRun polls ref until it is terminal and returns its status.
func waitRun(t *testing.T, c workflow.Client, ref workflow.RunRef) workflow.Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := c.Status(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if st.IsTerminal() {
			return st
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("run %v did not finish", ref)
	return ""
}

func enqueue(t *testing.T, c workflow.Client, job workflow.Job) workflow.RunRef {
	t.Helper()
	ref, err := c.Enqueue(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestMemoSkipsWorkOnRetry(t *testing.T) {
	var charges, attempts atomic.Int32
	store := workflow.NewMemoryJournal()
	wf := workflow.Workflow{Name: "bill", Retry: workflow.RetryPolicy{Max: 2}, Steps: []workflow.Step{{
		Name: "charge",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			id, err := workflow.Memo(ctx, "charge", func(context.Context) (string, error) {
				charges.Add(1)
				return "ch_1", nil
			})
			if err != nil {
				return nil, err
			}
			if attempts.Add(1) == 1 {
				return nil, errors.New("mail failed after the charge")
			}
			return []byte(id), nil
		},
	}}}
	c := flowClient(t, store, wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "bill"})
	if st := waitRun(t, c, ref); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	if charges.Load() != 1 || attempts.Load() != 2 {
		t.Fatalf("charges = %d, attempts = %d; want 1, 2", charges.Load(), attempts.Load())
	}
	waitForgotten(t, store)
}

// waitForgotten waits for the run-finished observer to empty store.
func waitForgotten(t *testing.T, store *workflow.MemoryJournal) {
	t.Helper()
	for range 500 {
		if len(store.Runs()) == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("journal still holds %v after the run finished", store.Runs())
}

func TestJournalKeepFailed(t *testing.T) {
	store := workflow.NewMemoryJournal()
	wf := workflow.Workflow{Name: "f", Steps: []workflow.Step{{Name: "s",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			if _, err := workflow.Memo(ctx, "a", func(context.Context) (int, error) { return 1, nil }); err != nil {
				return nil, err
			}
			return nil, errors.New("boom")
		}}}}
	c := flowClientOpts(t, store, []workflow.JournalOption{workflow.JournalKeepFailed()}, wf)
	ref := enqueue(t, c, workflow.Job{Workflow: "f"})
	if st := waitRun(t, c, ref); st != workflow.StatusFailed {
		t.Fatalf("status = %v", st)
	}
	time.Sleep(20 * time.Millisecond) // give the observer time to (wrongly) forget
	if got := store.Runs(); len(got) != 1 || got[0] != ref.RunID {
		t.Fatalf("journal runs = %v, want the failed run kept", got)
	}
}

func TestMemoRefusesDuplicateNameAndMissingJournal(t *testing.T) {
	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r1", Step: "s"})
	h := workflow.WithJournal(workflow.NewMemoryJournal())(func(ctx context.Context, _ []byte) ([]byte, error) {
		one := func(context.Context) (int, error) { return 1, nil }
		if _, err := workflow.Memo(ctx, "x", one); err != nil {
			return nil, err
		}
		_, err := workflow.Memo(ctx, "x", one)
		return nil, err
	})
	if _, err := h(ctx, nil); !errors.Is(err, workflow.ErrInvalid) || !workflow.IsPermanent(err) {
		t.Fatalf("duplicate name = %v, want Permanent ErrInvalid", err)
	}
	_, err := workflow.Memo(context.Background(), "x", func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, workflow.ErrNoJournal) || !workflow.IsPermanent(err) {
		t.Fatalf("no journal = %v, want Permanent ErrNoJournal", err)
	}
}

func TestMemoKeysDoNotCollideAcrossSteps(t *testing.T) {
	store := workflow.NewMemoryJournal()
	call := func(step, name string, v int) int {
		ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: step})
		var got int
		_, _ = workflow.WithJournal(store)(func(ctx context.Context, _ []byte) ([]byte, error) {
			var err error
			got, err = workflow.Memo(ctx, name, func(context.Context) (int, error) { return v, nil })
			return nil, err
		})(ctx, nil)
		return got
	}
	if call("a/b", "c", 1) != 1 || call("a", "b/c", 2) != 2 {
		t.Fatal("step a/b name c and step a name b/c shared an entry")
	}
}

func TestMapEachRerunsOnlyFailedItems(t *testing.T) {
	var mu sync.Mutex
	calls := map[int]int{}
	wf := workflow.Workflow{Name: "map", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "s",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			out, err := workflow.MapEach(ctx, "sq", []int{1, 2, 3, 4}, func(_ context.Context, i, n int) (int, error) {
				mu.Lock()
				calls[i]++
				first := calls[i] == 1
				mu.Unlock()
				if i == 2 && first {
					return 0, errors.New("flaky")
				}
				return n * n, nil
			}, workflow.MapConcurrency(3))
			if err != nil {
				return nil, err
			}
			if !slices.Equal(out, []int{1, 4, 9, 16}) {
				return nil, workflow.Permanent(errors.New("wrong order"))
			}
			return nil, nil
		}}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "map"})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	if calls[0] != 1 || calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("calls = %v; want only item 2 twice", calls)
	}
}

func TestLoopResumesAndStopsAtLimit(t *testing.T) {
	var bodies atomic.Int32
	failAt := atomic.Int32{}
	failAt.Store(3)
	wf := workflow.Workflow{Name: "loop", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "s",
		Run: func(ctx context.Context, _ []byte) ([]byte, error) {
			n, err := workflow.Loop(ctx, "page", 0, 10, func(_ context.Context, i, s int) (int, bool, error) {
				bodies.Add(1)
				if i == int(failAt.Load()) {
					failAt.Store(-1)
					return s, false, errors.New("page fetch failed")
				}
				return s + 1, s+1 == 5, nil
			})
			if err != nil {
				return nil, err
			}
			if n != 5 {
				return nil, workflow.Permanent(errors.New("wrong state"))
			}
			return nil, nil
		}}}}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "loop"})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	// Attempt 1 runs iterations 0..3 (3 fails); attempt 2 replays 0..2 from
	// the journal and runs 3 and 4: 4 + 2 bodies.
	if bodies.Load() != 6 {
		t.Fatalf("bodies = %d, want 6", bodies.Load())
	}

	ctx := workflow.WithStepInfo(context.Background(), workflow.StepInfo{RunID: "r", Step: "s"})
	_, _ = workflow.WithJournal(workflow.NewMemoryJournal())(func(ctx context.Context, _ []byte) ([]byte, error) {
		_, err := workflow.Loop(ctx, "forever", 0, 3, func(_ context.Context, _, s int) (int, bool, error) { return s, false, nil })
		if !errors.Is(err, workflow.ErrLoopLimit) || !workflow.IsPermanent(err) {
			t.Errorf("limit = %v, want Permanent ErrLoopLimit", err)
		}
		_, err = workflow.Loop(ctx, "zero", 0, 0, func(_ context.Context, _, s int) (int, bool, error) { return s, true, nil })
		if !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("limit 0 = %v, want ErrInvalid", err)
		}
		return nil, nil
	})(ctx, nil)
}

func TestChoiceRunsOneBranch(t *testing.T) {
	var ran sync.Map
	branch := func(name string) workflow.Branch {
		return workflow.Branch{Name: name, Run: func(_ context.Context, in []byte) ([]byte, error) {
			ran.Store(name, true)
			return []byte(name + ":" + string(in)), nil
		}}
	}
	steps, err := workflow.Choice(workflow.ChoiceSpec{
		Name: "route",
		Decide: func(_ context.Context, in []byte) (string, error) {
			if strings.HasPrefix(string(in), "big") {
				return "manual", nil
			}
			if string(in) == "bad" {
				return "nope", nil
			}
			return "auto", nil
		},
		Branches: []workflow.Branch{branch("auto"), branch("manual")},
	})
	if err != nil {
		t.Fatal(err)
	}
	var final atomic.Value
	steps = append(steps, workflow.Step{Name: "after", After: []string{"route"}, Run: func(_ context.Context, in []byte) ([]byte, error) {
		final.Store(string(in))
		return nil, nil
	}})
	wf := workflow.Workflow{Name: "choice", Steps: steps}
	if err := workflow.Validate(wf); err != nil {
		t.Fatal(err)
	}
	c := flowClient(t, workflow.NewMemoryJournal(), wf)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "choice", Payload: []byte("big order")})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	if final.Load() != "manual:big order" {
		t.Fatalf("after got %q", final.Load())
	}
	if _, ok := ran.Load("auto"); ok {
		t.Fatal("the branch not chosen ran its Run")
	}
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "choice", Payload: []byte("bad")})); st != workflow.StatusFailed {
		t.Fatalf("unknown branch: status = %v, want failed", st)
	}

	for _, bad := range []workflow.ChoiceSpec{
		{Decide: func(context.Context, []byte) (string, error) { return "", nil }, Branches: []workflow.Branch{branch("a")}},
		{Name: "x", Branches: []workflow.Branch{branch("a")}},
		{Name: "x", Decide: func(context.Context, []byte) (string, error) { return "", nil }},
		{Name: "x", Decide: func(context.Context, []byte) (string, error) { return "", nil }, Branches: []workflow.Branch{branch("a"), branch("a")}},
	} {
		if _, err := workflow.Choice(bad); !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("Choice(%+v) = %v, want ErrInvalid", bad.Name, err)
		}
	}
}

func TestChoiceSingleBranch(t *testing.T) {
	steps, err := workflow.Choice(workflow.ChoiceSpec{Name: "only",
		Decide:   func(context.Context, []byte) (string, error) { return "a", nil },
		Branches: []workflow.Branch{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return append(in, '!'), nil }}}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := workflow.RunInline(context.Background(), workflow.Workflow{Name: "one", Steps: steps}, workflow.Job{Workflow: "one", Payload: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Outputs["only"]); got != "hi!" {
		t.Fatalf("output = %q", got)
	}
	steps, _ = workflow.Choice(workflow.ChoiceSpec{Name: "bad",
		Decide:   func(context.Context, []byte) (string, error) { return "missing", nil },
		Branches: []workflow.Branch{{Name: "a", Run: func(_ context.Context, in []byte) ([]byte, error) { return in, nil }}}})
	_, err = workflow.RunInline(context.Background(), workflow.Workflow{Name: "bad", Steps: steps}, workflow.Job{Workflow: "bad"})
	if !errors.Is(err, workflow.ErrInvalid) || !strings.Contains(err.Error(), "not a branch") {
		t.Fatalf("unknown branch = %v, want the decide step to fail with ErrInvalid", err)
	}
}

// noKeyClient hides CapIdempotencyKey.
type noKeyClient struct{ workflow.Client }

func (n noKeyClient) Capabilities() workflow.Caps {
	return n.Client.Capabilities() &^ workflow.CapIdempotencyKey
}

func TestSpawnOncePerRun(t *testing.T) {
	var children atomic.Int32
	var attempts atomic.Int32
	var c workflow.Client
	child := workflow.Workflow{Name: "child", Steps: []workflow.Step{{Name: "s", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		info, _ := workflow.StepInfoFrom(ctx)
		if info.Labels[workflow.LabelParentRun] == "" || info.Labels[workflow.LabelParentStep] != "fan" {
			return nil, workflow.Permanent(errors.New("missing parent labels"))
		}
		children.Add(1)
		return nil, nil
	}}}}
	parent := workflow.Workflow{Name: "parent", Retry: workflow.RetryPolicy{Max: 1}, Steps: []workflow.Step{{Name: "fan", Run: func(ctx context.Context, _ []byte) ([]byte, error) {
		jobs := []workflow.Job{{Workflow: "child"}, {Workflow: "child"}, {Workflow: "child"}}
		refs, err := workflow.Spawn(ctx, c, "kids", jobs)
		if err != nil {
			return nil, err
		}
		if len(refs) != 3 {
			return nil, workflow.Permanent(errors.New("refs"))
		}
		if attempts.Add(1) == 1 {
			time.Sleep(50 * time.Millisecond) // let the children finish, so only the journal stops a re-spawn
			return nil, errors.New("fail after spawning")
		}
		if _, err := workflow.Spawn(ctx, noKeyClient{c}, "other", jobs); !errors.Is(err, workflow.ErrUnsupported) {
			return nil, workflow.Permanent(errors.New("no-key client was not refused"))
		}
		return nil, nil
	}}}}
	c = flowClient(t, workflow.NewMemoryJournal(), child, parent)
	if st := waitRun(t, c, enqueue(t, c, workflow.Job{Workflow: "parent"})); st != workflow.StatusSucceeded {
		t.Fatalf("status = %v", st)
	}
	time.Sleep(30 * time.Millisecond)
	if children.Load() != 3 {
		t.Fatalf("children ran %d times, want 3", children.Load())
	}
}

func TestSecretGuard(t *testing.T) {
	next := func(context.Context, workflow.Job) (workflow.RunRef, error) { return workflow.RunRef{RunID: "ok"}, nil }
	guard := workflow.SecretGuard(nil)(next)
	refuse := []workflow.Job{
		{Payload: []byte(`{"user":{"password":"hunter2"}}`)},
		{Payload: []byte(`{"api_key":"abc"}`)},
		{Payload: []byte(`{"Client-Secret":"abc"}`)},
		{Payload: []byte(`{"note":"AKIAABCDEFGHIJKLMNOP"}`)},
		{Payload: []byte(`{"k":["-----BEGIN RSA PRIVATE KEY-----"]}`)},
		{Payload: []byte(`raw sk_live_abcdefghijklmnop`)},
		{Labels: map[string]string{"token": "x"}},
		{Chain: []workflow.Job{{Payload: []byte(`{"passwd":"x"}`)}}},
	}
	for _, j := range refuse {
		_, err := guard(context.Background(), j)
		if !errors.Is(err, workflow.ErrSecretInPayload) || !errors.Is(err, workflow.ErrInvalid) {
			t.Errorf("job %s %v: err = %v, want ErrSecretInPayload", j.Payload, j.Labels, err)
		}
		if err != nil && (strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "AKIAABCD")) {
			t.Errorf("error leaks the value: %v", err)
		}
	}
	pass := []workflow.Job{
		{Payload: []byte(`{"password":{"secret_ref":"db"}}`)},
		{Payload: []byte(`{"password":""}`)},
		{Payload: []byte(`{"order":42,"tokens_used":3}`)},
		{Labels: map[string]string{"tenant": "acme"}},
	}
	for _, j := range pass {
		if _, err := guard(context.Background(), j); err != nil {
			t.Errorf("job %s: err = %v, want accepted", j.Payload, err)
		}
	}
	allow := workflow.SecretGuard(workflow.NewSecretDetector(workflow.SecretRules{AllowKeys: []string{"page_token"}, AllowRules: []string{"jwt"}}))(next)
	for _, p := range []string{`{"page_token":"abc"}`, `{"link":"eyJhbGciOiJIUzI1.eyJzdWIiOiIxMjM0.SflKxwRJSMeKKF2QT4f"}`} {
		if _, err := allow(context.Background(), workflow.Job{Payload: []byte(p)}); err != nil {
			t.Errorf("allowed %s: err = %v", p, err)
		}
	}
	if _, err := guard(context.Background(), workflow.Job{Payload: []byte(`{"link":"eyJhbGciOiJIUzI1.eyJzdWIiOiIxMjM0.SflKxwRJSMeKKF2QT4f"}`)}); !errors.Is(err, workflow.ErrSecretInPayload) {
		t.Errorf("jwt not reported by default: %v", err)
	}
}

type mapResolver map[string]string

func (m mapResolver) ResolveSecret(_ context.Context, name string) ([]byte, error) {
	v, ok := m[name]
	if !ok {
		return nil, workflow.ErrNotFound
	}
	return []byte(v), nil
}

func TestResolveSecret(t *testing.T) {
	v, err := workflow.ResolveSecret(context.Background(), mapResolver{"db": "pw"}, workflow.SecretRef{Ref: "db"})
	if err != nil || string(v) != "pw" {
		t.Fatalf("resolve = %q, %v", v, err)
	}
	if _, err := workflow.ResolveSecret(context.Background(), mapResolver{}, workflow.SecretRef{}); !errors.Is(err, workflow.ErrNoSecretRef) || !workflow.IsPermanent(err) {
		t.Fatalf("empty ref = %v", err)
	}
}
