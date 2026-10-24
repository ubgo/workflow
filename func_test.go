package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/memory"
)

type mailArg struct {
	To string `json:"to"`
}

func TestFuncWorkflowShape(t *testing.T) {
	retry := &workflow.RetryPolicy{Max: 3}
	f := workflow.NewFunc("send-mail", func(context.Context, mailArg) error { return nil },
		workflow.FuncQueue("mail"), workflow.FuncRetry(retry), workflow.FuncTimeout(time.Minute),
		workflow.FuncCatch(func(context.Context, mailArg) error { return nil }))
	wf := f.Workflow()
	if wf.Name != "send-mail" || wf.Queue != "mail" || len(wf.Steps) != 1 || wf.OnFailure == nil {
		t.Fatalf("Workflow() = %+v", wf)
	}
	s := wf.Steps[0]
	if s.Name != workflow.FuncStep || s.Retry != retry || s.Timeout != time.Minute || s.Run == nil {
		t.Fatalf("step = %+v", s)
	}
	if err := workflow.Validate(wf); err != nil {
		t.Fatal(err)
	}
}

func TestFuncNilFnFailsValidate(t *testing.T) {
	f := workflow.NewFunc[mailArg]("nil-fn", nil)
	if err := workflow.Validate(f.Workflow()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Validate(nil fn) = %v, want ErrInvalid", err)
	}
	if err := workflow.Validate(workflow.NewFunc("", func(context.Context, mailArg) error { return nil }).Workflow()); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Validate(no name) = %v, want ErrInvalid", err)
	}
}

func TestFuncStepDecodesAndRejectsBadInput(t *testing.T) {
	var got mailArg
	f := workflow.NewFunc("m", func(_ context.Context, in mailArg) error { got = in; return nil })
	run := f.Workflow().Steps[0].Run
	if _, err := run(context.Background(), []byte(`{"to":"a"}`)); err != nil || got.To != "a" {
		t.Fatalf("run = %v, arg %+v", err, got)
	}
	if _, err := run(context.Background(), []byte(`{`)); !workflow.IsPermanent(err) {
		t.Fatalf("bad input = %v, want Permanent", err)
	}
}

func TestFuncCatchGetsArgumentOrZero(t *testing.T) {
	var got []mailArg
	f := workflow.NewFunc("m", func(context.Context, mailArg) error { return nil },
		workflow.FuncCatch(func(_ context.Context, in mailArg) error { got = append(got, in); return nil }))
	h := f.Workflow().OnFailure
	if _, err := h(context.Background(), []byte(`{"to":"b"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := h(context.Background(), []byte(`not json`)); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].To != "b" || got[1].To != "" {
		t.Fatalf("catch saw %+v", got)
	}
	if workflow.NewFunc("m", func(context.Context, mailArg) error { return nil }, workflow.FuncCatch[mailArg](nil)).Workflow().OnFailure != nil {
		t.Fatal("FuncCatch(nil) set OnFailure")
	}
}

func TestFuncJobAndJob(t *testing.T) {
	f := workflow.NewFunc("m", func(context.Context, mailArg) error { return nil })
	job, err := f.Job(mailArg{To: "c"})
	if err != nil || job.Workflow != "m" || string(job.Payload) != `{"to":"c"}` {
		t.Fatalf("Job = %+v, %v", job, err)
	}
	byName, err := workflow.FuncJob("m", mailArg{To: "c"})
	if err != nil || byName.Workflow != job.Workflow || string(byName.Payload) != string(job.Payload) {
		t.Fatalf("FuncJob = %+v, %v; want %+v", byName, err, job)
	}
	if _, err := workflow.FuncJob("m", make(chan int)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("FuncJob(unencodable) = %v, want ErrInvalid", err)
	}
	if _, err := workflow.FuncJob("", 1); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("FuncJob(no name) = %v, want ErrInvalid", err)
	}
	ch := workflow.NewFunc("ch", func(context.Context, chan int) error { return nil })
	if _, err := ch.Job(make(chan int)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Job(unencodable) = %v, want ErrInvalid", err)
	}
}

func TestRegisterFuncWrapsError(t *testing.T) {
	c := memory.New()
	if err := workflow.RegisterFunc(c, workflow.NewFunc[mailArg]("bad", nil)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("RegisterFunc(nil fn) = %v, want ErrInvalid", err)
	}
	if err := workflow.RegisterFunc(c, workflow.NewFunc("ok", func(context.Context, mailArg) error { return nil })); err != nil {
		t.Fatal(err)
	}
}
