package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/ubgo/workflow/clock"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fired(t clock.Timer) (time.Time, bool) {
	select {
	case at := <-t.C():
		return at, true
	default:
		return time.Time{}, false
	}
}

func TestFakeAdvanceFiresDueTimersInOrder(t *testing.T) {
	f := clock.NewFake(start)
	late := f.NewTimer(3 * time.Hour)
	early := f.NewTimer(time.Hour)

	f.Advance(59 * time.Minute)
	if _, ok := fired(early); ok {
		t.Fatal("timer fired early")
	}
	f.Advance(time.Minute)
	if at, ok := fired(early); !ok || !at.Equal(start.Add(time.Hour)) {
		t.Fatalf("early timer: %v %v", at, ok)
	}
	if f.Pending() != 1 {
		t.Fatalf("Pending = %d", f.Pending())
	}
	f.Advance(72 * time.Hour)
	if at, ok := fired(late); !ok || !at.Equal(start.Add(3*time.Hour)) {
		t.Fatalf("late timer: %v %v", at, ok)
	}
	if !f.Now().Equal(start.Add(73 * time.Hour)) {
		t.Fatalf("Now = %v", f.Now())
	}
}

func TestFakeStopAndImmediate(t *testing.T) {
	f := clock.NewFake(start)
	tm := f.NewTimer(time.Minute)
	if !tm.Stop() || tm.Stop() {
		t.Fatal("Stop should report true once")
	}
	f.Advance(time.Hour)
	if _, ok := fired(tm); ok {
		t.Fatal("stopped timer fired")
	}
	if _, ok := fired(f.NewTimer(0)); !ok {
		t.Fatal("zero timer did not fire at once")
	}
	f.Advance(-time.Hour)
	f.Set(start)
	if !f.Now().Equal(start.Add(time.Hour)) {
		t.Fatal("clock went back")
	}
}

func TestFakeBlockUntil(t *testing.T) {
	f := clock.NewFake(start)
	done := make(chan time.Time)
	go func() { done <- <-f.NewTimer(time.Second).C() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.Advance(time.Second)
	if at := <-done; !at.Equal(start.Add(time.Second)) {
		t.Fatalf("fired at %v", at)
	}

	short, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	if err := f.BlockUntil(short, 1); err == nil {
		t.Fatal("BlockUntil returned with no timer pending")
	}
}

func TestRealClock(t *testing.T) {
	var c clock.Clock = clock.Real{}
	if c.Now().IsZero() {
		t.Fatal("zero time")
	}
	<-c.NewTimer(time.Millisecond).C()
}
