package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/workflow/examples/internal/capture"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/memory"
)

// The race test's sizes.
const (
	// racers is how many goroutines try to take one key at once.
	racers = 50
	// ownerLetters spreads the racers over owner names a to z.
	ownerLetters = 26
	// lockTTL is how long an acquired lock lives.
	lockTTL = time.Minute
	// pastExpiry is how far the clock moves so the lock has surely expired.
	pastExpiry = 2 * lockTTL
)

// TestRun runs the program and checks the lines the guides show.
func TestRun(t *testing.T) {
	out, err := capture.Stdout(func() error { return run(context.Background()) })
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{
		`no tenant: workflow: invalid spec: job for "invoice" has no "tenant" label`,
		`step invoice/send attempt 1 tenant=acme: ok`,
		`run run-? of invoice for tenant=acme: succeeded`,
		`second job for acme: workflow: duplicate idempotency key: unique key "acme/invoice" is held`,
	}
	if len(lines) != len(want) {
		t.Fatalf("output =\n%s\nwant %d lines", out, len(want))
	}
	for i, w := range want {
		prefix, suffix, _ := strings.Cut(w, "?")
		if !strings.HasPrefix(lines[i], prefix) || !strings.HasSuffix(lines[i], suffix) {
			t.Errorf("line %d = %q, want %q", i+1, lines[i], w)
		}
	}
}

// TestKVUniqueOneWinner checks the store's guarantee: of many enqueues
// racing for one key, exactly one takes it, and only its owner frees it.
func TestKVUniqueOneWinner(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	s := kvUnique{kv: newMemKV(clk), prefix: "u:"}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			ok, err := s.Acquire(ctx, "k", "owner-"+string(rune('a'+i%ownerLetters)), lockTTL)
			if err != nil {
				t.Error(err)
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d acquires won, want 1", wins.Load())
	}
	clk.Advance(pastExpiry)
	if ok, _ := s.Acquire(ctx, "k", "late", lockTTL); !ok {
		t.Fatal("an expired lock was not taken over")
	}
	_ = s.Release(ctx, "k", "someone-else")
	if ok, _ := s.Acquire(ctx, "k", "other", lockTTL); ok {
		t.Fatal("a release by another owner freed the lock")
	}
}

// TestUseRefusesDuplicateNames checks the refusal the plugins guide names.
func TestUseRefusesDuplicateNames(t *testing.T) {
	log := &lineLog{}
	p := &tenancy{log: log, unique: kvUnique{kv: newMemKV(clock.Real{})}}
	_, err := workflow.Use(memory.New(), p, p)
	if !errors.Is(err, workflow.ErrDuplicatePlugin) {
		t.Fatalf("err = %v, want ErrDuplicatePlugin", err)
	}
}
