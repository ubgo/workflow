package workflowtest

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ubgo/workflow/middleware"
)

// DisableStoreContract checks a middleware.DisableStore (WH-02) against
// the contract every store keeps, MemoryDisable's semantics: a key never
// seen is enabled; Fail switches a key off on the failure that brings its
// count to after and reports newly for that call only; later Fails and a
// Succeed leave a disabled key disabled; Succeed resets the count; Enable
// re-enables and resets the count; Succeed and Enable of an unknown key
// change nothing; keys never share state; each Fail compares its own
// after; and of many concurrent Fails crossing after, exactly one reports
// newly (run it under -race).
//
// a and b are two handles on the same backing data (two processes); pass
// the same store twice for an in-process one. The store must start empty
// for the keys used here.
//
//ds:def id=workflowtest-disablestorecontract-jzvzhkee owner=@khanakia stability=stable
func DisableStoreContract(t testing.TB, a, b middleware.DisableStore) {
	t.Helper()
	ctx := context.Background()
	disabled := func(s middleware.DisableStore, key string) bool {
		t.Helper()
		off, err := s.Disabled(ctx, key)
		if err != nil {
			t.Fatalf("Disabled(%q): %v", key, err)
		}
		return off
	}
	fail := func(s middleware.DisableStore, key string, after int) bool {
		t.Helper()
		newly, err := s.Fail(ctx, key, after)
		if err != nil {
			t.Fatalf("Fail(%q, %d): %v", key, after, err)
		}
		return newly
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	if disabled(a, "hook") {
		t.Fatal("a key never seen is disabled")
	}
	// Unknown keys: Succeed and Enable change nothing.
	must(a.Succeed(ctx, "ghost"))
	must(a.Enable(ctx, "ghost"))
	if disabled(b, "ghost") {
		t.Fatal("Succeed or Enable of an unknown key disabled it")
	}

	// Three failures through both handles: the third switches it off.
	if fail(a, "hook", 3) || fail(b, "hook", 3) {
		t.Fatal("Fail below after reported newly")
	}
	if disabled(a, "hook") {
		t.Fatal("disabled after two of three failures")
	}
	if !fail(a, "hook", 3) {
		t.Fatal("the failure that reached after did not report newly")
	}
	if !disabled(b, "hook") {
		t.Fatal("the other handle does not see the key disabled")
	}
	if fail(b, "hook", 3) || fail(a, "hook", 3) {
		t.Fatal("a Fail on a disabled key reported newly again")
	}
	must(b.Succeed(ctx, "hook"))
	if !disabled(a, "hook") {
		t.Fatal("Succeed re-enabled a disabled key; only Enable does")
	}
	if fail(a, "hook", 1) {
		t.Fatal("Fail after a Succeed on a disabled key reported newly")
	}
	if disabled(a, "other") {
		t.Fatal("another key shares the disabled state")
	}

	// Enable re-enables and resets the count.
	must(b.Enable(ctx, "hook"))
	if disabled(a, "hook") {
		t.Fatal("still disabled after Enable")
	}
	if fail(a, "hook", 3) || fail(b, "hook", 3) {
		t.Fatal("Enable did not reset the count")
	}
	if !fail(b, "hook", 3) {
		t.Fatal("after Enable, the third failure did not switch the key off")
	}

	// Succeed resets the count of an enabled key.
	if fail(a, "flaky", 3) || fail(b, "flaky", 3) {
		t.Fatal("Fail below after reported newly")
	}
	must(a.Succeed(ctx, "flaky"))
	if fail(b, "flaky", 3) || fail(a, "flaky", 3) {
		t.Fatal("Succeed did not reset the count")
	}
	if disabled(a, "flaky") {
		t.Fatal("disabled after a reset and two failures")
	}
	if !fail(a, "flaky", 3) {
		t.Fatal("the third failure after a reset did not switch the key off")
	}

	// after of one trips on the first failure; each Fail compares its own
	// after against the count so far.
	if !fail(a, "once", 1) {
		t.Fatal("after 1: the first failure did not switch the key off")
	}
	for range 3 {
		if fail(a, "mixed", 10) {
			t.Fatal("Fail below after reported newly")
		}
	}
	if !fail(b, "mixed", 4) {
		t.Fatal("a fourth failure with after 4 did not switch the key off")
	}

	// Keys are compared exactly: a prefix, a longer key or another case is
	// another key, and a long key works.
	long := "https://hooks.example.com/" + strings.Repeat("segment/", 64)
	for _, k := range []string{"hoo", "hook2", "HOOK", long} {
		if disabled(a, k) {
			t.Fatalf("key %.40q shares hook's state", k)
		}
	}
	if !fail(a, long, 1) || !disabled(b, long) {
		t.Fatal("a long key was not switched off")
	}

	// Of 50 concurrent Fails with after 10, exactly one reports newly.
	const n, after = 50, 10
	var newly atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		s := a
		if i%2 == 1 {
			s = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.Fail(ctx, "race", after)
			if err != nil {
				errs <- err
				return
			}
			if ok {
				newly.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Fail: %v", err)
	}
	if got := newly.Load(); got != 1 {
		t.Fatalf("%d of %d concurrent Fails crossing after %d reported newly, want exactly 1", got, n, after)
	}
	if !disabled(b, "race") {
		t.Fatal("not disabled after the concurrent Fails")
	}
}
