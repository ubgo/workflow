package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/workflow"
)

// DataStoreContract checks a workflow.DataStore (WX-01) against the
// contract every store keeps, so memory, SQL, Redis and an app's own
// store are held to one bar: Get of nothing; Put with match 0 creates
// version 1 and refuses a second create; a stale match is ErrDataConflict
// and writes nothing; AnyVersion writes over anything; an empty value is
// a value; scopes of different kinds or names never share an entry;
// Delete removes and is idempotent; and concurrent UpdateData increments
// through a and b lose no write.
//
// a and b are two handles on the same backing data (two processes); pass
// the same store twice for an in-process one. The store must start empty
// for the names used here (each test should use its own prefix or table).
func DataStoreContract(t *testing.T, a, b workflow.DataStore) {
	t.Helper()
	ctx := context.Background()
	f := workflow.Folder("acme")
	if _, _, ok, err := a.Get(ctx, f, "plan"); ok || err != nil {
		t.Fatalf("Get of nothing = %v, %v; want not found", ok, err)
	}
	v1, err := a.Put(ctx, f, "plan", []byte(`"free"`), 0)
	if err != nil || v1 <= 0 {
		t.Fatalf("create = %d, %v; want a positive version", v1, err)
	}
	if _, err := b.Put(ctx, f, "plan", []byte(`"x"`), 0); !errors.Is(err, workflow.ErrDataConflict) {
		t.Fatalf("second create = %v; want ErrDataConflict", err)
	}
	v2, err := b.Put(ctx, f, "plan", []byte(`"pro"`), v1)
	if err != nil || v2 <= v1 {
		t.Fatalf("update at the read version = %d, %v; want a version above %d", v2, err, v1)
	}
	if _, err := a.Put(ctx, f, "plan", []byte(`"stale"`), v1); !errors.Is(err, workflow.ErrDataConflict) {
		t.Fatalf("update at a stale version = %v; want ErrDataConflict", err)
	}
	if val, ver, ok, err := a.Get(ctx, f, "plan"); !ok || err != nil || string(val) != `"pro"` || ver != v2 {
		t.Fatalf("Get = %q, %d, %v, %v; want \"pro\" at %d (a refused write changed nothing; b's write is seen by a)", val, ver, ok, err, v2)
	}
	v3, err := a.Put(ctx, f, "plan", []byte(`"team"`), workflow.AnyVersion)
	if err != nil || v3 <= v2 {
		t.Fatalf("AnyVersion over an entry = %d, %v; want a version above %d", v3, err, v2)
	}
	if _, err := a.Put(ctx, f, "fresh", []byte(`1`), workflow.AnyVersion); err != nil {
		t.Fatalf("AnyVersion with no entry: %v", err)
	}
	if _, err := a.Put(ctx, f, "empty", nil, workflow.AnyVersion); err != nil {
		t.Fatal(err)
	}
	if val, _, ok, err := b.Get(ctx, f, "empty"); !ok || err != nil || len(val) != 0 {
		t.Fatalf("an empty value = %q, %v, %v; want found and empty", val, ok, err)
	}

	// Scopes are separate: same name under another folder, a workflow of
	// the same name, and the global scope.
	others := []workflow.DataScope{workflow.Folder("acme2"), workflow.Folder("acm"), workflow.WorkflowScope("acme"), workflow.GlobalScope()}
	for _, s := range others {
		if _, _, ok, _ := a.Get(ctx, s, "plan"); ok {
			t.Fatalf("%s sees folder acme's entry", s)
		}
	}
	for i, s := range others {
		if _, err := a.Put(ctx, s, "plan", fmt.Appendf(nil, "%d", i), 0); err != nil {
			t.Fatalf("create under %s: %v", s, err)
		}
	}
	for i, s := range others {
		if val, _, ok, _ := b.Get(ctx, s, "plan"); !ok || string(val) != fmt.Sprint(i) {
			t.Fatalf("%s = %q, %v; want %d", s, val, ok, i)
		}
	}
	// A name with the scope separator cannot reach another scope's entry.
	if _, _, ok, _ := a.Get(ctx, workflow.Folder("acme/plan"), ""); ok {
		t.Fatal("folder \"acme/plan\" with an empty name reached folder acme's plan")
	}

	if err := a.Delete(ctx, f, "plan"); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(ctx, f, "plan"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if _, _, ok, _ := b.Get(ctx, f, "plan"); ok {
		t.Fatal("entry still there after Delete")
	}
	if _, err := a.Put(ctx, f, "plan", []byte(`"again"`), 0); err != nil {
		t.Fatalf("create after Delete: %v", err)
	}
	if _, _, ok, _ := a.Get(ctx, workflow.Folder("acme2"), "plan"); !ok {
		t.Fatal("Delete of folder acme's entry removed folder acme2's")
	}

	// Concurrent read-modify-write through both handles loses nothing.
	const writers, each = 4, 10
	ca, cb := workflow.ContextWithData(ctx, a), workflow.ContextWithData(ctx, b)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func(c context.Context) {
			defer wg.Done()
			for range each {
				if _, err := workflow.UpdateData(c, workflow.GlobalScope(), "count", func(n int, _ bool) (int, error) { return n + 1, nil }); err != nil {
					errs <- err
					return
				}
			}
		}(map[bool]context.Context{true: ca, false: cb}[w%2 == 0])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("UpdateData: %v", err)
	}
	if n, ok, err := workflow.GetData[int](ca, workflow.GlobalScope(), "count"); !ok || err != nil || n != writers*each {
		t.Fatalf("count = %d, %v, %v; want %d (a concurrent write was lost)", n, ok, err, writers*each)
	}
}

// InboxContract checks a workflow.Inbox (MQ-15) against the contract
// every store keeps: one claim at a time per key; a processed key is done
// until its keep ends and claimable after; an expired lease is taken over
// and the old token can then neither complete nor release; Release frees
// a key; consumers and IDs never share entries; and of many concurrent
// claims exactly one wins. Times are given explicitly from now, never
// read from a clock.
//
// a and b are two handles on the same backing data (two processes); pass
// the same inbox twice for an in-process one. The inbox must start empty
// for the keys used here.
func InboxContract(t *testing.T, a, b workflow.Inbox) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	claim := func(in workflow.Inbox, k workflow.InboxKey, lease time.Duration, at time.Time) (workflow.InboxState, string) {
		t.Helper()
		st, tok, err := in.Claim(ctx, k, lease, at)
		if err != nil {
			t.Fatalf("Claim %v: %v", k, err)
		}
		if st == workflow.InboxClaimed && tok == "" {
			t.Fatalf("Claim %v: claimed with an empty token", k)
		}
		return st, tok
	}
	k := workflow.InboxKey{Consumer: "mailer", ID: "m-1"}
	st, t1 := claim(a, k, time.Minute, now)
	if st != workflow.InboxClaimed {
		t.Fatalf("first claim = %v; want claimed", st)
	}
	if st, _ := claim(b, k, time.Minute, now.Add(30*time.Second)); st != workflow.InboxBusy {
		t.Fatalf("claim while held = %v; want busy", st)
	}
	if err := b.Release(ctx, k, "not-the-token"); err != nil {
		t.Fatalf("Release with another token: %v", err)
	}
	if st, _ := claim(b, k, time.Minute, now.Add(30*time.Second)); st != workflow.InboxBusy {
		t.Fatalf("Release with another token freed the key: %v", st)
	}
	if err := b.Complete(ctx, k, "not-the-token", time.Hour, now); !errors.Is(err, workflow.ErrInboxLost) {
		t.Fatalf("Complete with another token = %v; want ErrInboxLost", err)
	}
	if err := a.Complete(ctx, k, t1, time.Hour, now.Add(10*time.Second)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if st, _ := claim(b, k, time.Minute, now.Add(30*time.Minute)); st != workflow.InboxDone {
		t.Fatalf("claim of a processed key = %v; want done", st)
	}
	if err := a.Release(ctx, k, t1); err != nil {
		t.Fatalf("Release of a completed key: %v", err)
	}
	if st, _ := claim(b, k, time.Minute, now.Add(30*time.Minute)); st != workflow.InboxDone {
		t.Fatalf("Release undid a completion: %v", st)
	}
	if st, _ := claim(b, k, time.Minute, now.Add(2*time.Hour)); st != workflow.InboxClaimed {
		t.Fatalf("claim after keep ended = %v; want claimed", st)
	}

	// An expired lease is taken over; the old token is fenced off.
	k2 := workflow.InboxKey{Consumer: "mailer", ID: "m-2"}
	_, old := claim(a, k2, time.Minute, now)
	st, cur := claim(b, k2, time.Minute, now.Add(2*time.Minute))
	if st != workflow.InboxClaimed || cur == old {
		t.Fatalf("claim after the lease ended = %v (token %q, old %q); want claimed with a new token", st, cur, old)
	}
	if err := a.Complete(ctx, k2, old, time.Hour, now.Add(2*time.Minute)); !errors.Is(err, workflow.ErrInboxLost) {
		t.Fatalf("Complete with the expired claim's token = %v; want ErrInboxLost", err)
	}
	if err := a.Release(ctx, k2, old); err != nil {
		t.Fatal(err)
	}
	if st, _ := claim(a, k2, time.Minute, now.Add(150*time.Second)); st != workflow.InboxBusy {
		t.Fatalf("the expired claim's Release freed the new claim: %v", st)
	}
	if err := b.Release(ctx, k2, cur); err != nil {
		t.Fatal(err)
	}
	if st, _ := claim(a, k2, time.Minute, now.Add(150*time.Second)); st != workflow.InboxClaimed {
		t.Fatalf("claim after Release = %v; want claimed", st)
	}

	// Consumers and IDs are separate, and the key form is unambiguous.
	for _, other := range []workflow.InboxKey{{Consumer: "indexer", ID: "m-1"}, {Consumer: "mailer", ID: "m-10"}, {Consumer: "m", ID: "ailer/m-1"}} {
		if st, _ := claim(a, other, time.Minute, now.Add(30*time.Minute)); st != workflow.InboxClaimed {
			t.Fatalf("%v shares an entry with %v: %v", other, k, st)
		}
	}
	if _, _, err := a.Claim(ctx, workflow.InboxKey{Consumer: "mailer"}, time.Minute, now); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatalf("Claim without an ID = %v; want ErrInvalid", err)
	}

	// Of many concurrent claims of one key exactly one wins.
	race := workflow.InboxKey{Consumer: "race", ID: "r-1"}
	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for i := range 8 {
		wg.Add(1)
		go func(in workflow.Inbox) {
			defer wg.Done()
			st, _, err := in.Claim(ctx, race, time.Minute, now)
			if err != nil {
				t.Errorf("concurrent Claim: %v", err)
				return
			}
			if st == workflow.InboxClaimed {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}(map[bool]workflow.Inbox{true: a, false: b}[i%2 == 0])
	}
	wg.Wait()
	if won != 1 {
		t.Fatalf("%d concurrent claims won; want exactly 1", won)
	}
}
