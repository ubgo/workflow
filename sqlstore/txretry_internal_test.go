package sqlstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTxRetryBackoff pins the retry wait: positive, never above attempt *
// txRetryStep, so it grows with the attempt but stays far under any lease.
func TestTxRetryBackoff(t *testing.T) {
	for attempt := 1; attempt <= 5; attempt++ {
		limit := time.Duration(attempt) * txRetryStep
		for range 200 {
			if d := txRetryBackoff(attempt); d <= 0 || d > limit {
				t.Fatalf("txRetryBackoff(%d) = %v, want in (0, %v]", attempt, d, limit)
			}
		}
	}
}

// TestSleepCtxReturnsWhenCancelled: a cancelled context ends the wait at
// once, so a retry never outlives its caller.
func TestSleepCtxReturnsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	sleepCtx(ctx, time.Minute)
	if time.Since(start) > time.Second {
		t.Fatal("sleepCtx did not return on a cancelled context")
	}
}

// TestStoreRetry: an operation that keeps losing the lock race is tried
// txAttempts times and then returns its error; one that succeeds on a later
// attempt returns nil; a non-retryable error is returned at once.
func TestStoreRetry(t *testing.T) {
	lockErr, hardErr := errors.New("lock race"), errors.New("hard")
	s := &store{txAttempts: 3, classify: func(err error) ErrorClass {
		return ErrorClass{Retryable: errors.Is(err, lockErr)}
	}}
	for _, tc := range []struct {
		name      string
		failFirst int
		err       error
		wantCalls int
		wantErr   error
	}{
		{"succeeds on the third try", 2, lockErr, 3, nil},
		{"gives up after the attempts", 99, lockErr, 3, lockErr},
		{"a hard error is not retried", 99, hardErr, 1, hardErr},
		{"no error runs once", 0, lockErr, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := s.retry(context.Background(), func() error {
				calls++
				if calls <= tc.failFirst {
					return tc.err
				}
				return nil
			})
			if calls != tc.wantCalls || !errors.Is(err, tc.wantErr) {
				t.Fatalf("calls = %d, err = %v; want %d, %v", calls, err, tc.wantCalls, tc.wantErr)
			}
		})
	}
}
