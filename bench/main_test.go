package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestMemorySmoke runs the whole harness on the memory driver with a tiny
// workload, so a broken phase fails here instead of in a long run.
func TestMemorySmoke(t *testing.T) {
	var out bytes.Buffer
	err := mainErr(context.Background(), []string{
		"-systems", "memory", "-jobs", "200", "-concurrency", "4", "-warmup", "10",
		"-latency-samples", "20", "-latency-gap", "1ms", "-latency-gap-span", "2ms",
		"-timeout", "30s", "-max-load", "1000",
	}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "| memory | ") || strings.Contains(out.String(), "failed") {
		t.Fatalf("no memory result row:\n%s", out.String())
	}
}

func TestEncodePadsToSize(t *testing.T) {
	b, err := encode(message{T: 1, K: kindTput, N: 2}, 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 256 {
		t.Fatalf("payload is %d bytes, want 256", len(b))
	}
}

func TestPercentileNearestRank(t *testing.T) {
	s := make([]time.Duration, 100)
	for i := range s {
		s[i] = time.Duration(i + 1)
	}
	for p, want := range map[int]time.Duration{50: 50, 95: 95, 99: 99, 100: 100} {
		if got := percentile(s, p); got != want {
			t.Errorf("p%d = %d, want %d", p, got, want)
		}
	}
	if got := percentile(s[:1], 99); got != 1 {
		t.Errorf("p99 of one sample = %d, want 1", got)
	}
}

func TestUnknownSystemRefused(t *testing.T) {
	var out bytes.Buffer
	if err := mainErr(context.Background(), []string{"-systems", "nope"}, &out); err == nil {
		t.Fatal("unknown system accepted")
	}
}
