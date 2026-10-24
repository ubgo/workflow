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

// TestGroupChunks pins how RunGroup splits a group: one workflow per
// chunk, workflows in first-appearance order, job order kept, and the run
// and byte limits never exceeded except by a single oversized job.
func TestGroupChunks(t *testing.T) {
	small := preparedRun{input: map[string]any{"a": "b"}, md: map[string]string{}}
	big := preparedRun{input: map[string]any{"a": strings.Repeat("x", DefaultGroupChunkBytes)}, md: map[string]string{}}

	t.Run("by workflow in first-appearance order", func(t *testing.T) {
		jobs := []workflow.Job{{Workflow: "b"}, {Workflow: "a"}, {Workflow: "b"}, {Workflow: "a"}}
		runs := []preparedRun{small, small, small, small}
		got := groupChunks(jobs, runs, DefaultGroupChunkRuns, DefaultGroupChunkBytes)
		want := [][]int{{0, 2}, {1, 3}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chunks = %v, want %v", got, want)
		}
	})

	t.Run("run limit", func(t *testing.T) {
		n := 2*DefaultGroupChunkRuns + 1
		jobs := make([]workflow.Job, n)
		runs := make([]preparedRun, n)
		for i := range jobs {
			jobs[i], runs[i] = workflow.Job{Workflow: "w"}, small
		}
		got := groupChunks(jobs, runs, DefaultGroupChunkRuns, DefaultGroupChunkBytes)
		if len(got) != 3 || len(got[0]) != DefaultGroupChunkRuns || len(got[1]) != DefaultGroupChunkRuns || len(got[2]) != 1 {
			t.Fatalf("chunk sizes = %d chunks, first %d; want 1000, 1000, 1", len(got), len(got[0]))
		}
		next := 0
		for _, c := range got {
			for _, i := range c {
				if i != next {
					t.Fatalf("job order broken: got index %d, want %d", i, next)
				}
				next++
			}
		}
	})

	t.Run("byte limit", func(t *testing.T) {
		jobs := []workflow.Job{{Workflow: "w"}, {Workflow: "w"}, {Workflow: "w"}}
		runs := []preparedRun{small, big, small}
		got := groupChunks(jobs, runs, DefaultGroupChunkRuns, DefaultGroupChunkBytes)
		want := [][]int{{0}, {1}, {2}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chunks = %v, want the oversized job alone: %v", got, want)
		}
	})
}

// TestBatchSourceRefuses pins BatchSource's Attach refusals, which need no
// engine: an interval that is not positive and a missing Store.
func TestBatchSourceRefuses(t *testing.T) {
	c := New(Config{})
	m := batch.New(c, batch.NewMemoryStore())
	for name, src := range map[string]batch.Source{
		"zero interval": c.BatchSource(batch.NewMemoryStore(), 0, nil),
		"nil store":     c.BatchSource(nil, time.Second, nil),
	} {
		if err := src.Attach(context.Background(), m); !errors.Is(err, workflow.ErrInvalid) {
			t.Fatalf("%s: Attach = %v, want ErrInvalid", name, err)
		}
	}
}
