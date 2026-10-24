package workflowtest

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// FakeBatch is one batch as a test double saw it: the recorded jobs that
// carry the same batch.LabelBatch, in index order (batch.LabelIndex), so
// jobs a batch.Manager added later with Add are included. The batch's
// name and callbacks live in the Manager's Store, not on the jobs; read
// them with Manager.Progress(ctx, ID) when a test needs them.
type FakeBatch struct {
	ID   batch.ID
	Jobs []workflow.Job
}

// BatchMatcher narrows a batch assertion. Build it with BatchSize,
// BatchHasJob or BatchWhere; its description appears in failure messages.
type BatchMatcher struct {
	desc  string
	match func(FakeBatch) bool
}

// String is the matcher's description.
func (m BatchMatcher) String() string { return m.desc }

// BatchWhere matches batches for which fn is true; desc names the
// condition in failure messages.
func BatchWhere(desc string, fn func(FakeBatch) bool) BatchMatcher {
	return BatchMatcher{desc: desc, match: fn}
}

// BatchSize matches a batch of exactly n jobs.
func BatchSize(n int) BatchMatcher {
	return BatchWhere(strconv.Itoa(n)+" jobs", func(b FakeBatch) bool { return len(b.Jobs) == n })
}

// BatchHasJob matches a batch with at least one job of workflow wf
// matching every m.
func BatchHasJob(wf string, m ...JobMatcher) BatchMatcher {
	return BatchWhere("a job of "+strconv.Quote(wf)+describe(m), func(b FakeBatch) bool {
		return slices.ContainsFunc(b.Jobs, func(j workflow.Job) bool { return j.Workflow == wf && matchAll(j, m) })
	})
}

// batchesOf groups jobs by their batch label, in the order each batch's
// first job was recorded, each batch's jobs in index order.
func batchesOf(jobs []workflow.Job) []FakeBatch {
	var out []FakeBatch
	at := map[batch.ID]int{}
	for _, j := range jobs {
		id := batch.ID(j.Labels[batch.LabelBatch])
		if id == "" {
			continue
		}
		i, ok := at[id]
		if !ok {
			i = len(out)
			at[id] = i
			out = append(out, FakeBatch{ID: id})
		}
		out[i].Jobs = append(out[i].Jobs, j)
	}
	for i := range out {
		slices.SortStableFunc(out[i].Jobs, func(a, b workflow.Job) int { return batchIndex(a) - batchIndex(b) })
	}
	return out
}

// batchIndex is a job's batch.LabelIndex; -1 when missing or malformed.
func batchIndex(j workflow.Job) int {
	n, err := strconv.Atoi(j.Labels[batch.LabelIndex])
	if err != nil {
		return -1
	}
	return n
}

// assertBatched fails t unless a batch in bs matches every m.
func assertBatched(t testing.TB, bs []FakeBatch, ms []BatchMatcher) {
	t.Helper()
	for _, b := range bs {
		if matchBatch(b, ms) {
			return
		}
	}
	t.Errorf("no batch%s was dispatched; %s", describeBatch(ms), listBatches(bs))
}

func matchBatch(b FakeBatch, ms []BatchMatcher) bool {
	for _, m := range ms {
		if !m.match(b) {
			return false
		}
	}
	return true
}

func describeBatch(ms []BatchMatcher) string {
	if len(ms) == 0 {
		return ""
	}
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.desc
	}
	return " with " + strings.Join(parts, ", ")
}

// listBatches describes bs for a failure message.
func listBatches(bs []FakeBatch) string {
	if len(bs) == 0 {
		return "no batch was dispatched"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d batches:", len(bs))
	for _, fb := range bs {
		counts := map[string]int{}
		var names []string
		for _, j := range fb.Jobs {
			if counts[j.Workflow] == 0 {
				names = append(names, j.Workflow)
			}
			counts[j.Workflow]++
		}
		parts := make([]string, len(names))
		for i, n := range names {
			parts[i] = fmt.Sprintf("%s x%d", n, counts[n])
		}
		fmt.Fprintf(&b, "\n  %s: %d jobs (%s)", fb.ID, len(fb.Jobs), strings.Join(parts, ", "))
	}
	return b.String()
}

// checkChain reports why wf's steps are not exactly the chain steps (each
// step after the one before it, the first after none), or "" when they
// are.
func checkChain(wf workflow.Workflow, steps []string) string {
	got := make([]string, len(wf.Steps))
	for i, s := range wf.Steps {
		got[i] = s.Name
	}
	if !slices.Equal(got, steps) {
		return fmt.Sprintf("its steps are %v, want %v", got, steps)
	}
	for i, s := range wf.Steps {
		var want []string
		if i > 0 {
			want = []string{steps[i-1]}
		}
		if !slices.Equal(s.After, want) {
			return fmt.Sprintf("step %q runs after %v, want %v", s.Name, s.After, want)
		}
	}
	return ""
}
