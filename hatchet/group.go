package hatchet

import (
	"context"
	"encoding/json"
	"fmt"

	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// Chunk limits of RunGroup. The SDK splits a bulk trigger itself (1000
// runs, 3 MiB under the engine's default 4 MiB gRPC message), but when one
// of its chunks fails it returns the IDs of the others without their
// positions, so a failed trigger could not be mapped back to its jobs.
// RunGroup therefore makes chunks the SDK never splits again: at most
// Config.GroupChunkRuns runs, and at most Config.GroupChunkBytes of JSON
// input and metadata, which by default stays under the SDK's proto-size
// limit with room for framing. A single larger job goes alone, as in the
// SDK.
const (
	// DefaultGroupChunkRuns is the default and the largest
	// Config.GroupChunkRuns: the SDK's own bulk-trigger run limit.
	DefaultGroupChunkRuns = 1000
	// DefaultGroupChunkBytes is the default and the largest
	// Config.GroupChunkBytes: 2 MiB of JSON stays under the SDK's 3 MiB
	// protobuf split and the engine's default 4 MiB gRPC message.
	DefaultGroupChunkBytes = 2 << 20
)

var _ batch.GroupRunner = (*Client)(nil)

// RunGroup implements batch.GroupRunner as Hatchet bulk triggers: one
// RunMany call per chunk of jobs of the same workflow, in the order each
// workflow first appears, so a batch of N jobs costs about N/1000 calls
// instead of N. Each run carries exactly what Enqueue stamps (input
// envelope, labels, idempotency metadata, priority), so Status, Cancel and
// the run-finished hook treat it like an enqueued run; group itself is not
// stamped, because the jobs' labels already name their batch.
//
// Every job is checked before anything starts, and a refusal returns nil
// refs with Enqueue's error for that job. When a chunk fails, the chunks
// already started keep their refs, the rest are zero, and the error is
// returned, so the caller can cancel what started.
func (c *Client) RunGroup(ctx context.Context, group string, jobs []workflow.Job) ([]workflow.RunRef, error) {
	runs := make([]preparedRun, len(jobs))
	for i, job := range jobs {
		if !job.RunAt.IsZero() || job.IdempotencyKey != "" {
			return nil, workflow.Unsupported("Job.RunAt/Job.IdempotencyKey", "group "+group+" job "+fmt.Sprint(i)+" needs Enqueue")
		}
		if err := workflow.CheckJobLabels(ctx, job); err != nil {
			return nil, fmt.Errorf("hatchet driver: group %s job %d: %w", group, i, err)
		}
		run, err := c.prepare(job)
		if err != nil {
			return nil, fmt.Errorf("hatchet driver: group %s job %d: %w", group, i, err)
		}
		runs[i] = run
	}
	refs := make([]workflow.RunRef, len(jobs))
	for _, chunk := range groupChunks(jobs, runs, c.cfg.GroupChunkRuns, c.cfg.GroupChunkBytes) {
		inputs := make([]hatchetsdk.RunManyOpt, len(chunk))
		for k, i := range chunk {
			inputs[k] = hatchetsdk.RunManyOpt{Input: runs[i].input, Opts: runs[i].runOptions()}
		}
		got, err := runs[chunk[0]].hw.RunMany(ctx, inputs)
		if err == nil && len(got) != len(chunk) {
			err = fmt.Errorf("engine returned %d runs for %d", len(got), len(chunk))
		}
		if err != nil {
			return refs, engineErr(err, "group %s: run %d jobs of %q", group, len(chunk), jobs[chunk[0]].Workflow)
		}
		for k, i := range chunk {
			refs[i] = workflow.RunRef{RunID: workflow.RunID(got[k].RunId)}
		}
		if c.afterChunk != nil {
			c.afterChunk()
		}
	}
	return refs, nil
}

// groupChunks splits the job indexes into bulk-trigger chunks (see
// chunkRuns).
func groupChunks(jobs []workflow.Job, runs []preparedRun, maxRuns, maxBytes int) [][]int {
	names := make([]string, len(jobs))
	sizes := make([]int, len(jobs))
	for i, j := range jobs {
		names[i], sizes[i] = j.Workflow, runSize(runs[i])
	}
	return chunkRuns(names, sizes, maxRuns, maxBytes)
}

// chunkRuns splits run indexes, given each run's workflow name and size,
// into bulk-trigger chunks: runs of one workflow only, in index order
// within a workflow, workflows in the order they first appear, each chunk
// within maxRuns runs and maxBytes bytes. RunGroup and the batch
// coordinator share it.
func chunkRuns(names []string, sizes []int, maxRuns, maxBytes int) [][]int {
	var order []string
	byWorkflow := map[string][]int{}
	for i, name := range names {
		if _, ok := byWorkflow[name]; !ok {
			order = append(order, name)
		}
		byWorkflow[name] = append(byWorkflow[name], i)
	}
	var chunks [][]int
	for _, name := range order {
		var cur []int
		size := 0
		for _, i := range byWorkflow[name] {
			n := sizes[i]
			if len(cur) > 0 && (len(cur) == maxRuns || size+n > maxBytes) {
				chunks = append(chunks, cur)
				cur, size = nil, 0
			}
			cur = append(cur, i)
			size += n
		}
		if len(cur) > 0 {
			chunks = append(chunks, cur)
		}
	}
	return chunks
}

// runSize estimates a run's share of a trigger message: its JSON input
// and metadata, which is what dominates the proto size.
func runSize(r preparedRun) int {
	in, _ := json.Marshal(r.input) // a map of JSON-safe values; Enqueue marshals the same
	md, _ := json.Marshal(r.md)
	return len(in) + len(md)
}
