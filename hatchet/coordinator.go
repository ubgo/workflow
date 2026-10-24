package hatchet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	hatchetsdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/batch"
)

// The batch coordinator (Config.BatchCoordinator): one engine run per
// batch whose only task starts every job as a child run.
const (
	// coordinatorWorkflow is the engine name of the coordinator. Every
	// client with the coordinator on registers it, so any of their
	// workers can run any batch's coordinator; it triggers its children
	// by name, so the worker that runs it need not register them.
	coordinatorWorkflow = "ubgo-workflow-batch-coordinator"

	// coordinatorKeyTTL is how long the engine refuses a second
	// coordinator for one batch (TTL idempotency on the batch ID). A
	// resumed dispatch calls SpawnGroup again within the batch's dispatch
	// lease (minutes), so a day is ample; holding by status instead would
	// let a resume after the coordinator finished start a second one,
	// whose children the engine would not dedup against the first's.
	coordinatorKeyTTL = 24 * time.Hour
)

// DefaultBatchCoordinatorTimeout is the default
// Config.BatchCoordinatorTimeout: starting a thousand children is one bulk
// call, so a batch of tens of thousands of jobs starts well within it.
const DefaultBatchCoordinatorTimeout = 5 * time.Minute

// DefaultBatchCoordinatorMaxInput is the default
// Config.BatchCoordinatorMaxInput: one trigger's JSON under the engine's
// default 4 MiB gRPC message, with room for framing.
const DefaultBatchCoordinatorMaxInput = 2 << 20

// DefaultBatchCoordinatorRetry returns the default
// Config.BatchCoordinatorRetry, a fresh value each call: 10 retries at
// 2^n seconds capped at two minutes, which gives an engine outage about
// ten minutes before a coordinator fails. A retry starts the children
// again with the same child keys, and the engine returns the existing
// child for a key it has seen (checked live: same run IDs), so no job runs
// twice however many retries are allowed.
func DefaultBatchCoordinatorRetry() *workflow.RetryPolicy {
	return &workflow.RetryPolicy{Max: 10, Backoff: workflow.Backoff{Initial: 2 * time.Second, Factor: 2, Max: 2 * time.Minute}}
}

// coordinatorSettle is how long after its coordinator ended a batch waits
// before recording the jobs the engine never listed as cancelled: the run
// list lags behind triggers by up to lag (Config.VisibilityLag), and twice
// that absorbs a slow read side.
func coordinatorSettle(lag time.Duration) time.Duration { return 2 * lag }

// coordinatorRetry maps p onto the coordinator task's retry options. The
// coordinator's errors are engine errors, so a NoRetryOn filter has
// nothing meaningful to match and is refused rather than ignored.
func coordinatorRetry(p workflow.RetryPolicy) ([]hatchetsdk.TaskOption, error) {
	if p.Max < 0 {
		return nil, fmt.Errorf("%w: Max %d is negative", workflow.ErrInvalid, p.Max)
	}
	if p.NoRetryOn != nil {
		return nil, workflow.Unsupported("BatchCoordinatorRetry.NoRetryOn", "the coordinator retries engine errors only")
	}
	if err := checkRetry(p); err != nil {
		return nil, err
	}
	if p.Max == 0 {
		return nil, nil
	}
	opts := []hatchetsdk.TaskOption{hatchetsdk.WithRetries(p.Max)}
	if b, ok, _ := retryBackoff(p.Backoff); ok { // checked by checkRetry
		opts = append(opts, hatchetsdk.WithRetryBackoff(b.factor, b.maxSeconds))
	}
	return opts, nil
}

// coordinatorInput is what SpawnGroup sends the coordinator: every job as
// the prepared run Enqueue would have triggered.
type coordinatorInput struct {
	Batch string     `json:"batch"`
	Runs  []coordRun `json:"runs"`
}

// coordRun is one child: its workflow, input envelope, metadata (job
// labels, idempotency and workflow stamps) and engine priority.
type coordRun struct {
	Workflow string            `json:"workflow"`
	Input    map[string]any    `json:"input"` // the driver's input envelope (encodeInput), the same map Enqueue sends; JSON in, JSON out
	Meta     map[string]string `json:"meta"`
	Priority *int              `json:"priority,omitempty"`
}

// coordinatorOutput is the coordinator run's output, shown in Hatchet's
// dashboard; nothing reads it back.
type coordinatorOutput struct {
	Children int `json:"children"`
}

var _ batch.GroupSpawner = (*Client)(nil)

// SpawnsGroups implements batch.GroupSpawner: true when
// Config.BatchCoordinator is set.
func (c *Client) SpawnsGroups() bool { return c.cfg.BatchCoordinator }

// SpawnGroup implements batch.GroupSpawner: it triggers the coordinator
// for group with every job prepared exactly as Enqueue would, so each
// child carries the same input envelope, labels, idempotency and workflow
// stamps as an enqueued run, and Status, the run-finished hook and
// BatchSource treat it as one. The trigger is idempotent per group for
// coordinatorKeyTTL: a second call returns the first coordinator.
//
// It returns an error wrapping workflow.ErrUnsupported, before anything
// starts, when the jobs' input and metadata exceed
// Config.BatchCoordinatorMaxInput (the
// coordinator's own input would near the engine's message limit), so
// Dispatch starts them as bulk triggers instead; and workflow.ErrInvalid
// when Config.BatchCoordinator is off, because no worker of this client
// runs the coordinator then.
func (c *Client) SpawnGroup(ctx context.Context, group string, jobs []workflow.Job) (workflow.RunRef, error) {
	if !c.cfg.BatchCoordinator {
		return workflow.RunRef{}, fmt.Errorf("%w: hatchet driver: Config.BatchCoordinator is off", workflow.ErrInvalid)
	}
	in := coordinatorInput{Batch: group, Runs: make([]coordRun, len(jobs))}
	size := 0
	for i, job := range jobs {
		if !job.RunAt.IsZero() || job.IdempotencyKey != "" {
			return workflow.RunRef{}, workflow.Unsupported("Job.RunAt/Job.IdempotencyKey", "group "+group+" job "+strconv.Itoa(i)+" needs Enqueue")
		}
		if err := workflow.CheckJobLabels(ctx, job); err != nil {
			return workflow.RunRef{}, fmt.Errorf("hatchet driver: group %s job %d: %w", group, i, err)
		}
		run, err := c.prepare(job)
		if err != nil {
			return workflow.RunRef{}, fmt.Errorf("hatchet driver: group %s job %d: %w", group, i, err)
		}
		size += runSize(run)
		cr := coordRun{Workflow: job.Workflow, Input: run.input, Meta: run.md}
		if run.priority != nil {
			p := int(*run.priority)
			cr.Priority = &p
		}
		in.Runs[i] = cr
	}
	if size > c.cfg.BatchCoordinatorMaxInput {
		return workflow.RunRef{}, workflow.Unsupported("batch coordinator", fmt.Sprintf("group %s carries %d bytes of input, over the coordinator's %d", group, size, c.cfg.BatchCoordinatorMaxInput))
	}
	c.mu.Lock()
	hw := c.coordinator
	c.mu.Unlock()
	if hw == nil {
		return workflow.RunRef{}, workflow.ErrNotStarted
	}
	ref, err := hw.RunNoWait(ctx, in, hatchetsdk.WithRunMetadata(map[string]string{batch.LabelBatch: group}))
	if collision, ok := hatchetsdk.IsIdempotencyCollisionError(err); ok {
		return workflow.RunRef{RunID: workflow.RunID(collision.ExistingRunExternalId)}, nil
	}
	if err != nil {
		return workflow.RunRef{}, engineErr(err, "start batch coordinator for %s", group)
	}
	return workflow.RunRef{RunID: workflow.RunID(ref.RunId)}, nil
}

// CancelGroup implements batch.GroupSpawner: it cancels the parent, when
// known, then every run labelled with group created since since, in one
// bulk cancel by metadata. The engine does not cancel children with their
// parent (checked live on hatchet-lite v0.110.5, for a running and for a
// completed parent), so the label is what reaches them.
func (c *Client) CancelGroup(ctx context.Context, group string, parent workflow.RunRef, since time.Time) error {
	sdk, err := c.ready()
	if err != nil {
		return err
	}
	var errs []error
	if !parent.IsZero() {
		errs = append(errs, c.Cancel(ctx, parent))
	}
	_, err = sdk.Runs().Cancel(ctx, rest.V1CancelTaskRequest{Filter: &rest.V1TaskFilter{
		Since:              since.Add(-batchListLag),
		AdditionalMetadata: &[]string{batch.LabelBatch + ":" + group},
	}})
	if err != nil {
		errs = append(errs, engineErr(err, "cancel group %s", group))
	}
	return errors.Join(errs...)
}

// declareCoordinator builds the coordinator workflow: TTL idempotency on
// the batch label, one task with retries that starts the children.
// Start has already accepted Config.BatchCoordinatorRetry (checkConfig).
func (c *Client) declareCoordinator() (*hatchetsdk.Workflow, error) {
	hw := c.sdk.NewWorkflow(coordinatorWorkflow, hatchetsdk.WithWorkflowIdempotency(hatchetsdk.IdempotencyConfig{
		Expression: coordinatorIdempotencyExpression(),
		Method:     hatchetsdk.IdempotencyMethodTTL,
		TTL:        coordinatorKeyTTL,
	}))
	opts, err := coordinatorRetry(*c.cfg.BatchCoordinatorRetry)
	if err != nil {
		return nil, fmt.Errorf("hatchet driver: BatchCoordinatorRetry: %w", err)
	}
	opts = append(opts, hatchetsdk.WithExecutionTimeout(c.cfg.BatchCoordinatorTimeout), hatchetsdk.WithScheduleTimeout(c.cfg.ScheduleTimeout))
	hw.NewTask("spawn", c.coordinate, opts...)
	return hw, nil
}

// coordinatorIdempotencyExpression is the CEL key of a coordinator
// trigger: the coordinator's name and the batch ID, which SpawnGroup puts
// in the run's metadata under batch.LabelBatch.
func coordinatorIdempotencyExpression() string {
	return strconv.Quote(coordinatorWorkflow+idemKeySep) + " + string(additional_metadata[" + strconv.Quote(batch.LabelBatch) + "])"
}

// coordinate is the coordinator's task: it starts the children in bulk
// calls of one workflow each, chunked as RunGroup chunks, every child
// keyed by its job index so a retry gets the existing child back.
func (c *Client) coordinate(hctx hatchetsdk.Context, in coordinatorInput) (coordinatorOutput, error) {
	names := make([]string, len(in.Runs))
	sizes := make([]int, len(in.Runs))
	for i, r := range in.Runs {
		names[i] = r.Workflow
		in, _ := json.Marshal(r.Input) // decoded from JSON, so it encodes
		md, _ := json.Marshal(r.Meta)
		sizes[i] = len(in) + len(md)
	}
	for _, chunk := range chunkRuns(names, sizes, c.cfg.GroupChunkRuns, c.cfg.GroupChunkBytes) {
		opts := make([]hatchetsdk.RunManyOpt, len(chunk))
		for k, i := range chunk {
			r := in.Runs[i]
			ro := []hatchetsdk.RunOptFunc{hatchetsdk.WithRunMetadata(r.Meta), hatchetsdk.WithRunKey(strconv.Itoa(i))}
			if r.Priority != nil {
				ro = append(ro, hatchetsdk.WithRunPriority(hatchetsdk.RunPriority(*r.Priority)))
			}
			opts[k] = hatchetsdk.RunManyOpt{Input: r.Input, Opts: ro}
		}
		_, err := c.sdk.RunMany(hctx, names[chunk[0]], opts)
		if _, collided := hatchetsdk.IsBulkTriggerIdempotencyCollisionError(err); collided {
			// Every child carries a fresh idempotency value of its own, so
			// a collision is this coordinator's earlier attempt's child.
			err = nil
		}
		if err != nil {
			return coordinatorOutput{}, fmt.Errorf("batch %s: start %d children of %q: %w", in.Batch, len(chunk), names[chunk[0]], err)
		}
		if c.afterChunk != nil {
			c.afterChunk()
		}
	}
	return coordinatorOutput{Children: len(in.Runs)}, nil
}
