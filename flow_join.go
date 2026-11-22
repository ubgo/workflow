package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// JoinRest says what JoinChildren does with the children still running
// once the join is decided (it has enough successes, or can no longer
// reach them).
type JoinRest string

// The JoinRest values.
const (
	// JoinKeepRest leaves them running; their ends are ignored by the
	// join (a structured discriminator or partial join, WC-09, WC-30,
	// WC-34). The zero value.
	//ds:def id=joinkeeprest-88sa9sg4 owner=@khanakia stability=stable
	JoinKeepRest JoinRest = ""

	// JoinCancelRest cancels them (Client.Cancel): a cancelling
	// discriminator or partial join (WC-29, WC-32, WC-35).
	//ds:def id=joincancelrest-j4ju6z9r owner=@khanakia stability=stable
	JoinCancelRest JoinRest = "cancel"

	// JoinCompleteRest force-completes them as succeeded without running
	// their remaining steps (RunForcer.ForceComplete, CapForceRun): the
	// instances still running count as done and the step moves on (WC-27,
	// complete multiple-instance task).
	//ds:def id=joincompleterest-ae3vwxd7 owner=@khanakia stability=stable
	JoinCompleteRest JoinRest = "complete"
)

// JoinSpec configures JoinChildren.
//
//ds:def id=joinspec-p4n46erj owner=@khanakia stability=stable
type JoinSpec struct {
	// Need is how many children must succeed for the join to pass; zero
	// means every child (an AND-join, as AwaitChildren). One makes a
	// discriminator: the first child to succeed wins.
	Need int

	// Rest is what happens to the children still running once the join
	// is decided; the zero value leaves them running.
	Rest JoinRest

	// More, when set, may add children while the join waits (WC-36,
	// dynamic partial join): it is called on every look and returns every
	// job added so far beyond the first ones, and closed true once no
	// more will come. It must be append-only (a later look returns the
	// same list or a longer one), since each job is started once, keyed by
	// its index. While More is open the join waits rather than fail.
	More func(ctx context.Context) (jobs []Job, closed bool, err error)

	// Poll is the snooze between looks at unfinished children; zero
	// means DefaultChildPoll.
	Poll time.Duration

	// Spawn are passed to Spawn (SpawnAllowDuplicates).
	Spawn []SpawnOption

	// Parent is the client the calling step's workflow runs on (it must
	// honour Snooze); nil means the client the children run on.
	Parent Client
}

// JoinResult is the outcome of a passed join.
//
//ds:def id=joinresult-uucbtd8w owner=@khanakia stability=stable
type JoinResult struct {
	// Refs are every child started, in job order (More's jobs after the
	// first ones).
	Refs []RunRef

	// Winners are the indexes into Refs of the Need children that counted,
	// in job order.
	Winners []int

	// Outputs are the winners' results, in the order of Winners.
	Outputs [][]byte
}

// JoinFailedError is the Permanent error of a join that can no longer
// reach Need: too many children ended without succeeding.
//
//ds:def id=joinfailederror-r52ajzv8 owner=@khanakia stability=stable
type JoinFailedError struct {
	// Name is the join's name.
	Name string
	// Need and Succeeded are the successes wanted and seen.
	Need, Succeeded int
	// Failed are the children that ended without succeeding, by index
	// into the children started, with how each ended.
	Failed map[int]*RunFailedError
}

// Error names the join and how far it got.
func (e *JoinFailedError) Error() string {
	return fmt.Sprintf("workflow: join %q: %d of %d needed children succeeded, %d ended otherwise", e.Name, e.Succeeded, e.Need, len(e.Failed))
}

// joinDecision is what a join decided, memoised so a retried step, or a
// look after a crash, returns the same winners.
type joinDecision struct {
	Refs    []RunRef `json:"refs"`
	Winners []int    `json:"winners"`
}

// JoinChildren starts jobs as child runs of the calling step (Spawn) and
// waits, without holding a worker, until Need of them have succeeded: the
// partial-join and discriminator family of control-flow patterns over
// child runs. Need 0 waits for all (as AwaitChildren); Need 1 continues on
// the first success (structured discriminator); Need n of m continues on
// n (partial join). Rest says what becomes of the children still running
// when the join passes: left running, cancelled, or force-completed. With
// More, children may be added while it waits (dynamic partial join).
//
// While the join is undecided it returns Snooze(Poll), so the parent
// step looks again later without spending an attempt (the parent's
// driver needs CapSnooze; without it JoinChildren refuses with a
// Permanent UnsupportedError before starting any child). Once decided,
// the decision is memoised, so a retried step returns the same winners.
// Which children count when more than Need have succeeded by a look is
// decided in job order, not finish order: a join cannot see the order of
// two ends that happened between two looks. Keep Poll short when the
// first finisher matters.
//
// When the children can no longer reach Need (and More adds none), the
// join fails with a Permanent *JoinFailedError; with JoinCancelRest or
// JoinCompleteRest the children still running are cancelled first, so a
// failed join leaves nothing behind. Reading results needs ResultReader
// on client (CapResult), JoinCancelRest needs CapCancel and
// JoinCompleteRest a RunForcer (CapForceRun); each is checked before
// any child starts. Spawn's requirements apply: the Journal plugin and
// CapIdempotencyKey (or SpawnAllowDuplicates).
//
//ds:def id=joinchildren-a95h7rs8 owner=@khanakia stability=stable
func JoinChildren(ctx context.Context, client Client, name string, jobs []Job, spec JoinSpec) (JoinResult, error) {
	if err := checkJoin(client, name, len(jobs), spec); err != nil {
		return JoinResult{}, err
	}
	j, err := journalFrom(ctx)
	if err != nil {
		return JoinResult{}, err
	}
	decisionKey := j.helperKey("join", name)
	if raw, ok, err := j.store.Load(ctx, j.run, decisionKey); err != nil {
		return JoinResult{}, fmt.Errorf("workflow: join %q: load decision: %w", name, err)
	} else if ok {
		var d joinDecision
		if err := json.Unmarshal(raw, &d); err != nil {
			return JoinResult{}, Permanent(fmt.Errorf("workflow: join %q: stored decision: %w", name, err))
		}
		return joinOutputs(ctx, client, name, d)
	}
	all, open := jobs, false
	if spec.More != nil {
		extra, closed, err := spec.More(ctx)
		if err != nil {
			return JoinResult{}, fmt.Errorf("workflow: join %q: more: %w", name, err)
		}
		all, open = append(append([]Job(nil), jobs...), extra...), !closed
	}
	if len(all) == 0 {
		return JoinResult{}, Snooze(joinPoll(spec)) // More is open and has added nothing yet
	}
	refs, err := Spawn(ctx, client, name, all, spec.Spawn...)
	if err != nil {
		return JoinResult{}, err
	}
	need := spec.Need
	if need <= 0 {
		need = len(refs)
	}
	if need > len(refs) && !open {
		return JoinResult{}, Permanent(invalidf("join %q: Need %d of %d children", name, need, len(refs)))
	}
	var succeeded []int
	failed := map[int]*RunFailedError{}
	var running []int
	for i, ref := range refs {
		st, err := client.Status(ctx, ref)
		if err != nil {
			return JoinResult{}, fmt.Errorf("workflow: join %q: child %d: %w", name, i, err)
		}
		switch {
		case st == StatusSucceeded:
			succeeded = append(succeeded, i)
		case st.IsTerminal():
			failed[i] = &RunFailedError{Ref: ref, Status: st, Err: runError(ctx, client, ref)}
		default:
			running = append(running, i)
		}
	}
	if len(succeeded) >= need {
		d := joinDecision{Refs: refs, Winners: succeeded[:need]}
		if err := settleRest(ctx, client, name, spec.Rest, refs, running); err != nil {
			return JoinResult{}, err
		}
		raw, err := encodeJSON("join "+name, d)
		if err != nil {
			return JoinResult{}, err
		}
		if err := j.store.Save(ctx, j.run, decisionKey, raw); err != nil {
			return JoinResult{}, fmt.Errorf("workflow: join %q: save decision: %w", name, err)
		}
		return joinOutputs(ctx, client, name, d)
	}
	if len(succeeded)+len(running) < need && !open {
		rest := spec.Rest
		if rest == JoinCompleteRest {
			rest = JoinCancelRest // a failed join completes nothing
		}
		if err := settleRest(ctx, client, name, rest, refs, running); err != nil {
			return JoinResult{}, err
		}
		return JoinResult{}, Permanent(&JoinFailedError{Name: name, Need: need, Succeeded: len(succeeded), Failed: failed})
	}
	return JoinResult{}, Snooze(joinPoll(spec))
}

// joinPoll is the snooze between two looks of a join.
func joinPoll(spec JoinSpec) time.Duration {
	if spec.Poll <= 0 {
		return DefaultChildPoll
	}
	return spec.Poll
}

// checkJoin refuses what a join could not honour, before any child starts.
//
//ds:def id=checkjoin-5zxqmnan owner=@khanakia stability=stable
func checkJoin(client Client, name string, n int, spec JoinSpec) error {
	if client == nil {
		return Permanent(invalidf("join %q: client is nil", name))
	}
	if spec.Need < 0 || (spec.More == nil && spec.Need > n) {
		return Permanent(invalidf("join %q: Need %d of %d children", name, spec.Need, n))
	}
	if n == 0 && spec.More == nil {
		return Permanent(invalidf("join %q: no children", name))
	}
	parent := spec.Parent
	if parent == nil {
		parent = client
	}
	if !parent.Capabilities().Has(CapSnooze) {
		return Permanent(&UnsupportedError{Field: "JoinChildren", Missing: CapSnooze,
			Reason: "the parent's driver would spend an attempt on every look at unfinished children"})
	}
	switch spec.Rest {
	case JoinKeepRest:
	case JoinCancelRest:
		if !client.Capabilities().Has(CapCancel) {
			return Permanent(&UnsupportedError{Field: "JoinSpec.Rest", Missing: CapCancel, Reason: "the children's driver cannot cancel a run"})
		}
	case JoinCompleteRest:
		if _, ok := client.(RunForcer); !ok || !client.Capabilities().Has(CapForceRun) || !client.Capabilities().Has(CapCancel) {
			return Permanent(&UnsupportedError{Field: "JoinSpec.Rest", Missing: CapForceRun, Reason: "the children's driver cannot force a run complete"})
		}
	default:
		return Permanent(invalidf("join %q: unknown Rest %q", name, spec.Rest))
	}
	return nil
}

// settleRest applies rest to the children at the indexes in running.
//
//ds:def id=settlerest-53yzjzvw owner=@khanakia stability=stable
func settleRest(ctx context.Context, client Client, name string, rest JoinRest, refs []RunRef, running []int) error {
	for _, i := range running {
		var err error
		switch rest {
		case JoinCancelRest:
			err = client.Cancel(ctx, refs[i])
		case JoinCompleteRest:
			err = client.(RunForcer).ForceComplete(ctx, refs[i])
			if errors.Is(err, ErrWrongState) {
				err = nil // it ended on its own since the look
			}
		}
		if err != nil {
			return fmt.Errorf("workflow: join %q: settle child %d: %w", name, i, err)
		}
	}
	return nil
}

// joinOutputs reads the winners' results.
func joinOutputs(ctx context.Context, client Client, name string, d joinDecision) (JoinResult, error) {
	res := JoinResult{Refs: d.Refs, Winners: d.Winners, Outputs: make([][]byte, len(d.Winners))}
	for k, i := range d.Winners {
		out, err := resultOf(ctx, client, d.Refs[i], StatusSucceeded)
		if err != nil {
			return JoinResult{}, Permanent(fmt.Errorf("workflow: join %q: child %d: %w", name, i, err))
		}
		res.Outputs[k] = out
	}
	return res, nil
}

// ChildLabels returns the labels every child a step spawned under the
// current run carries (LabelParentRun, and LabelParentStep when step is
// not empty), for a RunQuery that lists or cancels them.
//
//ds:def id=childlabels-se7ep9y5 owner=@khanakia stability=stable
func ChildLabels(run RunID, step string) map[string]string {
	l := map[string]string{LabelParentRun: run.String()}
	if step != "" {
		l[LabelParentStep] = step
	}
	return l
}

// CancelChildren cancels every unfinished child run that the step named
// step of run spawned (empty step: every step of the run), and returns
// how many it cancelled: cancel a multiple-instance task (WC-26) from
// inside the run or from outside it. It finds them by their parent
// labels, so it needs a BulkCanceller (CapCancelAll) on the children's
// client; a client without it is refused with an UnsupportedError.
//
// Unfinished means scheduled (a delayed start), queued, running and
// waiting, plus retrying on a client with CapRunStates. A client without
// that capability has no retrying status (a run in retry backoff reads
// queued or running there, so it is still matched), and asking it for
// one would be refused, as Hatchet does.
//
//ds:def id=cancelchildren-hh6njpmy owner=@khanakia stability=stable
func CancelChildren(ctx context.Context, client Client, run RunID, step string) (int, error) {
	bc, ok := client.(BulkCanceller)
	if client == nil || !ok || !client.Capabilities().Has(CapCancelAll) {
		return 0, &UnsupportedError{Field: "CancelChildren", Missing: CapCancelAll, Reason: "the children's driver cannot cancel runs by label"}
	}
	if run.IsZero() {
		return 0, invalidf("CancelChildren: run is zero")
	}
	n, err := bc.CancelAll(ctx, RunQuery{Labels: ChildLabels(run, step), Statuses: unfinishedStatuses(client.Capabilities())})
	if err != nil {
		return n, fmt.Errorf("workflow: cancel children of %s: %w", run, err)
	}
	return n, nil
}

// unfinishedStatuses lists every status a run can be in before it ends
// on a client with caps: StatusRetrying only with CapRunStates, because a
// driver without it reports that moment as queued or running and refuses
// a filter on a status it does not have.
//
//ds:def id=unfinishedstatuses-uvmyv6mp owner=@khanakia stability=stable
func unfinishedStatuses(caps Caps) []Status {
	s := []Status{StatusScheduled, StatusQueued, StatusRunning, StatusWaiting}
	if caps.Has(CapRunStates) {
		s = append(s, StatusRetrying)
	}
	return s
}

// helperKey is the journal key of a helper's own state (a join's
// decision, a map's progress) for this step: the step name's length and
// the step, then "#", the helper and the name. Memo keys put "/" right
// after the step, so the two never meet.
func (j *journalScope) helperKey(helper, name string) string {
	return strconv.Itoa(len(j.step)) + ":" + j.step + "#" + helper + "/" + name
}
