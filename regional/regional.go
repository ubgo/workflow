package regional

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/ubgo/workflow"
)

// ErrUnknownRegion refuses a region name the client was not built with,
// from a Router, a SignalRouter or an option. It is returned wrapped
// together with workflow.ErrInvalid, so errors.Is matches both: a job
// whose region is unknown is a mistake in the job or the router, and
// sending it to some default region would move its data where it must
// not go.
var ErrUnknownRegion = errors.New("regional: unknown region")

// Router picks the region a job runs in. It is asked once per Enqueue,
// and once per Client.Schedule with a Job made of the schedule's
// Workflow, Input, Labels and Tenant. It returns a region name the client
// was built with; any other name is refused with ErrUnknownRegion. An
// error from Region is returned to the caller as it is, wrapped with the
// client's name: a router that cannot decide refuses the job.
type Router interface {
	Region(ctx context.Context, job workflow.Job) (string, error)
}

// RouterFunc turns a function into a Router, for a rule ByLabel does not
// cover: by tenant, by a payload field, by the app's own customer table.
//
//	regional.RouterFunc(func(ctx context.Context, job workflow.Job) (string, error) {
//		return customers.Region(ctx, job.Tenant)
//	})
type RouterFunc func(ctx context.Context, job workflow.Job) (string, error)

// Region calls f.
func (f RouterFunc) Region(ctx context.Context, job workflow.Job) (string, error) { return f(ctx, job) }

// ByLabel routes a job by the value of its label key, the way most apps
// already tag a job with its customer's region. A job without the label,
// or with an empty value, goes to fallback; with an empty fallback such a
// job is refused with an error matching workflow.ErrInvalid, so a job
// that forgot its region is never placed by accident. A value naming no
// region of the client is refused by the client (ErrUnknownRegion).
func ByLabel(key, fallback string) Router {
	return RouterFunc(func(_ context.Context, job workflow.Job) (string, error) {
		if v := job.Labels[key]; v != "" {
			return v, nil
		}
		if fallback != "" {
			return fallback, nil
		}
		return "", fmt.Errorf("%w: regional: job for %q has no %q label and the router has no fallback region", workflow.ErrInvalid, job.Workflow, key)
	})
}

// SignalRouter picks the regions an event is delivered to. It returns at
// least one region name the client was built with.
type SignalRouter func(ctx context.Context, ev workflow.Event) ([]string, error)

// Option configures a Client.
type Option func(*config)

type config struct {
	schedRegion string
	signal      SignalRouter
}

// WithScheduleRegion names the region that fires every workflow's
// declared Cron (Workflow.Cron), which has no job for the Router to read.
// The other regions get each workflow without its Cron, so a tick fires
// once. With a single region it defaults to that region; with more,
// Register refuses a workflow with Cron until this is set. Client.Schedule
// is not affected: it asks the Router.
func WithScheduleRegion(name string) Option {
	return func(c *config) { c.schedRegion = name }
}

// WithSignalRouter delivers each Signal to the regions fn returns instead
// of every region, for an app whose event payloads must stay in one
// region. A nil fn keeps the default. A run waiting in a region fn leaves
// out never sees the event.
func WithSignalRouter(fn SignalRouter) Option {
	return func(c *config) {
		if fn != nil {
			c.signal = fn
		}
	}
}

// Client is a workflow.Client over one backend per region. Build it with
// New.
type Client struct {
	regions     map[string]workflow.Client
	names       []string // the region names, sorted, for a stable order
	router      Router
	schedRegion string
	signal      SignalRouter // nil: every region

	mu      sync.Mutex
	wfs     map[string]workflow.Workflow
	started bool
	// stopRegions cancels the ctx every region was started with. The
	// caller's Start ctx is its parent, so cancelling that stops them too.
	stopRegions context.CancelFunc

	doneOnce sync.Once     // builds done on the first Done call
	done     chan struct{} // closed once every region's Done closed
}

var (
	_ workflow.Client      = (*Client)(nil)
	_ workflow.RunNotifier = (*Client)(nil)
)

// maxRegion bounds a region name, which is put in front of every ID the
// client hands out.
const maxRegion = 64

// validRegion checks a region name: 1 to 64 letters, digits, ".", "_"
// or "-". The colon is left out because it ends the region in a prefixed
// ID. Errors wrap ErrInvalid.
func validRegion(name string) error {
	if name == "" || len(name) > maxRegion {
		return fmt.Errorf("%w: regional: region name %q must be 1 to %d characters", workflow.ErrInvalid, name, maxRegion)
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !ok {
			return fmt.Errorf("%w: regional: region name %q has the character %q; use letters, digits and . _ -", workflow.ErrInvalid, name, r)
		}
	}
	return nil
}

// New returns a client that sends each job to the region router picks.
// Every client in regions must be fresh, unstarted and owned by this one:
// it registers and starts them. It refuses, with an error matching
// workflow.ErrInvalid, an empty regions map, a nil client, a nil router,
// a region name that is not 1 to 64 letters, digits, ".", "_" or "-", and
// a WithScheduleRegion naming no region.
func New(regions map[string]workflow.Client, router Router, opts ...Option) (*Client, error) {
	if len(regions) == 0 {
		return nil, fmt.Errorf("%w: regional: no regions", workflow.ErrInvalid)
	}
	if router == nil {
		return nil, fmt.Errorf("%w: regional: nil Router", workflow.ErrInvalid)
	}
	var cfg config
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	c := &Client{
		regions: make(map[string]workflow.Client, len(regions)),
		router:  router,
		signal:  cfg.signal,
		wfs:     map[string]workflow.Workflow{},
	}
	for name, rc := range regions {
		if err := validRegion(name); err != nil {
			return nil, err
		}
		if rc == nil {
			return nil, fmt.Errorf("%w: regional: region %q has a nil client", workflow.ErrInvalid, name)
		}
		c.regions[name] = rc
		c.names = append(c.names, name)
	}
	slices.Sort(c.names)
	c.schedRegion = cfg.schedRegion
	if c.schedRegion == "" && len(c.names) == 1 {
		c.schedRegion = c.names[0]
	}
	if c.schedRegion != "" {
		if _, ok := c.regions[c.schedRegion]; !ok {
			return nil, unknownRegion(c.schedRegion, "WithScheduleRegion")
		}
	}
	return c, nil
}

// unknownRegion is the refusal of a region name the client lacks.
func unknownRegion(name, from string) error {
	return fmt.Errorf("%w: %q from %s (%w)", ErrUnknownRegion, name, from, workflow.ErrInvalid)
}

// Regions returns the region names, sorted.
func (c *Client) Regions() []string { return slices.Clone(c.names) }

// Region returns the client of one region, for what this client does not
// forward (scaling a region's workers, an in-process closure). Calls made
// on it directly hand out the driver's own, unprefixed IDs.
func (c *Client) Region(name string) (workflow.Client, bool) {
	rc, ok := c.regions[name]
	return rc, ok
}

// route asks the Router for job's region and checks the answer.
func (c *Client) route(ctx context.Context, job workflow.Job) (string, workflow.Client, error) {
	name, err := c.router.Region(ctx, job)
	if err != nil {
		return "", nil, fmt.Errorf("regional: router: %w", err)
	}
	rc, ok := c.regions[name]
	if !ok {
		return "", nil, unknownRegion(name, "the Router")
	}
	return name, rc, nil
}

// excludedCaps are never declared: see Capabilities.
var excludedCaps = workflow.CapsOf(workflow.CapClosure, workflow.CapWorkerScale)

// Capabilities is what every region can do, since the router may send a
// job, or a schedule, to any of them. CapRunFinished also needs every
// region to implement workflow.RunNotifier. CapClosure and CapWorkerScale
// are never declared: a closure carries no job for the router to read,
// and worker counts belong to one region (use Region to reach its
// client).
func (c *Client) Capabilities() workflow.Caps {
	caps := workflow.AllCaps()
	for _, name := range c.names {
		caps = caps.Intersect(c.regions[name].Capabilities())
	}
	caps = caps.Without(excludedCaps)
	if _, ok := c.notifiers(); !ok {
		caps = caps.Without(workflow.CapRunFinished)
	}
	return caps
}

// notifiers returns every region as a RunNotifier, and false unless each
// implements it.
func (c *Client) notifiers() (map[string]workflow.RunNotifier, bool) {
	out := make(map[string]workflow.RunNotifier, len(c.names))
	for _, name := range c.names {
		n, ok := c.regions[name].(workflow.RunNotifier)
		if !ok {
			return nil, false
		}
		out[name] = n
	}
	return out, true
}

// Register validates wf and registers it in every region, with handlers
// that report the prefixed RunID in StepInfo. Only the schedule region's
// copy keeps the declared Cron and its schedule fields; a workflow with
// Cron is refused (ErrInvalid) when no schedule region is set.
func (c *Client) Register(wf workflow.Workflow) error {
	if err := workflow.Validate(wf); err != nil {
		return err
	}
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if started {
		return workflow.ErrAlreadyStarted
	}
	if len(wf.Cron) > 0 && c.schedRegion == "" {
		return fmt.Errorf("%w: regional: workflow %q declares Cron, and with %d regions it needs regional.WithScheduleRegion to say which region fires it", workflow.ErrInvalid, wf.Name, len(c.names))
	}
	for _, name := range c.names {
		rwf := withRegion(wf, name)
		if name != c.schedRegion {
			// Only the schedule region fires the declared Cron, so the
			// other copies get none of its fields. ScheduleWhen stays on
			// every copy: it also conditions Client.Schedule ticks, and
			// the router may put a schedule in any region.
			rwf.Cron, rwf.CronWindows = nil, workflow.Windows{}
			rwf.Misfire, rwf.CronOverlap = workflow.Misfire{}, ""
			rwf.CronSeconds, rwf.CronActive, rwf.CronKeepRuns = false, workflow.Active{}, 0
		}
		if err := c.regions[name].Register(rwf); err != nil {
			return fmt.Errorf("regional: %s: %w", name, err)
		}
	}
	c.mu.Lock()
	c.wfs[wf.Name] = wf
	c.mu.Unlock()
	return nil
}

// withRegion returns wf with every Run handler, OnFailure and OnTimeout
// wrapped so the StepInfo the handler reads names the run by the ref this
// client hands out.
func withRegion(wf workflow.Workflow, region string) workflow.Workflow {
	steps := make([]workflow.Step, len(wf.Steps))
	for i, st := range wf.Steps {
		if st.Run != nil {
			st.Run = regionHandler(st.Run, region)
		}
		steps[i] = st
	}
	wf.Steps = steps
	if wf.OnFailure != nil {
		wf.OnFailure = regionHandler(wf.OnFailure, region)
	}
	if wf.OnTimeout != nil {
		wf.OnTimeout = regionHandler(wf.OnTimeout, region)
	}
	return wf
}

func regionHandler(h workflow.Handler, region string) workflow.Handler {
	return func(ctx context.Context, in []byte) ([]byte, error) {
		if info, ok := workflow.StepInfoFrom(ctx); ok {
			if !info.RunID.IsZero() {
				info.RunID = workflow.RunID(prefix(region) + string(info.RunID))
			}
			info.Labels = regionLabels(info.Labels, region)
			ctx = workflow.WithStepInfo(ctx, info)
		}
		return h(ctx, in)
	}
}

// Start checks every registered workflow against Capabilities, so a
// workflow some region cannot run is refused before anything starts,
// then starts every region in name order. When one fails to start, the
// regions already started are stopped again.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return workflow.ErrAlreadyStarted
	}
	caps := c.Capabilities()
	for _, wf := range c.wfs {
		if err := workflow.CheckCaps(caps, wf); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.started = true
	c.mu.Unlock()

	// The regions run on a ctx of their own whose parent is the caller's,
	// so cancelling the caller's stops them all, and a failed Start stops
	// the ones already running.
	ctx, cancel := context.WithCancel(ctx)
	for _, name := range c.names {
		if err := c.regions[name].Start(ctx); err != nil {
			cancel()
			return fmt.Errorf("regional: %s: %w", name, err)
		}
	}
	c.mu.Lock()
	c.stopRegions = cancel
	c.mu.Unlock()
	return nil
}

// Enqueue sends job to the region the Router picks and returns a RunRef
// prefixed with that region. It never sends the job to another region
// when that one fails.
func (c *Client) Enqueue(ctx context.Context, job workflow.Job) (workflow.RunRef, error) {
	name, rc, err := c.route(ctx, job)
	if err != nil {
		return workflow.RunRef{}, err
	}
	ref, err := rc.Enqueue(ctx, job)
	if err != nil {
		return workflow.RunRef{}, fmt.Errorf("regional: %s: %w", name, err)
	}
	return encodeRef(name, ref), nil
}

// Status asks the region the ref names. A ref naming no region of this
// client is ErrNotFound.
func (c *Client) Status(ctx context.Context, ref workflow.RunRef) (workflow.Status, error) {
	var st workflow.Status
	err := c.onRef(ref, func(_ string, rc workflow.Client, inner workflow.RunRef) error {
		var err error
		st, err = rc.Status(ctx, inner)
		return err
	})
	return st, err
}

// Cancel cancels the run in the region the ref names.
func (c *Client) Cancel(ctx context.Context, ref workflow.RunRef) error {
	return c.onRef(ref, func(_ string, rc workflow.Client, inner workflow.RunRef) error {
		return rc.Cancel(ctx, inner)
	})
}

// onRef calls fn with the region ref names, its client and the driver's
// own ref. A ref without a region prefix, naming a region this client
// lacks, or whose two IDs name different regions was not made by this
// client: ErrNotFound.
func (c *Client) onRef(ref workflow.RunRef, fn func(string, workflow.Client, workflow.RunRef) error) error {
	name, inner, ok := decodeRef(ref)
	rc, known := c.regions[name]
	if !ok || !known {
		return fmt.Errorf("%w: regional: run ref %+v names no region of this client", workflow.ErrNotFound, ref)
	}
	return fn(name, rc, inner)
}

// Signal delivers ev to every region, or to the regions WithSignalRouter
// picks, since a run waiting for it may sit in any of them. Every region
// is tried; when one fails, the others have already taken the event and
// the error names the failing region, so a retried Signal may reach a
// region twice.
func (c *Client) Signal(ctx context.Context, ev workflow.Event) error {
	targets := c.names
	if c.signal != nil {
		picked, err := c.signal(ctx, ev)
		if err != nil {
			return fmt.Errorf("regional: signal router: %w", err)
		}
		if len(picked) == 0 {
			return fmt.Errorf("%w: regional: the signal router picked no region for event %q", workflow.ErrInvalid, ev.Name)
		}
		for _, name := range picked {
			if _, ok := c.regions[name]; !ok {
				return unknownRegion(name, "the signal router")
			}
		}
		targets = picked
	}
	var errs []error
	for _, name := range targets {
		if err := c.regions[name].Signal(ctx, ev); err != nil {
			errs = append(errs, fmt.Errorf("regional: signal %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// Schedule registers s in the region the Router picks for a job made of
// s's Workflow, Input, Labels and Tenant, so the schedule lives where its
// runs would be sent, and returns the ScheduleID prefixed with that
// region.
func (c *Client) Schedule(ctx context.Context, s workflow.Schedule) (workflow.ScheduleID, error) {
	name, rc, err := c.route(ctx, workflow.Job{Workflow: s.Workflow, Payload: s.Input, Labels: s.Labels, Tenant: s.Tenant})
	if err != nil {
		return "", err
	}
	id, err := rc.Schedule(ctx, s)
	if err != nil {
		return "", fmt.Errorf("regional: %s: %w", name, err)
	}
	return encodeScheduleID(name, id), nil
}

// Unschedule removes the schedule from the region its ID names. An empty
// id is a no-op; an ID naming no region of this client is ErrNotFound.
func (c *Client) Unschedule(ctx context.Context, id workflow.ScheduleID) error {
	if id.IsZero() {
		return nil
	}
	name, rc, inner, err := c.scheduleRegion(id)
	if err != nil {
		return err
	}
	if err := rc.Unschedule(ctx, inner); err != nil {
		return fmt.Errorf("regional: %s: %w", name, err)
	}
	return nil
}

// scheduleRegion decodes a ScheduleID this client handed out.
func (c *Client) scheduleRegion(id workflow.ScheduleID) (string, workflow.Client, workflow.ScheduleID, error) {
	name, inner, ok := splitID(string(id))
	rc, known := c.regions[name]
	if !ok || !known {
		return "", nil, "", fmt.Errorf("%w: regional: schedule %q names no region of this client", workflow.ErrNotFound, id)
	}
	return name, rc, workflow.ScheduleID(inner), nil
}

// OnRunFinished registers fn in every region, with each event's Ref and
// schedule label prefixed by the region it came from. Every region must
// implement workflow.RunNotifier; otherwise it returns an
// UnsupportedError, since runs of the other regions would end unseen.
// Register before Start.
func (c *Client) OnRunFinished(fn workflow.RunFinishedFunc) error {
	ns, ok := c.notifiers()
	if !ok {
		return workflow.Unsupported("OnRunFinished", "every region must implement workflow.RunNotifier")
	}
	if c.isStarted() {
		return workflow.ErrAlreadyStarted
	}
	for _, name := range c.names {
		region := name
		err := ns[name].OnRunFinished(func(ctx context.Context, ev workflow.RunEvent) {
			ev.Ref = encodeRef(region, ev.Ref)
			ev.Labels = regionLabels(ev.Labels, region)
			fn(ctx, ev)
		})
		if err != nil {
			return fmt.Errorf("regional: %s: %w", name, err)
		}
	}
	return nil
}

func (c *Client) isStarted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}
