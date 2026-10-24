package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The JSON shapes the admin API answers with, as far as the CLI reads
// them. --json prints the raw answer, so a field missing here is still
// visible there.

type refJSON struct {
	RunID       string `json:"run_id,omitempty"`
	ScheduledID string `json:"scheduled_id,omitempty"`
}

func (r refJSON) String() string {
	if r.RunID != "" {
		return r.RunID
	}
	if r.ScheduledID != "" {
		return "scheduled:" + r.ScheduledID
	}
	return "-"
}

type runJSON struct {
	Ref           refJSON           `json:"ref"`
	Workflow      string            `json:"workflow"`
	Queue         string            `json:"queue"`
	Status        string            `json:"status"`
	Error         string            `json:"error"`
	FailedStep    string            `json:"failed_step"`
	Labels        map[string]string `json:"labels"`
	Priority      int               `json:"priority"`
	EnqueuedAt    time.Time         `json:"enqueued_at"`
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at"`
	Attempts      int               `json:"attempts"`
	NextAttempt   int               `json:"next_attempt"`
	NextAttemptAt time.Time         `json:"next_attempt_at"`
	Errors        []struct {
		Step    string    `json:"step"`
		Attempt int       `json:"attempt"`
		Error   string    `json:"error"`
		At      time.Time `json:"at"`
	} `json:"errors"`
}

type pageJSON struct {
	Runs      []runJSON `json:"runs"`
	Next      string    `json:"next"`
	Total     *int      `json:"total"`
	Truncated bool      `json:"truncated"`
}

type bulkJSON struct {
	Matched   int       `json:"matched"`
	Done      int       `json:"done"`
	Skipped   int       `json:"skipped"`
	Truncated bool      `json:"truncated"`
	DryRun    bool      `json:"dry_run"`
	Sample    []refJSON `json:"sample"`
	// Cancelled is the answer of a real POST /runs/cancel.
	Cancelled *int `json:"cancelled"`
}

// terminal is workflow.Status.IsTerminal, for dry-run verdicts.
var terminal = map[string]bool{"succeeded": true, "failed": true, "cancelled": true, "discarded": true}

// --- filters ---

// multiFlag collects a repeated flag; each value may also be
// comma-separated where the flag says so.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// filter is the run filter flags shared by list and bulk commands.
type filter struct {
	workflow, queue, tenant, from, to string
	statuses, labels                  multiFlag
}

func (f *filter) register(fs *flag.FlagSet, withStatus bool) {
	fs.StringVar(&f.workflow, "workflow", "", "only runs of this workflow")
	fs.StringVar(&f.queue, "queue", "", "only runs on this queue")
	fs.StringVar(&f.tenant, "tenant", "", "only runs of this tenant (the driver needs tenant support)")
	if withStatus {
		fs.Var(&f.statuses, "status", "only runs in these statuses, comma-separated or repeated")
	}
	fs.Var(&f.labels, "label", "only runs with label key=value (a value ending in * is a prefix), repeatable")
	fs.StringVar(&f.from, "from", "", "enqueued at or after this RFC 3339 time")
	fs.StringVar(&f.to, "to", "", "enqueued before this RFC 3339 time")
}

func (f *filter) set() bool {
	return f.workflow != "" || f.queue != "" || f.tenant != "" || f.from != "" || f.to != "" || len(f.statuses) > 0 || len(f.labels) > 0
}

func (f *filter) statusList() []string { return splitList(f.statuses) }

// splitList is a repeated flag's values, each split on commas, blanks
// dropped.
func splitList(m multiFlag) []string {
	var out []string
	for _, raw := range m {
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// check validates the parts the CLI can check before asking: label shape
// and time format. Everything else (status names, queue names) is the
// server's to refuse.
func (f *filter) check() error {
	for _, l := range f.labels {
		if k, _, ok := strings.Cut(l, "="); !ok || k == "" {
			return usagef("--label %q: want key=value or key=prefix*", l)
		}
	}
	for name, v := range map[string]string{"from": f.from, "to": f.to} {
		if v == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return usagef("--%s %q: want an RFC 3339 time such as 2026-10-14T09:00:00Z", name, v)
		}
	}
	return nil
}

// query is the filter as GET query parameters.
func (f *filter) query() url.Values {
	v := url.Values{}
	if f.workflow != "" {
		v.Set("workflow", f.workflow)
	}
	if f.queue != "" {
		v.Set("queue", f.queue)
	}
	if f.tenant != "" {
		v.Set("tenant", f.tenant)
	}
	if st := f.statusList(); len(st) > 0 {
		v.Set("status", strings.Join(st, ","))
	}
	for _, l := range f.labels {
		v.Add("label", l)
	}
	if f.from != "" {
		v.Set("from", f.from)
	}
	if f.to != "" {
		v.Set("to", f.to)
	}
	return v
}

// filterBody is the filter in a bulk POST body (admin's filterJSON) plus
// the bulk fields.
type filterBody struct {
	Workflow      string            `json:"workflow,omitempty"`
	Queue         string            `json:"queue,omitempty"`
	Tenant        string            `json:"tenant,omitempty"`
	Statuses      []string          `json:"statuses,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	LabelPrefixes map[string]string `json:"label_prefixes,omitempty"`
	From          string            `json:"from,omitempty"`
	To            string            `json:"to,omitempty"`
	DryRun        bool              `json:"dry_run,omitempty"`
	Max           int               `json:"max,omitempty"`
}

func (f *filter) body() filterBody {
	b := filterBody{Workflow: f.workflow, Queue: f.queue, Tenant: f.tenant, Statuses: f.statusList(), From: f.from, To: f.to}
	for _, l := range f.labels {
		k, v, _ := strings.Cut(l, "=")
		if p, ok := strings.CutSuffix(v, "*"); ok {
			if b.LabelPrefixes == nil {
				b.LabelPrefixes = map[string]string{}
			}
			b.LabelPrefixes[k] = p
			continue
		}
		if b.Labels == nil {
			b.Labels = map[string]string{}
		}
		b.Labels[k] = v
	}
	return b
}

// --- output ---

// printJSON writes the API's raw answer, indented.
func (e *env) printJSON(raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, bytes.TrimSpace(raw), "", "  "); err != nil {
		_, err = e.out.Write(raw)
		return err
	}
	buf.WriteByte('\n')
	_, err := e.out.Write(buf.Bytes())
	return err
}

// fmtTime is a time for a table: local, to the second, "-" when unset.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// fmtDur is a short duration for a table.
func fmtDur(secs float64) string {
	return (time.Duration(secs) * time.Second).Round(time.Second).String()
}

// oneLine shortens s to n runes on one line, for an error column.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// runPath is the API path of one run; scheduled picks the ScheduledID
// form (?scheduled=1).
func runPath(id string, scheduled bool, suffix string) string {
	p := "runs/" + url.PathEscape(id) + suffix
	if scheduled {
		p += "?scheduled=1"
	}
	return p
}

// --- commands ---

func capsCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("caps takes no arguments")
		}
		var out struct {
			Capabilities string `json:"capabilities"`
			Operations   []struct {
				Operation string `json:"operation"`
				Supported bool   `json:"supported"`
				Missing   string `json:"missing"`
			} `json:"operations"`
			Output  bool `json:"output"`
			Metrics bool `json:"metrics"`
		}
		raw, err := e.api.get(ctx, "capabilities", &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		t := newTable(e.out, "OPERATION", "SUPPORTED", "MISSING")
		for _, o := range out.Operations {
			t.row(o.Operation, yesNo(o.Supported), o.Missing)
		}
		t.row("output", yesNo(out.Output), "")
		t.row("metrics", yesNo(out.Metrics), "")
		return t.flush()
	}
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// listCmd lists runs from /runs, /failures or /discarded; withStatus is
// false for the two single-status routes.
func listCmd(kind string, withStatus bool) func(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(fs *flag.FlagSet) func(context.Context, *env, []string) error {
		var f filter
		f.register(fs, withStatus)
		limit := fs.Int("limit", 50, "runs per page, 1 to 1000")
		cursor := fs.String("cursor", "", "the page to show, from a previous page's 'next'")
		count := fs.Bool("count", false, "also count every match (bounded by the server's scan limit)")
		return func(ctx context.Context, e *env, args []string) error {
			if len(args) > 0 {
				return usagef("%s takes flags only, got %q", kind, args[0])
			}
			if err := f.check(); err != nil {
				return err
			}
			q := f.query()
			q.Set("limit", strconv.Itoa(*limit))
			if *cursor != "" {
				q.Set("cursor", *cursor)
			}
			if *count {
				q.Set("count", "1")
			}
			var page pageJSON
			raw, err := e.api.get(ctx, kind+"?"+q.Encode(), &page)
			if err != nil {
				return err
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			if len(page.Runs) == 0 {
				fmt.Fprintln(e.out, "no runs match")
				return nil
			}
			t := newTable(e.out, "RUN", "WORKFLOW", "QUEUE", "STATUS", "ATT", "ENQUEUED", "FINISHED", "ERROR")
			for _, r := range page.Runs {
				t.row(r.Ref.String(), r.Workflow, r.Queue, r.Status, strconv.Itoa(r.Attempts), fmtTime(r.EnqueuedAt), fmtTime(r.FinishedAt), oneLine(r.Error, 100))
			}
			if err := t.flush(); err != nil {
				return err
			}
			if page.Total != nil {
				more := ""
				if page.Truncated {
					more = "+"
				}
				fmt.Fprintf(e.out, "%d of %d%s\n", len(page.Runs), *page.Total, more)
			}
			if page.Next != "" {
				fmt.Fprintf(e.out, "next page: --cursor %s\n", page.Next)
			}
			return nil
		}
	}
}

func inspectCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	scheduled := fs.Bool("scheduled", false, "RUN is a ScheduledID (a delayed run that has not started)")
	output := fs.Bool("output", false, "also show captured step output (needs the server's WithOutputs)")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("inspect takes one run ID")
		}
		var r runJSON
		raw, err := e.api.get(ctx, runPath(args[0], *scheduled, ""), &r)
		if err != nil {
			return err
		}
		var outRaw []byte
		var out struct {
			Attempts []struct {
				Step      string `json:"step"`
				Attempt   int    `json:"attempt"`
				Text      string `json:"text"`
				Truncated bool   `json:"truncated"`
				Error     string `json:"error"`
			} `json:"attempts"`
		}
		if *output && r.Ref.RunID != "" {
			if outRaw, err = e.api.get(ctx, runPath(r.Ref.RunID, false, "/output"), &out); err != nil {
				return err
			}
		}
		if e.asJSON {
			if outRaw == nil {
				return e.printJSON(raw)
			}
			return e.printJSON([]byte(`{"run":` + string(raw) + `,"output":` + string(outRaw) + `}`))
		}
		kv := newTable(e.out)
		kv.row("run", r.Ref.String())
		if r.Ref.ScheduledID != "" && r.Ref.RunID != "" {
			kv.row("scheduled", r.Ref.ScheduledID)
		}
		kv.row("workflow", r.Workflow)
		kv.row("queue", r.Queue)
		kv.row("status", r.Status)
		kv.row("attempts", strconv.Itoa(r.Attempts))
		if r.NextAttempt > 0 {
			kv.row("next attempt", fmt.Sprintf("#%d at %s", r.NextAttempt, fmtTime(r.NextAttemptAt)))
		}
		kv.row("priority", strconv.Itoa(r.Priority))
		kv.row("enqueued", fmtTime(r.EnqueuedAt))
		kv.row("started", fmtTime(r.StartedAt))
		kv.row("finished", fmtTime(r.FinishedAt))
		if r.FailedStep != "" {
			kv.row("failed step", r.FailedStep)
		}
		if r.Error != "" {
			kv.row("error", oneLine(r.Error, 200))
		}
		for _, k := range sortedKeys(r.Labels) {
			kv.row("label", k+"="+r.Labels[k])
		}
		if err := kv.flush(); err != nil {
			return err
		}
		if len(r.Errors) > 0 {
			fmt.Fprintln(e.out, "\nfailed attempts:")
			t := newTable(e.out, "ATTEMPT", "STEP", "AT", "ERROR")
			for _, x := range r.Errors {
				t.row(strconv.Itoa(x.Attempt), x.Step, fmtTime(x.At), oneLine(x.Error, 100))
			}
			if err := t.flush(); err != nil {
				return err
			}
		}
		for _, o := range out.Attempts {
			state := "ok"
			if o.Error != "" {
				state = "error: " + oneLine(o.Error, 80)
			}
			fmt.Fprintf(e.out, "\noutput of %s #%d (%s)\n%s", o.Step, o.Attempt, state, o.Text)
			if o.Truncated {
				fmt.Fprint(e.out, "\n[truncated]")
			}
			fmt.Fprintln(e.out)
		}
		return nil
	}
}

// actionSpec describes one of retry, cancel and delete: the per-run
// route, the bulk route, and which statuses the server accepts, for a
// dry run's verdict.
type actionSpec struct {
	verb      string // "retry"
	past      string // "retried"
	method    string // per-run method
	suffix    string // per-run path suffix
	bulkPath  string // POST path for --all
	accepts   func(status string) bool
	needs     string // what accepts wants, for the refusal message
	withMax   bool   // the bulk route takes max
	allStatus bool   // --all takes --status
}

var (
	retrySpec = actionSpec{verb: "retry", past: "retried", method: "POST", suffix: "/retry", bulkPath: "runs/retry",
		accepts: func(s string) bool { return s == "failed" }, needs: "a failed run", withMax: true}
	cancelSpec = actionSpec{verb: "cancel", past: "cancelled", method: "POST", suffix: "/cancel", bulkPath: "runs/cancel",
		accepts: func(s string) bool { return !terminal[s] }, needs: "a run that has not finished", allStatus: true}
	deleteSpec = actionSpec{verb: "delete", past: "deleted", method: "DELETE", suffix: "", bulkPath: "runs/delete",
		accepts: func(s string) bool { return terminal[s] }, needs: "a finished run", withMax: true, allStatus: true}
)

func retryCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	return actionCmd(fs, retrySpec)
}
func cancelCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	return actionCmd(fs, cancelSpec)
}
func deleteCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	return actionCmd(fs, deleteSpec)
}

// actionCmd is retry, cancel or delete: one run by ID, or --all over a
// filter; --dry-run reads and changes nothing.
func actionCmd(fs *flag.FlagSet, sp actionSpec) func(context.Context, *env, []string) error {
	all := fs.Bool("all", false, "act on every run the filter flags match instead of one run")
	dry := fs.Bool("dry-run", false, "show what would be "+sp.past+" and change nothing")
	scheduled := fs.Bool("scheduled", false, "RUN is a ScheduledID")
	var f filter
	f.register(fs, sp.allStatus)
	var max *int
	if sp.withMax {
		max = fs.Int("max", 0, "with --all, act on at most this many runs (0: the server's bound)")
	}
	return func(ctx context.Context, e *env, args []string) error {
		if *all {
			if len(args) > 0 {
				return usagef("%s --all takes filter flags, not a run ID", sp.verb)
			}
			if err := f.check(); err != nil {
				return err
			}
			b := f.body()
			b.DryRun = *dry
			if max != nil {
				if *max < 0 {
					return usagef("--max %d is negative", *max)
				}
				b.Max = *max
			}
			var res bulkJSON
			raw, err := e.api.do(ctx, "POST", sp.bulkPath, b, &res)
			if err != nil {
				return err
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			return printBulk(e, sp, res)
		}
		if len(args) != 1 {
			return usagef("%s takes one run ID, or --all with filter flags", sp.verb)
		}
		if f.set() || (max != nil && *max != 0) {
			return usagef("filter flags and --max need --all")
		}
		id := args[0]
		if *dry {
			var r runJSON
			raw, err := e.api.get(ctx, runPath(id, *scheduled, ""), &r)
			if err != nil {
				return err
			}
			if !sp.accepts(r.Status) {
				return fmt.Errorf("dry run: %s would be refused: run %s is %s; %s needs %s", sp.verb, r.Ref, r.Status, sp.verb, sp.needs)
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fmt.Fprintf(e.out, "dry run: would %s run %s (%s, %s); nothing changed\n", sp.verb, r.Ref, r.Workflow, r.Status)
			return nil
		}
		var out struct {
			Ref refJSON `json:"ref"`
		}
		raw, err := e.api.do(ctx, sp.method, runPath(id, *scheduled, sp.suffix), nil, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		if out.Ref.RunID != "" || out.Ref.ScheduledID != "" {
			fmt.Fprintf(e.out, "%s %s as %s\n", sp.past, id, out.Ref)
		} else {
			fmt.Fprintf(e.out, "%s %s\n", sp.past, id)
		}
		return nil
	}
}

func printBulk(e *env, sp actionSpec, r bulkJSON) error {
	if r.Cancelled != nil {
		fmt.Fprintf(e.out, "cancelled %d runs\n", *r.Cancelled)
		return nil
	}
	if r.DryRun {
		more := ""
		if r.Truncated {
			more = " (more match; the server stops at its bound)"
		}
		fmt.Fprintf(e.out, "dry run: would %s %d runs%s; nothing changed\n", sp.verb, r.Matched, more)
		for _, ref := range r.Sample {
			fmt.Fprintf(e.out, "  %s\n", ref)
		}
		if r.Matched > len(r.Sample) {
			fmt.Fprintf(e.out, "  … and %d more\n", r.Matched-len(r.Sample))
		}
		return nil
	}
	fmt.Fprintf(e.out, "matched %d, %s %d, skipped %d (changed meanwhile)\n", r.Matched, sp.past, r.Done, r.Skipped)
	if r.Truncated {
		fmt.Fprintf(e.out, "more runs match: run the same command again for the next batch\n")
	}
	return nil
}

// pauseCmd is pause and resume: exactly one of --workflow, --queue and
// --everything.
func pauseCmd(verb string) func(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(fs *flag.FlagSet) func(context.Context, *env, []string) error {
		wf := fs.String("workflow", "", verb+" one workflow")
		q := fs.String("queue", "", verb+" one queue")
		everything := fs.Bool("everything", false, verb+" every workflow and queue")
		dry := fs.Bool("dry-run", false, "show what would be "+verb+"d and the active pauses; change nothing")
		return func(ctx context.Context, e *env, args []string) error {
			if len(args) > 0 {
				return usagef("%s takes --workflow, --queue or --everything, not %q", verb, args[0])
			}
			n := 0
			for _, set := range []bool{*wf != "", *q != "", *everything} {
				if set {
					n++
				}
			}
			if n != 1 {
				return usagef("%s needs exactly one of --workflow, --queue and --everything", verb)
			}
			body := pauseBody{*wf, *q, *everything}
			what := "everything"
			if *wf != "" {
				what = "workflow " + *wf
			} else if *q != "" {
				what = "queue " + *q
			}
			if *dry {
				var cur struct {
					Pauses []pauseBody `json:"pauses"`
				}
				raw, err := e.api.get(ctx, "pauses", &cur)
				if err != nil {
					return err
				}
				if e.asJSON {
					return e.printJSON(raw)
				}
				state := "not paused now"
				for _, p := range cur.Pauses {
					if p == body {
						state = "paused now"
					}
				}
				fmt.Fprintf(e.out, "dry run: would %s %s (%s; %d pauses active); nothing changed\n", verb, what, state, len(cur.Pauses))
				return nil
			}
			raw, err := e.api.do(ctx, "POST", verb, body, nil)
			if err != nil {
				return err
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fmt.Fprintf(e.out, "%sd %s\n", verb, what)
			return nil
		}
	}
}

// pauseBody is a pause target, as POST /pause takes it and GET /pauses
// lists it.
type pauseBody struct {
	Workflow string `json:"workflow,omitempty"`
	Queue    string `json:"queue,omitempty"`
	All      bool   `json:"all,omitempty"`
}

func pausesCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("pauses takes no arguments")
		}
		var out struct {
			Pauses []struct {
				Workflow string `json:"workflow"`
				Queue    string `json:"queue"`
				All      bool   `json:"all"`
			} `json:"pauses"`
		}
		raw, err := e.api.get(ctx, "pauses", &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		if len(out.Pauses) == 0 {
			fmt.Fprintln(e.out, "nothing is paused")
			return nil
		}
		for _, p := range out.Pauses {
			switch {
			case p.All:
				fmt.Fprintln(e.out, "everything")
			case p.Workflow != "":
				fmt.Fprintln(e.out, "workflow "+p.Workflow)
			default:
				fmt.Fprintln(e.out, "queue "+p.Queue)
			}
		}
		return nil
	}
}

type queuesJSON struct {
	Queues []struct {
		Queue          string    `json:"queue"`
		Depth          int       `json:"depth"`
		OldestEnqueued time.Time `json:"oldest_enqueued_at"`
		OldestWaitSecs float64   `json:"oldest_wait_seconds"`
		Workers        *int      `json:"workers"`
		Running        *int      `json:"running"`
	} `json:"queues"`
	Truncated bool `json:"truncated"`
}

func queuesCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("queues takes no arguments")
		}
		var out queuesJSON
		raw, err := e.api.get(ctx, "queues", &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		if len(out.Queues) == 0 {
			fmt.Fprintln(e.out, "no queued runs")
			return nil
		}
		t := newTable(e.out, "QUEUE", "DEPTH", "OLDEST WAIT", "WORKERS", "RUNNING")
		for _, q := range out.Queues {
			wait := "-"
			if q.Depth > 0 {
				wait = fmtDur(q.OldestWaitSecs)
			}
			t.row(q.Queue, strconv.Itoa(q.Depth), wait, optInt(q.Workers), optInt(q.Running))
		}
		if err := t.flush(); err != nil {
			return err
		}
		if out.Truncated {
			fmt.Fprintln(e.out, "depths are lower bounds: the server's scan limit was reached")
		}
		return nil
	}
}

func optInt(p *int) string {
	if p == nil {
		return "-"
	}
	return strconv.Itoa(*p)
}

type scheduleJSON struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Workflow string    `json:"workflow"`
	Tenant   string    `json:"tenant"`
	Cron     string    `json:"cron"`
	Location string    `json:"location"`
	Declared bool      `json:"declared"`
	Paused   bool      `json:"paused"`
	Next     time.Time `json:"next"`
	Overlap  string    `json:"overlap"`
	Last     *struct {
		Fire   time.Time `json:"fire"`
		Run    refJSON   `json:"run"`
		Status string    `json:"status"`
	} `json:"last"`
	Labels map[string]string `json:"labels"`
}

// schedulesPage is how many schedules one GET /schedules asks for; the
// command follows "next" until the list ends.
const schedulesPage = 1000

func schedulesCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("schedules takes no arguments")
		}
		var all []scheduleJSON
		var raws []json.RawMessage
		cursor := ""
		for {
			var page struct {
				Schedules []scheduleJSON `json:"schedules"`
				Next      string         `json:"next"`
			}
			raw, err := e.api.get(ctx, "schedules?limit="+strconv.Itoa(schedulesPage)+"&cursor="+url.QueryEscape(cursor), &page)
			if err != nil {
				return err
			}
			all = append(all, page.Schedules...)
			var rp struct {
				Schedules []json.RawMessage `json:"schedules"`
			}
			_ = json.Unmarshal(raw, &rp)
			raws = append(raws, rp.Schedules...)
			if page.Next == "" || page.Next == cursor {
				break
			}
			cursor = page.Next
		}
		if e.asJSON {
			b, err := json.Marshal(struct {
				Schedules []json.RawMessage `json:"schedules"`
			}{append([]json.RawMessage{}, raws...)})
			if err != nil {
				return err
			}
			return e.printJSON(b)
		}
		if len(all) == 0 {
			fmt.Fprintln(e.out, "no schedules")
			return nil
		}
		t := newTable(e.out, "ID", "NAME", "WORKFLOW", "TENANT", "CRON", "ZONE", "NEXT", "STATE")
		for _, s := range all {
			state, next := "active", fmtTime(s.Next)
			if s.Paused {
				state, next = "paused", "-"
			}
			t.row(s.ID, dash(s.Name), s.Workflow, dash(s.Tenant), s.Cron, dash(s.Location), next, state)
		}
		return t.flush()
	}
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func scheduleCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("schedule takes one schedule ID")
		}
		var s scheduleJSON
		raw, err := e.api.get(ctx, "schedules/"+url.PathEscape(args[0]), &s)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		printSchedule(e, s)
		return nil
	}
}

func printSchedule(e *env, s scheduleJSON) {
	kv := newTable(e.out)
	kv.row("id", s.ID)
	if s.Name != "" {
		kv.row("name", s.Name)
	}
	kv.row("workflow", s.Workflow)
	if s.Tenant != "" {
		kv.row("tenant", s.Tenant)
	}
	kv.row("cron", s.Cron)
	kv.row("zone", dash(s.Location))
	kv.row("overlap", dash(s.Overlap))
	if s.Paused {
		kv.row("state", "paused")
	} else {
		kv.row("state", "active")
		kv.row("next", fmtTime(s.Next))
	}
	if s.Last != nil {
		kv.row("last fire", fmtTime(s.Last.Fire))
		kv.row("last run", s.Last.Run.String()+" "+s.Last.Status)
	}
	for _, k := range sortedKeys(s.Labels) {
		kv.row("label", k+"="+s.Labels[k])
	}
	_ = kv.flush()
}

func triggerCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	dry := fs.Bool("dry-run", false, "show the schedule that would run and enqueue nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("trigger takes one schedule ID")
		}
		id := url.PathEscape(args[0])
		if *dry {
			var s scheduleJSON
			raw, err := e.api.get(ctx, "schedules/"+id, &s)
			if err != nil {
				return err
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fmt.Fprintf(e.out, "dry run: would run schedule %s (workflow %s) now; nothing enqueued\n", s.ID, s.Workflow)
			if s.Overlap == "skip" {
				fmt.Fprintln(e.out, "its overlap policy is skip: the real trigger is refused while its previous run is unfinished")
			}
			return nil
		}
		var out struct {
			Ref refJSON `json:"ref"`
		}
		raw, err := e.api.do(ctx, "POST", "schedules/"+id+"/trigger", nil, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		fmt.Fprintf(e.out, "triggered %s as run %s\n", args[0], out.Ref)
		return nil
	}
}

// drainStatuses are the statuses drain waits out: runs a worker holds or
// is about to take. Delayed runs (scheduled) and runs waiting for an event
// are not waited for, since either can wait for days.
const drainStatuses = "queued,running,retrying"

func drainCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	var queues multiFlag
	fs.Var(&queues, "queue", "queue to wait for, comma-separated or repeated (default: every queue)")
	wait := fs.Duration("wait", 10*time.Minute, "give up after this long (exit 1)")
	interval := fs.Duration("interval", 2*time.Second, "time between checks")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("drain takes flags only")
		}
		if *wait <= 0 || *interval <= 0 {
			return usagef("--wait and --interval must be positive")
		}
		names := splitList(queues)
		if len(names) == 0 {
			names = []string{""}
		}
		start := e.opts.Now()
		for {
			left := 0
			for _, q := range names {
				v := url.Values{"status": {drainStatuses}, "limit": {"1"}, "count": {"1"}}
				if q != "" {
					v.Set("queue", q)
				}
				var page pageJSON
				if _, err := e.api.get(ctx, "runs?"+v.Encode(), &page); err != nil {
					return err
				}
				if page.Total != nil {
					left += *page.Total
				}
			}
			waited := e.opts.Now().Sub(start)
			if left == 0 {
				if e.asJSON {
					return e.printJSON([]byte(fmt.Sprintf(`{"drained":true,"remaining":0,"waited_seconds":%g}`, waited.Seconds())))
				}
				fmt.Fprintf(e.out, "drained after %s\n", waited.Round(time.Second))
				return nil
			}
			if waited >= *wait {
				if e.asJSON {
					_ = e.printJSON([]byte(fmt.Sprintf(`{"drained":false,"remaining":%d,"waited_seconds":%g}`, left, waited.Seconds())))
				}
				return fmt.Errorf("drain: %d runs still queued or running after %s", left, wait.String())
			}
			if !e.asJSON {
				fmt.Fprintf(e.errw, "%d runs left\n", left)
			}
			if err := e.opts.Sleep(ctx, *interval); err != nil {
				return err
			}
		}
	}
}

func metricsCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	window := fs.String("window", "", "span ending now, e.g. 24h (default: the server's, one hour)")
	step := fs.String("step", "", "width of one row, e.g. 5m (default: the span over 60)")
	from := fs.String("from", "", "span start, RFC 3339 (instead of --window)")
	to := fs.String("to", "", "span end, RFC 3339 (default now)")
	wf := fs.String("workflow", "", "only this workflow")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("metrics takes flags only")
		}
		v := url.Values{}
		for k, s := range map[string]string{"window": *window, "step": *step, "from": *from, "to": *to, "workflow": *wf} {
			if s != "" {
				v.Set(k, s)
			}
		}
		var out struct {
			Since  time.Time `json:"since"`
			Points []struct {
				At        time.Time `json:"at"`
				Succeeded int64     `json:"succeeded"`
				Failed    int64     `json:"failed"`
				Cancelled int64     `json:"cancelled"`
				Discarded int64     `json:"discarded"`
			} `json:"points"`
		}
		raw, err := e.api.get(ctx, "metrics?"+v.Encode(), &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		t := newTable(e.out, "AT", "SUCCEEDED", "FAILED", "CANCELLED", "DISCARDED")
		var s, f, c, d int64
		for _, p := range out.Points {
			t.row(fmtTime(p.At), i64(p.Succeeded), i64(p.Failed), i64(p.Cancelled), i64(p.Discarded))
			s, f, c, d = s+p.Succeeded, f+p.Failed, c+p.Cancelled, d+p.Discarded
		}
		t.row("total", i64(s), i64(f), i64(c), i64(d))
		if err := t.flush(); err != nil {
			return err
		}
		if !out.Since.IsZero() {
			fmt.Fprintf(e.out, "data since %s\n", fmtTime(out.Since))
		}
		return nil
	}
}

func i64(n int64) string { return strconv.FormatInt(n, 10) }

func healthCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("health takes no arguments")
		}
		var out struct {
			Live   bool `json:"live"`
			Ready  bool `json:"ready"`
			Checks []struct {
				Name   string `json:"name"`
				OK     bool   `json:"ok"`
				Detail string `json:"detail"`
			} `json:"checks"`
		}
		raw, err := e.api.get(ctx, "health", &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			if err := e.printJSON(raw); err != nil {
				return err
			}
		} else {
			t := newTable(e.out, "CHECK", "OK", "DETAIL")
			t.row("live", yesNo(out.Live), "")
			t.row("ready", yesNo(out.Ready), "")
			for _, c := range out.Checks {
				t.row(c.Name, yesNo(c.OK), c.Detail)
			}
			if err := t.flush(); err != nil {
				return err
			}
		}
		if !out.Live || !out.Ready {
			return errors.New("health: not ready")
		}
		return nil
	}
}

func nodesCmd(*flag.FlagSet) func(context.Context, *env, []string) error {
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) > 0 {
			return usagef("nodes takes no arguments")
		}
		var out struct {
			Nodes []struct {
				ID       string    `json:"id"`
				Mode     string    `json:"mode"`
				Queues   []string  `json:"queues"`
				Workers  int       `json:"workers"`
				Running  int       `json:"running"`
				LastSeen time.Time `json:"last_seen"`
			} `json:"nodes"`
		}
		raw, err := e.api.get(ctx, "nodes", &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		if len(out.Nodes) == 0 {
			fmt.Fprintln(e.out, "no node is online")
			return nil
		}
		t := newTable(e.out, "NODE", "MODE", "QUEUES", "WORKERS", "RUNNING", "LAST SEEN")
		for _, n := range out.Nodes {
			qs := "all"
			if len(n.Queues) > 0 {
				qs = strings.Join(n.Queues, ",")
			}
			t.row(n.ID, n.Mode, qs, strconv.Itoa(n.Workers), strconv.Itoa(n.Running), fmtTime(n.LastSeen))
		}
		return t.flush()
	}
}

func rescheduleCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	at := fs.String("at", "", "the new due time, RFC 3339 (e.g. 2026-10-15T09:00:00Z)")
	scheduled := fs.Bool("scheduled", false, "RUN is a ScheduledID")
	dry := fs.Bool("dry-run", false, "check the run can be moved and change nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("reschedule takes one run ID")
		}
		t, err := time.Parse(time.RFC3339, *at)
		if err != nil {
			return usagef("--at %q: want an RFC 3339 time such as 2026-10-15T09:00:00Z", *at)
		}
		if *dry {
			var r runJSON
			raw, err := e.api.get(ctx, runPath(args[0], *scheduled, ""), &r)
			if err != nil {
				return err
			}
			if r.Status != "scheduled" {
				return fmt.Errorf("dry run: reschedule would be refused: run %s is %s; only a delayed run that has not started can be moved", r.Ref, r.Status)
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fmt.Fprintf(e.out, "dry run: would move run %s to %s; nothing changed\n", r.Ref, fmtTime(t))
			return nil
		}
		var out struct {
			Ref refJSON `json:"ref"`
		}
		raw, err := e.api.do(ctx, "POST", runPath(args[0], *scheduled, "/reschedule"), struct {
			At time.Time `json:"at"`
		}{t}, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		fmt.Fprintf(e.out, "rescheduled %s to %s\n", out.Ref, fmtTime(t))
		return nil
	}
}

func cancelStepCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	scheduled := fs.Bool("scheduled", false, "RUN is a ScheduledID")
	dry := fs.Bool("dry-run", false, "check the run is unfinished and change nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 2 {
			return usagef("cancel-step takes a run ID and a step name")
		}
		if *dry {
			var r runJSON
			raw, err := e.api.get(ctx, runPath(args[0], *scheduled, ""), &r)
			if err != nil {
				return err
			}
			if terminal[r.Status] {
				return fmt.Errorf("dry run: cancel-step would be refused: run %s is %s", r.Ref, r.Status)
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fmt.Fprintf(e.out, "dry run: would cancel step %s of run %s (%s); nothing changed\n", args[1], r.Ref, r.Status)
			return nil
		}
		raw, err := e.api.do(ctx, "POST", runPath(args[0], *scheduled, "/steps/"+url.PathEscape(args[1])+"/cancel"), nil, nil)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		fmt.Fprintf(e.out, "cancelled step %s of %s\n", args[1], args[0])
		return nil
	}
}
