package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
)

// resetCmd re-runs a finished run in place from --step: a failed run, or
// with --succeeded one that succeeded (POST /runs/{id}/reset).
//
//ds:def id=cli-resetcmd-q6esujd6 owner=@khanakia stability=stable
func resetCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	step := fs.String("step", "", "the step to run again from; it and every step after it run again (required)")
	succeeded := fs.Bool("succeeded", false, "also reset a run that succeeded (redo work that already succeeded)")
	dry := fs.Bool("dry-run", false, "check the run can be reset and change nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("reset takes one run ID")
		}
		if *step == "" {
			return usagef("reset needs --step")
		}
		id := args[0]
		if *dry {
			var r runJSON
			raw, err := e.api.get(ctx, runPath(id, false, ""), &r)
			if err != nil {
				return err
			}
			switch {
			case r.Status == "failed":
			case r.Status == "succeeded" && !*succeeded:
				return fmt.Errorf("dry run: reset would be refused: run %s succeeded; pass --succeeded to redo it", r.Ref)
			case r.Status != "succeeded":
				return fmt.Errorf("dry run: reset would be refused: run %s is %s; reset needs a failed run (or a succeeded one with --succeeded)", r.Ref, r.Status)
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fprintf(e.out, "dry run: would reset run %s (%s, %s) from step %s; nothing changed\n", r.Ref, r.Workflow, r.Status, *step)
			return nil
		}
		var out struct {
			Ref refJSON `json:"ref"`
		}
		raw, err := e.api.do(ctx, "POST", runPath(id, false, "/reset"), struct {
			Step      string `json:"step"`
			Succeeded bool   `json:"succeeded,omitempty"`
		}{*step, *succeeded}, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		fprintf(e.out, "reset %s from step %s\n", out.Ref, *step)
		return nil
	}
}

// forkCmd starts a new run from a finished one at --step (POST
// /runs/{id}/fork).
//
//ds:def id=cli-forkcmd-85emeetp owner=@khanakia stability=stable
func forkCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	step := fs.String("step", "", "the step the new run starts from; the steps before it keep the original's outputs (required)")
	key := fs.String("key", "", "the new run's idempotency key (the original's is never copied)")
	var labels multiFlag
	fs.Var(&labels, "label", "add label key=value to the new run, repeatable")
	dry := fs.Bool("dry-run", false, "check the run can be forked and start nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 {
			return usagef("fork takes one run ID")
		}
		if *step == "" {
			return usagef("fork needs --step")
		}
		var lm map[string]string
		for _, l := range labels {
			k, v, ok := strings.Cut(l, "=")
			if !ok || k == "" {
				return usagef("--label %q: want key=value", l)
			}
			if lm == nil {
				lm = map[string]string{}
			}
			lm[k] = v
		}
		id := args[0]
		if *dry {
			var r runJSON
			raw, err := e.api.get(ctx, runPath(id, false, ""), &r)
			if err != nil {
				return err
			}
			if !terminal[r.Status] {
				return fmt.Errorf("dry run: fork would be refused: run %s is %s; only a finished run can be forked", r.Ref, r.Status)
			}
			if e.asJSON {
				return e.printJSON(raw)
			}
			fprintf(e.out, "dry run: would fork run %s (%s, %s) from step %s; nothing started\n", r.Ref, r.Workflow, r.Status, *step)
			return nil
		}
		var out struct {
			Ref       refJSON `json:"ref"`
			Duplicate bool    `json:"duplicate"`
		}
		raw, err := e.api.do(ctx, "POST", runPath(id, false, "/fork"), struct {
			Step           string            `json:"step"`
			IdempotencyKey string            `json:"idempotency_key,omitempty"`
			Labels         map[string]string `json:"labels,omitempty"`
		}{*step, *key, lm}, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		if out.Duplicate {
			fprintf(e.out, "key %s is held by run %s; nothing started\n", *key, out.Ref)
			return nil
		}
		fprintf(e.out, "forked %s from step %s as run %s\n", id, *step, out.Ref)
		return nil
	}
}

// signalWaitingCmd delivers an event only to the runs waiting for it now
// (POST /signals/waiting).
//
//ds:def id=cli-signalwaitingcmd-t3jdendr owner=@khanakia stability=stable
func signalWaitingCmd(fs *flag.FlagSet) func(context.Context, *env, []string) error {
	key := fs.String("key", "", "the event key the waiting runs match")
	payload := fs.String("payload", "", "the event payload, as JSON")
	dry := fs.Bool("dry-run", false, "check the server can deliver it and send nothing")
	return func(ctx context.Context, e *env, args []string) error {
		if len(args) != 1 || args[0] == "" {
			return usagef("signal-waiting takes one event name")
		}
		if *payload != "" && !json.Valid([]byte(*payload)) {
			return usagef("--payload is not JSON")
		}
		if *dry {
			var caps struct {
				Operations []struct {
					Operation string `json:"operation"`
					Supported bool   `json:"supported"`
					Reason    string `json:"reason"`
				} `json:"operations"`
			}
			raw, err := e.api.get(ctx, "capabilities", &caps)
			if err != nil {
				return err
			}
			for _, o := range caps.Operations {
				if o.Operation != "signal_waiting" {
					continue
				}
				if !o.Supported {
					why := o.Reason
					if why == "" {
						why = "the server's client does not support it"
					}
					return fmt.Errorf("dry run: signal-waiting would be refused: %s", why)
				}
				if e.asJSON {
					return e.printJSON(raw)
				}
				fprintf(e.out, "dry run: would signal %s (key %q) to the runs waiting for it now; nothing sent\n", args[0], *key)
				return nil
			}
			return fmt.Errorf("dry run: signal-waiting would be refused: the server does not know the operation")
		}
		body := struct {
			Name    string          `json:"name"`
			Key     string          `json:"key,omitempty"`
			Payload json.RawMessage `json:"payload,omitempty"`
		}{Name: args[0], Key: *key}
		if *payload != "" {
			body.Payload = json.RawMessage(*payload)
		}
		var out struct {
			Resumed int `json:"resumed"`
		}
		raw, err := e.api.do(ctx, "POST", "signals/waiting", body, &out)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(raw)
		}
		fprintf(e.out, "signalled %s: %d waiting runs resumed\n", args[0], out.Resumed)
		return nil
	}
}
