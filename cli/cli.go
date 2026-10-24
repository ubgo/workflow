package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

// The environment variables Run reads through Options.Getenv.
const (
	// EnvURL is the admin API's mount point, the default of --url.
	EnvURL = "WORKFLOW_ADMIN_URL"
	// EnvToken is a bearer token, the default of --token.
	EnvToken = "WORKFLOW_ADMIN_TOKEN"
	// EnvHeader is one extra header, "Name: value", added before any
	// --header flag.
	EnvHeader = "WORKFLOW_ADMIN_HEADER"
)

// The exit codes Run returns.
const (
	ExitOK    = 0 // the command did what it was asked
	ExitError = 1 // the API refused or failed, or drain timed out
	ExitUsage = 2 // the command line is wrong
)

// DefaultTimeout bounds one HTTP request when Options.HTTPClient is nil.
const DefaultTimeout = 30 * time.Second

// Options are what Run takes from the process around it, so an app can
// embed the CLI and a test can drive it without a real terminal, network
// or clock.
type Options struct {
	// Stdout gets results; Stderr gets errors and usage. Nil discards.
	Stdout, Stderr io.Writer
	// Getenv reads EnvURL, EnvToken and EnvHeader; nil reads nothing (an
	// embedding app passes os.Getenv, or its own lookup).
	Getenv func(string) string
	// HTTPClient sends the requests; nil is a client with DefaultTimeout.
	HTTPClient *http.Client
	// Now and Sleep drive drain's polling; nil means the wall clock.
	// Sleep returns early with ctx's error when ctx ends.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Name is the program name in usage text; empty means "workflow".
	Name string
}

// errUsage marks a command line error: Run prints usage and exits 2.
var errUsage = errors.New("usage")

// usagef is a usage error with a message.
func usagef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, args...))
}

// env is one invocation's state: the API, the output and the flags every
// command shares.
type env struct {
	opts   Options
	out    io.Writer
	errw   io.Writer
	api    *api
	asJSON bool
}

// global holds the flags every command accepts.
type global struct {
	url     string
	token   string
	headers headerFlags
	asJSON  bool
	timeout time.Duration
}

// headerFlags collects repeated --header "Name: value".
type headerFlags []string

func (h *headerFlags) String() string     { return fmt.Sprintf("%d headers", len(*h)) }
func (h *headerFlags) Set(v string) error { *h = append(*h, v); return nil }

// register adds the global flags to fs with def as their defaults, so
// they work before or after the command name: the command's flag set is
// registered with the values the top level parsed, and a later flag wins.
func (g *global) register(fs *flag.FlagSet, def global) {
	fs.StringVar(&g.url, "url", def.url, "admin API mount point, e.g. https://app.example.com/admin (env "+EnvURL+")")
	fs.StringVar(&g.token, "token", def.token, "bearer token for the Authorization header (env "+EnvToken+"); never printed")
	fs.Var(&g.headers, "header", "extra request header 'Name: value', repeatable (env "+EnvHeader+" holds one)")
	fs.BoolVar(&g.asJSON, "json", def.asJSON, "print the API's JSON answer instead of a table")
	fs.DurationVar(&g.timeout, "timeout", def.timeout, "overall time limit for the command (0: none)")
}

// envDefaults is the global flags' defaults from the environment.
func envDefaults(getenv func(string) string) global {
	return global{url: getenv(EnvURL), token: getenv(EnvToken)}
}

// command is one subcommand.
type command struct {
	name    string
	args    string // positional arguments, for usage
	summary string
	// flags registers the command's own flags and returns the function
	// that runs it with the positional arguments.
	flags func(fs *flag.FlagSet) func(ctx context.Context, e *env, args []string) error
}

// commands is every subcommand, in usage order.
func commands() []command {
	return []command{
		{"caps", "", "operations the server's driver supports", capsCmd},
		{"runs", "", "list runs (filters below)", listCmd("runs", true)},
		{"failures", "", "list failed runs", listCmd("failures", false)},
		{"discarded", "", "list runs a step discarded", listCmd("discarded", false)},
		{"inspect", "RUN", "show one run and its attempt errors", inspectCmd},
		{"retry", "RUN | --all", "retry a failed run, or every failed run a filter matches", retryCmd},
		{"cancel", "RUN | --all", "cancel a run, or every run a filter matches", cancelCmd},
		{"delete", "RUN | --all", "forget a finished run, or every finished run a filter matches", deleteCmd},
		{"reschedule", "RUN", "move a delayed run to --at", rescheduleCmd},
		{"cancel-step", "RUN STEP", "withdraw one step of a run", cancelStepCmd},
		{"pause", "", "pause a workflow, a queue or everything", pauseCmd("pause")},
		{"resume", "", "resume a workflow, a queue or everything", pauseCmd("resume")},
		{"pauses", "", "list the active pauses", pausesCmd},
		{"queues", "", "queue depth and oldest wait", queuesCmd},
		{"schedules", "", "list schedules with their next fire", schedulesCmd},
		{"schedule", "ID", "show one schedule", scheduleCmd},
		{"trigger", "ID", "run a schedule now", triggerCmd},
		{"drain", "", "wait until queues are empty", drainCmd},
		{"metrics", "", "run ends per step by status", metricsCmd},
		{"health", "", "the server client's health; exit 1 when not ready", healthCmd},
		{"nodes", "", "the nodes that are online", nodesCmd},
	}
}

// Run runs the CLI with args (without the program name) and returns the
// exit code: ExitOK, ExitError or ExitUsage.
func Run(ctx context.Context, args []string, opts Options) int {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if opts.Getenv == nil {
		opts.Getenv = func(string) string { return "" }
	}
	if opts.Name == "" {
		opts.Name = "workflow"
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepCtx
	}
	err := run(ctx, args, opts)
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, flag.ErrHelp):
		return ExitOK
	case errors.Is(err, errUsage):
		fmt.Fprintf(opts.Stderr, "%s: %v\n", opts.Name, strings.TrimPrefix(err.Error(), errUsage.Error()+": "))
		fmt.Fprintf(opts.Stderr, "run '%s help' for usage\n", opts.Name)
		return ExitUsage
	default:
		fmt.Fprintf(opts.Stderr, "%s: %v\n", opts.Name, err)
		return ExitError
	}
}

func run(ctx context.Context, args []string, opts Options) error {
	var g global
	top := flag.NewFlagSet(opts.Name, flag.ContinueOnError)
	top.SetOutput(io.Discard)
	g.register(top, envDefaults(opts.Getenv))
	if err := top.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage(opts.Stdout, opts.Name)
			return err
		}
		return usagef("%v", err)
	}
	rest := top.Args()
	if len(rest) == 0 || rest[0] == "help" {
		if len(rest) > 1 {
			return commandHelp(opts, rest[1])
		}
		usage(opts.Stdout, opts.Name)
		if len(rest) == 0 {
			return usagef("no command")
		}
		return nil
	}
	var cmd *command
	for _, c := range commands() {
		if c.name == rest[0] {
			cmd = &c
			break
		}
	}
	if cmd == nil {
		return usagef("unknown command %q", rest[0])
	}
	fs := flag.NewFlagSet(opts.Name+" "+cmd.name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	g.register(fs, g)
	exec := cmd.flags(fs)
	pos, err := parseInterleaved(fs, rest[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printCommandUsage(opts.Stdout, opts, *cmd, fs)
			return err
		}
		return usagef("%s: %v", cmd.name, err)
	}
	headers, err := buildHeaders(g, opts.Getenv)
	if err != nil {
		return err
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: DefaultTimeout}
	}
	a, err := newAPI(g.url, headers, hc)
	if err != nil {
		return usagef("%v", err)
	}
	if g.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.timeout)
		defer cancel()
	}
	e := &env{opts: opts, out: opts.Stdout, errw: opts.Stderr, api: a, asJSON: g.asJSON}
	return exec(ctx, e, pos)
}

// buildHeaders turns --token, EnvHeader and --header into request
// headers. Header values are checked for a colon only; the server decides
// what it accepts.
func buildHeaders(g global, getenv func(string) string) (http.Header, error) {
	h := http.Header{}
	if g.token != "" {
		h.Set("Authorization", "Bearer "+g.token)
	}
	raw := []string(g.headers)
	if v := getenv(EnvHeader); v != "" {
		raw = append([]string{v}, raw...)
	}
	for _, line := range raw {
		k, v, ok := strings.Cut(line, ":")
		k = strings.TrimSpace(k)
		if !ok || k == "" || strings.ContainsAny(k, " \t") {
			// The value may be a secret: name the problem, not the value.
			return nil, usagef("--header wants 'Name: value' (got a value without a header name)")
		}
		h.Add(k, strings.TrimSpace(v))
	}
	return h, nil
}

// parseInterleaved parses fs over args, allowing flags after positional
// arguments ("retry r1 --dry-run"), which flag.Parse stops at. "--" ends
// the flags.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		if args[0] == "--" {
			return append(pos, args[1:]...), nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// sleepCtx waits d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func usage(w io.Writer, name string) {
	fmt.Fprintf(w, "%s is a client for the workflow admin API.\n\nUsage:\n  %s [--url URL] [--token T | --header 'K: V'] <command> [flags] [args]\n\nCommands:\n", name, name)
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-10s %-12s %s\n", c.name, c.args, c.summary)
	}
	fmt.Fprintf(w, `
Global flags (before or after the command):
  --url URL        admin API mount point (env %s)
  --token T        bearer token (env %s)
  --header 'K: V'  extra header, repeatable (env %s holds one)
  --json           print the API's JSON instead of a table
  --timeout D      overall time limit for the command

Examples:
  %[4]s failures --workflow send-email
  %[4]s retry --all --workflow send-email --dry-run
  %[4]s cancel --all --queue emails
  %[4]s pause --queue emails
  %[4]s drain --queue emails --wait 10m
  %[4]s --header 'Cookie: session=...' inspect 01J9Z...

Run '%[4]s help <command>' for a command's flags. Exit codes: 0 ok, 1 failed, 2 usage.
`, EnvURL, EnvToken, EnvHeader, name)
}

// commandHelp prints one command's usage.
func commandHelp(opts Options, name string) error {
	for _, c := range commands() {
		if c.name == name {
			fs := flag.NewFlagSet(opts.Name+" "+c.name, flag.ContinueOnError)
			var g global
			g.register(fs, envDefaults(opts.Getenv))
			c.flags(fs)
			printCommandUsage(opts.Stdout, opts, c, fs)
			return nil
		}
	}
	return usagef("unknown command %q", name)
}

func printCommandUsage(w io.Writer, opts Options, c command, fs *flag.FlagSet) {
	fmt.Fprintf(w, "Usage: %s %s [flags] %s\n\n%s.\n\nFlags:\n", opts.Name, c.name, c.args, c.summary)
	fs.VisitAll(func(f *flag.Flag) {
		def := f.DefValue
		if f.Name == "token" || def == "" || def == "false" || def == "0s" || def == "0" || strings.HasSuffix(def, " headers") {
			def = "" // a token default came from the environment: never print it
		}
		if def != "" {
			def = " (default " + def + ")"
		}
		fmt.Fprintf(w, "  --%-10s %s%s\n", f.Name, f.Usage, def)
	})
}

// Main is the stand-alone binary's body: Run over the process's
// arguments, standard streams and environment, cancelled by an interrupt,
// then exit with its code.
func Main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := Run(ctx, os.Args[1:], Options{Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv})
	stop()
	os.Exit(code)
}
