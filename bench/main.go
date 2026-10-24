// Command bench measures enqueue throughput, end-to-end throughput and
// pickup latency for the ubgo/workflow drivers. Systems run one at a time, never in
// parallel, each on storage created for the run and removed after it.
//
// Every system gets the same job count, worker concurrency and payload.
// The run prints the machine, Go and library versions and the load
// average first, and the load again before each system; on a machine
// busier than -max-load it waits (up to -load-wait) and says so.
//
// Usage:
//
//	go run . -systems memory,sqlite,postgres,redis,hatchet
//
// Postgres-backed systems need -pg (a DSN of a maintenance database such
// as postgres); redis needs -redis; hatchet reads the HATCHET_CLIENT_*
// environment, and REDIS_PASSWORD when Redis has auth. run.sh sets these for the shared local stack.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := mainErr(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

// options are the command-line flags.
type options struct {
	cfg      config
	systems  []string
	pgDSN    string
	pgPool   int
	redis    string
	maxLoad  float64
	loadWait time.Duration
}

// allSystems is every system the harness knows, in the order they run.
var allSystems = []string{"memory", "sqlite", "postgres", "postgres-notify", "redis", "hatchet"}

func parseFlags(args []string, out io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(out)
	systems := fs.String("systems", strings.Join(allSystems, ","), "comma-separated systems to run, in order: "+strings.Join(allSystems, ", "))
	fs.IntVar(&o.cfg.Jobs, "jobs", 5000, "jobs in the throughput phase")
	fs.IntVar(&o.cfg.Concurrency, "concurrency", 10, "worker slots, and goroutines enqueuing in the throughput phase")
	fs.IntVar(&o.cfg.PayloadSize, "payload", 256, "payload size in bytes")
	fs.IntVar(&o.cfg.Warmup, "warmup", 100, "untimed jobs run before the throughput phase")
	fs.IntVar(&o.cfg.LatSamples, "latency-samples", 200, "sequential pickup-latency probes")
	fs.DurationVar(&o.cfg.LatGapMin, "latency-gap", 10*time.Millisecond, "shortest idle pause before a latency probe")
	fs.DurationVar(&o.cfg.LatGapSpan, "latency-gap-span", time.Second, "probe pauses are spread evenly over [gap, gap+span); keep it at least the longest poll interval measured, so probes land at every phase of a poll")
	fs.BoolVar(&o.cfg.Separate, "separate-producer", false, "enqueue through a second client that runs no workers (systems without one report n/a)")
	fs.DurationVar(&o.cfg.Timeout, "timeout", 10*time.Minute, "longest wait for one phase")
	fs.StringVar(&o.pgDSN, "pg", "", "Postgres DSN of a maintenance database (bench databases are created and dropped beside it)")
	fs.IntVar(&o.pgPool, "pg-pool", 0, "sqlstore's Postgres pool size (0 keeps the sqlb pgx driver's default of 8)")
	fs.StringVar(&o.redis, "redis", "", "Redis address host:port for the Redis driver")
	fs.Float64Var(&o.maxLoad, "max-load", 15, "wait before a system while the 1-minute load average is above this")
	fs.DurationVar(&o.loadWait, "load-wait", 10*time.Minute, "longest wait for the load to drop; past it the system runs anyway and the load is reported")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	for name := range strings.SplitSeq(*systems, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !known(name) {
			return o, fmt.Errorf("unknown system %q (known: %s)", name, strings.Join(allSystems, ", "))
		}
		o.systems = append(o.systems, name)
	}
	if o.cfg.Jobs < 1 || o.cfg.Concurrency < 1 || o.cfg.LatSamples < 1 || o.cfg.Warmup < 0 {
		return o, errors.New("-jobs, -concurrency and -latency-samples must be at least 1, -warmup at least 0")
	}
	if o.pgDSN != "" {
		if _, err := url.Parse(o.pgDSN); err != nil {
			return o, fmt.Errorf("-pg: %w", err)
		}
	}
	return o, nil
}

func known(name string) bool {
	for _, s := range allSystems {
		if s == name {
			return true
		}
	}
	return false
}

func mainErr(ctx context.Context, args []string, out io.Writer) error {
	o, err := parseFlags(args, out)
	if err != nil {
		return err
	}
	printEnv(ctx, out, o)
	pg := &pgServer{admin: o.pgDSN}
	var results []result
	for _, name := range o.systems {
		sys, skip := build(name, o, pg)
		if skip != "" {
			fmt.Fprintf(out, "\n%s: skipped (%s)\n", name, skip)
			continue
		}
		load := waitForLoad(ctx, out, o)
		fmt.Fprintf(out, "\n%s: running (load %.2f)\n", name, load)
		r := run(ctx, name, sys, o.cfg)
		// Each row as soon as it exists, so a later system that hangs or
		// is interrupted does not lose the earlier numbers.
		printRow(out, r)
		results = append(results, r)
		if ctx.Err() != nil {
			break
		}
	}
	printResults(out, results)
	return nil
}

// build returns the system for name, or why it is skipped.
func build(name string, o options, pg *pgServer) (system, string) {
	switch name {
	case "memory":
		return newMemory(), ""
	case "sqlite":
		return newSQLite(), ""
	case "postgres", "postgres-notify":
		if o.pgDSN == "" {
			return nil, "no -pg DSN"
		}
		return newPostgres(pg, name == "postgres-notify", o.pgPool), ""
	case "redis":
		if o.redis == "" {
			return nil, "no -redis address"
		}
		return newRedis(o.redis), ""
	case "hatchet":
		if os.Getenv("HATCHET_CLIENT_TOKEN") == "" {
			return nil, "HATCHET_CLIENT_TOKEN is not set"
		}
		return newHatchet(), ""
	}
	return nil, "unknown"
}

// waitForLoad waits while the 1-minute load average is above o.maxLoad,
// up to o.loadWait, and returns the load it starts the system at.
func waitForLoad(ctx context.Context, out io.Writer, o options) float64 {
	deadline := time.Now().Add(o.loadWait)
	for {
		load, err := loadAvg()
		if err != nil || load <= o.maxLoad {
			return load
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(out, "load %.2f is still above %.0f after %s: running anyway\n", load, o.maxLoad, o.loadWait)
			return load
		}
		fmt.Fprintf(out, "load %.2f is above %.0f: waiting\n", load, o.maxLoad)
		select {
		case <-ctx.Done():
			return load
		case <-time.After(30 * time.Second):
		}
	}
}

// loadAvg is the 1-minute load average.
func loadAvg() (float64, error) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(strings.Fields(string(b))[0], 64)
	}
	s, err := sysctl("vm.loadavg") // "{ 1.23 2.34 3.45 }"
	if err != nil {
		return 0, err
	}
	f := strings.Fields(strings.Trim(s, "{} "))
	if len(f) == 0 {
		return 0, fmt.Errorf("vm.loadavg: %q", s)
	}
	return strconv.ParseFloat(f[0], 64)
}

func sysctl(name string) (string, error) {
	b, err := exec.Command("sysctl", "-n", name).Output()
	return strings.TrimSpace(string(b)), err
}

// printEnv prints what a reader needs to judge the numbers: machine, OS,
// Go, library and server versions, load, and the workload.
func printEnv(ctx context.Context, out io.Writer, o options) {
	fmt.Fprintln(out, "# ubgo/workflow bench")
	fmt.Fprintln(out, "date:", time.Now().UTC().Format(time.RFC3339))
	cpu, _ := sysctl("machdep.cpu.brand_string")
	mem, _ := sysctl("hw.memsize")
	memGiB := ""
	if n, err := strconv.ParseInt(mem, 10, 64); err == nil {
		memGiB = fmt.Sprintf(", %d GiB", n>>30)
	}
	osver, _ := exec.Command("sw_vers", "-productVersion").Output()
	fmt.Fprintf(out, "machine: %s, %d CPUs%s, %s/%s %s\n", cpu, runtime.NumCPU(), memGiB, runtime.GOOS, runtime.GOARCH, strings.TrimSpace(string(osver)))
	fmt.Fprintln(out, "go:", runtime.Version())
	if load, err := loadAvg(); err == nil {
		fmt.Fprintf(out, "load (1 min) at start: %.2f\n", load)
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if interesting(d.Path) {
				v := d.Version
				if d.Replace != nil {
					v = "working tree (" + d.Replace.Path + ")"
				}
				fmt.Fprintf(out, "lib: %s %s\n", d.Path, v)
			}
		}
	}
	if o.pgDSN != "" {
		if v, err := (&pgServer{admin: o.pgDSN}).version(ctx); err == nil {
			fmt.Fprintln(out, "postgres server:", v)
		}
	}
	if o.redis != "" {
		rdb := redis.NewClient(&redis.Options{Addr: o.redis, Password: os.Getenv(envRedisPassword)})
		if info, err := rdb.Info(ctx, "server").Result(); err == nil {
			for line := range strings.SplitSeq(info, "\n") {
				if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
					fmt.Fprintln(out, "redis server:", v)
				}
			}
		}
		rdb.Close()
	}
	c := o.cfg
	producer := "the worker's own client"
	if c.Separate {
		producer = "a separate worker-less client"
	}
	fmt.Fprintf(out, "workload: %d jobs, concurrency %d, payload %d B, warm-up %d, %d latency probes after %s-%s idle pauses, enqueued through %s\n",
		c.Jobs, c.Concurrency, c.PayloadSize, c.Warmup, c.LatSamples, c.LatGapMin, c.LatGapMin+c.LatGapSpan, producer)
	if o.pgPool > 0 {
		fmt.Fprintf(out, "sqlstore Postgres pool: %d connections\n", o.pgPool)
	}
}

// interesting names the dependencies whose versions affect the numbers.
func interesting(path string) bool {
	for _, p := range []string{
		"github.com/ubgo/workflow", "github.com/ubgo/sqlb",
		"github.com/hatchet-dev/hatchet", "github.com/jackc/pgx/v5",
		"github.com/redis/go-redis/v9", "modernc.org/sqlite",
	} {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// printResults prints one Markdown table row per system.
func printResults(out io.Writer, rs []result) {
	fmt.Fprintf(out, "\n| System | Enqueue jobs/s | End-to-end jobs/s | Pickup p50 | p95 | p99 | max | Duplicates |\n")
	fmt.Fprintf(out, "|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rs {
		printRow(out, r)
	}
}

// printRow prints one system's Markdown table row.
func printRow(out io.Writer, r result) {
	switch {
	case errors.Is(r.Err, errNoProducer):
		fmt.Fprintf(out, "| %s | n/a: %v | | | | | | |\n", r.System, r.Err)
	case r.Err != nil:
		// One table row: a joined error's lines become "; ".
		msg := strings.ReplaceAll(r.Err.Error(), "\n", "; ")
		fmt.Fprintf(out, "| %s | failed: %s | | | | | | |\n", r.System, msg)
	default:
		fmt.Fprintf(out, "| %s | %.0f | %.0f | %s | %s | %s | %s | %d |\n",
			r.System, r.EnqueuePerS, r.E2EPerS, ms(r.P50), ms(r.P95), ms(r.P99), ms(r.Max), r.Dups)
	}
	if r.CleanupErr != nil {
		fmt.Fprintf(out, "%s: cleanup failed, check for leftovers: %s\n", r.System, strings.ReplaceAll(r.CleanupErr.Error(), "\n", "; "))
	}
}

// ms formats a duration in milliseconds with two decimals.
func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 2, 64) + " ms"
}
