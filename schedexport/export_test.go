package schedexport_test

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/clock"
	"github.com/ubgo/workflow/cron"
	"github.com/ubgo/workflow/schedexport"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func zone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := cron.LoadZone(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// golden compares got with testdata/name, or rewrites it with -update.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("%s differs:\n--- got\n%s\n--- want\n%s", name, got, want)
	}
}

func check(t *testing.T, f schedexport.File, err error, name, file string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if f.Name != name {
		t.Errorf("file name %q, want %q", f.Name, name)
	}
	golden(t, file, f.Data)
}

// report is the schedule most tests export: 03:00 on weekdays in Berlin.
func report(t *testing.T) workflow.Schedule {
	return workflow.Schedule{
		Name:     "nightly-report",
		Workflow: "report",
		Cron:     "0 3 * * MON-FRI",
		Location: zone(t, "Europe/Berlin"),
		Overlap:  workflow.OverlapSkip,
		Misfire:  workflow.Misfire{Policy: workflow.MisfireFireOnce},
	}
}

var app = schedexport.Target{Command: []string{"/usr/local/bin/app", "run-schedule"}, Args: []string{"nightly-report"}}

func TestCronJobGolden(t *testing.T) {
	s := report(t)
	s.Misfire.Within = 10 * time.Minute
	f, err := schedexport.CronJob(s, app, schedexport.CronJobOptions{Image: "ghcr.io/example/app:1.4.2", Namespace: "jobs"})
	check(t, f, err, "nightly-report.yaml", "cronjob.golden")

	utc := workflow.Schedule{Name: "digest", Workflow: "digest", Cron: "@daily", Location: time.UTC, Misfire: workflow.Misfire{Policy: workflow.MisfireSkip}}
	f, err = schedexport.CronJob(utc, schedexport.Target{Args: []string{"digest"}}, schedexport.CronJobOptions{Image: "app:2", Suspend: true, RestartPolicy: "Never"})
	check(t, f, err, "digest.yaml", "cronjob_utc.golden")
}

// TestCronJobSchedule checks the rewritten five-field expression keeps
// our day rule under the controller's parser, which ORs the day fields
// unless one is a literal "*".
func TestCronJobSchedule(t *testing.T) {
	cases := []struct{ expr, want string }{
		{"0 9 * * MON-FRI", "0 9 * * 1-5"},
		{"*/15 * * * *", "*/15 * * * *"},
		{"0 0 1 * MON", "0 0 1 * 1"},      // OR: both restricted, no star
		{"0 0 1-31 * MON", "0 0 * * *"},   // OR with one field matching every day
		{"0 0 */2 * *", "0 0 1-31/2 * *"}, // AND, written without a leading "*"
		{"0 0 * * 7", "0 0 * * 0"},
		{"@weekly", "0 0 * * 0"},
		{"5,10,30 8-18/2 * JAN,JUL *", "5,10,30 8-18/2 * 1,7 *"},
	}
	for _, c := range cases {
		s := workflow.Schedule{Name: "x", Workflow: "w", Cron: c.expr, Location: time.UTC}
		f, err := schedexport.CronJob(s, app, schedexport.CronJobOptions{Image: "app"})
		if err != nil {
			t.Errorf("%q: %v", c.expr, err)
			continue
		}
		if line := `schedule: "` + c.want + `"`; !strings.Contains(string(f.Data), line) {
			t.Errorf("%q: manifest lacks %s:\n%s", c.expr, line, f.Data)
		}
	}
}

func TestSystemdGolden(t *testing.T) {
	files, err := schedexport.SystemdTimer(report(t), app)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Name != "nightly-report.timer" || files[1].Name != "nightly-report.service" {
		t.Fatalf("files %v", files)
	}
	golden(t, "systemd.timer.golden", files[0].Data)
	golden(t, "systemd.service.golden", files[1].Data)
}

// TestSystemdOnCalendar covers seconds, years, steps and the OR day rule
// written as two OnCalendar= lines.
func TestSystemdOnCalendar(t *testing.T) {
	cases := []struct {
		expr   string
		parser cron.Parser
		want   []string
	}{
		{"0 3 * * MON-FRI", nil, []string{"Mon..Fri *-*-* 03:00:00 UTC"}},
		{"*/15 9-17 * * *", nil, []string{"*-*-* 09..17:00/15:00 UTC"}},
		{"0 0 1,15 * SUN", nil, []string{"*-*-01,15 00:00:00 UTC", "Sun *-*-* 00:00:00 UTC"}},
		{"0 0 */2 * SAT,SUN", nil, []string{"Sat,Sun *-*-01/2 00:00:00 UTC"}},
		{"*/20 30 8 * 1,6 * 2026-2028", cron.ParseYear, []string{"2026..2028-01,06-* 08:30:00/20 UTC"}},
		{"0 0 12 29 FEB * 2028,2032", cron.ParseYear, []string{"2028,2032-02-29 12:00:00 UTC"}},
	}
	for _, c := range cases {
		s := workflow.Schedule{Name: "x", Workflow: "w", Cron: c.expr, Location: time.UTC, Overlap: workflow.OverlapSkip}
		tg := app
		tg.Parser = c.parser
		files, err := schedexport.SystemdTimer(s, tg)
		if err != nil {
			t.Errorf("%q: %v", c.expr, err)
			continue
		}
		var got []string
		for _, l := range strings.Split(string(files[0].Data), "\n") {
			if v, ok := strings.CutPrefix(l, "OnCalendar="); ok {
				got = append(got, v)
			}
		}
		if strings.Join(got, "|") != strings.Join(c.want, "|") {
			t.Errorf("%q: OnCalendar %q, want %q", c.expr, got, c.want)
		}
	}
}

func TestLaunchdGolden(t *testing.T) {
	s := report(t)
	s.Cron = "*/30 9 * * 1-5"
	s.Misfire = workflow.Misfire{}
	f, err := schedexport.Launchd(s, schedexport.Target{Name: "com.example.report", Command: app.Command, Args: app.Args}, schedexport.LaunchdOptions{Zone: s.Location})
	check(t, f, err, "com.example.report.plist", "launchd.golden")
}

func TestTaskSchedulerGolden(t *testing.T) {
	s := report(t)
	s.Cron = "*/15 9-17 * * MON-FRI"
	s.Active = workflow.Active{From: time.Date(2026, 3, 2, 12, 0, 0, 0, s.Location), Until: time.Date(2027, 1, 1, 0, 0, 0, 0, s.Location)}
	tg := schedexport.Target{Name: "Nightly report", Command: []string{`C:\Program Files\App\app.exe`, "run-schedule"}, Args: []string{"nightly report", `say "hi"`}}
	f, err := schedexport.TaskScheduler(s, tg, schedexport.TaskSchedulerOptions{Zone: s.Location})
	check(t, f, err, "Nightly report.xml", "taskscheduler.golden")

	monthly := report(t)
	monthly.Cron = "0 8,20 1,15 JAN,JUL *"
	monthly.Overlap = ""
	monthly.Misfire = workflow.Misfire{}
	f, err = schedexport.TaskScheduler(monthly, tg, schedexport.TaskSchedulerOptions{Zone: monthly.Location})
	check(t, f, err, "Nightly report.xml", "taskscheduler_monthly.golden")
}

// TestNextFireGolden writes each OS format's one-shot form at a fixed
// clock: Monday 5 January 2026, 10:00 UTC, so the next weekday 03:00 in
// Berlin is Tuesday 02:00 UTC.
func TestNextFireGolden(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC))
	s := report(t)
	files, err := schedexport.SystemdTimerNextFire(s, app, clk)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "systemd_next.timer.golden", files[0].Data)

	f, err := schedexport.TaskSchedulerNextFire(s, app, clk)
	check(t, f, err, "nightly-report.xml", "taskscheduler_next.golden")

	s.Misfire = workflow.Misfire{}
	f, err = schedexport.LaunchdNextFire(s, app, schedexport.LaunchdOptions{Zone: zone(t, "America/New_York")}, clk)
	check(t, f, err, "nightly-report.plist", "launchd_next.golden")
}

// TestNextFireHonoursWindowsActiveAndYears: the one-shot forms skip
// ticks outside the windows, before Active and in another year, and
// report ErrNoNextFire once the schedule is over.
func TestNextFireHonoursWindowsActiveAndYears(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC))
	s := workflow.Schedule{Name: "x", Workflow: "w", Cron: "0 * * * *", Location: time.UTC, Windows: workflow.Windows{Between: []workflow.Window{{From: "22:00", To: "23:00"}}}}
	files, err := schedexport.SystemdTimerNextFire(s, app, clk)
	if err != nil || !strings.Contains(string(files[0].Data), "OnCalendar=2026-01-05 22:00:00 UTC\n") {
		t.Fatalf("windows: %v\n%s", err, files)
	}
	s = workflow.Schedule{Name: "x", Workflow: "w", Cron: "0 0 9 1 * * 2027", Location: time.UTC}
	f, err := schedexport.TaskSchedulerNextFire(s, schedexport.Target{Command: app.Command, Parser: cron.ParseYear}, clk)
	if err != nil || !strings.Contains(string(f.Data), "<StartBoundary>2027-01-01T09:00:00Z</StartBoundary>") {
		t.Fatalf("year: %v\n%s", err, f.Data)
	}
	clk.Set(time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
	if _, err := schedexport.TaskSchedulerNextFire(s, schedexport.Target{Command: app.Command, Parser: cron.ParseYear}, clk); !errors.Is(err, schedexport.ErrNoNextFire) {
		t.Fatalf("after the last year: %v, want ErrNoNextFire", err)
	}
	s = workflow.Schedule{Name: "x", Workflow: "w", Cron: "0 9 * * *", Location: time.UTC, Active: workflow.Active{Until: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)}}
	if _, err := schedexport.SystemdTimerNextFire(s, app, clk); !errors.Is(err, schedexport.ErrNoNextFire) {
		t.Fatalf("after Active: %v, want ErrNoNextFire", err)
	}
}

// TestRefusals: what a format cannot express exactly is refused with a
// NotExpressibleError naming the field, never written without it.
//
//ds:def id=schedexport-testrefusals-8mg3qn96 owner=@khanakia stability=stable
func TestRefusals(t *testing.T) {
	berlin := zone(t, "Europe/Berlin")
	base := func(mod func(*workflow.Schedule)) workflow.Schedule {
		s := workflow.Schedule{Name: "x", Workflow: "w", Cron: "0 9 * * *", Location: berlin, Overlap: workflow.OverlapSkip}
		mod(&s)
		return s
	}
	k8s := func(s workflow.Schedule, tg schedexport.Target) error {
		_, err := schedexport.CronJob(s, tg, schedexport.CronJobOptions{Image: "app"})
		return err
	}
	systemd := func(s workflow.Schedule, tg schedexport.Target) error {
		_, err := schedexport.SystemdTimer(s, tg)
		return err
	}
	launchd := func(s workflow.Schedule, tg schedexport.Target) error {
		_, err := schedexport.Launchd(s, tg, schedexport.LaunchdOptions{Zone: berlin})
		return err
	}
	tasks := func(s workflow.Schedule, tg schedexport.Target) error {
		_, err := schedexport.TaskScheduler(s, tg, schedexport.TaskSchedulerOptions{Zone: berlin})
		return err
	}
	year := app
	year.Parser = cron.ParseYear
	cases := []struct {
		name   string
		export func(workflow.Schedule, schedexport.Target) error
		s      workflow.Schedule
		target schedexport.Target
		field  string
	}{
		{"k8s seconds", k8s, base(func(s *workflow.Schedule) { s.Cron, s.Seconds = "30 0 9 * * *", true }), app, "second"},
		{"k8s year", k8s, base(func(s *workflow.Schedule) { s.Cron = "0 0 9 * * * 2027" }), year, "year"},
		{"k8s every", k8s, base(func(s *workflow.Schedule) { s.Cron = "@every 90s" }), app, "Cron"},
		{"k8s stepped day AND", k8s, base(func(s *workflow.Schedule) { s.Cron = "0 0 */2 * MON" }), app, "day-of-month"},
		{"k8s fire all", k8s, base(func(s *workflow.Schedule) { s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireAll, Max: 3} }), app, "Misfire"},
		{"k8s sub-second within", k8s, base(func(s *workflow.Schedule) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: 1500 * time.Millisecond}
		}), app, "Misfire"},
		{"k8s active", k8s, base(func(s *workflow.Schedule) { s.Active.Until = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }), app, "Active"},
		{"k8s windows", k8s, base(func(s *workflow.Schedule) { s.Windows.Between = []workflow.Window{{From: "08:00", To: "18:00"}} }), app, "Windows"},
		{"k8s fixed zone", k8s, base(func(s *workflow.Schedule) { s.Location = time.FixedZone("X", 3600) }), app, "Location"},
		{"systemd overlap allow", systemd, base(func(s *workflow.Schedule) { s.Overlap = "" }), app, "Overlap"},
		{"systemd within", systemd, base(func(s *workflow.Schedule) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: time.Hour}
		}), app, "Misfire"},
		{"systemd every", systemd, base(func(s *workflow.Schedule) { s.Cron = "@every 1h" }), app, "Cron"},
		{"systemd active", systemd, base(func(s *workflow.Schedule) { s.Active.From = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }), app, "Active"},
		{"launchd other zone", launchd, base(func(s *workflow.Schedule) { s.Location = time.UTC }), app, "Location"},
		{"launchd seconds", launchd, base(func(s *workflow.Schedule) { s.Cron, s.Seconds = "30 0 9 * * *", true }), app, "second"},
		{"launchd misfire", launchd, base(func(s *workflow.Schedule) { s.Misfire = workflow.Misfire{Policy: workflow.MisfireSkip} }), app, "Misfire"},
		{"launchd overlap allow", launchd, base(func(s *workflow.Schedule) { s.Overlap = workflow.OverlapAllow }), app, "Overlap"},
		{"launchd day AND", launchd, base(func(s *workflow.Schedule) { s.Cron = "0 9 */2 * 1" }), app, "day-of-month"},
		{"launchd too many", launchd, base(func(s *workflow.Schedule) { s.Cron = "*/2 */2 1-30 * *" }), app, "Cron"},
		{"tasks day OR", tasks, base(func(s *workflow.Schedule) { s.Cron = "0 9 1 * MON" }), app, "day-of-month"},
		{"tasks day AND", tasks, base(func(s *workflow.Schedule) { s.Cron = "0 9 */2 * MON" }), app, "day-of-month"},
		{"tasks year gaps", tasks, base(func(s *workflow.Schedule) { s.Cron = "0 0 9 * * * 2026,2028" }), year, "year"},
		{"tasks too many times", tasks, base(func(s *workflow.Schedule) { s.Cron, s.Seconds = "*/10 * 9 * * *", true }), app, "Cron"},
		{"tasks other zone", tasks, base(func(s *workflow.Schedule) { s.Location = time.UTC }), app, "Location"},
		{"tasks within", tasks, base(func(s *workflow.Schedule) {
			s.Misfire = workflow.Misfire{Policy: workflow.MisfireFireOnce, Within: time.Hour}
		}), app, "Misfire"},
	}
	for _, c := range cases {
		err := c.export(c.s, c.target)
		var ne *schedexport.NotExpressibleError
		if !errors.As(err, &ne) || !errors.Is(err, schedexport.ErrNotExpressible) {
			t.Errorf("%s: %v, want a NotExpressibleError", c.name, err)
			continue
		}
		if ne.Field != c.field {
			t.Errorf("%s: field %q, want %q (%v)", c.name, ne.Field, c.field, err)
		}
	}
}

// TestInvalid: input a format cannot take at all is ErrInvalid, before
// anything is written.
func TestInvalid(t *testing.T) {
	ok := workflow.Schedule{Name: "x", Workflow: "w", Cron: "0 9 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip}
	noName := ok
	noName.Name = ""
	badCron := ok
	badCron.Cron = "0 25 * * *"
	noZone := ok
	noZone.Location = nil
	cases := []struct {
		name string
		err  error
	}{
		{"no zone", func() error { _, err := schedexport.SystemdTimer(noZone, app); return err }()},
		{"bad cron", func() error { _, err := schedexport.SystemdTimer(badCron, app); return err }()},
		{"no name", func() error { _, err := schedexport.SystemdTimer(noName, app); return err }()},
		{"no command", func() error { _, err := schedexport.SystemdTimer(ok, schedexport.Target{}); return err }()},
		{"exec prefix", func() error {
			_, err := schedexport.SystemdTimer(ok, schedexport.Target{Command: []string{"-/bin/true"}})
			return err
		}()},
		{"newline", func() error {
			_, err := schedexport.SystemdTimer(ok, schedexport.Target{Command: []string{"/bin/true"}, Description: "a\nExecStart=/bin/sh"})
			return err
		}()},
		{"k8s no image", func() error { _, err := schedexport.CronJob(ok, app, schedexport.CronJobOptions{}); return err }()},
		{"k8s name", func() error {
			_, err := schedexport.CronJob(ok, schedexport.Target{Name: "Nightly_Report"}, schedexport.CronJobOptions{Image: "app"})
			return err
		}()},
		{"k8s restart", func() error {
			_, err := schedexport.CronJob(ok, app, schedexport.CronJobOptions{Image: "app", RestartPolicy: "Always"})
			return err
		}()},
		{"launchd no zone", func() error { _, err := schedexport.Launchd(ok, app, schedexport.LaunchdOptions{}); return err }()},
		{"tasks no zone", func() error {
			_, err := schedexport.TaskScheduler(ok, app, schedexport.TaskSchedulerOptions{})
			return err
		}()},
		{"tasks name", func() error {
			_, err := schedexport.TaskScheduler(ok, schedexport.Target{Name: `a\b`, Command: app.Command}, schedexport.TaskSchedulerOptions{Zone: time.UTC})
			return err
		}()},
	}
	for _, c := range cases {
		if !errors.Is(c.err, workflow.ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", c.name, c.err)
		}
	}
}

// TestQuoting: arguments with spaces, quotes and expansion characters
// reach the program as written.
func TestQuoting(t *testing.T) {
	s := workflow.Schedule{Name: "q", Workflow: "w", Cron: "0 9 * * *", Location: time.UTC, Overlap: workflow.OverlapSkip}
	tg := schedexport.Target{Command: []string{"/bin/app"}, Args: []string{"a b", `say "hi"`, "100%", "$HOME", ""}, Description: "50% <done>"}
	files, err := schedexport.SystemdTimer(s, tg)
	if err != nil {
		t.Fatal(err)
	}
	if want := `ExecStart=/bin/app "a b" "say \"hi\"" 100%% $$HOME ""` + "\n"; !strings.Contains(string(files[1].Data), want) {
		t.Errorf("service:\n%s\nwant line %s", files[1].Data, want)
	}
	if !strings.Contains(string(files[0].Data), "Description=50%% <done>\n") {
		t.Errorf("timer description not escaped:\n%s", files[0].Data)
	}
	f, err := schedexport.TaskSchedulerNextFire(s, tg, clock.NewFake(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	if want := `<Arguments>"a b" "say \"hi\"" 100% $HOME ""</Arguments>`; !strings.Contains(string(f.Data), want) {
		t.Errorf("task:\n%s\nwant %s", f.Data, want)
	}
	if !strings.Contains(string(f.Data), "<Description>50% &lt;done&gt;</Description>") {
		t.Errorf("task description not escaped:\n%s", f.Data)
	}
}
