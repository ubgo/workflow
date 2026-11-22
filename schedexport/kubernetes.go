package schedexport

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ubgo/workflow"
	"github.com/ubgo/workflow/cron"
)

// CronJobOptions are the parts of a Kubernetes CronJob a Schedule does not
// carry.
//
//ds:def id=schedexport-cronjoboptions-hzyhec52 owner=@khanakia stability=stable
type CronJobOptions struct {
	// Image is the container image. Required.
	Image string
	// Namespace is metadata.namespace; empty leaves it to kubectl.
	Namespace string
	// Container is the container's name; empty means "job".
	Container string
	// RestartPolicy is "OnFailure" (the default when empty) or "Never".
	RestartPolicy string
	// Suspend writes spec.suspend: true, so the CronJob exists but
	// starts nothing until it is unsuspended.
	Suspend bool
}

// The CronJob controller adds a suffix to the CronJob's name for each
// Job, so the name is held to 52 characters.
const maxCronJobName = 52

var (
	dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	dnsLabel     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

// CronJob writes s as a Kubernetes batch/v1 CronJob manifest in YAML. The
// mapping:
//
//   - spec.schedule is the five-field expression rewritten from its parsed
//     fields (names, macros and Sunday as 7 resolved), so the controller's
//     parser reads the same days ours does. A seconds field other than 0,
//     a year field, "@every" and a day-of-month/day-of-week pair that the
//     controller combines differently are refused.
//   - spec.timeZone is the Location's IANA name, left out for UTC.
//   - spec.concurrencyPolicy is Allow for OverlapAllow and Forbid for
//     OverlapSkip.
//   - spec.startingDeadlineSeconds: MisfireSkip writes workflow.MisfireGrace
//     (60), MisfireFireOnce with Within writes Within in whole seconds,
//     MisfireFireOnce without Within and the zero policy leave it out (the
//     controller then starts the newest missed tick, as fire-once does).
//     MisfireFireAll is refused: the controller never starts every missed
//     tick.
//   - Active and Windows are refused: a CronJob has neither.
//
// Target.Command overrides the image's entrypoint and Target.Args are the
// container's args; both may be empty. The file is named <name>.yaml.
//
//ds:def id=schedexport-cronjob-haa7av5t owner=@khanakia stability=stable
func CronJob(s workflow.Schedule, t Target, o CronJobOptions) (File, error) {
	const f = FormatKubernetes
	j, err := prepare(f, s, t)
	if err != nil {
		return File{}, err
	}
	if len(j.name) > maxCronJobName || !dnsSubdomain.MatchString(j.name) {
		return File{}, invalid(f, "name %q must be lower-case letters, digits, '-' and '.', at most %d characters", j.name, maxCronJobName)
	}
	if o.Namespace != "" && !dnsLabel.MatchString(o.Namespace) {
		return File{}, invalid(f, "namespace %q is not a DNS label", o.Namespace)
	}
	container := o.Container
	if container == "" {
		container = "job"
	}
	if len(container) > 63 || !dnsLabel.MatchString(container) {
		return File{}, invalid(f, "container name %q is not a DNS label", container)
	}
	restart := o.RestartPolicy
	switch restart {
	case "":
		restart = "OnFailure"
	case "OnFailure", "Never":
	default:
		return File{}, invalid(f, "RestartPolicy %q: want OnFailure or Never", restart)
	}
	if strings.TrimSpace(o.Image) == "" || strings.IndexFunc(o.Image, isControl) >= 0 {
		return File{}, invalid(f, "CronJobOptions.Image is required")
	}
	if err := j.noWindows(f); err != nil {
		return File{}, err
	}
	if !s.Active.IsZero() {
		return File{}, refuse(f, "Active", "a CronJob has no start or end; create and delete it at those times")
	}
	fl, err := j.fields(f)
	if err != nil {
		return File{}, err
	}
	expr, err := fiveFields(f, fl)
	if err != nil {
		return File{}, err
	}
	zone, err := zoneName(f, s.Location)
	if err != nil {
		return File{}, err
	}
	policy := "Allow"
	if s.Overlap == workflow.OverlapSkip {
		policy = "Forbid"
	}
	m, err := classify(f, s.Misfire)
	if err != nil {
		return File{}, err
	}
	deadline := ""
	switch m {
	case misfireSkip:
		deadline = strconv.Itoa(int(workflow.MisfireGrace / time.Second))
	case misfireDeadline:
		if s.Misfire.Within%time.Second != 0 {
			return File{}, refuse(f, "Misfire", "Within %s: startingDeadlineSeconds takes whole seconds", s.Misfire.Within)
		}
		deadline = strconv.Itoa(int(s.Misfire.Within / time.Second))
	}

	var b strings.Builder
	w := func(indent int, line string) {
		b.WriteString(strings.Repeat("  ", indent))
		b.WriteString(line)
		b.WriteByte('\n')
	}
	w(0, "apiVersion: batch/v1")
	w(0, "kind: CronJob")
	w(0, "metadata:")
	w(1, "name: "+yamlString(j.name))
	if o.Namespace != "" {
		w(1, "namespace: "+yamlString(o.Namespace))
	}
	w(0, "spec:")
	w(1, "schedule: "+yamlString(expr))
	if zone != "" {
		w(1, "timeZone: "+yamlString(zone))
	}
	w(1, "concurrencyPolicy: "+policy)
	if deadline != "" {
		w(1, "startingDeadlineSeconds: "+deadline)
	}
	w(1, "suspend: "+strconv.FormatBool(o.Suspend))
	w(1, "jobTemplate:")
	w(2, "spec:")
	w(3, "template:")
	w(4, "spec:")
	w(5, "restartPolicy: "+restart)
	w(5, "containers:")
	w(6, "- name: "+yamlString(container))
	w(7, "image: "+yamlString(o.Image))
	if len(t.Command) > 0 {
		w(7, "command: "+yamlList(t.Command))
	}
	if len(t.Args) > 0 {
		w(7, "args: "+yamlList(t.Args))
	}
	return File{Name: j.name + ".yaml", Data: []byte(b.String())}, nil
}

// fiveFields writes fl as a standard five-field expression that the
// Kubernetes controller's parser reads with our meaning. That parser ORs
// the two day fields unless one of them is a literal "*" (a stepped "*/2"
// counts as restricted), so the day fields are written to keep the day
// rule: "*" only where the rule is AND with that field unrestricted.
//
//ds:def id=schedexport-fivefields-zajpjqpq owner=@khanakia stability=stable
func fiveFields(f Format, fl cron.Fields) (string, error) {
	if len(fl.Second) != 1 || fl.Second[0] != 0 {
		return "", refuse(f, "second", "it fires at most once a minute, at second 0")
	}
	if fl.Year != nil {
		return "", refuse(f, "year", "it has no year field")
	}
	dom, dow, err := dayFields(f, fl, func(vals []int, lo, hi int) string { return cronList(vals, lo, hi, false) })
	if err != nil {
		return "", err
	}
	return strings.Join([]string{
		cronList(fl.Minute, 0, 59, true),
		cronList(fl.Hour, 0, 23, true),
		dom,
		cronList(fl.Month, 1, 12, true),
		dow,
	}, " "), nil
}

// dayFields writes the day-of-month and day-of-week fields for a parser
// that ANDs them when one is "*" and ORs them otherwise. list writes a
// restricted field without a leading "*".
//
//ds:def id=schedexport-dayfields-2vyg9nm3 owner=@khanakia stability=stable
func dayFields(f Format, fl cron.Fields, list func([]int, int, int) string) (string, string, error) {
	domFull, dowFull := full(fl.DayOfMonth, 1, 31), full(fl.DayOfWeek, 0, 6)
	switch {
	case fl.DayOr && !domFull && !dowFull:
		return list(fl.DayOfMonth, 1, 31), list(fl.DayOfWeek, 0, 6), nil
	case fl.DayOr || (domFull && dowFull):
		return "*", "*", nil // one field matches every day, so every day does
	case domFull:
		return "*", list(fl.DayOfWeek, 0, 6), nil
	case dowFull:
		return list(fl.DayOfMonth, 1, 31), "*", nil
	}
	return "", "", refuse(f, "day-of-month", "both day fields are restricted with a stepped \"*\", which needs both to match; its parser would match either")
}

// cronList writes a field's values in cron syntax: "*" for every value,
// runs as "a-b" and stepped runs as "a-b/n" (or "*/n" when star is true
// and the run spans the field).
func cronList(vals []int, lo, hi int, star bool) string {
	if full(vals, lo, hi) {
		return "*"
	}
	var items []string
	for _, sp := range spans(vals) {
		switch {
		case sp.lo == sp.hi:
			items = append(items, strconv.Itoa(sp.lo))
		case sp.step == 1:
			items = append(items, strconv.Itoa(sp.lo)+"-"+strconv.Itoa(sp.hi))
		case star && sp.lo == lo && sp.hi+sp.step > hi:
			items = append(items, "*/"+strconv.Itoa(sp.step))
		default:
			items = append(items, strconv.Itoa(sp.lo)+"-"+strconv.Itoa(sp.hi)+"/"+strconv.Itoa(sp.step))
		}
	}
	return strings.Join(items, ",")
}

// yamlString writes s as a YAML double-quoted scalar. Go's quoting
// escapes (\n, \t, \\, \", \xNN, \uNNNN) are all valid in YAML.
func yamlString(s string) string { return strconv.Quote(s) }

// yamlList writes a flow sequence of double-quoted scalars.
func yamlList(vals []string) string {
	items := make([]string, len(vals))
	for i, v := range vals {
		items[i] = yamlString(v)
	}
	return "[" + strings.Join(items, ", ") + "]"
}
