package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ubgo/workflow/cron"
)

// ScheduleJSONVersion is the version of the JSON form of a Schedule. A
// reader refuses any other version, and any field it does not know, so a
// schedule stored by a newer release is never loaded with part of its
// meaning (a window, a misfire policy) silently dropped.
const ScheduleJSONVersion = 1

// ErrScheduleJSON is wrapped by every error of the JSON form.
var ErrScheduleJSON = errors.New("workflow: invalid schedule JSON")

// scheduleJSON is the stored form: the zone by IANA name, durations and
// instants as text, so the document is readable and diffable.
type scheduleJSON struct {
	V        int               `json:"v"`
	Name     string            `json:"name,omitempty"`
	Workflow string            `json:"workflow"`
	Cron     string            `json:"cron"`
	Location string            `json:"location,omitempty"`
	Input    []byte            `json:"input,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	Misfire  *misfireJSON      `json:"misfire,omitempty"`
	Windows  *windowsJSON      `json:"windows,omitempty"`
	Overlap  OverlapPolicy     `json:"overlap,omitempty"`
	Seconds  bool              `json:"seconds,omitempty"`
	Active   *activeJSON       `json:"active,omitempty"`
	KeepRuns int               `json:"keep_runs,omitempty"`
	Tenant   string            `json:"tenant,omitempty"`
}

// activeJSON holds the period's bounds as RFC 3339 instants; an absent
// bound is no bound.
type activeJSON struct {
	From  *time.Time `json:"from,omitempty"`
	Until *time.Time `json:"until,omitempty"`
}

type misfireJSON struct {
	Policy MisfirePolicy `json:"policy,omitempty"`
	Within string        `json:"within,omitempty"`
	Max    int           `json:"max,omitempty"`
}

type windowsJSON struct {
	Between []windowJSON `json:"between,omitempty"`
	Unless  []windowJSON `json:"unless,omitempty"`
}

type windowJSON struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// MarshalJSON writes s in its versioned JSON form (ScheduleJSONVersion),
// for storing schedules as data: a settings table, a config file, an
// admin form. The zone is written by its IANA name, so a Location without
// one (time.Local, a time.FixedZone) is refused rather than written as a
// name no reader can load. It does not validate the rest; Validate does.
func (s Schedule) MarshalJSON() ([]byte, error) {
	out := scheduleJSON{V: ScheduleJSONVersion, Name: s.Name, Workflow: s.Workflow, Cron: s.Cron, Input: s.Input, Labels: s.Labels, Overlap: s.Overlap, Seconds: s.Seconds, KeepRuns: s.KeepRuns, Tenant: s.Tenant}
	if !s.Active.IsZero() {
		out.Active = &activeJSON{From: timeOrNil(s.Active.From), Until: timeOrNil(s.Active.Until)}
	}
	if s.Location != nil {
		name := s.Location.String()
		if s.Location == time.Local {
			return nil, fmt.Errorf("%w: time.Local has no portable name; name the zone", ErrScheduleJSON)
		}
		if _, err := cron.LoadZone(name); err != nil {
			return nil, fmt.Errorf("%w: location %q is not a loadable IANA zone: %w", ErrScheduleJSON, name, err)
		}
		out.Location = name
	}
	if !s.Misfire.IsZero() {
		m := &misfireJSON{Policy: s.Misfire.Policy, Max: s.Misfire.Max}
		if s.Misfire.Within != 0 {
			m.Within = s.Misfire.Within.String()
		}
		out.Misfire = m
	}
	if !s.Windows.IsZero() {
		out.Windows = &windowsJSON{Between: windowsOut(s.Windows.Between), Unless: windowsOut(s.Windows.Unless)}
	}
	return json.Marshal(out)
}

// UnmarshalJSON reads the form MarshalJSON writes, loading the zone with
// cron.LoadZone. Use UnmarshalSchedule to load zones another way.
func (s *Schedule) UnmarshalJSON(data []byte) error {
	got, err := UnmarshalSchedule(data, nil)
	if err != nil {
		return err
	}
	*s = got
	return nil
}

// UnmarshalSchedule reads a Schedule's JSON form. loadZone resolves the
// zone name; nil means cron.LoadZone, which refuses "Local" and unknown
// zones, so a stored schedule with a mistyped zone fails at load, not as
// a schedule firing at the wrong hour. A version other than
// ScheduleJSONVersion and unknown fields are refused. Every error wraps
// ErrScheduleJSON.
func UnmarshalSchedule(data []byte, loadZone func(name string) (*time.Location, error)) (Schedule, error) {
	if loadZone == nil {
		loadZone = cron.LoadZone
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var in scheduleJSON
	if err := dec.Decode(&in); err != nil {
		return Schedule{}, fmt.Errorf("%w: %w", ErrScheduleJSON, err)
	}
	if in.V != ScheduleJSONVersion {
		return Schedule{}, fmt.Errorf("%w: version %d, this release reads %d", ErrScheduleJSON, in.V, ScheduleJSONVersion)
	}
	s := Schedule{Name: in.Name, Workflow: in.Workflow, Cron: in.Cron, Input: in.Input, Labels: in.Labels, Overlap: in.Overlap, Seconds: in.Seconds, KeepRuns: in.KeepRuns, Tenant: in.Tenant}
	if in.Active != nil {
		if in.Active.From != nil {
			s.Active.From = *in.Active.From
		}
		if in.Active.Until != nil {
			s.Active.Until = *in.Active.Until
		}
	}
	if in.Location != "" {
		loc, err := loadZone(in.Location)
		if err != nil {
			return Schedule{}, fmt.Errorf("%w: %w", ErrScheduleJSON, err)
		}
		s.Location = loc
	}
	if in.Misfire != nil {
		s.Misfire = Misfire{Policy: in.Misfire.Policy, Max: in.Misfire.Max}
		if in.Misfire.Within != "" {
			d, err := time.ParseDuration(in.Misfire.Within)
			if err != nil {
				return Schedule{}, fmt.Errorf("%w: misfire within: %w", ErrScheduleJSON, err)
			}
			s.Misfire.Within = d
		}
	}
	if in.Windows != nil {
		s.Windows = Windows{Between: windowsIn(in.Windows.Between), Unless: windowsIn(in.Windows.Unless)}
	}
	return s, nil
}

func windowsOut(ws []Window) []windowJSON {
	if len(ws) == 0 {
		return nil
	}
	out := make([]windowJSON, len(ws))
	for i, w := range ws {
		out[i] = windowJSON(w)
	}
	return out
}

func windowsIn(ws []windowJSON) []Window {
	if len(ws) == 0 {
		return nil
	}
	out := make([]Window, len(ws))
	for i, w := range ws {
		out[i] = Window(w)
	}
	return out
}

// timeOrNil is nil for the zero time, so an open bound is left out.
func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
