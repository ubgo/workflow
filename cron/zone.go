package cron

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrZone is wrapped by every error LoadZone returns.
var ErrZone = errors.New("cron: invalid time zone")

// LoadZone loads an IANA zone by name for a schedule, so an unknown or
// implicit zone fails when the app boots, not as a schedule that silently
// fires at the wrong hour. It refuses "" and "Local" (both mean the host's
// zone, which differs from machine to machine; time.LoadLocation turns ""
// into UTC and "Local" into the host zone without a word) and any name
// time.LoadLocation cannot find. The error wraps ErrZone and names the
// zone.
//
// A container image without a zoneinfo database finds no zone but UTC;
// import the cron/tzdata package (or time/tzdata) to embed one.
func LoadZone(name string) (*time.Location, error) {
	trimmed := strings.TrimSpace(name)
	switch trimmed {
	case "":
		return nil, fmt.Errorf("%w: empty zone name; use \"UTC\" for UTC", ErrZone)
	case "Local":
		return nil, fmt.Errorf("%w: %q is the host's zone and differs between machines; name the zone", ErrZone, name)
	}
	loc, err := time.LoadLocation(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w (import github.com/ubgo/workflow/cron/tzdata when the host has no zoneinfo)", ErrZone, name, err)
	}
	return loc, nil
}
