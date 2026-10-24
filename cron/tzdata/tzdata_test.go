package tzdata_test

import (
	"testing"
	"time"

	"github.com/ubgo/workflow/cron"
	_ "github.com/ubgo/workflow/cron/tzdata"
)

// TestZoneLoadsWithPackageLinked loads a zone with the package linked. On
// a host with a zoneinfo database Go reads the host's copy first, so this
// proves the import builds and leaves loading intact; the embedded copy
// is the standard library's time/tzdata, whose fallback Go itself tests.
func TestZoneLoadsWithPackageLinked(t *testing.T) {
	loc, err := cron.LoadZone("Europe/Berlin")
	if err != nil {
		t.Fatalf("LoadZone: %v", err)
	}
	at := time.Date(2026, 7, 1, 12, 0, 0, 0, loc)
	if _, off := at.Zone(); off != 2*3600 {
		t.Fatalf("Berlin summer offset = %d, want 7200", off)
	}
}
