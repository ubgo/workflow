// Package tzdata embeds the IANA time zone database in the binary, for
// apps that run schedules in a container image without one (scratch,
// distroless and many slim images ship no /usr/share/zoneinfo).
//
// Without a database, time.LoadLocation finds only "UTC", so a schedule
// in "Europe/Berlin" fails to load; with cron.LoadZone that failure is
// loud at boot, but the fix is this package. Import it once, for its side
// effect, in the main package:
//
//	import _ "github.com/ubgo/workflow/cron/tzdata"
//
// It is opt-in because it adds about 450 KB to the binary, which an app
// on a host with zoneinfo does not need. It is the standard library's
// time/tzdata under a name that says why a scheduler imports it; Go uses
// the embedded copy only when the host has no database (or ZONEINFO
// points nowhere), so a host's newer rules still win.
package tzdata

import _ "time/tzdata" // embeds the zone database; see the package doc
