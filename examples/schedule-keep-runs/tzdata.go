package main

// The zone import sits in a file of its own so the guide can show it as a
// complete Go file: one blank import in the main package is all it takes.

// snippet:tzdata
// Embeds the IANA zone database, so cron.LoadZone("Europe/Berlin") works
// in a scratch or distroless image; a host's own copy still wins.
import _ "github.com/ubgo/workflow/cron/tzdata"

// snippet:end
