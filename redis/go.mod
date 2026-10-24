module github.com/ubgo/workflow/redis

go 1.24.0

require (
	github.com/redis/go-redis/v9 v9.22.0
	github.com/ubgo/workflow v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

// The core is developed beside this module and not tagged yet, so it
// resolves from the working tree; tagging the core is a release blocker
// for this module, because a consumer cannot build it through a replace.
replace github.com/ubgo/workflow => ../
