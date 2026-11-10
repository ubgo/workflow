module github.com/ubgo/workflow/recorder/sqlrecorder

go 1.24.0

require (
	github.com/ubgo/workflow v0.1.0
	github.com/ubgo/workflow/recorder v0.0.0
)

// Both resolve from this repository until their next tags; the release
// requires the tags and drops these lines. No database driver is required
// here: the app opens its own *sql.DB (the tests live in recorder/e2e).
replace (
	github.com/ubgo/workflow => ../../
	github.com/ubgo/workflow/recorder => ../
)
