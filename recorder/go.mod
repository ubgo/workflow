module github.com/ubgo/workflow/recorder

go 1.24.0

require github.com/ubgo/workflow v0.1.0

// The recorder needs core API added after v0.1.0 (SourceOf, RecordEvent,
// SetOutcome), so it resolves the core from this repository until the
// next core tag; the release then requires that tag and drops this line.
replace github.com/ubgo/workflow => ../
