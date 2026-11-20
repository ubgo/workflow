module github.com/ubgo/workflow/otel/sdktest

go 1.25.0

// Test-only: the OpenTelemetry SDK this module needs never reaches the otel
// module. Everything resolves from this repository.
replace (
	github.com/ubgo/workflow => ../../
	github.com/ubgo/workflow/otel => ../
)

require (
	github.com/ubgo/workflow v0.1.0
	github.com/ubgo/workflow/otel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
	go.opentelemetry.io/otel/trace v1.46.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.46.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
