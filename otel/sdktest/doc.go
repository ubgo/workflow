// Package sdktest proves the otel instrumentation against the OpenTelemetry
// SDK's in-memory span exporter and manual metric reader.
//
// It is its own module, never released, so the SDK it needs stays out of
// the otel module: an app that uses otel requires only the OpenTelemetry
// API and brings whichever SDK or exporter it already runs.
package sdktest
