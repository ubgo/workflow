// Package deps pins, by blank import, every module the planned examples
// need, so the examples' go.mod carries them before the programs that use
// them exist and `go mod tidy` keeps them. Writers of examples never edit
// go.mod; a missing dependency is added here by the module's owner.
package deps

import (
	_ "github.com/ubgo/sqlb"
	_ "github.com/ubgo/sqlb/contrib/driver-mysql"
	_ "github.com/ubgo/sqlb/contrib/driver-pgx"
	_ "github.com/ubgo/sqlb/contrib/driver-sqlite"
	_ "github.com/ubgo/sqlb/contrib/migrate"
	_ "github.com/ubgo/workflow/hatchet"
	_ "github.com/ubgo/workflow/otel"
	_ "github.com/ubgo/workflow/sqlstore"
	_ "github.com/ubgo/workflow/sqlstore/outbox"
	_ "go.opentelemetry.io/otel"
	_ "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	_ "go.opentelemetry.io/otel/sdk/metric"
	_ "go.opentelemetry.io/otel/sdk/metric/metricdata"
	_ "go.opentelemetry.io/otel/sdk/trace"
	_ "go.opentelemetry.io/otel/sdk/trace/tracetest"
)
