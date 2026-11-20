// Package enginetest proves the sqlstore driver, its outbox and the pgxtx
// view on real databases: SQLite always, Postgres when WFTEST_PG_DSN is set
// and MySQL when WFTEST_MYSQL_DSN is set (task sqlstore:test:pg and
// sqlstore:test:mysql start throwaway servers for them).
//
// It is its own module, never released, so the database drivers it opens
// those engines with stay out of the sqlstore module: an app that uses
// sqlstore requires no database driver and brings the one it already uses.
//
// The shared conformance suite's SQLite run is split into shares, and the
// tests of retention.go, cluster.go and childflows.go are plain functions
// wrapped by Test functions of the same names; each lives in a package of
// its own under shards/, so `go test ./...` here runs them as parallel
// processes beside this package (see conformance.go for why not parallel
// subtests). `go test .` alone no longer runs them.
package enginetest
