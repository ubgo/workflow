// Package enginetest proves the sqlstore driver, its outbox and the pgxtx
// view on real databases: SQLite always, Postgres when WFTEST_PG_DSN is set
// and MySQL when WFTEST_MYSQL_DSN is set (task sqlstore:test:pg and
// sqlstore:test:mysql start throwaway servers for them).
//
// It is its own module, never released, so the database drivers it opens
// those engines with stay out of the sqlstore module: an app that uses
// sqlstore requires no database driver and brings the one it already uses.
package enginetest
