// Package e2e holds no library code. Its tests prove the recorder against
// every workflow driver (sqlstore on SQLite, Postgres and MySQL, Redis,
// Hatchet) and the sqlrecorder store against every SQL engine, through the
// shared recordertest suites. It is a module of its own so the database and
// engine clients those tests need never reach a library module's go.mod.
//
// Without a service's environment variable its tests skip with the reason:
// WFTEST_PG_DSN (Postgres), WFTEST_MYSQL_DSN (MySQL), WFTEST_REDIS_URL
// (Redis), HATCHET_CLIENT_TOKEN (Hatchet). The Taskfile in this directory
// starts throwaway services for each and removes them after the run.
package e2e
