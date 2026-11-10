// Package sqlitetest holds no library code. Its tests prove the sqlrecorder
// store on a real SQLite database: the recordertest Store contract, the
// rows each write leaves, and every error path of a write, read, migrate
// and prune, driven by a fault-injecting wrapper around the SQLite driver.
//
// It is a module of its own so the SQLite driver it needs never reaches the
// sqlrecorder module's go.mod: that module imports database/sql only. The
// tests need no service and no environment variable; measure the store's
// coverage from here with
// -coverpkg=github.com/ubgo/workflow/recorder/sqlrecorder (task cover in
// recorder/Taskfile.yml merges it with the package's own tests).
package sqlitetest
