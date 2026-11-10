package sqlrecorder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ubgo/workflow"
)

// Dialect is the SQL engine the app's *sql.DB talks to. The store writes
// plain SQL on database/sql for each, so this module imports no database
// driver: the app opens the database with the driver it already uses.
type Dialect string

// The supported engines, the same three as the sqlstore driver.
const (
	// Postgres: PostgreSQL 13 or later.
	Postgres Dialect = "postgres"
	// MySQL: MySQL 8.0 or later (InnoDB).
	MySQL Dialect = "mysql"
	// SQLite: SQLite 3.35 or later.
	SQLite Dialect = "sqlite"
)

// dialects is the canonical list, for validation and messages.
var dialects = [...]Dialect{Postgres, MySQL, SQLite}

// Dialects returns every supported dialect, as a fresh slice.
func Dialects() []Dialect { return append([]Dialect(nil), dialects[:]...) }

// Valid reports whether d is a supported dialect.
func (d Dialect) Valid() bool {
	for _, k := range dialects {
		if d == k {
			return true
		}
	}
	return false
}

// validate refuses an unknown dialect with workflow.ErrInvalid.
func (d Dialect) validate() error {
	if !d.Valid() {
		return fmt.Errorf("%w: sqlrecorder: unknown dialect %q (want one of %v)", workflow.ErrInvalid, d, dialects)
	}
	return nil
}

// quote quotes an identifier. Table names come from a validated prefix and
// fixed suffixes, never from input, so quoting is for reserved words only.
func (d Dialect) quote(id string) string {
	if d == MySQL {
		return "`" + id + "`"
	}
	return `"` + id + `"`
}

// forUpdate is the row-lock suffix of a SELECT inside a write transaction.
// SQLite has none: its write lock covers the whole database.
func (d Dialect) forUpdate() string {
	if d == SQLite {
		return ""
	}
	return " FOR UPDATE"
}

// Column types, per dialect. Identifiers sort and compare byte by byte on
// every engine (C collation on Postgres, utf8mb4_bin on MySQL, BINARY on
// SQLite), so listing order and equality match the in-memory store.
func (d Dialect) idType(n int) string {
	switch d {
	case Postgres:
		return "VARCHAR(" + strconv.Itoa(n) + `) COLLATE "C"`
	case MySQL:
		return "VARCHAR(" + strconv.Itoa(n) + ") CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
	}
	return "TEXT"
}

func (d Dialect) textType() string {
	if d == MySQL {
		return "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
	}
	return "TEXT"
}

func (d Dialect) blobType() string {
	switch d {
	case Postgres:
		return "BYTEA"
	case MySQL:
		return "LONGBLOB"
	}
	return "BLOB"
}

func (d Dialect) intType() string {
	if d == SQLite {
		return "INTEGER"
	}
	return "BIGINT"
}

// args collects query arguments and renders their placeholders: $1, $2 on
// Postgres, ? elsewhere.
type args struct {
	d    Dialect
	vals []any
}

// add appends v and returns its placeholder.
func (a *args) add(v any) string {
	a.vals = append(a.vals, v)
	if a.d == Postgres {
		return "$" + strconv.Itoa(len(a.vals))
	}
	return "?"
}

// in renders "(p1, p2, ...)" for vs; vs must not be empty.
func in[T any](a *args, vs []T) string {
	ps := make([]string, len(vs))
	for i, v := range vs {
		ps[i] = a.add(v)
	}
	return "(" + strings.Join(ps, ", ") + ")"
}

// likeEscape is the LIKE escape character. '!' is used rather than a
// backslash, which MySQL string literals would treat as an escape of their
// own.
const likeEscape = "!"

// escapeLike makes s match itself in a LIKE pattern.
func escapeLike(s string) string {
	r := strings.NewReplacer(likeEscape, likeEscape+likeEscape, "%", likeEscape+"%", "_", likeEscape+"_")
	return r.Replace(s)
}

// retryableMarkers are fragments of the errors a write transaction may hit
// under contention on each engine: a unique violation from two first
// writes of one run racing, a deadlock or serialization failure, a busy
// SQLite database. The transaction is retried on them. They are matched as
// text so this module needs no driver's error types.
var retryableMarkers = []string{
	"23505", "duplicate key", "unique constraint", // Postgres unique violation
	"40001", "40P01", "deadlock", "could not serialize", // Postgres
	"Error 1062", "Error 1213", "Error 1205", "Duplicate entry", "Deadlock found", "Lock wait timeout", // MySQL
	"UNIQUE constraint failed", "database is locked", "SQLITE_BUSY", // SQLite
}

// DefaultRetryable reports whether err looks like a contention error worth
// retrying the transaction for (see WithRetryable).
func DefaultRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, m := range retryableMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}
