module github.com/ubgo/workflow/sqlstore/enginetest

go 1.24.0

// Test-only: the database drivers this module opens SQLite, Postgres and
// MySQL with never reach the sqlstore module. Everything resolves from
// this repository.
replace (
	github.com/ubgo/workflow => ../../
	github.com/ubgo/workflow/sqlstore => ../
	github.com/ubgo/workflow/sqlstore/pgnotify => ../pgnotify
	github.com/ubgo/workflow/sqlstore/pgxtx => ../pgxtx
)

require (
	github.com/jackc/pgx/v5 v5.7.5
	github.com/ubgo/sqlb v0.1.1
	github.com/ubgo/sqlb/contrib/driver-mysql v0.1.0
	github.com/ubgo/sqlb/contrib/driver-pgx v0.1.0
	github.com/ubgo/sqlb/contrib/driver-sqlite v0.1.0
	github.com/ubgo/sqlb/contrib/migrate v0.1.1
	github.com/ubgo/workflow v0.1.0
	github.com/ubgo/workflow/sqlstore v0.0.0-00010101000000-000000000000
	github.com/ubgo/workflow/sqlstore/pgnotify v0.0.0-00010101000000-000000000000
	github.com/ubgo/workflow/sqlstore/pgxtx v0.0.0-00010101000000-000000000000
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.45.0 // indirect
)
