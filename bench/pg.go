package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"

	driverpgx "github.com/ubgo/sqlb/contrib/driver-pgx"
)

// pgServer creates and drops one database per system run, so a run never
// shares tables with another system or another run, and leaves nothing
// behind.
type pgServer struct {
	admin string // DSN of a maintenance database (postgres) on the server
}

// dsn is the admin DSN with its database replaced by name.
func (p *pgServer) dsn(name string) string {
	u, err := url.Parse(p.admin)
	if err != nil {
		return p.admin // validated in main; unreachable
	}
	u.Path = "/" + name
	return u.String()
}

// create makes a database named ubgo_bench_<what>_<random> and returns
// its name.
func (p *pgServer) create(ctx context.Context, what string) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	name := "ubgo_bench_" + what + "_" + hex.EncodeToString(b[:])
	return name, p.exec(ctx, `CREATE DATABASE "`+name+`"`)
}

// drop removes a database create made, disconnecting any session left on
// it.
func (p *pgServer) drop(ctx context.Context, name string) error {
	return p.exec(ctx, `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
}

func (p *pgServer) exec(ctx context.Context, stmt string) error {
	db, err := driverpgx.Open(p.admin)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}
	return nil
}

// version is the server's version string.
func (p *pgServer) version(ctx context.Context) (string, error) {
	db, err := driverpgx.Open(p.admin)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var v string
	err = db.QueryRowContext(ctx, `SHOW server_version`).Scan(&v)
	return v, err
}
