#!/bin/sh
# Runs the benchmark against the shared local stack: the host Postgres for
# sqlstore (a database per system, dropped after), the stack's Redis for
# the Redis driver (a key prefix per run, deleted after), and the stack's Hatchet, whose token
# is read from the stack's .env and never printed. REDIS_ADDR overrides
# the Redis address (host:port). Extra arguments go to the bench command, e.g. `sh run.sh -systems memory`.
set -eu
cd "$(dirname "$0")"
STACK_ENV=/Volumes/D/www/projects/khanakia/docker_setup_mac/.env
# Only the two values the bench needs leave the stack's .env.
stackvar() { [ -f "$STACK_ENV" ] && sed -n "s/^$1=//p" "$STACK_ENV" | tail -1 | tr -d '"' || true; }
HATCHET_CLIENT_TOKEN=${HATCHET_CLIENT_TOKEN:-$(stackvar HATCHET_CLIENT_TOKEN)}
REDIS_PASSWORD=${REDIS_PASSWORD:-$(stackvar REDIS_PASSWORD)}
export HATCHET_CLIENT_TOKEN REDIS_PASSWORD
export HATCHET_CLIENT_HOST_PORT=localhost:7004 HATCHET_CLIENT_TLS_STRATEGY=none HATCHET_CLIENT_LOG_LEVEL=error
exec go run . -pg "postgres://${USER}@localhost:5432/postgres?sslmode=disable" -redis "${REDIS_ADDR:-localhost:7012}" "$@"
