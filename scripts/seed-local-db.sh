#!/usr/bin/env bash
# seed-local-db.sh — Apply the three local seed files to fr_batch_local.
#
# Usage:
#   bash scripts/seed-local-db.sh [--help]
#
# Runs from any working directory — paths are resolved relative to this script.
#
# Environment variables (all optional — defaults shown below):
#   DB_NAME          Database name          (default: fr_batch_local)
#   DB_USER          MySQL username         (default: root)
#   DB_PASSWORD      MySQL password         (default: empty string)
#   MYSQL_HOST       MySQL host             (default: 127.0.0.1)   [host client mode]
#   MYSQL_PORT       MySQL port             (default: 3306)        [host client mode]
#   DB_CLIENT        auto | host | podman   (default: auto; prefers podman)
#   COMPOSE_PROJECT  Compose project label  (default: frbatch-local) [podman mode]
#   COMPOSE_SERVICE  Compose service label  (default: db)            [podman mode]
#
# Prerequisites:
#   - A MySQL client: either `mysql` in PATH (host mode), OR the Podman DB
#     container running (podman mode — no host client needed). DB_CLIENT=auto
#     prefers the running Podman container, otherwise uses a host client.
#   - The backend must have started and run Flyway migrations at least once,
#     otherwise the target tables do not exist yet.
#
# Notes:
#   - Do NOT run 00_create_schema.sql — it is superseded by Flyway.
#   - Seeds are idempotent if they use INSERT IGNORE or ON DUPLICATE KEY.
#     If they do not, re-running may produce duplicate-key errors; those are safe
#     to ignore once data is already seeded.

set -Eeuo pipefail

# ─── Script location ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
SEED_DIR="$REPO_ROOT/fr-batch-service/_devenvironment/db"

# ─── Configuration ────────────────────────────────────────────────────────────
DB_NAME="${DB_NAME:-fr_batch_local}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
DB_CLIENT="${DB_CLIENT:-auto}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-frbatch-local}"
COMPOSE_SERVICE="${COMPOSE_SERVICE:-db}"
MIGRATION_TABLE="${MIGRATION_TABLE:-flyway_schema_history}"

# ─── Help ─────────────────────────────────────────────────────────────────────
usage() {
    printf 'Usage: %s [--help]\n\n' "$(basename "$0")"
    printf 'Apply local seed SQL files to the fr_batch_local database.\n\n'
    printf 'Environment variables:\n'
    printf '  DB_NAME      Database name    (default: fr_batch_local)\n'
    printf '  DB_USER      MySQL user       (default: root)\n'
    printf '  DB_PASSWORD  MySQL password   (default: empty)\n'
    printf '  MYSQL_HOST   MySQL host       (default: 127.0.0.1)\n'
    printf '  MYSQL_PORT   MySQL port       (default: 3306)\n'
    printf '  DB_CLIENT    auto|host|podman (default: auto)\n'
    printf '  MIGRATION_TABLE  Flyway table used to confirm migrations (default: %s)\n' "$MIGRATION_TABLE"
    exit 0
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
fi

# ─── Resolve MySQL client ─────────────────────────────────────────────────────
# Prefer the running Podman DB container, which is the project-managed database.
# Fall back to a host client only when no project container is running.
find_db_container() {
    command -v podman &>/dev/null || return 0
    podman ps -q \
        --filter "label=com.docker.compose.project=${COMPOSE_PROJECT}" \
        --filter "label=com.docker.compose.service=${COMPOSE_SERVICE}" \
        | head -1
}

CLIENT_MODE=""
DB_CONTAINER=""
case "$DB_CLIENT" in
    host)
        command -v mysql &>/dev/null || {
            printf '❌ [ERROR] DB_CLIENT=host but mysql client not found in PATH.\n' >&2
            exit 1
        }
        CLIENT_MODE="host" ;;
    podman)
        DB_CONTAINER="$(find_db_container)"
        [[ -n "$DB_CONTAINER" ]] || {
            printf '❌ [ERROR] DB_CLIENT=podman but no running container for project "%s" / service "%s".\n' \
                "$COMPOSE_PROJECT" "$COMPOSE_SERVICE" >&2
            exit 1
        }
        CLIENT_MODE="podman" ;;
    auto)
        if DB_CONTAINER="$(find_db_container)" && [[ -n "$DB_CONTAINER" ]]; then
            CLIENT_MODE="podman"
        elif command -v mysql &>/dev/null; then
            CLIENT_MODE="host"
        else
            printf '❌ [ERROR] No MySQL client available.\n' >&2
            printf '        Install a mysql client, or start the Podman DB container:\n' >&2
            printf '          cd fr-batch-service/_devenvironment && podman-compose up -d\n' >&2
            exit 1
        fi ;;
    *)
        printf '❌ [ERROR] Invalid DB_CLIENT="%s" (use auto|host|podman).\n' "$DB_CLIENT" >&2
        exit 1 ;;
esac
printf 'ℹ️  [INFO] MySQL client mode: %s%s\n' "$CLIENT_MODE" "${DB_CONTAINER:+ (container ${DB_CONTAINER})}"

# ─── Seed files in order ──────────────────────────────────────────────────────
declare -a SEED_FILES=(
    "$SEED_DIR/10_seed_event_tickets.sql"
    "$SEED_DIR/20_seed_harvest_source.sql"
    "$SEED_DIR/30_seed_usage_record.sql"
)

# ─── Helpers ──────────────────────────────────────────────────────────────────
run_mysql() {
    # Accepts -e "SQL" or piped input. Dispatches to the resolved client mode.
    # Empty password: omit the --password flag to suppress the mysql warning.
    if [[ "$CLIENT_MODE" == "podman" ]]; then
        # -i forwards stdin so `run_mysql "$DB_NAME" < file` pipes the seed in.
        if [[ -n "${DB_PASSWORD}" ]]; then
            podman exec -i "$DB_CONTAINER" mysql --user="$DB_USER" --password="$DB_PASSWORD" "$@"
        else
            podman exec -i "$DB_CONTAINER" mysql --user="$DB_USER" "$@"
        fi
    else
        if [[ -n "${DB_PASSWORD}" ]]; then
            mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$DB_USER" --password="$DB_PASSWORD" "$@"
        else
            mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$DB_USER" "$@"
        fi
    fi
}

# ─── Verify connectivity ──────────────────────────────────────────────────────
printf '🔎 [INFO] Checking MySQL connectivity at %s:%s ...\n' "$MYSQL_HOST" "$MYSQL_PORT"
if ! run_mysql -e "SELECT 1;" &>/dev/null; then
    printf '❌ [ERROR] Cannot connect to MySQL at %s:%s as user %s.\n' \
        "$MYSQL_HOST" "$MYSQL_PORT" "$DB_USER" >&2
    printf '        Is the database container running? (cd fr-batch-service/_devenvironment && podman-compose up -d)\n' >&2
    exit 1
fi
printf '✅ [OK] Connected.\n'

# ─── Ensure database exists ───────────────────────────────────────────────────
printf '🔎 [INFO] Ensuring database "%s" exists ...\n' "$DB_NAME"
run_mysql -e "CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
printf '✅ [OK] Database "%s" ready.\n' "$DB_NAME"

# ─── Verify schema migrations ──────────────────────────────────────────────────
printf '🔎 [INFO] Checking Flyway migrations in database "%s" ...\n' "$DB_NAME"
if ! run_mysql "$DB_NAME" -N -s -e "SELECT 1 FROM information_schema.tables WHERE table_schema = '${DB_NAME}' AND table_name = '${MIGRATION_TABLE}' LIMIT 1;" | grep -qx '1'; then
    printf '❌ [ERROR] Flyway migrations are not complete: table "%s" was not found in database "%s".\n' \
        "$MIGRATION_TABLE" "$DB_NAME" >&2
    printf '        Start the backend with the local profile and wait for it to become ready, then retry.\n' >&2
    exit 1
fi
printf '✅ [OK] Flyway migrations detected.\n'

# ─── Apply seeds ──────────────────────────────────────────────────────────────
for seed in "${SEED_FILES[@]}"; do
    if [[ ! -f "$seed" ]]; then
        printf '❌ [ERROR] Seed file not found: %s\n' "$seed" >&2
        exit 1
    fi

    filename="$(basename "$seed")"
    printf '🔎 [INFO] Applying seed: %s ...\n' "$filename"

    if run_mysql "$DB_NAME" < "$seed"; then
        printf '✅ [OK]   %s applied successfully.\n' "$filename"
    else
        printf '❌ [ERROR] Failed to apply %s — check the output above.\n' "$filename" >&2
        exit 1
    fi
done

printf '\n✅ [DONE] All seeds applied to database "%s".\n' "$DB_NAME"
