#!/bin/bash
set -euo pipefail

: "${PORT:=10000}"

# Database setup 
: "${PG_HOST:?PG_HOST is required}"
: "${PG_PASSWORD:?PG_PASSWORD is required}"
: "${PG_DATABASE:=postgres}"
: "${PG_USER:=postgres}"
: "${PG_PORT:=5432}"

: "${PGFTS_HOST:?PGFTS_HOST is required}"
: "${PGFTS_PASSWORD:?PGFTS_PASSWORD is required}"
: "${PGFTS_DATABASE:=postgres}"
: "${PGFTS_USER:=postgres}"
: "${PGFTS_PORT:=5432}"

if [[ -z "${PG_PORT}" ]]; then PG_PORT=5432; fi
if [[ -z "${PGFTS_PORT}" ]]; then PGFTS_PORT=5432; fi

run_psql () {
  local host="$1" port="$2" db="$3" user="$4" file="$5"
  echo "Running $file on $host:$port/$db as $user"
  # Note: -v ON_ERROR_STOP=1 makes failures stop immediately (good for prod)
  psql -v ON_ERROR_STOP=1 -h "$host" -U "$user" -p "$port" -d "$db" -a -f "$file"
}

export PGPASSWORD="$PG_PASSWORD"
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_submissions.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_comments.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_subreddits.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_comments_index.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_submissions_index.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_status_comments.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_status_submissions.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_progress.sql
run_psql "$PG_HOST" "$PG_PORT" "$PG_DATABASE" "$PG_USER" scripts/db_watchedsubreddits.sql
unset PGPASSWORD

export PGPASSWORD="$PGFTS_PASSWORD"
run_psql "$PGFTS_HOST" "$PGFTS_PORT" "$PGFTS_DATABASE" "$PGFTS_USER" scripts/db_fts.sql
unset PGPASSWORD

# Start API (internal)
: "${API_PORT:=18000}"

cd /redarc/api
gunicorn \
  --workers="${GUNICORN_WORKERS:-4}" \
  --bind="127.0.0.1:${API_PORT}" \
  --timeout="${GUNICORN_TIMEOUT:-600}" \
  --graceful-timeout="${GUNICORN_GRACEFUL_TIMEOUT:-30}" \
  --access-logfile - \
  --error-logfile - \
  app:app &

# Build frontend 
cd /redarc/frontend
echo "VITE_API_DOMAIN=${REDARC_FE_API:-/api/}" > .env
npm run build

mkdir -p /var/www/html/redarc/
cp -R dist/* /var/www/html/redarc/

# NGINX config 
cd /redarc/nginx

python3 nginx_envar.py
mv redarc.conf /etc/nginx/http.d/redarc.conf

sed -i -E "s/listen[[:space:]]+[0-9]+;/listen ${PORT};/g" /etc/nginx/http.d/redarc.conf

nginx -g "daemon off;"