# RedArc (Fork)

Modern self-hosted Reddit archive with:
- browse + thread views
- full-text search (advanced filters, partial-word matching, emoji matching)
- file upload ingestion (`.json`, `.ndjson`, `.zst`, `.zstd`)
- admin tooling (watch/unlist/progress + filter-based delete)

This fork keeps the original RedArc data model and workers, while modernizing the frontend and API surface.

## What You Get

- React frontend (`/`, `/search`, `/upload`, `/admin`)
- Falcon API behind NGINX (`/api/*`)
- Main Postgres DB for canonical submissions/comments
- FTS Postgres DB for text search
- Redis-backed job queues/status for ingestion and workers
- Optional background workers for thread fetch, subreddit polling, indexing, and image downloads

## Quick Start (Docker Compose)

### Prerequisites

- Docker + Docker Compose

### Start

```bash
cp default.env .env
# edit .env values (passwords, Reddit API credentials, hostnames)
docker compose up -d --build
```

Open:
- UI: `http://localhost:8088`
- API through NGINX: `http://localhost:8088/api`

### Main Services

- `redarc`: NGINX + API + built frontend
- `postgres` (`pgsql-dev`): main DB
- `postgres_fts` (`pgsql-fts`): FTS DB
- `redis`: queue + upload status store
- workers: `reddit_worker`, `subreddit_worker`, `index_worker`, `image_downloader`

## Configuration

Edit `.env` before running.

### Core

- `ADMIN_PASSWORD`: required for admin actions
- `INGEST_PASSWORD`: required for `/submit` and `/upload` when set
- `SEARCH_ENABLED=true|false`: enables `/search` route
- `INGEST_ENABLED=true|false`: enables `/submit` URL ingestion route
- `PG_*`: main Postgres connection vars
- `PGFTS_*`: FTS Postgres connection vars
- `REDIS_HOST`, `REDIS_PORT`

### Networking / proxy

- `SERVER_NAME`
- `REDARC_API` and `REDARC_FE_API` (used by NGINX + frontend build)
- `API_PORT` and `API_UPSTREAM`

### Optional upload tuning

- `UPLOAD_MAX_BYTES`: max accepted upload size in bytes (`0` = unlimited)
- `ADMIN_DELETE_MAX_ROWS`: safety cap for admin delete operation (default `100000`)

## Ingestion

### 1) Upload page (recommended)

Use `/upload` UI.

Supported files:
- `.json`
- `.ndjson`
- `.zst`
- `.zstd`

Import options:
- `type`: `auto | submissions | comments`
- `target`: `main | fts | both`
- `on_conflict`: `skip | update`
- `auto_index`: `yes | no`

Notes:
- uploads are processed async in background jobs
- status is polled from `/api/upload/status`
- `.zst`/`.zstd` decompression requires `zstandard` (already installed in the container)
- in `auto` mode, a file must contain one record type (submissions or comments), not mixed

### 2) Submit Reddit URL

Use `/upload` -> “Submit Reddit URL”, or call `POST /api/submit`.

- Requires `INGEST_ENABLED=true`
- Requires `INGEST_PASSWORD` if configured
- Enqueues thread fetch into Redis/RQ workers

### 3) Legacy CLI ingestion (still available)

If you prefer the old path, run scripts manually.

Important:
- disable ingest/reddit workers while doing bulk manual loads
- install Python deps locally (`python3`, `pip`, `psycopg2-binary`)
- decompress `.zst` files first

```bash
unzstd <submission_file>.zst
unzstd <comment_file>.zst
pip install psycopg2-binary

python3 scripts/load_sub.py <path_to_submission_file>
python3 scripts/load_sub_fts.py <path_to_submission_file>
python3 scripts/load_comments.py <path_to_comment_file>
python3 scripts/load_comments_fts.py <path_to_comment_file>
python3 scripts/index.py [subreddit_name]

# optional
python3 scripts/unlist.py <subreddit> <true|false>
python3 scripts/backfill_images.py <subreddit> <after_timestamp_utc> <num_urls>
```

Legacy script DB credentials are hardcoded defaults; edit scripts if your local DB credentials differ.

## Search

`/search` supports:
- submissions or comments search
- optional subreddit (or all subreddits)
- author filter
- keywords filter
- date range (after/before)
- score / gilded / comment-count filters
- domain and self-post filters (submissions)
- partial-word match mode
- phrase match mode
- emoji matching
- sort by new/old/relevance/score/gilded/comment-count (where applicable)
- pagination with configurable page size

Search endpoint exists only when `SEARCH_ENABLED=true` and FTS DB is configured.

## Admin

`/admin` includes:
- watch/unwatch subreddit
- unlist/relist subreddit
- ingest job progress
- Danger Zone delete-by-filter

Danger Zone behavior:
- delete target: submissions or comments
- exactly one subreddit per delete
- first click runs review (`dry_run`)
- execute step requires typing `DELETE`
- admin password is required only for execute step

## API Overview

### Public/browse

- `GET /api/search/subreddits`
- `GET /api/search/submissions`
- `GET /api/search/comments`
- `GET /api/status`
- `GET /api/stats`
- `GET /api/media`

### Search

- `GET /api/search` (when enabled)

### Ingest

- `POST /api/upload`
- `GET /api/upload/status`
- `POST /api/submit` (when enabled)

### Admin

- `POST /api/watch`
- `POST /api/unlist`
- `POST /api/progress`
- `POST /api/admin/delete`

## Development Notes

- API app entry: `api/app.py`
- Upload pipeline: `api/upload.py`
- Search logic: `api/search.py`
- Admin delete resource: `api/admin_delete.py`
- Frontend API client: `frontend/src/utils/api.js`

Frontend commands:

```bash
cd frontend
npm install
npm run dev
npm run build
```

## Troubleshooting

- `401 Invalid password` on admin actions:
  - verify `ADMIN_PASSWORD` in container env
  - for Danger Zone, password is required on execute step
- `/api/search` missing:
  - set `SEARCH_ENABLED=true`
  - ensure FTS DB env vars are valid
- upload status not shared across workers:
  - ensure Redis is reachable (`REDIS_HOST` / `REDIS_PORT`)

## License

MIT (same as upstream RedArc).

## Credits

- Upstream project: [Yakabuff/redarc](https://github.com/Yakabuff/redarc)
