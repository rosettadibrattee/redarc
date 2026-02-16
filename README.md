# RedArc v2 - Modernized Self-Hosted Reddit Archive

A complete reframe of [Yakabuff/redarc](https://github.com/Yakabuff/redarc), modernizing the frontend, search capabilities, and data ingestion workflow.

## What Changed

### Frontend (Complete Rewrite)
| Before | After |
|--------|-------|
| Bootstrap 2 era styling | Dark theme with JetBrains Mono + Source Serif 4 |
| `class` instead of `className` | Proper React patterns throughout |
| Direct DOM manipulation for pagination | State-driven cursor-based pagination |
| Hardcoded year dropdown (stops at 2023) | Dynamic date range filters |
| No loading/error/empty states | Full loading skeletons, error boundaries, empty states |
| Search requires ALL fields | Optional filters, search across all subreddits |
| No file upload UI | Full drag-and-drop upload with progress tracking |
| Inline fetch calls everywhere | Centralized API client (`src/utils/api.js`) |
| No responsive design | Mobile-first responsive layout |

### Backend (New Endpoints)
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `POST /upload` | POST | Upload NDJSON files via multipart form |
| `GET /upload/status` | GET | Check upload job progress |
| `GET /stats` | GET | Archive-wide statistics |

### Architecture Changes

```
BEFORE:                              AFTER:
                                     
CLI only data ingestion:             UI + CLI data ingestion:
  $ python3 load_sub.py <file>         Drag & drop in browser
  $ python3 load_comments.py <file>    OR python3 load_sub.py <file>
  $ python3 load_sub_fts.py <file>     
  $ python3 load_comments_fts.py       /upload endpoint handles:
  $ python3 index.py [subreddit]       - Parsing NDJSON
                                       - Inserting to main PG
                                       - Inserting to FTS PG  
                                       - Auto-indexing subreddits
                                       - Background processing
                                       - Progress reporting
```

## Project Structure

```
redarc/
├── api/
│   ├── app.py              # Updated: CORS middleware, new routes
│   ├── upload.py            # NEW: File upload + processing + stats
│   ├── comments.py          # Unchanged
│   ├── submissions.py       # Unchanged
│   ├── subreddits.py        # Unchanged
│   ├── search.py            # Unchanged
│   ├── submit.py            # Unchanged
│   ├── media.py             # Unchanged
│   ├── progress.py          # Unchanged
│   ├── status.py            # Unchanged
│   ├── unlist.py            # Unchanged
│   ├── watch.py             # Unchanged
│   └── redarc_logger.py     # Unchanged
│
├── frontend/
│   ├── src/
│   │   ├── main.jsx         # Entry point with router
│   │   ├── utils/
│   │   │   └── api.js       # NEW: Centralized API client
│   │   ├── components/      # NEW: Reusable UI components
│   │   │   ├── Header.jsx
│   │   │   ├── Breadcrumb.jsx
│   │   │   ├── EmptyState.jsx
│   │   │   ├── Loading.jsx
│   │   │   ├── Pagination.jsx
│   │   │   ├── CommentTree.jsx
│   │   │   └── Toast.jsx
│   │   └── pages/           # NEW: Page-level components
│   │       ├── Index.jsx        # Subreddit grid + stats
│   │       ├── Subreddit.jsx    # Submission list
│   │       ├── Thread.jsx       # Post + threaded comments
│   │       ├── Search.jsx       # Full-text search with filters
│   │       ├── Upload.jsx       # File upload + URL submit
│   │       └── Admin.jsx        # Watch/unlist/progress
│   ├── package.json          # Updated deps (Tailwind, Lucide)
│   └── vite.config.js
│
├── scripts/                  # Unchanged (CLI still works)
├── ingest/                   # Unchanged
├── docker-compose.yml        # Unchanged
└── Dockerfile                # Unchanged
```

## New API: `/upload`

### POST /upload
Upload an NDJSON file for processing.

**Request:** `multipart/form-data`
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | File | Yes | NDJSON file (.json, .ndjson) |
| `type` | String | No | `submissions`, `comments`, or `auto` (default: auto) |
| `password` | String | No | Ingest password if INGEST_PASSWORD is set |
| `target` | String | No | `main`, `fts`, or `both` (default: both) |
| `auto_index` | String | No | `true` or `false` (default: true) |

**Response:** `202 Accepted`
```json
{
  "status": "accepted",
  "job_id": "a3f2b1c8",
  "filename": "RS_2023-01.ndjson",
  "file_size": 1048576
}
```

### GET /upload/status
Check upload job progress.

**Query params:** `?job_id=a3f2b1c8` (optional — omit for all jobs)

**Response:**
```json
{
  "id": "a3f2b1c8",
  "filename": "RS_2023-01.ndjson",
  "status": "processing",
  "lines_processed": 45000,
  "inserted": 44800,
  "skipped": 150,
  "errors": 50,
  "subreddits": ["programming", "python"]
}
```

### GET /stats
Archive statistics.

**Response:**
```json
{
  "subreddits": 42,
  "submissions": 284521,
  "comments": 3842156,
  "total_records": 4126677
}
```

## Frontend API Client

All API calls are centralized in `frontend/src/utils/api.js`:

```javascript
import { fetchSubreddits, search, uploadFile, fetchUploadStatus } from './utils/api';

// Fetch subreddits with abort support
const controller = new AbortController();
const subs = await fetchSubreddits(controller.signal);

// Full-text search
const results = await search({
  type: 'submission',
  subreddit: 'programming',
  query: 'rust',
  after: '1672531200',
});

// Upload file
const job = await uploadFile(fileObject, {
  type: 'submissions',
  password: 'mypass',
  target: 'both',
  autoIndex: true,
});

// Poll job status
const status = await fetchUploadStatus(job.job_id);
```

## Upload Processing Pipeline

The upload endpoint replaces the need to run 4 separate CLI scripts. Here's what happens internally:

```
1. File received via multipart POST
2. Saved to /tmp/redarc_uploads/
3. Background thread spawned
4. NDJSON parsed line-by-line (streaming, low memory)
5. Each line parsed with same logic as load_sub.py / load_comments.py
6. Batch INSERT (500 rows) into main PG
7. Batch INSERT into FTS PG (if target=both|fts)
8. ON CONFLICT (id) DO NOTHING (skip duplicates)
9. Auto-index: UPDATE subreddits table with counts
10. Temp file cleaned up
11. Job status updated (poll via /upload/status)
```

**Auto-detection:** When `type=auto`, the parser checks for `title`/`selftext` fields (→ submission) vs `body` field (→ comment). This handles mixed files gracefully.

## Migration Guide

### From v1 to v2

1. **Backend:** Replace `api/app.py` with the new version. Add `api/upload.py`. No other API files changed.

2. **Frontend:** Complete replacement. Delete old `frontend/src/` and replace with new structure.

3. **Docker:** Host port mappings are configurable in `docker-compose.yml`:
   - `REDARC_HTTP_PORT` (default: `8088`)
   - `PG_HOST_PORT` (default: `55432`)
   - `PGFTS_HOST_PORT` (default: `55433`)
   - `REDIS_HOST_PORT` (default: `56379`)
   - Internal API bind uses `API_PORT` (default: `18000`)

4. **Data:** No database schema changes. All existing data is preserved.

5. **Env vars:** Add the port env vars above to `.env` if you need custom bindings.

### Breaking Changes
- Frontend routes changed (but old URL patterns still work via the router)
- Bootstrap CSS removed entirely (replaced with Tailwind + custom CSS)
- `class` → `className` throughout (was a React anti-pattern)

## Design System

The modernized frontend uses:

- **Typography:** JetBrains Mono (UI/code) + Source Serif 4 (prose/titles)
- **Colors:** Dark base (#0a0a0b) with Reddit orange (#ff4500) accent
- **Components:** All custom — no component library dependency
- **Icons:** Lucide React (tree-shakeable, 1KB per icon)
- **Layout:** CSS Grid + Flexbox, responsive breakpoints at 768px

## License

MIT — same as original.
