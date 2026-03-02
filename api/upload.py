"""
upload.py — API endpoint for UI-based data ingestion.

Accepts NDJSON (newline-delimited JSON) file uploads for submissions and comments.
Processes them server-side and inserts into both the main PG and FTS databases.
Replaces the need for CLI scripts: load_sub.py, load_comments.py, load_sub_fts.py, load_comments_fts.py.
"""

import json
import time
import os
import io
import asyncio
import inspect
import threading
import uuid
import re
import falcon
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import redis

logger = logging.getLogger('redarc')


# Job storage — uses Redis when available (shared across gunicorn workers),
# falls back to in-memory dict (works but not shared across workers).
_redis_client = None
_redis_available = None
_memory_jobs = {}  # fallback

def _get_redis():
    global _redis_client, _redis_available
    if _redis_available is False:
        return None
    if _redis_client is None:
        try:
            _redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                decode_responses=True,
                socket_connect_timeout=2,
            )
            _redis_client.ping()
            _redis_available = True
            logger.info(f"Redis connected at {os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', 6379)}")
        except Exception as e:
            logger.warning(f"Redis unavailable ({e}), using in-memory job store")
            _redis_available = False
            _redis_client = None
            return None
    return _redis_client

JOB_KEY_PREFIX = 'redarc:upload_job:'
JOB_TTL = 86400  # expire after 24h


def _safe_filename(filename):
    """Normalize user-provided filenames to avoid path traversal and bad chars."""
    base = os.path.basename((filename or '').strip())
    base = re.sub(r'[^A-Za-z0-9._-]', '_', base)
    return base[:180] or 'upload.ndjson'


def _resolve_awaitable(value):
    """Resolve awaitables in sync context when multipart parsers expose async APIs."""
    if inspect.isawaitable(value):
        try:
            return asyncio.run(value)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(value)
            finally:
                loop.close()
        except Exception:
            return None
    return value


def _resolve_attr(obj, attr_name):
    """Read an attribute, calling it when callable, and resolve awaitables."""
    if not hasattr(obj, attr_name):
        return None
    try:
        value = getattr(obj, attr_name)
    except Exception:
        return None

    if callable(value):
        try:
            value = value()
        except TypeError:
            return None
        except Exception:
            return None

    return _resolve_awaitable(value)


def _get_attr_no_call(obj, attr_name):
    """Read an attribute safely without invoking callables."""
    if not hasattr(obj, attr_name):
        return None
    try:
        return getattr(obj, attr_name)
    except Exception:
        return None


def _to_bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode('utf-8')
    return None


def _to_text(value):
    if isinstance(value, str):
        return value
    raw = _to_bytes(value)
    if raw is None:
        return None
    return raw.decode('utf-8', errors='replace')


def _normalized_name(value):
    text = _to_text(value)
    return text.strip() if isinstance(text, str) else ''


def _resolve_upload_filename(upload, fallback):
    for attr in ('filename', 'file_name', 'original_filename', 'name'):
        candidate = _resolve_attr(upload, attr)
        text = _normalized_name(candidate)
        if not text:
            continue
        # Avoid using the multipart field name as a filename.
        if text.lower() == 'file':
            continue
        return _safe_filename(text)
    return _safe_filename(fallback)


def _check_upload_size(total_bytes, max_upload_bytes):
    if max_upload_bytes and total_bytes > max_upload_bytes:
        raise ValueError("Upload exceeds configured UPLOAD_MAX_BYTES")


def _write_chunk(out_fh, chunk, bytes_written, max_upload_bytes):
    chunk_bytes = _to_bytes(chunk)
    if chunk_bytes is None:
        raise ValueError(f"Unsupported upload stream chunk type: {type(chunk).__name__}")
    bytes_written += len(chunk_bytes)
    _check_upload_size(bytes_written, max_upload_bytes)
    out_fh.write(chunk_bytes)
    return bytes_written


def _copy_reader_to_file(reader, out_fh, bytes_written, max_upload_bytes, chunk_size=8192):
    wrote = False
    while True:
        try:
            chunk = _resolve_awaitable(reader(chunk_size))
        except TypeError:
            chunk = _resolve_awaitable(reader())
        if not chunk:
            break
        bytes_written = _write_chunk(out_fh, chunk, bytes_written, max_upload_bytes)
        wrote = True
    return bytes_written, wrote


def _resolve_stream_source(upload):
    """Resolve upload stream/file source, handling callable accessors safely."""
    for attr in ('stream', 'file'):
        candidate = _get_attr_no_call(upload, attr)
        if candidate is None:
            continue
        if callable(candidate):
            try:
                candidate = candidate()
            except TypeError:
                continue
            except Exception:
                continue
            candidate = _resolve_awaitable(candidate)
        if candidate is not None:
            return candidate
    return None


def _parse_multipart_with_cgi(req):
    """
    Parse multipart form-data using the raw Content-Type header (with boundary).
    This avoids Falcon BodyPart wrappers that may expose placeholder payloads.
    """
    form_data = {}
    upload = None

    content_type = req.get_header('Content-Type') or req.content_type or ''
    if 'multipart/form-data' not in content_type.lower():
        return form_data, upload

    content_length = req.content_length or 0
    if content_length <= 0:
        return form_data, upload

    try:
        import cgi
        form = cgi.FieldStorage(
            fp=req.bounded_stream,
            environ={
                'REQUEST_METHOD': 'POST',
                'CONTENT_TYPE': content_type,
                'CONTENT_LENGTH': str(content_length),
            },
            keep_blank_values=True,
        )
    except Exception as e:
        logger.warning(f"cgi multipart parse failed: {e}")
        return form_data, upload

    items = getattr(form, 'list', None) or []
    for item in items:
        key_name = _normalized_name(getattr(item, 'name', ''))
        if not key_name:
            continue

        if key_name.lower() == 'file':
            if upload is None:
                upload = item
            continue

        value = getattr(item, 'value', None)
        text_value = _to_text(value)
        if text_value is not None:
            form_data[key_name] = text_value

    return form_data, upload


def _write_upload_to_path(upload, file_path, max_upload_bytes):
    bytes_written = 0
    with open(file_path, 'wb') as out_fh:
        # Prefer stream/file sources first so we preserve exact bytes
        # (especially for compressed dumps like .zst).
        stream = _resolve_stream_source(upload)

        direct_bytes = _to_bytes(stream)
        if direct_bytes is not None:
            bytes_written = len(direct_bytes)
            _check_upload_size(bytes_written, max_upload_bytes)
            out_fh.write(direct_bytes)
        elif stream is not None and hasattr(stream, 'read'):
            if hasattr(stream, 'seek'):
                try:
                    stream.seek(0)
                except Exception:
                    pass
            bytes_written, _ = _copy_reader_to_file(stream.read, out_fh, bytes_written, max_upload_bytes)
        elif stream is not None and hasattr(stream, '__iter__'):
            for chunk in stream:
                if not chunk:
                    continue
                bytes_written = _write_chunk(out_fh, chunk, bytes_written, max_upload_bytes)

        if bytes_written == 0 and hasattr(upload, 'read'):
            bytes_written, _ = _copy_reader_to_file(upload.read, out_fh, bytes_written, max_upload_bytes)

        # Keep text fallback last; it can corrupt binary uploads.
        if bytes_written == 0:
            for attr in ('get_text', 'text'):
                candidate = _resolve_attr(upload, attr)
                payload = _to_bytes(candidate)
                if payload is None:
                    continue
                bytes_written = len(payload)
                _check_upload_size(bytes_written, max_upload_bytes)
                out_fh.write(payload)
                break

        if bytes_written == 0:
            payload = _to_bytes(upload)
            if payload is not None:
                bytes_written = len(payload)
                _check_upload_size(bytes_written, max_upload_bytes)
                out_fh.write(payload)

    if bytes_written == 0:
        attrs = []
        for attr in ('filename', 'name', 'text', 'value', 'data', 'get_data', 'stream', 'file', 'read'):
            if hasattr(upload, attr):
                attrs.append(attr)
        raise ValueError(f"Unsupported uploaded file payload: {type(upload).__name__} attrs={','.join(attrs)}")

    return bytes_written


def _is_zstd_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            return f.read(4) == b'\x28\xb5\x2f\xfd'
    except Exception:
        return False


def get_job(job_id):
    r = _get_redis()
    if r:
        raw = r.get(f'{JOB_KEY_PREFIX}{job_id}')
        return json.loads(raw) if raw else None
    return _memory_jobs.get(job_id)

def set_job(job_id, data):
    r = _get_redis()
    if r:
        r.setex(f'{JOB_KEY_PREFIX}{job_id}', JOB_TTL, json.dumps(data))
    else:
        _memory_jobs[job_id] = data

def update_job(job_id, **updates):
    job = get_job(job_id)
    if job:
        job.update(updates)
        set_job(job_id, job)

def list_jobs(limit=50):
    r = _get_redis()
    if r:
        keys = r.keys(f'{JOB_KEY_PREFIX}*')
        jobs = []
        for k in keys:
            raw = r.get(k)
            if raw:
                jobs.append(json.loads(raw))
    else:
        jobs = list(_memory_jobs.values())
    jobs.sort(key=lambda x: x.get('created_at', 0), reverse=True)
    return jobs[:limit]


def parse_submission(line_dict):
    """Parse a single submission JSON line into DB-ready tuple. Mirrors load_sub.py logic."""
    d = line_dict

    # ID
    if 'id' in d and isinstance(d['id'], str):
        identifier = d['id'].strip().replace("\u0000", "").lower()
    elif 'name' in d and isinstance(d['name'], str) and len(d['name'].split('_')) > 1:
        identifier = d['name'].strip().replace("\u0000", "").lower().split('_')[1]
    else:
        return None

    subreddit = d.get('subreddit', '').strip().replace("\u0000", "").lower() if isinstance(d.get('subreddit'), str) else None
    if not subreddit:
        return None

    title = d.get('title', '').strip().replace("\u0000", "") if isinstance(d.get('title'), str) else ""
    author = d.get('author', '[unknown]').strip().replace("\u0000", "").lower() if isinstance(d.get('author'), str) else "[unknown]"
    permalink = d.get('permalink', f'/r/{subreddit}/comments/{identifier}/foobar').strip().replace("\u0000", "") if isinstance(d.get('permalink'), str) else f'/r/{subreddit}/comments/{identifier}/foobar'

    num_comments = d.get('num_comments', 0)
    if isinstance(num_comments, str) and num_comments.isdigit():
        num_comments = int(num_comments)
    elif not isinstance(num_comments, int):
        num_comments = 0

    url = d.get('url', f'http://reddit.com/r/{subreddit}/comments/{identifier}/blah').strip().replace("\u0000", "") if isinstance(d.get('url'), str) else f'http://reddit.com/r/{subreddit}/comments/{identifier}/blah'

    score = d.get('score', 0)
    if isinstance(score, str) and score.isdigit():
        score = int(score)
    elif not isinstance(score, int):
        score = 0

    gilded = d.get('gilded', 0)
    if isinstance(gilded, str) and gilded.isdigit():
        gilded = int(gilded)
    elif not isinstance(gilded, int):
        gilded = 0

    created_utc = d.get('created_utc', 0)
    if isinstance(created_utc, str) and created_utc.isdigit():
        created_utc = int(created_utc)
    elif not isinstance(created_utc, int):
        created_utc = 0

    self_text = d.get('selftext', '').strip().replace("\u0000", "") if isinstance(d.get('selftext'), str) else ""
    is_self = d.get('is_self', True if "reddit.com/r/" in url else False)
    if not isinstance(is_self, bool):
        is_self = True if "reddit.com/r/" in url else False

    thumbnail = d.get('thumbnail', 'self' if is_self else 'default').strip().replace("\u0000", "") if isinstance(d.get('thumbnail'), str) else ('self' if is_self else 'default')

    return (identifier, subreddit, title, author, permalink, thumbnail, num_comments, url, score, gilded, created_utc, self_text, is_self, int(time.time()))


def parse_comment(line_dict):
    """Parse a single comment JSON line into DB-ready tuple. Mirrors load_comments.py logic."""
    d = line_dict

    if 'id' in d and isinstance(d['id'], str):
        identifier = d['id'].strip()
    else:
        return None

    subreddit = d.get('subreddit', '').strip().lower() if isinstance(d.get('subreddit'), str) else None
    if not subreddit:
        return None

    author = d.get('author', '[unknown]').strip().lower() if isinstance(d.get('author'), str) else "[unknown]"

    score = d.get('score', 0)
    if isinstance(score, str) and score.isdigit():
        score = int(score)
    elif not isinstance(score, int):
        score = 0

    gilded = d.get('gilded', 0)
    if isinstance(gilded, str) and gilded.isdigit():
        gilded = int(gilded)
    elif not isinstance(gilded, int):
        gilded = 0

    created_utc = d.get('created_utc', 0)
    if isinstance(created_utc, str) and created_utc.isdigit():
        created_utc = int(created_utc)
    elif not isinstance(created_utc, int):
        created_utc = 0

    body = d.get('body', '').strip().replace("\u0000", "") if isinstance(d.get('body'), str) else ""

    link_id_raw = d.get('link_id')
    if isinstance(link_id_raw, str) and link_id_raw.strip():
        link_id_stripped = link_id_raw.strip()
        if len(link_id_stripped.split('_')) > 1:
            link_id = link_id_stripped.split('_')[1]
        else:
            link_id = link_id_stripped
    else:
        return None

    parent_id_raw = d.get('parent_id')
    if isinstance(parent_id_raw, str) and parent_id_raw.strip():
        parent_id_stripped = parent_id_raw.strip()
        if len(parent_id_stripped.split('_')) > 1:
            parent_id = parent_id_stripped.split('_')[1]
        else:
            parent_id = parent_id_stripped
    else:
        parent_id = link_id

    return (identifier, subreddit, body, author, score, gilded, created_utc, parent_id, link_id, int(time.time()))


def iter_json_objects(text_stream, chunk_size=1024 * 1024):
    """
    Yield JSON objects from either NDJSON or concatenated JSON payloads.
    Supports payloads shaped like: {...}\n{...} or {...}{...}.
    """
    decoder = json.JSONDecoder()
    buffer = ""

    while True:
        chunk = text_stream.read(chunk_size)
        if not chunk:
            break
        buffer += chunk

        pos = 0
        length = len(buffer)
        while True:
            while pos < length and buffer[pos].isspace():
                pos += 1
            if pos >= length:
                break

            try:
                obj, end = decoder.raw_decode(buffer, pos)
            except json.JSONDecodeError:
                # Need more bytes to complete current object.
                break

            yield obj
            pos = end

        if pos > 0:
            buffer = buffer[pos:]

    # Final pass on remaining buffered content.
    tail = buffer.strip()
    if not tail:
        return

    pos = 0
    length = len(tail)
    while True:
        while pos < length and tail[pos].isspace():
            pos += 1
        if pos >= length:
            break
        obj, end = decoder.raw_decode(tail, pos)
        yield obj
        pos = end


def process_upload(job_id, file_path, data_type, pg_pool, pgfts_pool, auto_index, on_conflict='skip'):
    """Background worker that processes an uploaded NDJSON file."""
    update_job(job_id, status='processing', started_at=time.time())

    inserted = 0
    skipped = 0
    errors = 0
    line_number = 0
    subreddits_seen = set()
    detected_type = None
    pg_con = None
    pg_cursor = None
    fts_con = None
    fts_cursor = None

    if on_conflict not in ('skip', 'update'):
        on_conflict = 'skip'

    try:
        if not pg_pool and not pgfts_pool:
            update_job(job_id, status='failed', error='No target database configured', finished_at=time.time())
            return

        # Get connections
        if pg_pool:
            pg_con = pg_pool.getconn()
            pg_cursor = pg_con.cursor()

        if pgfts_pool:
            fts_con = pgfts_pool.getconn()
            fts_cursor = fts_con.cursor()

        # Open file — decompress .zst if needed (by extension or magic bytes).
        file_path_lower = file_path.lower()
        is_zst = file_path_lower.endswith('.zst') or file_path_lower.endswith('.zstd') or _is_zstd_file(file_path)
        if is_zst:
            try:
                import zstandard as zstd
            except ImportError:
                update_job(job_id, status='failed', error='zstandard not installed — cannot decompress .zst files')
                return
            dctx = zstd.ZstdDecompressor()
            raw_fh = open(file_path, 'rb')
            stream = dctx.stream_reader(raw_fh)
            f = io.TextIOWrapper(stream, encoding='utf-8', errors='replace')
        else:
            raw_fh = None
            f = open(file_path, 'r', encoding='utf-8', errors='replace')

        try:
            batch_pg = []
            batch_fts = []
            BATCH_SIZE = 500

            def flush_batch(batch_type, rows_pg, rows_fts):
                nonlocal inserted, errors
                if not rows_pg:
                    return [], []

                try:
                    if batch_type == 'submissions':
                        if pg_cursor:
                            args_str = ','.join(pg_cursor.mogrify(
                                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row
                            ).decode() for row in rows_pg)
                            conflict_main = (
                                "ON CONFLICT(id) DO UPDATE SET subreddit=EXCLUDED.subreddit, title=EXCLUDED.title, author=EXCLUDED.author, "
                                "permalink=EXCLUDED.permalink, thumbnail=EXCLUDED.thumbnail, num_comments=EXCLUDED.num_comments, url=EXCLUDED.url, "
                                "score=EXCLUDED.score, gilded=EXCLUDED.gilded, created_utc=EXCLUDED.created_utc, self_text=EXCLUDED.self_text, "
                                "is_self=EXCLUDED.is_self, retrieved_utc=EXCLUDED.retrieved_utc"
                            ) if on_conflict == 'update' else "ON CONFLICT(id) DO NOTHING"
                            pg_cursor.execute(
                                "INSERT INTO submissions(id,subreddit,title,author,permalink,thumbnail,num_comments,url,score,gilded,created_utc,self_text,is_self,retrieved_utc) "
                                f"VALUES {args_str} {conflict_main}"
                            )

                        if fts_cursor and rows_fts:
                            args_fts = ','.join(fts_cursor.mogrify(
                                "(%s,%s,%s,%s,%s,%s,%s,%s)", row
                            ).decode() for row in rows_fts)
                            conflict_fts = (
                                "ON CONFLICT(id) DO UPDATE SET subreddit=EXCLUDED.subreddit, title=EXCLUDED.title, num_comments=EXCLUDED.num_comments, "
                                "score=EXCLUDED.score, gilded=EXCLUDED.gilded, created_utc=EXCLUDED.created_utc, self_text=EXCLUDED.self_text"
                            ) if on_conflict == 'update' else "ON CONFLICT(id) DO NOTHING"
                            fts_cursor.execute(
                                "INSERT INTO submissions(id,subreddit,title,num_comments,score,gilded,created_utc,self_text) "
                                f"VALUES {args_fts} {conflict_fts}"
                            )
                    else:
                        if pg_cursor:
                            args_str = ','.join(pg_cursor.mogrify(
                                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row
                            ).decode() for row in rows_pg)
                            conflict_main = (
                                "ON CONFLICT(id) DO UPDATE SET subreddit=EXCLUDED.subreddit, body=EXCLUDED.body, author=EXCLUDED.author, "
                                "score=EXCLUDED.score, gilded=EXCLUDED.gilded, created_utc=EXCLUDED.created_utc, parent_id=EXCLUDED.parent_id, "
                                "link_id=EXCLUDED.link_id, retrieved_utc=EXCLUDED.retrieved_utc"
                            ) if on_conflict == 'update' else "ON CONFLICT(id) DO NOTHING"
                            pg_cursor.execute(
                                "INSERT INTO comments(id,subreddit,body,author,score,gilded,created_utc,parent_id,link_id,retrieved_utc) "
                                f"VALUES {args_str} {conflict_main}"
                            )

                        if fts_cursor and rows_fts:
                            args_fts = ','.join(fts_cursor.mogrify(
                                "(%s,%s,%s,%s,%s,%s,%s)", row
                            ).decode() for row in rows_fts)
                            conflict_fts = (
                                "ON CONFLICT(id) DO UPDATE SET subreddit=EXCLUDED.subreddit, body=EXCLUDED.body, score=EXCLUDED.score, "
                                "gilded=EXCLUDED.gilded, created_utc=EXCLUDED.created_utc, link_id=EXCLUDED.link_id"
                            ) if on_conflict == 'update' else "ON CONFLICT(id) DO NOTHING"
                            fts_cursor.execute(
                                "INSERT INTO comments(id,subreddit,body,score,gilded,created_utc,link_id) "
                                f"VALUES {args_fts} {conflict_fts}"
                            )

                    inserted += len(rows_pg)
                    if pg_con:
                        pg_con.commit()
                    if fts_con:
                        fts_con.commit()
                except Exception as e:
                    logger.error(f"Batch insert error: {e}")
                    if pg_con:
                        pg_con.rollback()
                    if fts_con:
                        fts_con.rollback()
                    errors += len(rows_pg)

                return [], []

            try:
                payload_iter = iter_json_objects(f)
                for d in payload_iter:
                    line_number += 1
                    if not isinstance(d, dict):
                        errors += 1
                        continue

                    # Auto-detect type if needed
                    effective_type = data_type
                    if data_type == 'auto':
                        if 'title' in d or 'selftext' in d:
                            effective_type = 'submissions'
                        elif 'body' in d:
                            effective_type = 'comments'
                        else:
                            errors += 1
                            continue
                        if detected_type is None:
                            detected_type = effective_type
                        elif detected_type != effective_type:
                            errors += 1
                            continue

                    if effective_type == 'submissions':
                        parsed = parse_submission(d)
                        if not parsed:
                            skipped += 1
                            continue

                        subreddits_seen.add(parsed[1])
                        batch_pg.append(parsed)

                        # FTS tuple (fewer fields)
                        if fts_cursor:
                            batch_fts.append((parsed[0], parsed[1], parsed[2], parsed[6], parsed[8], parsed[9], parsed[10], parsed[11]))

                    elif effective_type == 'comments':
                        parsed = parse_comment(d)
                        if not parsed:
                            skipped += 1
                            continue

                        subreddits_seen.add(parsed[1])
                        batch_pg.append(parsed)

                        # FTS tuple
                        if fts_cursor:
                            batch_fts.append((parsed[0], parsed[1], parsed[2], parsed[4], parsed[5], parsed[6], parsed[8]))

                    # Flush batch
                    if len(batch_pg) >= BATCH_SIZE:
                        batch_pg, batch_fts = flush_batch(effective_type, batch_pg, batch_fts)

                    # Update progress every 1000 records
                    if line_number % 1000 == 0:
                        update_job(job_id, lines_processed=line_number, inserted=inserted, errors=errors)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error in upload job {job_id}: {e}")
                errors += 1

            # Flush remaining
            if batch_pg:
                final_type = detected_type if data_type == 'auto' else data_type
                batch_pg, batch_fts = flush_batch(final_type, batch_pg, batch_fts)

            # Auto-index subreddits
            if auto_index and subreddits_seen and pg_cursor and pg_con:
                for sub in subreddits_seen:
                    try:
                        pg_cursor.execute("SELECT COUNT(*) FROM submissions WHERE subreddit = %s", (sub,))
                        num_subs = pg_cursor.fetchone()[0]
                        pg_cursor.execute("SELECT COUNT(*) FROM comments WHERE subreddit = %s", (sub,))
                        num_coms = pg_cursor.fetchone()[0]
                        pg_cursor.execute(
                            "INSERT INTO subreddits (name, unlisted, num_submissions, num_comments) "
                            "VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO UPDATE SET "
                            "(num_submissions, num_comments) = (%s, %s)",
                            (sub, False, num_subs, num_coms, num_subs, num_coms)
                        )
                        pg_con.commit()
                    except Exception as e:
                        logger.error(f"Index error for {sub}: {e}")
                        pg_con.rollback()

        finally:
            # Close file handles
            f.close()
            if raw_fh:
                raw_fh.close()

        update_job(job_id,
            status='complete',
            finished_at=time.time(),
            lines_processed=line_number,
            inserted=inserted,
            skipped=skipped,
            errors=errors,
            subreddits=list(subreddits_seen),
        )

    except Exception as e:
        logger.error(f"Upload job {job_id} failed: {e}")
        update_job(job_id, status='failed', error=str(e), finished_at=time.time())

    finally:
        if pg_pool and pg_con:
            pg_pool.putconn(pg_con)
        if pgfts_pool and fts_con:
            pgfts_pool.putconn(fts_con)

        # Cleanup temp file
        try:
            os.remove(file_path)
        except:
            pass


class Upload:
    def __init__(self, pg_pool, pgfts_pool=None):
        self.pg_pool = pg_pool
        self.pgfts_pool = pgfts_pool
        self.upload_dir = '/tmp/redarc_uploads'
        os.makedirs(self.upload_dir, exist_ok=True)

    def on_post(self, req, resp):
        """Handle file upload. Expects multipart form data."""
        # Parse multipart form fields
        form_data, upload = _parse_multipart_with_cgi(req)

        # Falcon multipart parsing fallback
        if not upload and hasattr(req, 'get_media'):
            try:
                form = req.get_media()
                if hasattr(form, 'items'):
                    for key, val in form.items():
                        key_name = _normalized_name(key)
                        item = val[0] if isinstance(val, list) and val else val
                        if key_name.lower() == 'file':
                            upload = item
                            continue
                        if key_name in form_data:
                            continue
                        text_value = _to_text(_resolve_attr(item, 'text'))
                        if text_value is None:
                            text_value = _to_text(_resolve_attr(item, 'value'))
                        if text_value is None and item is not None:
                            text_value = _to_text(item)
                        if text_value is not None:
                            form_data[key_name] = text_value

                if not upload:
                    for part in form:
                        if not hasattr(part, 'name'):
                            continue
                        part_name = _normalized_name(part.name)
                        if part_name.lower() == 'file':
                            upload = part
                        else:
                            if part_name in form_data:
                                continue
                            text_value = _to_text(_resolve_attr(part, 'text'))
                            if text_value is None:
                                text_value = _to_text(_resolve_attr(part, 'value')) or ''
                            form_data[part_name] = text_value
            except Exception as e:
                logger.warning(f"Multipart parse failed: {e}")

        # Auth check - read password from form data or query param as fallback
        pw = form_data.get('password') or req.get_param('password') or ''
        ingest_pw = os.getenv('INGEST_PASSWORD', '').strip().strip('"').strip("'")
        if ingest_pw:
            if pw != ingest_pw:
                resp.status = falcon.HTTP_401
                resp.text = json.dumps({"error": "Invalid password"})
                resp.content_type = falcon.MEDIA_JSON
                return

        data_type = form_data.get('type') or req.get_param('type') or 'auto'
        if data_type not in ('auto', 'submissions', 'comments'):
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": "type must be auto, submissions, or comments"})
            resp.content_type = falcon.MEDIA_JSON
            return

        auto_index_raw = str(form_data.get('auto_index', req.get_param('auto_index') or 'true')).strip().lower()
        auto_index = auto_index_raw not in ('false', '0', 'no', 'off')

        target_db = form_data.get('target') or req.get_param('target') or 'both'
        if target_db not in ('main', 'fts', 'both'):
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": "target must be main, fts, or both"})
            resp.content_type = falcon.MEDIA_JSON
            return

        on_conflict = form_data.get('on_conflict') or req.get_param('on_conflict') or 'skip'
        if on_conflict not in ('skip', 'update'):
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": "on_conflict must be skip or update"})
            resp.content_type = falcon.MEDIA_JSON
            return

        if not upload:
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": "No file provided"})
            resp.content_type = falcon.MEDIA_JSON
            return

        # Save to temp file
        job_id = str(uuid.uuid4())[:8]
        filename = _resolve_upload_filename(upload, f"upload_{job_id}.ndjson")
        file_path = os.path.join(self.upload_dir, f"{job_id}_{filename}")

        max_upload_bytes = int(os.getenv('UPLOAD_MAX_BYTES', '0') or 0)
        try:
            bytes_written = _write_upload_to_path(upload, file_path, max_upload_bytes)
        except ValueError as e:
            try:
                os.remove(file_path)
            except Exception:
                pass
            status = falcon.HTTP_413 if 'UPLOAD_MAX_BYTES' in str(e) else falcon.HTTP_400
            resp.status = status
            resp.text = json.dumps({"error": str(e)})
            resp.content_type = falcon.MEDIA_JSON
            return
        except Exception as e:
            logger.error(f"Failed to save upload: {e}")
            try:
                os.remove(file_path)
            except Exception:
                pass
            resp.status = falcon.HTTP_500
            resp.text = json.dumps({"error": "Failed to store upload"})
            resp.content_type = falcon.MEDIA_JSON
            return

        file_size = bytes_written

        # Create job record in Redis
        set_job(job_id, {
            'id': job_id,
            'filename': filename,
            'file_size': file_size,
            'data_type': data_type,
            'target': target_db,
            'on_conflict': on_conflict,
            'status': 'queued',
            'created_at': time.time(),
            'lines_processed': 0,
            'inserted': 0,
            'skipped': 0,
            'errors': 0,
        })

        # Determine which pools to use
        main_pool = self.pg_pool if target_db in ('main', 'both') else None
        fts_pool = self.pgfts_pool if target_db in ('fts', 'both') else None

        # Process in background thread
        thread = threading.Thread(
            target=process_upload,
            args=(job_id, file_path, data_type, main_pool, fts_pool, auto_index, on_conflict),
            daemon=True
        )
        thread.start()

        resp.status = falcon.HTTP_202
        resp.text = json.dumps({
            "status": "accepted",
            "job_id": job_id,
            "filename": filename,
            "file_size": file_size,
        })
        resp.content_type = falcon.MEDIA_JSON


class UploadStatus:
    """Check status of an upload job."""

    def on_get(self, req, resp):
        job_id = req.get_param('job_id')

        if job_id:
            job = get_job(job_id)
            if not job:
                resp.status = falcon.HTTP_404
                resp.text = json.dumps({"error": "Job not found"})
                return
            resp.text = json.dumps(job)
        else:
            # Return all recent jobs
            jobs = list_jobs(50)
            resp.text = json.dumps(jobs)

        resp.content_type = falcon.MEDIA_JSON
        resp.status = falcon.HTTP_200


class Stats:
    """Return archive statistics."""

    def __init__(self, pool):
        self.pool = pool

    def on_get(self, req, resp):
        pg_con = None
        try:
            pg_con = self.pool.getconn()
            cursor = pg_con.cursor(cursor_factory=RealDictCursor)

            cursor.execute("SELECT COUNT(*) as count FROM subreddits WHERE unlisted = false")
            num_subreddits = cursor.fetchone()['count']

            cursor.execute("SELECT COALESCE(SUM(num_submissions), 0) as total FROM subreddits")
            total_submissions = cursor.fetchone()['total']

            cursor.execute("SELECT COALESCE(SUM(num_comments), 0) as total FROM subreddits")
            total_comments = cursor.fetchone()['total']

            resp.text = json.dumps({
                'subreddits': num_subreddits,
                'submissions': total_submissions,
                'comments': total_comments,
                'total_records': total_submissions + total_comments,
            })
            resp.content_type = falcon.MEDIA_JSON
            resp.status = falcon.HTTP_200

        except Exception as error:
            logger.error(error)
            resp.status = falcon.HTTP_500
        finally:
            if pg_con:
                self.pool.putconn(pg_con)
