"""
app.py — RedArc API server (modernized).

Changes from original:
- Added /upload endpoint for UI-based data ingestion
- Added /upload/status endpoint for job tracking
- Added /stats endpoint for archive statistics
- Added CORS middleware with proper headers
- Added multipart form handling for file uploads
- Falcon 3.x compatible
"""

import sys
import redarc_logger

logger = redarc_logger.init_logger('redarc')
logger.info('Starting redarc...')

import falcon
import os
from psycopg2 import pool
import psycopg2
from rq import Queue
from redis import Redis
from submit import Submit
from comments import Comments
from subreddits import Subreddits
from progress import Progress
from submissions import Submissions
from status import Status
from search import Search
from media import Media
from unlist import Unlist
from watch import Watch
from admin_delete import AdminDelete
from upload import Upload, UploadStatus, Stats
from dotenv import load_dotenv

load_dotenv()

# ---- CORS Middleware ----
class CORSMiddleware:
    def process_response(self, req, resp, resource, req_succeeded):
        origin = req.get_header('Origin')
        if origin:
            resp.set_header('Access-Control-Allow-Origin', origin)
        else:
            resp.set_header('Access-Control-Allow-Origin', '*')
        resp.set_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        resp.set_header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With')
        resp.set_header('Access-Control-Max-Age', '86400')

    def process_request(self, req, resp):
        if req.method == 'OPTIONS':
            resp.status = falcon.HTTP_200
            raise falcon.HTTPStatus(falcon.HTTP_200)


# ---- Database Connections ----
try:
    pg_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT'),
        database=os.getenv('PG_DATABASE')
    )
except Exception as error:
    logger.error(error)
    sys.exit(4)

pgfts_pool = None
if os.getenv('SEARCH_ENABLED') == "true":
    try:
        pgfts_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            user=os.getenv('PGFTS_USER'),
            password=os.getenv('PGFTS_PASSWORD'),
            host=os.getenv('PGFTS_HOST'),
            port=os.getenv('PGFTS_PORT'),
            database=os.getenv('PGFTS_DATABASE')
        )
    except Exception as error:
        logger.error(error)
        sys.exit(4)


# ---- App Setup ----
app = application = falcon.App(
    middleware=[CORSMiddleware()],
    cors_enable=True
)

# Multipart form handling — try to register if available (Falcon 3.1+)
try:
    import falcon.media.multipart
    app.req_options.media_handlers[falcon.MEDIA_MULTIPART] = falcon.media.multipart.MultipartFormHandler()
except (ImportError, AttributeError):
    logger.warning('falcon.media.multipart not available — file upload will use raw stream parsing')

# ---- Resource Instances ----
comments = Comments(pg_pool)
subreddits = Subreddits(pg_pool)
progress = Progress(pg_pool)
submissions = Submissions(pg_pool)
status = Status(pg_pool)
media = Media(os.getenv('IMAGE_PATH'))
unlist = Unlist(pg_pool)
watch = Watch(pg_pool)
admin_delete = AdminDelete(pg_pool, pgfts_pool)
stats = Stats(pg_pool)
upload = Upload(pg_pool, pgfts_pool)
upload_status = UploadStatus()

# ---- Routes ----
# Existing
app.add_route('/search/comments', comments)
app.add_route('/search/submissions', submissions)
app.add_route('/search/subreddits', subreddits)
app.add_route('/progress', progress)
app.add_route('/status', status)
app.add_route('/media', media)
app.add_route('/unlist', unlist)
app.add_route('/watch', watch)
app.add_route('/admin/delete', admin_delete)

# New
app.add_route('/stats', stats)
app.add_route('/upload', upload)
app.add_route('/upload/status', upload_status)

# Conditional
if os.getenv('SEARCH_ENABLED') == "true" and pgfts_pool:
    search = Search(pgfts_pool)
    app.add_route('/search', search)

if os.getenv('INGEST_ENABLED') == "true":
    try:
        redis_conn = Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except Exception as error:
        logger.error(error)
        sys.exit(4)

    url_queue = Queue("url_submit", connection=redis_conn)
    submit = Submit(url_queue)
    app.add_route('/submit', submit)

logger.info('RedArc API ready.')
