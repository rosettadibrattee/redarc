import json
import os
import re
import falcon
import logging

logger = logging.getLogger('redarc')


class AdminDelete:
    def __init__(self, main_pool, fts_pool=None):
        self.main_pool = main_pool
        self.fts_pool = fts_pool

    def _parse_bool(self, value, default=False):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ('1', 'true', 'yes', 'on'):
            return True
        if text in ('0', 'false', 'no', 'off'):
            return False
        return default

    def _parse_int(self, value, field, allow_negative=False):
        if value is None or value == '':
            return None
        text = str(value).strip()
        if allow_negative:
            if not re.fullmatch(r'-?\d+', text):
                raise ValueError(f"{field} must be an integer")
        else:
            if not text.isdigit():
                raise ValueError(f"{field} must be an integer")
        return int(text)

    def _parse_subreddit(self, obj):
        raw = obj.get('subreddit')
        if raw is None:
            raise ValueError("subreddit is required")
        sub = str(raw).strip().lower()
        if sub.startswith('r/'):
            sub = sub[2:]
        if not sub:
            raise ValueError("subreddit is required")
        if ',' in sub:
            raise ValueError("Only one subreddit is allowed per delete")
        if not re.fullmatch(r'[a-z0-9_]+', sub):
            raise ValueError(f"Invalid subreddit name: {sub}")
        return sub

    def _build_filters(self, obj):
        target = str(obj.get('target', 'submissions')).strip().lower()
        if target not in ('submissions', 'comments'):
            raise ValueError("target must be submissions or comments")

        subreddit = self._parse_subreddit(obj)
        author = (obj.get('author') or '').strip().lower()
        keywords = (obj.get('keywords') or '').strip()
        before = self._parse_int(obj.get('before'), 'before')
        after = self._parse_int(obj.get('after'), 'after')

        if before is not None and after is not None and before <= after:
            raise ValueError("before must be greater than after")
        if len(keywords) > 200:
            raise ValueError("keywords query too long (max 200 chars)")
        if len(author) > 80:
            raise ValueError("author too long (max 80 chars)")

        filters = {
            'target': target,
            'subreddit': subreddit,
            'author': author or None,
            'keywords': keywords or None,
            'before': before,
            'after': after,
        }
        return filters

    def _build_where(self, filters):
        target = filters['target']
        conditions = ['subreddit = %s']
        values = [filters['subreddit']]

        if filters['author']:
            conditions.append('author = %s')
            values.append(filters['author'])
        if filters['after'] is not None:
            conditions.append('created_utc > %s')
            values.append(filters['after'])
        if filters['before'] is not None:
            conditions.append('created_utc < %s')
            values.append(filters['before'])
        if filters['keywords']:
            if target == 'submissions':
                conditions.append('(title ILIKE %s OR self_text ILIKE %s OR url ILIKE %s)')
                keyword_like = f"%{filters['keywords']}%"
                values.extend([keyword_like, keyword_like, keyword_like])
            else:
                conditions.append('body ILIKE %s')
                values.append(f"%{filters['keywords']}%")

        return ' AND '.join(conditions), values

    def _count(self, con, target, where_sql, params):
        cursor = con.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {target} WHERE {where_sql}", params)
        return int(cursor.fetchone()[0])

    def _refresh_subreddit_counts(self, con, subreddits):
        cursor = con.cursor()
        for sub in subreddits:
            cursor.execute("SELECT COUNT(*) FROM submissions WHERE subreddit = %s", [sub])
            num_subs = int(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM comments WHERE subreddit = %s", [sub])
            num_comments = int(cursor.fetchone()[0])
            cursor.execute(
                "INSERT INTO subreddits (name, unlisted, num_submissions, num_comments) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (name) DO UPDATE SET "
                "(num_submissions, num_comments) = (%s, %s)",
                [sub, False, num_subs, num_comments, num_subs, num_comments],
            )

    def on_post(self, req, resp):
        obj = req.get_media() or {}
        dry_run = self._parse_bool(obj.get('dry_run'), default=True)
        admin_pw = (os.getenv('ADMIN_PASSWORD') or '').strip().strip('"').strip("'")
        if not dry_run and admin_pw and obj.get('password') != admin_pw:
            resp.status = falcon.HTTP_401
            resp.text = json.dumps({"error": "Invalid password"})
            resp.content_type = falcon.MEDIA_JSON
            return

        confirm_text = (obj.get('confirm_text') or '').strip()

        try:
            filters = self._build_filters(obj)
        except ValueError as e:
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": str(e)})
            resp.content_type = falcon.MEDIA_JSON
            return

        if not dry_run and confirm_text != 'DELETE':
            resp.status = falcon.HTTP_400
            resp.text = json.dumps({"error": "confirm_text must be DELETE"})
            resp.content_type = falcon.MEDIA_JSON
            return

        where_sql, where_params = self._build_where(filters)
        table = filters['target']
        max_delete_rows = int(os.getenv('ADMIN_DELETE_MAX_ROWS', '100000'))

        main_con = None
        fts_con = None
        try:
            main_con = self.main_pool.getconn()
            main_count = self._count(main_con, table, where_sql, where_params)

            if dry_run:
                fts_count = 0
                if self.fts_pool and main_count > 0:
                    if main_count <= max_delete_rows:
                        cursor = main_con.cursor()
                        cursor.execute(f"SELECT id FROM {table} WHERE {where_sql}", where_params)
                        ids = [row[0] for row in cursor.fetchall()]
                        if ids:
                            fts_con = self.fts_pool.getconn()
                            fts_cursor = fts_con.cursor()
                            fts_cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id = ANY(%s)", [ids])
                            fts_count = int(fts_cursor.fetchone()[0])
                    else:
                        # Too many IDs to materialize in preview safely.
                        fts_count = None

                payload = {
                    "status": "preview",
                    "target": table,
                    "filters": filters,
                    "counts": {
                        "main": main_count,
                        "fts": fts_count,
                    },
                    "fts_enabled": bool(self.fts_pool),
                }
                resp.text = json.dumps(payload)
                resp.content_type = falcon.MEDIA_JSON
                resp.status = falcon.HTTP_200
                return

            if main_count > max_delete_rows:
                resp.status = falcon.HTTP_400
                resp.text = json.dumps({
                    "error": f"Refusing to delete {main_count} rows. Tighten filters or raise ADMIN_DELETE_MAX_ROWS."
                })
                resp.content_type = falcon.MEDIA_JSON
                return

            cursor = main_con.cursor()
            cursor.execute(f"SELECT id FROM {table} WHERE {where_sql}", where_params)
            ids = [row[0] for row in cursor.fetchall()]

            deleted_main = 0
            deleted_fts = 0

            if ids:
                cursor.execute(f"DELETE FROM {table} WHERE id = ANY(%s)", [ids])
                deleted_main = int(cursor.rowcount)

                if self.fts_pool:
                    fts_con = self.fts_pool.getconn()
                    fts_cursor = fts_con.cursor()
                    fts_cursor.execute(f"DELETE FROM {table} WHERE id = ANY(%s)", [ids])
                    deleted_fts = int(fts_cursor.rowcount)
                    fts_con.commit()

                self._refresh_subreddit_counts(main_con, [filters['subreddit']])

            main_con.commit()

            resp.text = json.dumps({
                "status": "deleted",
                "target": table,
                "filters": filters,
                "deleted": {
                    "main": deleted_main,
                    "fts": deleted_fts,
                },
                "fts_enabled": bool(self.fts_pool),
            })
            resp.content_type = falcon.MEDIA_JSON
            resp.status = falcon.HTTP_200

        except Exception as e:
            logger.error(f"admin delete error: {e}")
            if main_con:
                main_con.rollback()
            if fts_con:
                fts_con.rollback()
            resp.status = falcon.HTTP_500
            resp.text = json.dumps({"error": "Delete request failed"})
            resp.content_type = falcon.MEDIA_JSON
        finally:
            if main_con:
                self.main_pool.putconn(main_con)
            if fts_con and self.fts_pool:
                self.fts_pool.putconn(fts_con)
