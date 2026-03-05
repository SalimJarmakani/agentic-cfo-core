from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import get_settings


@contextmanager
def get_postgres_cursor():
    settings = get_settings()
    conn = psycopg2.connect(
        host=settings.pghost,
        port=settings.pgport,
        dbname=settings.pgdatabase,
        user=settings.pguser,
        password=settings.pgpassword,
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()

