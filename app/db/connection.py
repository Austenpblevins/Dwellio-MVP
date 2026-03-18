from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from app.core.config import get_settings


@contextmanager
def get_connection() -> Generator[Any, None, None]:
    """
    Yield a PostgreSQL connection using runtime settings.

    Importing psycopg lazily keeps module import lightweight in environments where
    database dependencies are not yet installed.
    """
    import psycopg
    from psycopg.rows import dict_row

    settings = get_settings()
    connection = psycopg.connect(
        settings.database_url,
        row_factory=dict_row,
        connect_timeout=settings.db_connect_timeout_seconds,
    )
    try:
        yield connection
    finally:
        connection.close()


def is_postgis_enabled() -> bool:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            result = cursor.fetchone()
    return bool(result and result["exists"])
