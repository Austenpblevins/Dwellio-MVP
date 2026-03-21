from __future__ import annotations

from app.db.connection import get_connection


class SearchIndexService:
    def rebuild_search_documents(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
    ) -> int:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT dwellio_refresh_search_documents(%s, %s) AS refreshed_count",
                    (county_id, tax_year),
                )
                row = cursor.fetchone()
            connection.commit()

        if row is None:
            return 0
        return int(row["refreshed_count"] or 0)
