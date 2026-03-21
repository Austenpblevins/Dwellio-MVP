from __future__ import annotations

from app.db.connection import get_connection
from app.models.parcel import ParcelSummaryResponse


class ParcelSummaryService:
    def get_summary(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> ParcelSummaryResponse:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM parcel_summary_view
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND account_number = %s
                    LIMIT 1
                    """,
                    (county_id, tax_year, account_number),
                )
                row = cursor.fetchone()

        if row is None:
            raise LookupError(
                f"Parcel summary not found for {county_id}/{tax_year}/{account_number}."
            )

        payload = {
            field_name: row.get(field_name) for field_name in ParcelSummaryResponse.model_fields
        }
        return ParcelSummaryResponse.model_validate(payload)
