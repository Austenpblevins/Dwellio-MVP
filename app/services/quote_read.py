from __future__ import annotations

from app.db.connection import get_connection
from app.models.quote import QuoteExplanationResponse, QuoteResponse


class QuoteReadService:
    def get_quote(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteResponse:
        row = self._fetch_quote_row(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
        payload = {field_name: row.get(field_name) for field_name in QuoteResponse.model_fields}
        return QuoteResponse.model_validate(payload)

    def get_explanation(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteExplanationResponse:
        row = self._fetch_quote_row(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
        return QuoteExplanationResponse(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
            explanation_json=row.get("explanation_json") or {},
            explanation_bullets=list(row.get("explanation_bullets") or []),
        )

    def _fetch_quote_row(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> dict[str, object]:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM v_quote_read_model
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND account_number = %s
                    ORDER BY valuation_created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (county_id, tax_year, account_number),
                )
                row = cursor.fetchone()

        if row is None:
            raise LookupError(f"Quote not found for {county_id}/{tax_year}/{account_number}.")
        return row
