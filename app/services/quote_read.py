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
            requested_tax_year=tax_year,
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
            requested_tax_year=tax_year,
            account_number=account_number,
        )
        return QuoteExplanationResponse(
            county_id=county_id,
            tax_year=int(row["served_tax_year"]),
            account_number=account_number,
            requested_tax_year=int(row["requested_tax_year"]),
            served_tax_year=int(row["served_tax_year"]),
            tax_year_fallback_applied=bool(row["tax_year_fallback_applied"]),
            tax_year_fallback_reason=row.get("tax_year_fallback_reason"),
            data_freshness_label=row.get("data_freshness_label"),
            explanation_json=row.get("explanation_json") or {},
            explanation_bullets=list(row.get("explanation_bullets") or []),
        )

    def _fetch_quote_row(
        self,
        *,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, object]:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      *,
                      %s AS requested_tax_year,
                      tax_year AS served_tax_year,
                      (tax_year <> %s) AS tax_year_fallback_applied,
                      CASE
                        WHEN tax_year <> %s THEN 'requested_year_unavailable'
                        ELSE NULL
                      END AS tax_year_fallback_reason,
                      CASE
                        WHEN tax_year <> %s THEN 'prior_year_fallback'
                        ELSE 'current_year'
                      END AS data_freshness_label
                    FROM v_quote_read_model
                    WHERE county_id = %s
                      AND account_number = %s
                      AND tax_year <= %s
                    ORDER BY tax_year DESC, valuation_created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (
                        requested_tax_year,
                        requested_tax_year,
                        requested_tax_year,
                        requested_tax_year,
                        county_id,
                        account_number,
                        requested_tax_year,
                    ),
                )
                row = cursor.fetchone()

        if row is None:
            raise LookupError(
                f"Quote not found for {county_id}/{requested_tax_year}/{account_number}."
            )
        return row
