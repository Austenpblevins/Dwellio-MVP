from __future__ import annotations

from typing import Any

from app.db.connection import get_connection
from app.models.parcel import ParcelSearchResult
from app.models.quote import SearchRequest
from app.services.ownership_reconciliation import normalize_owner_name
from app.utils.text_normalization import normalize_address_query


class AddressResolverService:
    def search(self, request: SearchRequest) -> list[ParcelSearchResult]:
        return self.search_by_query(request.address)

    def search_by_query(self, query: str, *, limit: int = 10) -> list[ParcelSearchResult]:
        raw_query = query.strip()
        normalized_address = normalize_address_query(raw_query)
        normalized_owner = normalize_owner_name(raw_query) or ""

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH ranked_candidates AS (
                      SELECT
                        sd.county_id,
                        sd.tax_year,
                        sd.account_number,
                        sd.parcel_id,
                        COALESCE(NULLIF(sd.document_json ->> 'address', ''), sd.display_address) AS address,
                        NULLIF(sd.document_json ->> 'situs_zip', '') AS situs_zip,
                        NULLIF(sd.document_json ->> 'owner_name', '') AS owner_name,
                        CASE
                          WHEN sd.account_number = %s THEN 'account_exact'
                          WHEN sd.normalized_address = %s THEN 'address_exact'
                          WHEN sd.normalized_address %% %s THEN 'address_trigram'
                          WHEN sd.search_text %% %s THEN 'search_text_trigram'
                          WHEN %s <> '' AND sd.normalized_owner_name IS NOT NULL AND sd.normalized_owner_name %% %s
                            THEN 'owner_fallback'
                          ELSE 'search_text_fallback'
                        END AS match_basis,
                        CASE
                          WHEN sd.account_number = %s THEN 1.0000::numeric
                          WHEN sd.normalized_address = %s THEN 0.9800::numeric
                          ELSE GREATEST(
                            similarity(sd.normalized_address, %s) * 0.95,
                            similarity(sd.search_text, %s) * 0.85,
                            CASE
                              WHEN %s = '' OR sd.normalized_owner_name IS NULL THEN 0::numeric
                              ELSE similarity(sd.normalized_owner_name, %s) * 0.75
                            END
                          )
                        END AS match_score
                      FROM search_documents sd
                      WHERE sd.account_number = %s
                        OR sd.normalized_address = %s
                        OR sd.normalized_address %% %s
                        OR sd.search_text %% %s
                        OR (%s <> '' AND sd.normalized_owner_name IS NOT NULL AND sd.normalized_owner_name %% %s)
                    )
                    SELECT *
                    FROM ranked_candidates
                    WHERE match_score >= %s
                    ORDER BY
                      CASE match_basis
                        WHEN 'account_exact' THEN 1
                        WHEN 'address_exact' THEN 2
                        WHEN 'address_trigram' THEN 3
                        WHEN 'search_text_trigram' THEN 4
                        WHEN 'owner_fallback' THEN 5
                        ELSE 6
                      END,
                      match_score DESC,
                      county_id ASC,
                      account_number ASC
                    LIMIT %s
                    """,
                    (
                        raw_query,
                        normalized_address,
                        normalized_address,
                        normalized_address,
                        normalized_owner,
                        normalized_owner,
                        raw_query,
                        normalized_address,
                        normalized_address,
                        normalized_address,
                        normalized_owner,
                        normalized_owner,
                        raw_query,
                        normalized_address,
                        normalized_address,
                        normalized_address,
                        normalized_owner,
                        normalized_owner,
                        0.35,
                        limit,
                    ),
                )
                rows = cursor.fetchall()

        return [self._build_result(row) for row in rows]

    def autocomplete(self, query: str, *, limit: int = 8) -> list[ParcelSearchResult]:
        raw_query = query.strip()
        normalized_address = normalize_address_query(raw_query)
        normalized_owner = normalize_owner_name(raw_query) or ""

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH prefix_candidates AS (
                      SELECT
                        sd.county_id,
                        sd.tax_year,
                        sd.account_number,
                        sd.parcel_id,
                        COALESCE(NULLIF(sd.document_json ->> 'address', ''), sd.display_address) AS address,
                        NULLIF(sd.document_json ->> 'situs_zip', '') AS situs_zip,
                        NULLIF(sd.document_json ->> 'owner_name', '') AS owner_name,
                        CASE
                          WHEN sd.account_number LIKE %s THEN 'account_prefix'
                          WHEN sd.normalized_address LIKE %s THEN 'address_prefix'
                          WHEN %s <> '' AND sd.normalized_owner_name LIKE %s THEN 'owner_prefix'
                          ELSE 'search_text_trigram'
                        END AS match_basis,
                        CASE
                          WHEN sd.account_number LIKE %s THEN 0.99::numeric
                          WHEN sd.normalized_address LIKE %s THEN 0.97::numeric
                          WHEN %s <> '' AND sd.normalized_owner_name LIKE %s THEN 0.85::numeric
                          ELSE GREATEST(
                            similarity(sd.search_text, %s) * 0.85,
                            similarity(sd.normalized_address, %s) * 0.95
                          )
                        END AS match_score
                      FROM search_documents sd
                      WHERE sd.account_number LIKE %s
                        OR sd.normalized_address LIKE %s
                        OR (%s <> '' AND sd.normalized_owner_name LIKE %s)
                        OR sd.search_text %% %s
                    )
                    SELECT *
                    FROM prefix_candidates
                    ORDER BY
                      CASE match_basis
                        WHEN 'account_prefix' THEN 1
                        WHEN 'address_prefix' THEN 2
                        WHEN 'owner_prefix' THEN 3
                        ELSE 4
                      END,
                      match_score DESC,
                      county_id ASC,
                      account_number ASC
                    LIMIT %s
                    """,
                    (
                        f"{raw_query}%",
                        f"{normalized_address}%",
                        normalized_owner,
                        f"{normalized_owner}%",
                        f"{raw_query}%",
                        f"{normalized_address}%",
                        normalized_owner,
                        f"{normalized_owner}%",
                        normalized_address,
                        normalized_address,
                        f"{raw_query}%",
                        f"{normalized_address}%",
                        normalized_owner,
                        f"{normalized_owner}%",
                        normalized_address,
                        limit,
                    ),
                )
                rows = cursor.fetchall()

        return [self._build_result(row) for row in rows]

    def _build_result(self, row: dict[str, Any]) -> ParcelSearchResult:
        score = float(row["match_score"])
        return ParcelSearchResult(
            county_id=row["county_id"],
            tax_year=row.get("tax_year"),
            account_number=row["account_number"],
            parcel_id=row["parcel_id"],
            address=row["address"],
            situs_zip=row.get("situs_zip"),
            owner_name=row.get("owner_name"),
            match_basis=row["match_basis"],
            match_score=score,
            confidence_label=self._confidence_label(score),
        )

    def _confidence_label(self, score: float) -> str:
        if score >= 0.99:
            return "very_high"
        if score >= 0.85:
            return "high"
        if score >= 0.65:
            return "medium"
        return "low"
