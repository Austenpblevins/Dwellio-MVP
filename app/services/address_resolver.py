from __future__ import annotations

from typing import Any

from app.db.connection import get_connection
from app.models.admin import (
    AdminSearchInspectCandidate,
    AdminSearchInspectResponse,
    AdminSearchScoreComponents,
)
from app.models.parcel import ParcelSearchResult
from app.models.quote import SearchRequest
from app.services.ownership_reconciliation import normalize_owner_name
from app.utils.text_normalization import normalize_address_query


class AddressResolverService:
    def search(self, request: SearchRequest) -> list[ParcelSearchResult]:
        return self.search_by_query(request.address)

    def search_by_query(self, query: str, *, limit: int = 10) -> list[ParcelSearchResult]:
        rows = self._search_candidate_rows(query=query, limit=limit, include_debug=False)
        return [self._build_result(row) for row in rows]

    def inspect_search(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> AdminSearchInspectResponse:
        raw_query = query.strip()
        normalized_address = normalize_address_query(raw_query)
        normalized_owner = normalize_owner_name(raw_query) or ""
        rows = self._search_candidate_rows(query=query, limit=limit, include_debug=True)

        return AdminSearchInspectResponse(
            query=raw_query,
            normalized_address_query=normalized_address,
            normalized_owner_query=normalized_owner or None,
            candidates=[self._build_debug_result(row) for row in rows],
        )

    def _search_candidate_rows(
        self,
        *,
        query: str,
        limit: int,
        include_debug: bool,
    ) -> list[dict[str, Any]]:
        raw_query = query.strip()
        normalized_address = normalize_address_query(raw_query)
        normalized_owner = normalize_owner_name(raw_query) or ""
        debug_select = ""
        if include_debug:
            debug_select = """
                        ,
                        similarity(sd.normalized_address, %s) AS address_similarity,
                        similarity(sd.search_text, %s) AS search_text_similarity,
                        CASE
                          WHEN %s = '' OR sd.normalized_owner_name IS NULL THEN 0::numeric
                          ELSE similarity(sd.normalized_owner_name, %s)
                        END AS owner_similarity,
                        CASE
                          WHEN sd.account_number = %s THEN ARRAY['account_number']::text[]
                          WHEN sd.normalized_address = %s THEN ARRAY['normalized_address']::text[]
                          WHEN %s <> '' AND sd.normalized_owner_name IS NOT NULL AND sd.normalized_owner_name %% %s
                            THEN ARRAY['normalized_owner_name']::text[]
                          WHEN sd.normalized_address %% %s THEN ARRAY['normalized_address', 'search_text']::text[]
                          ELSE ARRAY['search_text']::text[]
                        END AS matched_fields
            """
        sql = f"""
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
                        {debug_select}
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
                    """

        params: list[object] = [
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
        ]
        if include_debug:
            params.extend(
                [
                    normalized_address,
                    normalized_address,
                    normalized_owner,
                    normalized_owner,
                    raw_query,
                    normalized_address,
                    normalized_owner,
                    normalized_owner,
                    normalized_address,
                ]
            )
        params.extend(
            [
                raw_query,
                normalized_address,
                normalized_address,
                normalized_address,
                normalized_owner,
                normalized_owner,
                0.35,
                limit,
            ]
        )

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return cursor.fetchall()

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
        match_basis = str(row["match_basis"])
        return ParcelSearchResult(
            county_id=row["county_id"],
            tax_year=row.get("tax_year"),
            account_number=row["account_number"],
            parcel_id=row["parcel_id"],
            address=row["address"],
            situs_zip=row.get("situs_zip"),
            owner_name=row.get("owner_name"),
            match_basis=match_basis,
            match_score=score,
            confidence_label=self._confidence_label(score, match_basis),
        )

    def _build_debug_result(self, row: dict[str, Any]) -> AdminSearchInspectCandidate:
        score = float(row["match_score"])
        match_basis = str(row["match_basis"])
        return AdminSearchInspectCandidate(
            county_id=row["county_id"],
            tax_year=row.get("tax_year"),
            account_number=row["account_number"],
            parcel_id=str(row["parcel_id"]),
            address=row["address"],
            situs_zip=row.get("situs_zip"),
            owner_name=row.get("owner_name"),
            match_basis=match_basis,
            match_score=score,
            confidence_label=self._confidence_label(score, match_basis),
            confidence_reasons=self._confidence_reasons(score, match_basis),
            matched_fields=[str(field) for field in row.get("matched_fields") or []],
            score_components=AdminSearchScoreComponents(
                basis_rank=self._basis_rank(match_basis),
                address_similarity=float(row.get("address_similarity") or 0.0),
                search_text_similarity=float(row.get("search_text_similarity") or 0.0),
                owner_similarity=float(row.get("owner_similarity") or 0.0),
            ),
        )

    def _confidence_label(self, score: float, match_basis: str) -> str:
        if match_basis == "account_exact":
            return "very_high"
        if match_basis == "address_exact":
            return "very_high"
        if match_basis in {"account_prefix", "address_prefix"}:
            return "high" if score >= 0.90 else "medium"
        if match_basis == "owner_prefix":
            return "medium" if score >= 0.80 else "low"
        if match_basis == "address_trigram":
            return "high" if score >= 0.80 else "medium"
        if match_basis == "search_text_trigram":
            return "medium" if score >= 0.60 else "low"
        if match_basis == "owner_fallback":
            return "medium" if score >= 0.50 else "low"
        if score >= 0.99:
            return "very_high"
        if score >= 0.85:
            return "high"
        if score >= 0.65:
            return "medium"
        return "low"

    def _confidence_reasons(self, score: float, match_basis: str) -> list[str]:
        reasons: list[str] = []
        if match_basis in {"account_exact", "address_exact"}:
            reasons.append("exact_match")
        elif match_basis in {"account_prefix", "address_prefix", "owner_prefix"}:
            reasons.append("prefix_match")
        elif match_basis == "address_trigram":
            reasons.append("address_similarity")
        elif match_basis == "search_text_trigram":
            reasons.append("search_text_similarity")
        elif match_basis == "owner_fallback":
            reasons.append("owner_name_fallback")
        else:
            reasons.append("fallback_match")

        if score >= 0.90:
            reasons.append("score_strong")
        elif score >= 0.60:
            reasons.append("score_moderate")
        else:
            reasons.append("score_weak")
        return reasons

    def _basis_rank(self, match_basis: str) -> int:
        order = {
            "account_exact": 1,
            "address_exact": 2,
            "account_prefix": 3,
            "address_prefix": 4,
            "address_trigram": 5,
            "search_text_trigram": 6,
            "owner_prefix": 7,
            "owner_fallback": 8,
            "search_text_fallback": 9,
        }
        return order.get(match_basis, 99)
