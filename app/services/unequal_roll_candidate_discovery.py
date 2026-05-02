from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.unequal_roll_candidate_normalization import (
    ordinal_gap,
    property_class_relation,
)
from app.services.unequal_roll_candidate_scoring import compute_similarity_score
from app.services.unequal_roll_subject_snapshot import FORT_BEND_BATHROOM_SOURCE_TABLE

DISCOVERY_TIER_SAME_NEIGHBORHOOD = "same_neighborhood"
DISCOVERY_TIER_COUNTY_SFR_FALLBACK = "county_sfr_fallback"
PREFERRED_RAW_CANDIDATE_POOL = 25
MAX_AUTO_HARVEST = 100
SPARSE_UNIVERSE_WARNING_THRESHOLD = 15
FALLBACK_PREFILTER_MULTIPLIER = 8
FALLBACK_PREFILTER_MAX_ROWS = 800
FORT_BEND_AUTO_USABLE_BATHROOM_STATUSES = {
    "exact_supported",
    "reconciled_fractional_plumbing",
    "quarter_bath_present",
}

@dataclass(frozen=True)
class UnequalRollCandidateDiscoveryResult:
    unequal_roll_run_id: str
    county_id: str
    tax_year: int
    subject_parcel_id: str
    discovered_count: int
    same_neighborhood_count: int
    county_sfr_fallback_count: int
    eligible_count: int
    review_count: int
    excluded_count: int
    fallback_used: bool
    sparse_universe_warning: bool


class UnequalRollCandidateDiscoveryService:
    def discover_candidates_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollCandidateDiscoveryResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                subject_snapshot = self._fetch_subject_snapshot(
                    cursor, unequal_roll_run_id=unequal_roll_run_id
                )
                if subject_snapshot is None:
                    raise LookupError(
                        f"Unequal-roll subject snapshot not found for run {unequal_roll_run_id}."
                    )
                self._validate_subject_snapshot(subject_snapshot)
                self._delete_existing_candidates(cursor, unequal_roll_run_id=unequal_roll_run_id)

                same_neighborhood_rows = self._fetch_same_neighborhood_candidates(
                    cursor, subject_snapshot=subject_snapshot
                )
                rows_to_persist: list[tuple[str, dict[str, Any]]] = [
                    (DISCOVERY_TIER_SAME_NEIGHBORHOOD, row) for row in same_neighborhood_rows
                ]

                if len(rows_to_persist) < PREFERRED_RAW_CANDIDATE_POOL:
                    fallback_rows = self._fetch_county_sfr_fallback_candidates(
                        cursor,
                        subject_snapshot=subject_snapshot,
                        already_selected_parcel_ids={
                            str(row["parcel_id"]) for row in same_neighborhood_rows
                        },
                        remaining_limit=MAX_AUTO_HARVEST - len(rows_to_persist),
                    )
                    rows_to_persist.extend(
                        (DISCOVERY_TIER_COUNTY_SFR_FALLBACK, row) for row in fallback_rows
                    )

                same_neighborhood_count = 0
                county_sfr_fallback_count = 0
                eligible_count = 0
                review_count = 0
                excluded_count = 0
                for discovery_tier, row in rows_to_persist:
                    valuation_bathroom_features_json = (
                        self._fetch_candidate_valuation_bathroom_features_json(
                            cursor,
                            county_id=str(subject_snapshot["county_id"]),
                            candidate_parcel_id=str(row["parcel_id"]),
                            tax_year=int(row["tax_year"]),
                        )
                    )
                    eligibility_status, eligibility_reason_code, eligibility_detail_json = (
                        self._evaluate_candidate_eligibility(
                            subject_snapshot=subject_snapshot,
                            row=row,
                            discovery_tier=discovery_tier,
                            valuation_bathroom_features_json=valuation_bathroom_features_json,
                        )
                    )
                    similarity_score_result = compute_similarity_score(
                        subject_snapshot=subject_snapshot,
                        row=row,
                        discovery_tier=discovery_tier,
                        eligibility_status=eligibility_status,
                        eligibility_detail_json=eligibility_detail_json,
                        valuation_bathroom_features_json=valuation_bathroom_features_json,
                    )
                    self._insert_candidate(
                        cursor,
                        unequal_roll_run_id=unequal_roll_run_id,
                        discovery_tier=discovery_tier,
                        subject_snapshot=subject_snapshot,
                        row=row,
                        eligibility_status=eligibility_status,
                        eligibility_reason_code=eligibility_reason_code,
                        eligibility_detail_json=eligibility_detail_json,
                        similarity_score_result=similarity_score_result,
                        valuation_bathroom_features_json=valuation_bathroom_features_json,
                    )
                    if discovery_tier == DISCOVERY_TIER_SAME_NEIGHBORHOOD:
                        same_neighborhood_count += 1
                    else:
                        county_sfr_fallback_count += 1
                    if eligibility_status == "eligible":
                        eligible_count += 1
                    elif eligibility_status == "review":
                        review_count += 1
                    else:
                        excluded_count += 1

                self._update_run_candidate_discovery_summary(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                    subject_snapshot=subject_snapshot,
                    discovered_count=len(rows_to_persist),
                    same_neighborhood_count=same_neighborhood_count,
                    county_sfr_fallback_count=county_sfr_fallback_count,
                    eligible_count=eligible_count,
                    review_count=review_count,
                    excluded_count=excluded_count,
                )
            connection.commit()

        discovered_count = same_neighborhood_count + county_sfr_fallback_count
        return UnequalRollCandidateDiscoveryResult(
            unequal_roll_run_id=unequal_roll_run_id,
            county_id=str(subject_snapshot["county_id"]),
            tax_year=int(subject_snapshot["tax_year"]),
            subject_parcel_id=str(subject_snapshot["parcel_id"]),
            discovered_count=discovered_count,
            same_neighborhood_count=same_neighborhood_count,
            county_sfr_fallback_count=county_sfr_fallback_count,
            eligible_count=eligible_count,
            review_count=review_count,
            excluded_count=excluded_count,
            fallback_used=county_sfr_fallback_count > 0,
            sparse_universe_warning=discovered_count < SPARSE_UNIVERSE_WARNING_THRESHOLD,
        )

    def _fetch_subject_snapshot(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT
              urr.run_status,
              urr.readiness_status,
              urr.support_status,
              urr.subject_snapshot_status,
              urr.summary_json,
              urss.*
            FROM unequal_roll_runs AS urr
            JOIN unequal_roll_subject_snapshots AS urss
              ON urss.unequal_roll_run_id = urr.unequal_roll_run_id
            WHERE urr.unequal_roll_run_id = %s
            LIMIT 1
            """,
            (unequal_roll_run_id,),
        )
        return cursor.fetchone()

    def _validate_subject_snapshot(self, subject_snapshot: dict[str, Any]) -> None:
        if subject_snapshot.get("run_status") != "completed":
            raise ValueError(
                "Unequal-roll candidate discovery requires a completed unequal-roll run."
            )

        if subject_snapshot.get("readiness_status") != "ready":
            raise ValueError(
                "Unequal-roll candidate discovery requires a run with readiness_status=ready."
            )

        if subject_snapshot.get("support_status") not in {"supported", "supported_with_review"}:
            raise ValueError(
                "Unequal-roll candidate discovery requires support_status of supported or "
                "supported_with_review."
            )

        if subject_snapshot.get("subject_snapshot_status") != "completed":
            raise ValueError("Unequal-roll candidate discovery requires a completed subject snapshot.")

        if str(subject_snapshot.get("property_type_code") or "").lower() != "sfr":
            raise ValueError("Unequal-roll candidate discovery currently supports SFR subjects only.")

        if not str(subject_snapshot.get("neighborhood_code") or "").strip():
            raise ValueError(
                "Unequal-roll candidate discovery requires a subject neighborhood_code."
            )

    def _delete_existing_candidates(self, cursor: Any, *, unequal_roll_run_id: str) -> None:
        cursor.execute(
            "DELETE FROM unequal_roll_candidates WHERE unequal_roll_run_id = %s",
            (unequal_roll_run_id,),
        )

    def _fetch_same_neighborhood_candidates(
        self,
        cursor: Any,
        *,
        subject_snapshot: dict[str, Any],
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              pys.parcel_id,
              pys.county_id,
              pys.tax_year,
              pys.account_number,
              pa.situs_address AS address,
              pc.neighborhood_code,
              pc.subdivision_name,
              pc.property_type_code,
              pc.property_class_code,
              pi.living_area_sf,
              pi.year_built,
              pi.effective_age,
              pi.bedrooms,
              pi.full_baths,
              pi.half_baths,
              pi.total_rooms,
              pi.stories,
              pi.quality_code,
              pi.condition_code,
              pi.pool_flag,
              pl.land_sf,
              pl.land_acres,
              ass.market_value,
              ass.assessed_value,
              ass.appraised_value,
              ass.certified_value,
              ass.notice_value
            FROM parcel_year_snapshots AS pys
            JOIN property_characteristics AS pc
              ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
            JOIN parcel_assessments AS ass
              ON ass.parcel_id = pys.parcel_id
             AND ass.tax_year = pys.tax_year
            JOIN parcel_improvements AS pi
              ON pi.parcel_id = pys.parcel_id
             AND pi.tax_year = pys.tax_year
            LEFT JOIN parcel_lands AS pl
              ON pl.parcel_id = pys.parcel_id
             AND pl.tax_year = pys.tax_year
            LEFT JOIN LATERAL (
              SELECT
                pa.situs_address
              FROM parcel_addresses AS pa
              WHERE pa.parcel_id = pys.parcel_id
                AND pa.is_current = true
              ORDER BY
                pa.updated_at DESC,
                pa.created_at DESC,
                pa.parcel_address_id DESC
              LIMIT 1
            ) AS pa ON true
            WHERE pys.is_current = true
              AND pys.county_id = %s
              AND pys.tax_year = %s
              AND pys.parcel_id <> %s
              AND lower(coalesce(pc.property_type_code, '')) = 'sfr'
              AND pc.neighborhood_code = %s
              AND coalesce(pi.living_area_sf, 0) > 0
              AND coalesce(ass.appraised_value, 0) > 0
            ORDER BY
              CASE
                WHEN %s IS NOT NULL
                  AND btrim(%s) <> ''
                  AND pc.subdivision_name = %s
                THEN 0
                ELSE 1
              END,
              pys.account_number
            LIMIT %s
            """,
            (
                subject_snapshot["county_id"],
                subject_snapshot["tax_year"],
                subject_snapshot["parcel_id"],
                subject_snapshot["neighborhood_code"],
                subject_snapshot.get("subdivision_name"),
                subject_snapshot.get("subdivision_name"),
                subject_snapshot.get("subdivision_name"),
                MAX_AUTO_HARVEST,
            ),
        )
        return list(cursor.fetchall())

    def _fetch_county_sfr_fallback_candidates(
        self,
        cursor: Any,
        *,
        subject_snapshot: dict[str, Any],
        already_selected_parcel_ids: set[str],
        remaining_limit: int,
    ) -> list[dict[str, Any]]:
        if remaining_limit <= 0:
            return []

        prefilter_limit = min(
            FALLBACK_PREFILTER_MAX_ROWS,
            max(remaining_limit, remaining_limit * FALLBACK_PREFILTER_MULTIPLIER),
        )
        cursor.execute(
            """
            WITH candidate_keys AS (
              SELECT
                pys.parcel_id,
                pys.tax_year,
                pys.account_number,
                pc.neighborhood_code,
                pc.subdivision_name
              FROM parcel_year_snapshots AS pys
              JOIN property_characteristics AS pc
                ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
              WHERE pys.is_current = true
                AND pys.county_id = %s
                AND pys.tax_year = %s
                AND pys.parcel_id <> %s
                AND lower(coalesce(pc.property_type_code, '')) = 'sfr'
                AND (
                  pc.neighborhood_code IS DISTINCT FROM %s
                )
              ORDER BY
                CASE
                  WHEN COALESCE(%s::text, '') <> ''
                    AND pc.subdivision_name = %s
                  THEN 0
                  ELSE 1
                END,
                pc.neighborhood_code,
                pys.account_number
              LIMIT %s
            )
            SELECT
              pys.parcel_id,
              pys.county_id,
              pys.tax_year,
              pys.account_number,
              pa.situs_address AS address,
              pc.neighborhood_code,
              pc.subdivision_name,
              pc.property_type_code,
              pc.property_class_code,
              pi.living_area_sf,
              pi.year_built,
              pi.effective_age,
              pi.bedrooms,
              pi.full_baths,
              pi.half_baths,
              pi.total_rooms,
              pi.stories,
              pi.quality_code,
              pi.condition_code,
              pi.pool_flag,
              pl.land_sf,
              pl.land_acres,
              ass.market_value,
              ass.assessed_value,
              ass.appraised_value,
              ass.certified_value,
              ass.notice_value
            FROM candidate_keys AS ck
            JOIN parcel_year_snapshots AS pys
              ON pys.parcel_id = ck.parcel_id
             AND pys.tax_year = ck.tax_year
             AND pys.is_current = true
            JOIN property_characteristics AS pc
              ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
            JOIN parcel_assessments AS ass
              ON ass.parcel_id = pys.parcel_id
             AND ass.tax_year = pys.tax_year
            JOIN parcel_improvements AS pi
              ON pi.parcel_id = pys.parcel_id
             AND pi.tax_year = pys.tax_year
            LEFT JOIN parcel_lands AS pl
              ON pl.parcel_id = pys.parcel_id
             AND pl.tax_year = pys.tax_year
            LEFT JOIN LATERAL (
              SELECT
                pa.situs_address
              FROM parcel_addresses AS pa
              WHERE pa.parcel_id = pys.parcel_id
                AND pa.is_current = true
              ORDER BY
                pa.updated_at DESC,
                pa.created_at DESC,
                pa.parcel_address_id DESC
              LIMIT 1
            ) AS pa ON true
            WHERE pys.county_id = %s
              AND pys.tax_year = %s
              AND coalesce(pi.living_area_sf, 0) > 0
              AND coalesce(ass.appraised_value, 0) > 0
            ORDER BY
              CASE
                WHEN COALESCE(%s::text, '') <> ''
                  AND ck.subdivision_name = %s
                THEN 0
                ELSE 1
              END,
              ck.neighborhood_code,
              ck.account_number
            LIMIT %s
            """,
            (
                subject_snapshot["county_id"],
                subject_snapshot["tax_year"],
                subject_snapshot["parcel_id"],
                subject_snapshot["neighborhood_code"],
                subject_snapshot.get("subdivision_name"),
                subject_snapshot.get("subdivision_name"),
                prefilter_limit,
                subject_snapshot["county_id"],
                subject_snapshot["tax_year"],
                subject_snapshot.get("subdivision_name"),
                subject_snapshot.get("subdivision_name"),
                remaining_limit,
            ),
        )
        return [
            row
            for row in cursor.fetchall()
            if str(row["parcel_id"]) not in already_selected_parcel_ids
        ]

    def _fetch_candidate_valuation_bathroom_features_json(
        self,
        cursor: Any,
        *,
        county_id: str,
        candidate_parcel_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        if county_id != "fort_bend":
            return None

        cursor.execute(
            """
            SELECT
              quick_ref_id,
              account_number,
              selected_improvement_number,
              selected_improvement_rule_version,
              normalization_rule_version,
              source_file_version,
              source_file_name,
              selected_improvement_source_row_count,
              plumbing_raw,
              half_baths_raw,
              quarter_baths_raw,
              plumbing_raw_values,
              half_baths_raw_values,
              quarter_baths_raw_values,
              full_baths_derived,
              half_baths_derived,
              quarter_baths_derived,
              bathroom_equivalent_derived,
              bathroom_count_status,
              bathroom_count_confidence,
              bathroom_flags
            FROM fort_bend_valuation_bathroom_features
            WHERE parcel_id = %s
              AND tax_year = %s
            """,
            (candidate_parcel_id, tax_year),
        )
        row = cursor.fetchone()
        if row is None:
            return {
                "attachment_status": "missing",
                "source_table": FORT_BEND_BATHROOM_SOURCE_TABLE,
                "county_contract": "fort_bend_valuation_only",
            }

        return {
            "attachment_status": "attached",
            "source_table": FORT_BEND_BATHROOM_SOURCE_TABLE,
            "county_contract": "fort_bend_valuation_only",
            "quick_ref_id": row.get("quick_ref_id"),
            "account_number": row.get("account_number"),
            "selected_improvement_number": row.get("selected_improvement_number"),
            "selected_improvement_rule_version": row.get("selected_improvement_rule_version"),
            "normalization_rule_version": row.get("normalization_rule_version"),
            "source_file_version": row.get("source_file_version"),
            "source_file_name": row.get("source_file_name"),
            "selected_improvement_source_row_count": row.get("selected_improvement_source_row_count"),
            "plumbing_raw": _as_float(row.get("plumbing_raw")),
            "half_baths_raw": _as_float(row.get("half_baths_raw")),
            "quarter_baths_raw": _as_float(row.get("quarter_baths_raw")),
            "plumbing_raw_values": list(row.get("plumbing_raw_values") or []),
            "half_baths_raw_values": list(row.get("half_baths_raw_values") or []),
            "quarter_baths_raw_values": list(row.get("quarter_baths_raw_values") or []),
            "full_baths_derived": _as_float(row.get("full_baths_derived")),
            "half_baths_derived": _as_float(row.get("half_baths_derived")),
            "quarter_baths_derived": _as_float(row.get("quarter_baths_derived")),
            "bathroom_equivalent_derived": _as_float(row.get("bathroom_equivalent_derived")),
            "bathroom_count_status": row.get("bathroom_count_status"),
            "bathroom_count_confidence": row.get("bathroom_count_confidence"),
            "bathroom_flags": [str(flag) for flag in row.get("bathroom_flags") or []],
        }

    def _insert_candidate(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        discovery_tier: str,
        subject_snapshot: dict[str, Any],
        row: dict[str, Any],
        eligibility_status: str,
        eligibility_reason_code: str | None,
        eligibility_detail_json: dict[str, Any],
        similarity_score_result: Any,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> None:
        source_provenance_json = self._build_source_provenance_json(
            subject_snapshot=subject_snapshot,
            row=row,
            discovery_tier=discovery_tier,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        candidate_snapshot_json = self._build_candidate_snapshot_json(
            subject_snapshot=subject_snapshot,
            row=row,
            discovery_tier=discovery_tier,
            eligibility_status=eligibility_status,
            eligibility_reason_code=eligibility_reason_code,
            eligibility_detail_json=eligibility_detail_json,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        cursor.execute(
            """
            INSERT INTO unequal_roll_candidates (
              unequal_roll_run_id,
              candidate_parcel_id,
              county_id,
              tax_year,
              account_number,
              address,
              neighborhood_code,
              subdivision_name,
              property_type_code,
              property_class_code,
              living_area_sf,
              year_built,
              effective_age,
              bedrooms,
              full_baths,
              half_baths,
              total_rooms,
              stories,
              quality_code,
              condition_code,
              pool_flag,
              land_sf,
              land_acres,
              market_value,
              assessed_value,
              appraised_value,
              certified_value,
              notice_value,
              discovery_tier,
              candidate_status,
              eligibility_status,
              eligibility_reason_code,
              eligibility_detail_json,
              source_provenance_json,
              candidate_snapshot_json
              ,
              raw_similarity_score,
              normalized_similarity_score,
              scoring_version,
              scoring_config_version,
              similarity_score_detail_json
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                unequal_roll_run_id,
                row["parcel_id"],
                row["county_id"],
                row["tax_year"],
                row.get("account_number"),
                row.get("address"),
                row.get("neighborhood_code"),
                row.get("subdivision_name"),
                row.get("property_type_code"),
                row.get("property_class_code"),
                _as_float(row.get("living_area_sf")),
                _as_int(row.get("year_built")),
                _as_float(row.get("effective_age")),
                _as_int(row.get("bedrooms")),
                _as_float(row.get("full_baths")),
                _as_float(row.get("half_baths")),
                _as_int(row.get("total_rooms")),
                _as_float(row.get("stories")),
                row.get("quality_code"),
                row.get("condition_code"),
                _as_bool(row.get("pool_flag")),
                _as_float(row.get("land_sf")),
                _as_float(row.get("land_acres")),
                _as_float(row.get("market_value")),
                _as_float(row.get("assessed_value")),
                _as_float(row.get("appraised_value")),
                _as_float(row.get("certified_value")),
                _as_float(row.get("notice_value")),
                discovery_tier,
                "discovered",
                eligibility_status,
                eligibility_reason_code,
                Jsonb(eligibility_detail_json),
                Jsonb(source_provenance_json),
                Jsonb(candidate_snapshot_json),
                similarity_score_result.raw_similarity_score,
                similarity_score_result.normalized_similarity_score,
                similarity_score_result.scoring_version,
                similarity_score_result.scoring_config_version,
                Jsonb(similarity_score_result.score_detail_json),
            ),
        )

    def _build_source_provenance_json(
        self,
        *,
        subject_snapshot: dict[str, Any],
        row: dict[str, Any],
        discovery_tier: str,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "discovery_tier": discovery_tier,
            "subject_source": "unequal_roll_subject_snapshots",
            "candidate_source": {
                "type": "derived_view",
                "name": "parcel_summary_view",
                "backing_tables": [
                    "parcel_year_snapshots",
                    "parcel_addresses",
                    "parcel_improvements",
                    "parcel_lands",
                    "parcel_assessments",
                    "parcel_exemptions",
                ],
            },
            "total_rooms_source": "parcel_improvements",
            "same_neighborhood_flag": (
                row.get("neighborhood_code") == subject_snapshot.get("neighborhood_code")
            ),
            "same_subdivision_flag": (
                str(row.get("subdivision_name") or "").strip() != ""
                and row.get("subdivision_name") == subject_snapshot.get("subdivision_name")
            ),
            "valuation_bathroom_attachment_status": (
                valuation_bathroom_features_json.get("attachment_status")
                if valuation_bathroom_features_json is not None
                else "not_applicable"
            ),
        }

    def _build_candidate_snapshot_json(
        self,
        *,
        subject_snapshot: dict[str, Any],
        row: dict[str, Any],
        discovery_tier: str,
        eligibility_status: str,
        eligibility_reason_code: str | None,
        eligibility_detail_json: dict[str, Any],
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "discovery_tier": discovery_tier,
            "candidate": {
                "parcel_id": str(row["parcel_id"]),
                "county_id": row.get("county_id"),
                "tax_year": row.get("tax_year"),
                "account_number": row.get("account_number"),
                "address": row.get("address"),
                "neighborhood_code": row.get("neighborhood_code"),
                "subdivision_name": row.get("subdivision_name"),
                "property_type_code": row.get("property_type_code"),
                "property_class_code": row.get("property_class_code"),
                "living_area_sf": _as_float(row.get("living_area_sf")),
                "year_built": _as_int(row.get("year_built")),
                "effective_age": _as_float(row.get("effective_age")),
                "bedrooms": _as_int(row.get("bedrooms")),
                "full_baths": _as_float(row.get("full_baths")),
                "half_baths": _as_float(row.get("half_baths")),
                "total_rooms": _as_int(row.get("total_rooms")),
                "stories": _as_float(row.get("stories")),
                "quality_code": row.get("quality_code"),
                "condition_code": row.get("condition_code"),
                "pool_flag": _as_bool(row.get("pool_flag")),
                "land_sf": _as_float(row.get("land_sf")),
                "land_acres": _as_float(row.get("land_acres")),
                "market_value": _as_float(row.get("market_value")),
                "assessed_value": _as_float(row.get("assessed_value")),
                "appraised_value": _as_float(row.get("appraised_value")),
                "certified_value": _as_float(row.get("certified_value")),
                "notice_value": _as_float(row.get("notice_value")),
            },
            "subject_relationship": {
                "same_neighborhood_flag": (
                    row.get("neighborhood_code") == subject_snapshot.get("neighborhood_code")
                ),
                "same_subdivision_flag": (
                    str(row.get("subdivision_name") or "").strip() != ""
                    and row.get("subdivision_name") == subject_snapshot.get("subdivision_name")
                ),
                "same_property_class_flag": (
                    str(row.get("property_class_code") or "").strip() != ""
                    and row.get("property_class_code")
                    == subject_snapshot.get("property_class_code")
                ),
            },
            "eligibility": {
                "eligibility_status": eligibility_status,
                "eligibility_reason_code": eligibility_reason_code,
                **eligibility_detail_json,
            },
            "valuation_bathroom_features": valuation_bathroom_features_json,
        }

    def _evaluate_candidate_eligibility(
        self,
        *,
        subject_snapshot: dict[str, Any],
        row: dict[str, Any],
        discovery_tier: str,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> tuple[str, str | None, dict[str, Any]]:
        same_neighborhood_flag = row.get("neighborhood_code") == subject_snapshot.get("neighborhood_code")
        same_subdivision_flag = (
            str(row.get("subdivision_name") or "").strip() != ""
            and row.get("subdivision_name") == subject_snapshot.get("subdivision_name")
        )
        same_property_class_flag = (
            str(row.get("property_class_code") or "").strip() != ""
            and row.get("property_class_code") == subject_snapshot.get("property_class_code")
        )
        subject_living_area_sf = _as_float(subject_snapshot.get("living_area_sf"))
        candidate_living_area_sf = _as_float(row.get("living_area_sf"))
        living_area_diff_pct = _pct_diff(subject_living_area_sf, candidate_living_area_sf)
        subject_land_size = _preferred_land_size(subject_snapshot)
        candidate_land_size = _preferred_land_size(row)
        land_size_diff_pct = _pct_diff(subject_land_size, candidate_land_size)
        subject_bedrooms = _as_int(subject_snapshot.get("bedrooms"))
        candidate_bedrooms = _as_int(row.get("bedrooms"))
        bedroom_diff_abs = _abs_diff(subject_bedrooms, candidate_bedrooms)
        subject_effective_baths = _effective_baths(
            subject_snapshot.get("full_baths"),
            subject_snapshot.get("half_baths"),
        )
        if (
            valuation_bathroom_features_json is not None
            and _as_float(row.get("full_baths")) is None
        ):
            candidate_effective_baths = None
        else:
            candidate_effective_baths = _effective_baths(
                row.get("full_baths"),
                row.get("half_baths"),
            )
        effective_bath_diff = _abs_diff(subject_effective_baths, candidate_effective_baths)
        subject_stories = _as_float(subject_snapshot.get("stories"))
        candidate_stories = _as_float(row.get("stories"))
        story_diff_abs = _abs_diff(subject_stories, candidate_stories)
        property_class_relation_value = property_class_relation(
            county_id=str(subject_snapshot.get("county_id") or ""),
            subject_value=subject_snapshot.get("property_class_code"),
            candidate_value=row.get("property_class_code"),
        )
        quality_gap_steps = ordinal_gap(
            county_id=str(subject_snapshot.get("county_id") or ""),
            field_name="quality",
            subject_value=subject_snapshot.get("quality_code"),
            candidate_value=row.get("quality_code"),
        )
        condition_gap_steps = ordinal_gap(
            county_id=str(subject_snapshot.get("county_id") or ""),
            field_name="condition",
            subject_value=subject_snapshot.get("condition_code"),
            candidate_value=row.get("condition_code"),
        )
        fort_bend_bathroom_review = _fort_bend_bathroom_review(
            valuation_bathroom_features_json
        )

        threshold_observations = {
            "living_area_diff_pct": living_area_diff_pct,
            "land_size_diff_pct": land_size_diff_pct,
            "bedroom_diff_abs": bedroom_diff_abs,
            "effective_bath_diff": effective_bath_diff,
            "story_diff_abs": story_diff_abs,
            "quality_gap_steps": quality_gap_steps,
            "condition_gap_steps": condition_gap_steps,
            "fallback_tier_used": discovery_tier == DISCOVERY_TIER_COUNTY_SFR_FALLBACK,
        }
        subject_relationship = {
            "same_neighborhood_flag": same_neighborhood_flag,
            "same_subdivision_flag": same_subdivision_flag,
            "same_property_class_flag": same_property_class_flag,
            "property_class_relation": property_class_relation_value,
        }
        exclusion_reasons: list[str] = []
        review_reasons: list[str] = []

        property_type_code = str(row.get("property_type_code") or "").lower()
        if property_type_code and property_type_code != "sfr":
            exclusion_reasons.append("unsupported_property_type")
        if candidate_living_area_sf is None or candidate_living_area_sf <= 0:
            exclusion_reasons.append("missing_living_area")
        if _as_float(row.get("appraised_value")) in {None, 0.0}:
            exclusion_reasons.append("missing_appraised_value")
        if living_area_diff_pct is not None and living_area_diff_pct > 0.20:
            exclusion_reasons.append("living_area_out_of_bounds")
        if bedroom_diff_abs is not None and bedroom_diff_abs > 2:
            exclusion_reasons.append("bedroom_count_out_of_bounds")
        if story_diff_abs is not None and story_diff_abs > 1.0:
            exclusion_reasons.append("story_count_out_of_bounds")
        if effective_bath_diff is not None and effective_bath_diff > 2.0:
            exclusion_reasons.append("effective_bath_count_out_of_bounds")
        if property_class_relation_value == "non_adjacent":
            exclusion_reasons.append("property_class_non_adjacent")
        if quality_gap_steps is not None and quality_gap_steps > 1:
            exclusion_reasons.append("quality_non_adjacent")
        if condition_gap_steps is not None and condition_gap_steps > 1:
            exclusion_reasons.append("condition_non_adjacent")
        if _acreage_profile_mismatch(subject_snapshot, row):
            exclusion_reasons.append("acreage_profile_mismatch")

        if discovery_tier == DISCOVERY_TIER_COUNTY_SFR_FALLBACK:
            review_reasons.append("fallback_geography_used")
        if living_area_diff_pct is not None and living_area_diff_pct > 0.15:
            review_reasons.append("wide_living_area_gap")
        if land_size_diff_pct is not None and land_size_diff_pct > 0.25:
            review_reasons.append("wide_lot_size_gap")
        if bedroom_diff_abs is None:
            review_reasons.append("missing_bedrooms")
        elif bedroom_diff_abs > 1:
            review_reasons.append("bedroom_count_review")
        if candidate_stories is None or subject_stories is None:
            review_reasons.append("missing_stories")
        if (
            effective_bath_diff is None
            and subject_effective_baths is not None
            and valuation_bathroom_features_json is None
        ):
            review_reasons.append("missing_effective_baths")
        elif effective_bath_diff is not None and effective_bath_diff > 1.0:
            review_reasons.append("effective_bath_count_review")
        if str(row.get("property_class_code") or "").strip() == "":
            review_reasons.append("missing_property_class_code")
        elif property_class_relation_value == "adjacent_family":
            review_reasons.append("property_class_adjacent_family")
        if str(row.get("quality_code") or "").strip() == "":
            review_reasons.append("missing_quality_code")
        elif quality_gap_steps == 1:
            review_reasons.append("quality_adjacent")
        elif (
            quality_gap_steps is None
            and str(subject_snapshot.get("quality_code") or "").strip() != ""
            and str(row.get("quality_code") or "").strip() != ""
            and row.get("quality_code") != subject_snapshot.get("quality_code")
        ):
            review_reasons.append("quality_mismatch_unmapped")
        if str(row.get("condition_code") or "").strip() == "":
            review_reasons.append("missing_condition_code")
        elif condition_gap_steps == 1:
            review_reasons.append("condition_adjacent")
        elif (
            condition_gap_steps is None
            and str(subject_snapshot.get("condition_code") or "").strip() != ""
            and str(row.get("condition_code") or "").strip() != ""
            and row.get("condition_code") != subject_snapshot.get("condition_code")
        ):
            review_reasons.append("condition_mismatch_unmapped")
        if fort_bend_bathroom_review.get("review_required"):
            review_reasons.append("fort_bend_bathroom_status_review")

        if exclusion_reasons:
            primary_reason_code = exclusion_reasons[0]
            secondary_reason_codes = exclusion_reasons[1:] + review_reasons
            return (
                "excluded",
                primary_reason_code,
                {
                    "primary_reason_code": primary_reason_code,
                    "secondary_reason_codes": secondary_reason_codes,
                    "threshold_observations": threshold_observations,
                    "subject_relationship": subject_relationship,
                    "fort_bend_bathroom_review": fort_bend_bathroom_review,
                },
            )

        if review_reasons:
            primary_reason_code = review_reasons[0]
            secondary_reason_codes = review_reasons[1:]
            return (
                "review",
                primary_reason_code,
                {
                    "primary_reason_code": primary_reason_code,
                    "secondary_reason_codes": secondary_reason_codes,
                    "threshold_observations": threshold_observations,
                    "subject_relationship": subject_relationship,
                    "fort_bend_bathroom_review": fort_bend_bathroom_review,
                },
            )

        return (
            "eligible",
            None,
            {
                "primary_reason_code": None,
                "secondary_reason_codes": [],
                "threshold_observations": threshold_observations,
                "subject_relationship": subject_relationship,
                "fort_bend_bathroom_review": fort_bend_bathroom_review,
            },
        )

    def _update_run_candidate_discovery_summary(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        subject_snapshot: dict[str, Any],
        discovered_count: int,
        same_neighborhood_count: int,
        county_sfr_fallback_count: int,
        eligible_count: int,
        review_count: int,
        excluded_count: int,
    ) -> None:
        fallback_used = county_sfr_fallback_count > 0
        sparse_universe_warning = discovered_count < SPARSE_UNIVERSE_WARNING_THRESHOLD
        warning_codes: list[str] = []
        same_neighborhood_insufficient_reason = None
        if fallback_used:
            warning_codes.append("fallback_geography_used")
            same_neighborhood_insufficient_reason = "same_neighborhood_supply_below_preferred_pool"
        if sparse_universe_warning:
            warning_codes.append("sparse_candidate_universe")

        summary_json = dict(subject_snapshot.get("summary_json") or {})
        summary_json["candidate_discovery_summary"] = {
            "discovered_count": discovered_count,
            "same_neighborhood_count": same_neighborhood_count,
            "county_sfr_fallback_count": county_sfr_fallback_count,
            "eligible_count": eligible_count,
            "review_count": review_count,
            "excluded_count": excluded_count,
            "fallback_used": fallback_used,
            "sparse_universe_warning": sparse_universe_warning,
            "same_neighborhood_insufficient_reason": same_neighborhood_insufficient_reason,
            "warning_codes": warning_codes,
        }
        cursor.execute(
            """
            UPDATE unequal_roll_runs
            SET summary_json = %s,
                updated_at = now()
            WHERE unequal_roll_run_id = %s
            """,
            (Jsonb(summary_json), unequal_roll_run_id),
        )


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _pct_diff(subject_value: float | None, candidate_value: float | None) -> float | None:
    if subject_value in {None, 0.0} or candidate_value is None:
        return None
    return abs(candidate_value - subject_value) / subject_value


def _abs_diff(subject_value: float | int | None, candidate_value: float | int | None) -> float | None:
    if subject_value is None or candidate_value is None:
        return None
    return abs(float(candidate_value) - float(subject_value))


def _effective_baths(full_baths: Any, half_baths: Any) -> float | None:
    full_baths_value = _as_float(full_baths)
    half_baths_value = _as_float(half_baths)
    if full_baths_value is None and half_baths_value is None:
        return None
    return (full_baths_value or 0.0) + 0.5 * (half_baths_value or 0.0)


def _preferred_land_size(row: dict[str, Any]) -> float | None:
    land_sf = _as_float(row.get("land_sf"))
    if land_sf not in {None, 0.0}:
        return land_sf
    land_acres = _as_float(row.get("land_acres"))
    if land_acres in {None, 0.0}:
        return None
    return land_acres * 43560.0


def _acreage_profile_mismatch(subject_snapshot: dict[str, Any], row: dict[str, Any]) -> bool:
    subject_land_acres = _as_float(subject_snapshot.get("land_acres"))
    candidate_land_acres = _as_float(row.get("land_acres"))
    if subject_land_acres is None or candidate_land_acres is None:
        return False
    smaller = min(subject_land_acres, candidate_land_acres)
    larger = max(subject_land_acres, candidate_land_acres)
    return smaller <= 0.5 and larger >= 1.5


def _fort_bend_bathroom_review(
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> dict[str, Any]:
    if valuation_bathroom_features_json is None:
        return {
            "attachment_status": "not_applicable",
            "review_required": False,
        }

    bathroom_count_status = valuation_bathroom_features_json.get("bathroom_count_status")
    review_required = (
        valuation_bathroom_features_json.get("attachment_status") == "attached"
        and bathroom_count_status not in FORT_BEND_AUTO_USABLE_BATHROOM_STATUSES
    )
    return {
        "attachment_status": valuation_bathroom_features_json.get("attachment_status"),
        "bathroom_count_status": bathroom_count_status,
        "bathroom_count_confidence": valuation_bathroom_features_json.get(
            "bathroom_count_confidence"
        ),
        "review_required": review_required,
    }


def _has_usable_fort_bend_bathroom_support(
    valuation_bathroom_features_json: dict[str, Any] | None,
) -> bool:
    if valuation_bathroom_features_json is None:
        return False
    return (
        valuation_bathroom_features_json.get("attachment_status") == "attached"
        and valuation_bathroom_features_json.get("bathroom_count_status")
        in FORT_BEND_AUTO_USABLE_BATHROOM_STATUSES
    )
