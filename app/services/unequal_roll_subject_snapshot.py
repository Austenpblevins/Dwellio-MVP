from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.unequal_roll_bathroom_support import (
    FORT_BEND_BATHROOM_SOURCE_TABLE,
    attach_bathroom_support_context,
    build_bathroom_support_context,
)

MODEL_VERSION = "unequal_roll_mvp_foundation_v1"
CONFIG_VERSION = "unequal_roll_mvp_foundation_v1"
REVIEW_WARNING_CODES = {
    "missing_address",
    "missing_characteristics",
    "missing_improvement",
    "missing_land",
    "missing_assessment",
}


@dataclass(frozen=True)
class UnequalRollSubjectSnapshotResult:
    unequal_roll_run_id: str
    unequal_roll_subject_snapshot_id: str | None
    county_id: str
    requested_tax_year: int
    served_tax_year: int | None
    account_number: str
    run_status: str
    readiness_status: str
    support_status: str
    support_blocker_code: str | None
    source_coverage_status: str
    subject_snapshot_status: str


class UnequalRollSubjectSnapshotService:
    def create_run_with_subject_snapshot(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> UnequalRollSubjectSnapshotResult:
        run_id = str(uuid4())

        with get_connection() as connection:
            with connection.cursor() as cursor:
                self._insert_run(
                    cursor,
                    unequal_roll_run_id=run_id,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                row = self._fetch_subject_row(
                    cursor,
                    county_id=county_id,
                    requested_tax_year=tax_year,
                    account_number=account_number,
                )
                if row is None:
                    summary_json = {
                        "requested_tax_year": tax_year,
                        "served_tax_year": None,
                        "tax_year_fallback_applied": False,
                        "account_number": account_number,
                        "blocker_reason": "subject_not_found",
                    }
                    self._update_run(
                        cursor,
                        unequal_roll_run_id=run_id,
                        parcel_id=None,
                        run_status="blocked",
                        readiness_status="not_ready",
                        support_status="unsupported",
                        support_blocker_code="subject_not_found",
                        source_coverage_status="missing_subject_source",
                        subject_snapshot_status="missing_subject_source",
                        summary_json=summary_json,
                    )
                    connection.commit()
                    return UnequalRollSubjectSnapshotResult(
                        unequal_roll_run_id=run_id,
                        unequal_roll_subject_snapshot_id=None,
                        county_id=county_id,
                        requested_tax_year=tax_year,
                        served_tax_year=None,
                        account_number=account_number,
                        run_status="blocked",
                        readiness_status="not_ready",
                        support_status="unsupported",
                        support_blocker_code="subject_not_found",
                        source_coverage_status="missing_subject_source",
                        subject_snapshot_status="missing_subject_source",
                    )

                served_tax_year = int(row["tax_year"])
                parcel_id = str(row["parcel_id"])
                valuation_bathroom_features_json = self._build_valuation_bathroom_features_json(
                    cursor,
                    county_id=county_id,
                    parcel_id=parcel_id,
                    tax_year=served_tax_year,
                )
                valuation_bathroom_features_json = attach_bathroom_support_context(
                    county_id=county_id,
                    canonical_full_baths=row.get("full_baths"),
                    canonical_half_baths=row.get("half_baths"),
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
                support_status, readiness_status, support_blocker_code = self._derive_support_status(
                    row,
                    county_id=county_id,
                    requested_tax_year=tax_year,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
                source_coverage_status = self._derive_source_coverage_status(
                    county_id=county_id,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
                snapshot_id = str(uuid4())
                snapshot_json = self._build_snapshot_json(
                    row=row,
                    requested_tax_year=tax_year,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
                source_provenance_json = self._build_source_provenance_json(
                    row=row,
                    requested_tax_year=tax_year,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
                self._insert_subject_snapshot(
                    cursor,
                    unequal_roll_subject_snapshot_id=snapshot_id,
                    unequal_roll_run_id=run_id,
                    row=row,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                    snapshot_json=snapshot_json,
                    source_provenance_json=source_provenance_json,
                )
                self._update_run(
                    cursor,
                    unequal_roll_run_id=run_id,
                    parcel_id=parcel_id,
                    run_status="completed",
                    readiness_status=readiness_status,
                    support_status=support_status,
                    support_blocker_code=support_blocker_code,
                    source_coverage_status=source_coverage_status,
                    subject_snapshot_status="completed",
                    summary_json={
                        "requested_tax_year": tax_year,
                        "served_tax_year": served_tax_year,
                        "tax_year_fallback_applied": served_tax_year != tax_year,
                        "account_number": account_number,
                        "support_status": support_status,
                        "warning_codes": [str(code) for code in row.get("warning_codes") or []],
                        "completeness_score": float(row.get("completeness_score") or 0.0),
                        "source_coverage_status": source_coverage_status,
                        "valuation_bathroom_attachment_status": (
                            valuation_bathroom_features_json.get("attachment_status")
                            if valuation_bathroom_features_json is not None
                            else "not_applicable"
                        ),
                    },
                )
            connection.commit()

        return UnequalRollSubjectSnapshotResult(
            unequal_roll_run_id=run_id,
            unequal_roll_subject_snapshot_id=snapshot_id,
            county_id=county_id,
            requested_tax_year=tax_year,
            served_tax_year=served_tax_year,
            account_number=account_number,
            run_status="completed",
            readiness_status=readiness_status,
            support_status=support_status,
            support_blocker_code=support_blocker_code,
            source_coverage_status=source_coverage_status,
            subject_snapshot_status="completed",
        )

    def _insert_run(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        county_id: str,
        tax_year: int,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO unequal_roll_runs (
              unequal_roll_run_id,
              county_id,
              tax_year,
              run_status,
              readiness_status,
              support_status,
              model_version,
              config_version,
              source_coverage_status,
              subject_snapshot_status,
              finalized_for_packet,
              summary_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                unequal_roll_run_id,
                county_id,
                tax_year,
                "pending",
                "pending",
                "pending",
                MODEL_VERSION,
                CONFIG_VERSION,
                "pending",
                "pending",
                False,
                Jsonb({}),
            ),
        )

    def _fetch_subject_row(
        self,
        cursor: Any,
        *,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            WITH subject_snapshot AS (
              SELECT
                pys.parcel_year_snapshot_id,
                pys.parcel_id,
                pys.county_id,
                pys.tax_year,
                pys.account_number,
                pys.cad_owner_name,
                pys.cad_owner_name_normalized
              FROM parcel_year_snapshots AS pys
              WHERE pys.is_current = true
                AND pys.county_id = %s
                AND pys.account_number = %s
                AND pys.tax_year <= %s
              ORDER BY pys.tax_year DESC
              LIMIT 1
            ),
            current_address AS (
              SELECT DISTINCT ON (pa.parcel_id)
                pa.parcel_id,
                pa.situs_address,
                pa.situs_city,
                COALESCE(pa.situs_state, 'TX') AS situs_state,
                pa.situs_zip,
                pa.normalized_address
              FROM parcel_addresses AS pa
              JOIN subject_snapshot AS ss
                ON ss.parcel_id = pa.parcel_id
              WHERE pa.is_current = true
              ORDER BY pa.parcel_id, pa.updated_at DESC, pa.created_at DESC, pa.parcel_address_id DESC
            ),
            exemption_rollup AS (
              SELECT
                pe.parcel_id,
                pe.tax_year,
                COUNT(*) AS exemption_record_count,
                COALESCE(
                  SUM(pe.exemption_amount) FILTER (WHERE pe.granted_flag AND pe.exemption_amount IS NOT NULL),
                  0::numeric
                ) AS granted_exemption_amount_total,
                COALESCE(
                  array_agg(DISTINCT pe.exemption_type_code ORDER BY pe.exemption_type_code)
                  FILTER (WHERE pe.exemption_type_code IS NOT NULL),
                  ARRAY[]::text[]
                ) AS exemption_type_codes,
                COALESCE(
                  array_agg(DISTINCT raw_code ORDER BY raw_code)
                  FILTER (WHERE btrim(raw_code) <> ''),
                  ARRAY[]::text[]
                ) AS raw_exemption_codes,
                COALESCE(BOOL_OR(
                  COALESCE((et.metadata_json -> 'summary_flags') ? 'homestead', false)
                ), false) AS homestead_flag,
                COALESCE(BOOL_OR(
                  COALESCE((et.metadata_json -> 'summary_flags') ? 'over65', false)
                ), false) AS over65_flag,
                COALESCE(BOOL_OR(
                  COALESCE((et.metadata_json -> 'summary_flags') ? 'disabled', false)
                ), false) AS disabled_flag,
                COALESCE(BOOL_OR(
                  COALESCE((et.metadata_json -> 'summary_flags') ? 'disabled_veteran', false)
                ), false) AS disabled_veteran_flag,
                COALESCE(BOOL_OR(
                  COALESCE((et.metadata_json -> 'summary_flags') ? 'freeze', false)
                ), false) AS freeze_flag,
                COALESCE(
                  BOOL_OR(pe.amount_missing_flag OR (pe.granted_flag AND pe.exemption_amount IS NULL)),
                  false
                ) AS missing_exemption_amount_flag
              FROM parcel_exemptions AS pe
              JOIN subject_snapshot AS ss
                ON ss.parcel_id = pe.parcel_id
               AND ss.tax_year = pe.tax_year
              LEFT JOIN exemption_types AS et
                ON et.exemption_type_code = pe.exemption_type_code
              CROSS JOIN LATERAL unnest(
                CASE
                  WHEN pe.raw_exemption_codes IS NULL OR cardinality(pe.raw_exemption_codes) = 0
                    THEN ARRAY[COALESCE(pe.exemption_type_code, '')]
                  ELSE pe.raw_exemption_codes
                END
              ) AS raw_code
              GROUP BY pe.parcel_id, pe.tax_year
            ),
            taxing_assignment_counts AS (
              SELECT
                ptu.parcel_id,
                ptu.tax_year,
                COUNT(*) FILTER (WHERE tu.unit_type_code = 'county') AS county_assignment_count,
                COUNT(*) FILTER (WHERE tu.unit_type_code = 'school') AS school_assignment_count
              FROM parcel_taxing_units AS ptu
              JOIN subject_snapshot AS ss
                ON ss.parcel_id = ptu.parcel_id
               AND ss.tax_year = ptu.tax_year
              JOIN taxing_units AS tu
                ON tu.taxing_unit_id = ptu.taxing_unit_id
               AND tu.tax_year = ptu.tax_year
              GROUP BY ptu.parcel_id, ptu.tax_year
            ),
            geometry_flags AS (
              SELECT
                pg.parcel_id,
                pg.tax_year,
                BOOL_OR(pg.geometry_role = 'parcel_polygon' AND pg.is_current) AS has_parcel_polygon,
                BOOL_OR(pg.geometry_role = 'parcel_centroid' AND pg.is_current) AS has_parcel_centroid
              FROM parcel_geometries AS pg
              JOIN subject_snapshot AS ss
                ON ss.parcel_id = pg.parcel_id
               AND ss.tax_year = pg.tax_year
              GROUP BY pg.parcel_id, pg.tax_year
            )
            SELECT
              ss.county_id,
              ss.parcel_id,
              ss.tax_year,
              ss.account_number,
              p.cad_property_id,
              COALESCE(ca.situs_address, p.situs_address) AS situs_address,
              COALESCE(ca.situs_city, p.situs_city) AS situs_city,
              COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')) AS situs_state,
              COALESCE(ca.situs_zip, p.situs_zip) AS situs_zip,
              COALESCE(
                ca.normalized_address,
                upper(regexp_replace(COALESCE(ca.situs_address, p.situs_address, ''), '[^A-Za-z0-9 ]', '', 'g'))
              ) AS normalized_address,
              concat_ws(
                ', ',
                COALESCE(ca.situs_address, p.situs_address),
                COALESCE(ca.situs_city, p.situs_city),
                concat_ws(' ', COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')), COALESCE(ca.situs_zip, p.situs_zip))
              ) AS address,
              COALESCE(cor.owner_name, ss.cad_owner_name, p.owner_name) AS owner_name,
              COALESCE(cor.owner_name_normalized, ss.cad_owner_name_normalized) AS owner_name_normalized,
              cor.source_basis AS owner_source_basis,
              cor.confidence_score AS owner_confidence_score,
              COALESCE(cor.override_flag, false) AS owner_override_flag,
              ss.cad_owner_name,
              ss.cad_owner_name_normalized,
              COALESCE(pc.property_type_code, p.property_type_code) AS property_type_code,
              COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
              COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code,
              COALESCE(pc.subdivision_name, p.subdivision_name) AS subdivision_name,
              COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name,
              pi.living_area_sf,
              pi.year_built,
              pi.effective_year_built,
              COALESCE(pi.effective_age, pc.effective_age) AS effective_age,
              pi.bedrooms,
              pi.full_baths,
              pi.half_baths,
              pi.total_rooms,
              pi.stories,
              pi.quality_code,
              pi.condition_code,
              pi.garage_spaces,
              pi.pool_flag,
              pl.land_sf,
              pl.land_acres,
              pl.frontage_sf,
              pl.depth_sf,
              pa.market_value,
              pa.assessed_value,
              pa.capped_value,
              pa.appraised_value,
              pa.certified_value,
              pa.notice_value,
              pa.exemption_value_total,
              COALESCE(er.exemption_record_count, 0) AS exemption_record_count,
              COALESCE(er.exemption_type_codes, ARRAY[]::text[]) AS exemption_type_codes,
              COALESCE(er.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
              COALESCE(er.homestead_flag, false) AS homestead_flag,
              COALESCE(er.over65_flag, false) AS over65_flag,
              COALESCE(er.disabled_flag, false) AS disabled_flag,
              COALESCE(er.disabled_veteran_flag, false) AS disabled_veteran_flag,
              COALESCE(er.freeze_flag, false) AS freeze_flag,
              etr.effective_tax_rate,
              COALESCE(gf.has_parcel_polygon, false) AS has_parcel_polygon,
              COALESCE(gf.has_parcel_centroid, false) AS has_parcel_centroid,
              CASE
                WHEN COALESCE(pa.certified_value, pa.appraised_value, pa.assessed_value, pa.market_value, pa.notice_value) IS NULL
                  THEN NULL
                ELSE GREATEST(
                  COALESCE(pa.certified_value, pa.appraised_value, pa.assessed_value, pa.market_value, pa.notice_value)
                  - COALESCE(pa.exemption_value_total, 0),
                  0
                )
              END AS estimated_taxable_value,
              CASE
                WHEN etr.effective_tax_rate IS NULL
                  OR COALESCE(pa.certified_value, pa.appraised_value, pa.assessed_value, pa.market_value, pa.notice_value) IS NULL
                  THEN NULL
                ELSE GREATEST(
                  COALESCE(pa.certified_value, pa.appraised_value, pa.assessed_value, pa.market_value, pa.notice_value)
                  - COALESCE(pa.exemption_value_total, 0),
                  0
                ) * etr.effective_tax_rate
              END AS estimated_annual_tax,
              ROUND(
                (
                  (
                    (CASE WHEN COALESCE(ca.situs_address, p.situs_address) IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN pc.property_characteristic_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN pi.parcel_improvement_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN pl.parcel_land_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN pa.parcel_assessment_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(er.exemption_record_count, 0) > 0 OR pa.exemption_value_total IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(tac.county_assignment_count, 0) > 0 OR COALESCE(tac.school_assignment_count, 0) > 0 THEN 1 ELSE 0 END) +
                    (CASE WHEN etr.effective_tax_rate IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN cor.current_owner_rollup_id IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false) THEN 1 ELSE 0 END)
                  )::numeric / 10::numeric
                ) * 100.0,
                2
              ) AS completeness_score,
              (
                COALESCE(ca.situs_address, p.situs_address) IS NOT NULL
                AND pa.parcel_assessment_id IS NOT NULL
                AND etr.effective_tax_rate IS NOT NULL
                AND cor.current_owner_rollup_id IS NOT NULL
              ) AS public_summary_ready_flag,
              ARRAY_REMOVE(
                ARRAY[
                  CASE WHEN COALESCE(ca.situs_address, p.situs_address) IS NULL THEN 'missing_address' END,
                  CASE WHEN pc.property_characteristic_id IS NULL THEN 'missing_characteristics' END,
                  CASE WHEN pi.parcel_improvement_id IS NULL THEN 'missing_improvement' END,
                  CASE WHEN pl.parcel_land_id IS NULL THEN 'missing_land' END,
                  CASE WHEN pa.parcel_assessment_id IS NULL THEN 'missing_assessment' END,
                  CASE WHEN COALESCE(er.exemption_record_count, 0) = 0 AND pa.exemption_value_total IS NULL THEN 'missing_exemption_data' END,
                  CASE WHEN COALESCE(tac.county_assignment_count, 0) = 0 THEN 'missing_county_assignment' END,
                  CASE WHEN COALESCE(tac.school_assignment_count, 0) = 0 THEN 'missing_school_assignment' END,
                  CASE WHEN etr.effective_tax_rate IS NULL THEN 'missing_effective_tax_rate' END,
                  CASE WHEN cor.current_owner_rollup_id IS NULL THEN 'missing_owner_rollup' END,
                  CASE
                    WHEN cor.owner_name IS NOT NULL
                      AND ss.cad_owner_name IS NOT NULL
                      AND cor.owner_name IS DISTINCT FROM ss.cad_owner_name
                      AND COALESCE(cor.override_flag, false) = false
                    THEN 'cad_owner_mismatch'
                  END,
                  CASE WHEN COALESCE(er.missing_exemption_amount_flag, false) THEN 'missing_exemption_amount' END,
                  CASE
                    WHEN pa.exemption_value_total IS NOT NULL
                      AND ABS(COALESCE(er.granted_exemption_amount_total, 0) - pa.exemption_value_total) > 0.01
                    THEN 'assessment_exemption_total_mismatch'
                  END,
                  CASE
                    WHEN pa.homestead_flag IS NOT NULL
                      AND pa.homestead_flag IS DISTINCT FROM COALESCE(er.homestead_flag, false)
                    THEN 'homestead_flag_mismatch'
                  END,
                  CASE
                    WHEN COALESCE(er.freeze_flag, false)
                      AND NOT (
                        COALESCE(er.over65_flag, false)
                        OR COALESCE(er.disabled_flag, false)
                        OR COALESCE(er.disabled_veteran_flag, false)
                      )
                    THEN 'freeze_without_qualifying_exemption'
                  END,
                  CASE
                    WHEN NOT (COALESCE(gf.has_parcel_polygon, false) OR COALESCE(gf.has_parcel_centroid, false))
                    THEN 'missing_geometry'
                  END
                ],
                NULL
              ) AS warning_codes
            FROM subject_snapshot AS ss
            JOIN parcels AS p
              ON p.parcel_id = ss.parcel_id
            LEFT JOIN current_address AS ca
              ON ca.parcel_id = ss.parcel_id
            LEFT JOIN property_characteristics AS pc
              ON pc.parcel_year_snapshot_id = ss.parcel_year_snapshot_id
            LEFT JOIN parcel_improvements AS pi
              ON pi.parcel_id = ss.parcel_id
             AND pi.tax_year = ss.tax_year
            LEFT JOIN parcel_lands AS pl
              ON pl.parcel_id = ss.parcel_id
             AND pl.tax_year = ss.tax_year
            LEFT JOIN parcel_assessments AS pa
              ON pa.parcel_id = ss.parcel_id
             AND pa.tax_year = ss.tax_year
            LEFT JOIN exemption_rollup AS er
              ON er.parcel_id = ss.parcel_id
             AND er.tax_year = ss.tax_year
            LEFT JOIN effective_tax_rates AS etr
              ON etr.parcel_id = ss.parcel_id
             AND etr.tax_year = ss.tax_year
            LEFT JOIN taxing_assignment_counts AS tac
              ON tac.parcel_id = ss.parcel_id
             AND tac.tax_year = ss.tax_year
            LEFT JOIN current_owner_rollups AS cor
              ON cor.parcel_id = ss.parcel_id
             AND cor.tax_year = ss.tax_year
            LEFT JOIN geometry_flags AS gf
              ON gf.parcel_id = ss.parcel_id
             AND gf.tax_year = ss.tax_year
            """,
            (county_id, account_number, requested_tax_year),
        )
        return cursor.fetchone()

    def _build_valuation_bathroom_features_json(
        self,
        cursor: Any,
        *,
        county_id: str,
        parcel_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        if county_id != "fort_bend":
            return None

        cursor.execute(
            """
            SELECT
              county_id,
              tax_year,
              parcel_id,
              account_number,
              quick_ref_id,
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
            (parcel_id, tax_year),
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

    def _derive_support_status(
        self,
        row: dict[str, Any],
        *,
        county_id: str,
        requested_tax_year: int,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> tuple[str, str, str | None]:
        property_type_code = str(row.get("property_type_code") or "").lower()
        living_area_sf = _as_float(row.get("living_area_sf"))
        appraised_value = _as_float(row.get("appraised_value"))
        neighborhood_code = str(row.get("neighborhood_code") or "").strip()
        warning_codes = {str(code) for code in row.get("warning_codes") or []}
        completeness_score = float(row.get("completeness_score") or 0.0)
        served_tax_year = int(row["tax_year"])

        if property_type_code and property_type_code != "sfr":
            return "unsupported", "not_ready", "unsupported_property_type"
        if living_area_sf is None or living_area_sf <= 0:
            return "unsupported", "not_ready", "missing_living_area"
        if appraised_value is None or appraised_value <= 0:
            return "unsupported", "not_ready", "missing_appraised_value"
        if not neighborhood_code:
            return "unsupported", "not_ready", "missing_neighborhood_code"
        if warning_codes & REVIEW_WARNING_CODES:
            return "manual_review_required", "not_ready", "subject_source_requires_review"
        if completeness_score < 85.0:
            return "manual_review_required", "not_ready", "subject_source_requires_review"
        if served_tax_year != requested_tax_year:
            return "supported_with_review", "ready", None
        bathroom_support = build_bathroom_support_context(
            county_id=county_id,
            canonical_full_baths=row.get("full_baths"),
            canonical_half_baths=row.get("half_baths"),
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        if county_id == "fort_bend" and bathroom_support["unresolved_bathroom_support_flag"]:
            return "supported_with_review", "ready", None
        return "supported", "ready", None

    def _derive_source_coverage_status(
        self,
        *,
        county_id: str,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> str:
        if county_id == "fort_bend" and valuation_bathroom_features_json is not None:
            if valuation_bathroom_features_json.get("attachment_status") == "attached":
                return "canonical_snapshot_with_additive_bathroom_metadata"
            if valuation_bathroom_features_json.get("attachment_status") == "missing":
                return "canonical_snapshot_with_missing_additive_bathroom_metadata"
        return "canonical_snapshot_only"

    def _insert_subject_snapshot(
        self,
        cursor: Any,
        *,
        unequal_roll_subject_snapshot_id: str,
        unequal_roll_run_id: str,
        row: dict[str, Any],
        valuation_bathroom_features_json: dict[str, Any] | None,
        snapshot_json: dict[str, Any],
        source_provenance_json: dict[str, Any],
    ) -> None:
        bathroom_support = build_bathroom_support_context(
            county_id=str(row.get("county_id") or ""),
            canonical_full_baths=row.get("full_baths"),
            canonical_half_baths=row.get("half_baths"),
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        cursor.execute(
            """
            INSERT INTO unequal_roll_subject_snapshots (
              unequal_roll_subject_snapshot_id,
              unequal_roll_run_id,
              parcel_id,
              county_id,
              tax_year,
              account_number,
              address,
              property_type_code,
              property_class_code,
              neighborhood_code,
              subdivision_name,
              school_district_name,
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
              exemption_value_total,
              homestead_flag,
              over65_flag,
              disabled_flag,
              disabled_veteran_flag,
              freeze_flag,
              subject_appraised_psf,
              valuation_bathroom_features_json,
              snapshot_json,
              source_provenance_json
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                unequal_roll_subject_snapshot_id,
                unequal_roll_run_id,
                row["parcel_id"],
                row["county_id"],
                row["tax_year"],
                row["account_number"],
                row.get("address"),
                row.get("property_type_code"),
                row.get("property_class_code"),
                row.get("neighborhood_code"),
                row.get("subdivision_name"),
                row.get("school_district_name"),
                _as_float(row.get("living_area_sf")),
                _as_int(row.get("year_built")),
                _as_float(row.get("effective_age")),
                _as_int(row.get("bedrooms")),
                _as_float(bathroom_support["resolved_full_baths"]),
                _as_float(bathroom_support["resolved_half_baths"]),
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
                _as_float(row.get("exemption_value_total")),
                _as_bool(row.get("homestead_flag")),
                _as_bool(row.get("over65_flag")),
                _as_bool(row.get("disabled_flag")),
                _as_bool(row.get("disabled_veteran_flag")),
                _as_bool(row.get("freeze_flag")),
                _subject_appraised_psf(row),
                Jsonb(valuation_bathroom_features_json)
                if valuation_bathroom_features_json is not None
                else None,
                Jsonb(snapshot_json),
                Jsonb(source_provenance_json),
            ),
        )

    def _update_run(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        parcel_id: str | None,
        run_status: str,
        readiness_status: str,
        support_status: str,
        support_blocker_code: str | None,
        source_coverage_status: str,
        subject_snapshot_status: str,
        summary_json: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_runs
            SET parcel_id = %s,
                run_status = %s,
                readiness_status = %s,
                support_status = %s,
                support_blocker_code = %s,
                source_coverage_status = %s,
                subject_snapshot_status = %s,
                summary_json = %s,
                updated_at = now()
            WHERE unequal_roll_run_id = %s
            """,
            (
                parcel_id,
                run_status,
                readiness_status,
                support_status,
                support_blocker_code,
                source_coverage_status,
                subject_snapshot_status,
                Jsonb(summary_json),
                unequal_roll_run_id,
            ),
        )

    def _build_snapshot_json(
        self,
        *,
        row: dict[str, Any],
        requested_tax_year: int,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        bathroom_support = build_bathroom_support_context(
            county_id=str(row.get("county_id") or ""),
            canonical_full_baths=row.get("full_baths"),
            canonical_half_baths=row.get("half_baths"),
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        return {
            "requested_tax_year": requested_tax_year,
            "served_tax_year": int(row["tax_year"]),
            "tax_year_fallback_applied": int(row["tax_year"]) != requested_tax_year,
            "warning_codes": [str(code) for code in row.get("warning_codes") or []],
            "completeness_score": float(row.get("completeness_score") or 0.0),
            "public_summary_ready_flag": bool(row.get("public_summary_ready_flag")),
            "subject": {
                "parcel_id": str(row["parcel_id"]),
                "county_id": row["county_id"],
                "account_number": row["account_number"],
                "address": row.get("address"),
                "property_type_code": row.get("property_type_code"),
                "property_class_code": row.get("property_class_code"),
                "neighborhood_code": row.get("neighborhood_code"),
                "subdivision_name": row.get("subdivision_name"),
                "school_district_name": row.get("school_district_name"),
                "living_area_sf": _as_float(row.get("living_area_sf")),
                "year_built": _as_int(row.get("year_built")),
                "effective_age": _as_float(row.get("effective_age")),
                "bedrooms": _as_int(row.get("bedrooms")),
                "full_baths": _as_float(bathroom_support["resolved_full_baths"]),
                "half_baths": _as_float(bathroom_support["resolved_half_baths"]),
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
                "exemption_value_total": _as_float(row.get("exemption_value_total")),
                "homestead_flag": _as_bool(row.get("homestead_flag")),
                "over65_flag": _as_bool(row.get("over65_flag")),
                "disabled_flag": _as_bool(row.get("disabled_flag")),
                "disabled_veteran_flag": _as_bool(row.get("disabled_veteran_flag")),
                "freeze_flag": _as_bool(row.get("freeze_flag")),
                "subject_appraised_psf": _subject_appraised_psf(row),
            },
            "bathroom_support": bathroom_support,
            "valuation_bathroom_features": valuation_bathroom_features_json,
        }

    def _build_source_provenance_json(
        self,
        *,
        row: dict[str, Any],
        requested_tax_year: int,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        bathroom_support = build_bathroom_support_context(
            county_id=str(row.get("county_id") or ""),
            canonical_full_baths=row.get("full_baths"),
            canonical_half_baths=row.get("half_baths"),
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        return {
            "requested_tax_year": requested_tax_year,
            "served_tax_year": int(row["tax_year"]),
            "tax_year_fallback_applied": int(row["tax_year"]) != requested_tax_year,
            "subject_source": {
                "type": "derived_query",
                "name": "subject_snapshot_base_rollup",
                "lookup_rule": "county_id + account_number + tax_year <= requested_tax_year",
                "backing_tables": [
                    "parcel_year_snapshots",
                    "parcels",
                    "parcel_addresses",
                    "property_characteristics",
                    "parcel_improvements",
                    "parcel_lands",
                    "parcel_assessments",
                    "parcel_exemptions",
                    "effective_tax_rates",
                    "current_owner_rollups",
                    "parcel_taxing_units",
                    "taxing_units",
                    "parcel_geometries",
                ],
            },
            "total_rooms_source": "parcel_improvements",
            "warning_codes": [str(code) for code in row.get("warning_codes") or []],
            "completeness_score": float(row.get("completeness_score") or 0.0),
            "public_summary_ready_flag": bool(row.get("public_summary_ready_flag")),
            "valuation_bathroom_source": (
                valuation_bathroom_features_json.get("source_table")
                if valuation_bathroom_features_json is not None
                else None
            ),
            "bathroom_support": bathroom_support,
            "valuation_bathroom_attachment_status": (
                valuation_bathroom_features_json.get("attachment_status")
                if valuation_bathroom_features_json is not None
                else "not_applicable"
            ),
            "source_coverage_status": self._derive_source_coverage_status(
                county_id=str(row["county_id"]),
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            ),
        }


def _subject_appraised_psf(row: dict[str, Any]) -> float | None:
    appraised_value = _as_float(row.get("appraised_value"))
    living_area_sf = _as_float(row.get("living_area_sf"))
    if appraised_value is None or living_area_sf in {None, 0.0}:
        return None
    return appraised_value / living_area_sf


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
