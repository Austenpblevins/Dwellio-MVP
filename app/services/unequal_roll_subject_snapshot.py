from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

MODEL_VERSION = "unequal_roll_mvp_foundation_v1"
CONFIG_VERSION = "unequal_roll_mvp_foundation_v1"
FORT_BEND_BATHROOM_SOURCE_TABLE = "fort_bend_valuation_bathroom_features"
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
                support_status, readiness_status, support_blocker_code = self._derive_support_status(
                    row,
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
            SELECT
              psv.*,
              pi.total_rooms
            FROM parcel_summary_view AS psv
            LEFT JOIN parcel_improvements AS pi
              ON pi.parcel_id = psv.parcel_id
             AND pi.tax_year = psv.tax_year
            WHERE psv.county_id = %s
              AND psv.account_number = %s
              AND psv.tax_year <= %s
            ORDER BY psv.tax_year DESC
            LIMIT 1
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
        if (
            valuation_bathroom_features_json is not None
            and valuation_bathroom_features_json.get("attachment_status") == "missing"
        ):
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
                "exemption_value_total": _as_float(row.get("exemption_value_total")),
                "homestead_flag": _as_bool(row.get("homestead_flag")),
                "over65_flag": _as_bool(row.get("over65_flag")),
                "disabled_flag": _as_bool(row.get("disabled_flag")),
                "disabled_veteran_flag": _as_bool(row.get("disabled_veteran_flag")),
                "freeze_flag": _as_bool(row.get("freeze_flag")),
                "subject_appraised_psf": _subject_appraised_psf(row),
            },
            "valuation_bathroom_features": valuation_bathroom_features_json,
        }

    def _build_source_provenance_json(
        self,
        *,
        row: dict[str, Any],
        requested_tax_year: int,
        valuation_bathroom_features_json: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "requested_tax_year": requested_tax_year,
            "served_tax_year": int(row["tax_year"]),
            "tax_year_fallback_applied": int(row["tax_year"]) != requested_tax_year,
            "subject_source": {
                "type": "derived_view",
                "name": "parcel_summary_view",
                "lookup_rule": "county_id + account_number + tax_year <= requested_tax_year",
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
            "warning_codes": [str(code) for code in row.get("warning_codes") or []],
            "completeness_score": float(row.get("completeness_score") or 0.0),
            "public_summary_ready_flag": bool(row.get("public_summary_ready_flag")),
            "valuation_bathroom_source": (
                valuation_bathroom_features_json.get("source_table")
                if valuation_bathroom_features_json is not None
                else None
            ),
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
