from __future__ import annotations

import csv
import hashlib
import math
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.services.fort_bend_residential_segments import (
    FORT_BEND_CHARACTERISTIC_SEGMENT_TYPES,
    FORT_BEND_EXPLICIT_STORY_SEGMENTS,
    FORT_BEND_PRIMARY_IMPROVEMENT_RULE_VERSION,
    select_fort_bend_primary_residential_candidate,
)

FORT_BEND_BATHROOM_NORMALIZATION_RULE_VERSION = "fort_bend_bathroom_features_v1"


@dataclass(frozen=True)
class FortBendBathroomFeatureRecord:
    county_id: str
    tax_year: int
    parcel_id: str | None
    account_number: str | None
    quick_ref_id: str
    selected_improvement_number: str
    selected_improvement_rule_version: str
    normalization_rule_version: str
    source_file_version: str
    source_file_name: str
    selected_improvement_source_row_count: int
    plumbing_raw: float | None
    half_baths_raw: float | None
    quarter_baths_raw: float | None
    plumbing_raw_values: list[float]
    half_baths_raw_values: list[float]
    quarter_baths_raw_values: list[float]
    full_baths_derived: float | None
    half_baths_derived: float | None
    quarter_baths_derived: float | None
    bathroom_equivalent_derived: float | None
    bathroom_count_status: str
    bathroom_count_confidence: str
    bathroom_flags: list[str]


@dataclass
class _ImprovementCandidate:
    account_number: str | None
    quick_ref_id: str
    improvement_num: str
    main_area_sqft: int = 0
    source_row_count: int = 0
    has_characteristic_segment: bool = False
    story_segments: set[str] | None = None
    plumbing_values: set[float] | None = None
    half_bath_values: set[float] | None = None
    quarter_bath_values: set[float] | None = None
    invalid_negative_fields: set[str] | None = None
    diagnostic_flags: set[str] | None = None

    def __post_init__(self) -> None:
        self.story_segments = set(self.story_segments or set())
        self.plumbing_values = set(self.plumbing_values or set())
        self.half_bath_values = set(self.half_bath_values or set())
        self.quarter_bath_values = set(self.quarter_bath_values or set())
        self.invalid_negative_fields = set(self.invalid_negative_fields or set())
        self.diagnostic_flags = set(self.diagnostic_flags or set())

    def as_selection_candidate(self) -> dict[str, Any]:
        return {
            "account_number": self.account_number,
            "quick_ref_id": self.quick_ref_id,
            "improvement_num": self.improvement_num,
            "main_area_sqft": self.main_area_sqft,
            "source_row_count": self.source_row_count,
            "has_characteristic_segment": self.has_characteristic_segment,
            "story_segments": self.story_segments,
            "plumbing_values": self.plumbing_values,
            "half_bath_values": self.half_bath_values,
            "quarter_bath_values": self.quarter_bath_values,
            "invalid_negative_fields": self.invalid_negative_fields,
            "diagnostic_flags": self.diagnostic_flags,
        }


class FortBendBathroomFeatureService:
    def materialize_features(
        self,
        *,
        county_id: str,
        tax_year: int,
        source_path: Path | None = None,
    ) -> dict[str, Any]:
        if county_id != "fort_bend":
            raise ValueError("Fort Bend derived bathroom features are only supported for county_id=fort_bend.")

        resolved_source_path = self.resolve_source_path(tax_year=tax_year, source_path=source_path)
        source_file_version = self._build_source_file_version(resolved_source_path)
        account_to_parcel_id = self._fetch_account_to_parcel_id(county_id=county_id)
        feature_rows = self._build_feature_rows(
            county_id=county_id,
            tax_year=tax_year,
            source_path=resolved_source_path,
            source_file_version=source_file_version,
            account_to_parcel_id=account_to_parcel_id,
        )

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM fort_bend_valuation_bathroom_features
                    WHERE county_id = %s
                      AND tax_year = %s
                    """,
                    (county_id, tax_year),
                )
                if feature_rows:
                    cursor.executemany(
                        """
                        INSERT INTO fort_bend_valuation_bathroom_features (
                          parcel_id,
                          county_id,
                          tax_year,
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
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        [
                            (
                                record.parcel_id,
                                record.county_id,
                                record.tax_year,
                                record.account_number,
                                record.quick_ref_id,
                                record.selected_improvement_number,
                                record.selected_improvement_rule_version,
                                record.normalization_rule_version,
                                record.source_file_version,
                                record.source_file_name,
                                record.selected_improvement_source_row_count,
                                record.plumbing_raw,
                                record.half_baths_raw,
                                record.quarter_baths_raw,
                                Jsonb(record.plumbing_raw_values),
                                Jsonb(record.half_baths_raw_values),
                                Jsonb(record.quarter_baths_raw_values),
                                record.full_baths_derived,
                                record.half_baths_derived,
                                record.quarter_baths_derived,
                                record.bathroom_equivalent_derived,
                                record.bathroom_count_status,
                                record.bathroom_count_confidence,
                                Jsonb(record.bathroom_flags),
                            )
                            for record in feature_rows
                        ],
                    )
            connection.commit()

        return self._build_materialization_summary(
            county_id=county_id,
            tax_year=tax_year,
            source_path=resolved_source_path,
            source_file_version=source_file_version,
            feature_rows=feature_rows,
        )

    def fetch_feature(self, *, parcel_id: str, tax_year: int) -> dict[str, Any] | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
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
                return cursor.fetchone()

    def resolve_source_path(self, *, tax_year: int, source_path: Path | None = None) -> Path:
        if source_path is not None:
            if not source_path.exists():
                raise FileNotFoundError(f"Fort Bend bathroom source file not found: {source_path}")
            return source_path

        base_path = Path.home() / "county-data" / str(tax_year) / "raw" / "fort_bend"
        for candidate in (
            base_path / "WebsiteResidentialSegs.csv",
            base_path / "Fort Bend_Website_ResidentialSegments.txt",
        ):
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            "Fort Bend bathroom source file not found. Expected WebsiteResidentialSegs.csv "
            "or Fort Bend_Website_ResidentialSegments.txt under ~/county-data/<tax_year>/raw/fort_bend/."
        )

    def _build_feature_rows(
        self,
        *,
        county_id: str,
        tax_year: int,
        source_path: Path,
        source_file_version: str,
        account_to_parcel_id: dict[str, str],
    ) -> list[FortBendBathroomFeatureRecord]:
        buckets: dict[str, dict[str, _ImprovementCandidate]] = {}
        with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                quick_ref_id = _strip(row.get("QuickRefID"))
                if not quick_ref_id:
                    continue
                segment_type = _strip(row.get("fSegType")).upper()
                account_number = _strip(row.get("PropertyNumber")) or None
                improvement_num = _strip(row.get("vTSGRSeg_ImpNum")) or "0"
                candidate = buckets.setdefault(quick_ref_id, {}).setdefault(
                    improvement_num,
                    _ImprovementCandidate(
                        account_number=account_number,
                        quick_ref_id=quick_ref_id,
                        improvement_num=improvement_num,
                    ),
                )
                if candidate.account_number is None and account_number is not None:
                    candidate.account_number = account_number
                candidate.source_row_count += 1
                if segment_type not in FORT_BEND_CHARACTERISTIC_SEGMENT_TYPES:
                    continue
                candidate.has_characteristic_segment = True
                segment_sqft = max(_as_int(row.get("vTSGRSeg_AdjArea")) or _as_int(row.get("fArea")) or 0, 0)
                candidate.main_area_sqft += segment_sqft
                if segment_type in FORT_BEND_EXPLICIT_STORY_SEGMENTS:
                    candidate.story_segments.add(segment_type)
                _collect_numeric_signal(
                    row.get("fPlumbing"),
                    candidate.plumbing_values,
                    candidate.invalid_negative_fields,
                    invalid_negative_flag="invalid_negative_plumbing",
                )
                _collect_numeric_signal(
                    row.get("fNumHalfBath"),
                    candidate.half_bath_values,
                    candidate.invalid_negative_fields,
                    invalid_negative_flag="invalid_negative_half_bath",
                )
                _collect_numeric_signal(
                    row.get("fNumQuaterBath"),
                    candidate.quarter_bath_values,
                    candidate.invalid_negative_fields,
                    invalid_negative_flag="invalid_negative_quarter_bath",
                )

        feature_rows: list[FortBendBathroomFeatureRecord] = []
        for candidates_by_improvement in buckets.values():
            selected = select_fort_bend_primary_residential_candidate(
                candidate.as_selection_candidate() for candidate in candidates_by_improvement.values()
            )
            if selected is None:
                continue
            feature_rows.append(
                self._derive_feature_record(
                    county_id=county_id,
                    tax_year=tax_year,
                    account_to_parcel_id=account_to_parcel_id,
                    source_file_version=source_file_version,
                    source_file_name=source_path.name,
                    selected_candidate=selected,
                )
            )
        return feature_rows

    def _derive_feature_record(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_to_parcel_id: dict[str, str],
        source_file_version: str,
        source_file_name: str,
        selected_candidate: dict[str, Any],
    ) -> FortBendBathroomFeatureRecord:
        plumbing_values = _sorted_unique_values(selected_candidate.get("plumbing_values") or [])
        half_values = _sorted_unique_values(selected_candidate.get("half_bath_values") or [])
        quarter_values = _sorted_unique_values(selected_candidate.get("quarter_bath_values") or [])
        flags = {str(flag) for flag in (selected_candidate.get("invalid_negative_fields") or set())}
        flags.update(str(flag) for flag in (selected_candidate.get("diagnostic_flags") or set()))
        account_number = selected_candidate.get("account_number")
        parcel_id = account_to_parcel_id.get(str(account_number)) if account_number else None

        plumbing_raw = plumbing_values[0] if len(plumbing_values) == 1 else None
        half_raw = half_values[0] if len(half_values) == 1 else None
        quarter_raw = quarter_values[0] if len(quarter_values) == 1 else None

        if len(plumbing_values) > 1:
            flags.add("multiple_plumbing_values")
        if len(half_values) > 1:
            flags.add("multiple_half_bath_values")
        if len(quarter_values) > 1:
            flags.add("multiple_quarter_bath_values")
        if selected_candidate.get("has_characteristic_segment", True) is False:
            flags.add("source_present_without_characteristic_segment")
            flags.add("selected_improvement_without_characteristic_segment")

        status = "no_bathroom_source"
        confidence = "none"
        full_baths_derived: float | None = None
        half_baths_derived: float | None = None
        quarter_baths_derived: float | None = None
        bathroom_equivalent_derived: float | None = None

        if plumbing_values or half_values or quarter_values:
            if flags:
                status = "ambiguous_bathroom_count"
                confidence = "low"
            elif plumbing_raw is None:
                status = "incomplete_bathroom_count"
                confidence = "low"
                half_baths_derived = _as_nonnegative_integer_float(half_raw)
                quarter_baths_derived = _as_nonnegative_integer_float(quarter_raw)
            elif _is_nonnegative_integer(plumbing_raw):
                plumbing_int = int(plumbing_raw)
                half_int = _as_nonnegative_integer(half_raw)
                quarter_int = _as_nonnegative_integer(quarter_raw)
                if quarter_int and quarter_int > 0:
                    status = "quarter_bath_present"
                    confidence = "medium"
                    flags.add("quarter_bath_present")
                    full_baths_derived = float(plumbing_int)
                    half_baths_derived = float(half_int or 0)
                    quarter_baths_derived = float(quarter_int)
                    bathroom_equivalent_derived = float(
                        plumbing_int + (0.5 * (half_int or 0)) + (0.25 * quarter_int)
                    )
                elif (quarter_int or 0) == 0 and (half_int is not None or half_raw is None):
                    status = "exact_supported"
                    confidence = "high"
                    full_baths_derived = float(plumbing_int)
                    half_baths_derived = float(half_int or 0)
                    quarter_baths_derived = 0.0
                    bathroom_equivalent_derived = float(plumbing_int + (0.5 * (half_int or 0)))
                else:
                    status = "ambiguous_bathroom_count"
                    confidence = "low"
            elif _is_half_step(plumbing_raw) and (quarter_raw is None or quarter_raw == 0):
                half_int = _as_nonnegative_integer(half_raw)
                if half_raw is not None and half_int is None:
                    status = "ambiguous_bathroom_count"
                    confidence = "low"
                else:
                    status = "reconciled_fractional_plumbing"
                    confidence = "medium"
                    flags.add("fractional_plumbing_source")
                    full_baths_derived = float(math.floor(plumbing_raw))
                    half_baths_derived = float(max(half_int or 0, 1))
                    quarter_baths_derived = 0.0
                    bathroom_equivalent_derived = float(plumbing_raw)
                    if (half_int or 0) < 1:
                        flags.add("half_bath_imputed_from_fractional_plumbing")
            else:
                status = "ambiguous_bathroom_count"
                confidence = "low"

        return FortBendBathroomFeatureRecord(
            county_id=county_id,
            tax_year=tax_year,
            parcel_id=parcel_id,
            account_number=account_number,
            quick_ref_id=selected_candidate["quick_ref_id"],
            selected_improvement_number=selected_candidate["improvement_num"],
            selected_improvement_rule_version=FORT_BEND_PRIMARY_IMPROVEMENT_RULE_VERSION,
            normalization_rule_version=FORT_BEND_BATHROOM_NORMALIZATION_RULE_VERSION,
            source_file_version=source_file_version,
            source_file_name=source_file_name,
            selected_improvement_source_row_count=int(selected_candidate.get("source_row_count") or 0),
            plumbing_raw=plumbing_raw,
            half_baths_raw=half_raw,
            quarter_baths_raw=quarter_raw,
            plumbing_raw_values=plumbing_values,
            half_baths_raw_values=half_values,
            quarter_baths_raw_values=quarter_values,
            full_baths_derived=full_baths_derived,
            half_baths_derived=half_baths_derived,
            quarter_baths_derived=quarter_baths_derived,
            bathroom_equivalent_derived=bathroom_equivalent_derived,
            bathroom_count_status=status,
            bathroom_count_confidence=confidence,
            bathroom_flags=sorted(flags),
        )

    def _fetch_account_to_parcel_id(self, *, county_id: str) -> dict[str, str]:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT account_number, parcel_id::text AS parcel_id
                    FROM parcels
                    WHERE county_id = %s
                    """,
                    (county_id,),
                )
                rows = cursor.fetchall()
        return {str(row["account_number"]): str(row["parcel_id"]) for row in rows if row.get("account_number")}

    def _build_source_file_version(self, source_path: Path) -> str:
        digest = hashlib.sha256()
        with source_path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return f"{source_path.name}:sha256:{digest.hexdigest()}"

    def _build_materialization_summary(
        self,
        *,
        county_id: str,
        tax_year: int,
        source_path: Path,
        source_file_version: str,
        feature_rows: Sequence[FortBendBathroomFeatureRecord],
    ) -> dict[str, Any]:
        status_counts = Counter(record.bathroom_count_status for record in feature_rows)
        confidence_counts = Counter(record.bathroom_count_confidence for record in feature_rows)
        flag_counts = Counter(flag for record in feature_rows for flag in record.bathroom_flags)
        return {
            "county_id": county_id,
            "tax_year": tax_year,
            "row_count": len(feature_rows),
            "resolved_parcel_count": sum(record.parcel_id is not None for record in feature_rows),
            "unresolved_parcel_count": sum(record.parcel_id is None for record in feature_rows),
            "source_path": str(source_path),
            "source_file_version": source_file_version,
            "status_counts": dict(status_counts),
            "confidence_counts": dict(confidence_counts),
            "flag_counts": dict(flag_counts),
            "derived_fill_counts": {
                "full_baths_derived": sum(record.full_baths_derived is not None for record in feature_rows),
                "half_baths_derived": sum(record.half_baths_derived is not None for record in feature_rows),
                "quarter_baths_derived": sum(
                    record.quarter_baths_derived is not None for record in feature_rows
                ),
                "bathroom_equivalent_derived": sum(
                    record.bathroom_equivalent_derived is not None for record in feature_rows
                ),
            },
        }


def _collect_numeric_signal(
    raw_value: Any,
    target_values: set[float],
    invalid_flags: set[str],
    *,
    invalid_negative_flag: str,
) -> None:
    numeric_value = _as_float(raw_value)
    if numeric_value is None:
        return
    target_values.add(numeric_value)
    if numeric_value < 0:
        invalid_flags.add(invalid_negative_flag)


def _sorted_unique_values(values: Iterable[float]) -> list[float]:
    return sorted({float(value) for value in values})


def _is_nonnegative_integer(value: float | None) -> bool:
    return value is not None and value >= 0 and float(value).is_integer()


def _as_nonnegative_integer(value: float | None) -> int | None:
    if not _is_nonnegative_integer(value):
        return None
    return int(value)


def _as_nonnegative_integer_float(value: float | None) -> float | None:
    integer_value = _as_nonnegative_integer(value)
    return None if integer_value is None else float(integer_value)


def _is_half_step(value: float | None) -> bool:
    if value is None or value < 0:
        return False
    return math.isclose(value % 1, 0.5, abs_tol=1e-9)


def _as_int(value: Any) -> int | None:
    numeric = _as_float(value)
    if numeric is None or not float(numeric).is_integer():
        return None
    return int(numeric)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    text = _strip(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _strip(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
