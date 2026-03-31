from __future__ import annotations

import math
import statistics
from atexit import register as register_atexit
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import perf_counter
from threading import BoundedSemaphore
from typing import Any, Literal
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.db.connection import get_connection
from app.models.quote import (
    InstantQuoteEstimate,
    InstantQuoteEstimateStrengthLabel,
    InstantQuoteExplanation,
    InstantQuoteResponse,
    InstantQuoteSubject,
)
from app.utils.logging import get_logger

logger = get_logger(__name__)
TELEMETRY_MAX_WORKERS = 2
TELEMETRY_MAX_INFLIGHT_TASKS = 64
_PERSISTENCE_EXECUTOR = ThreadPoolExecutor(
    max_workers=TELEMETRY_MAX_WORKERS,
    thread_name_prefix="instant-quote-telemetry",
)
_PERSISTENCE_SLOTS = BoundedSemaphore(TELEMETRY_MAX_INFLIGHT_TASKS)

QUOTE_VERSION = "stage17_instant_quote_v1"
SUPPORTED_PROPERTY_TYPES = {"sfr"}
SEGMENT_MIN_COUNT = 8
NEIGHBORHOOD_MIN_COUNT = 20
STRONG_SEGMENT_COUNT = 20
MATERIAL_CAP_GAP_RATIO = 0.03
CONSTRAINED_SAVINGS_NOTE = (
    "Your current tax protections may limit this year's savings even if your value is reduced."
)
REFINED_REVIEW_CTA = (
    "We found possible protest signals, but this property needs a refined review."
)

FallbackTier = Literal["segment_within_neighborhood", "neighborhood_only", "unsupported"]
TaxLimitationOutcome = Literal["normal", "constrained", "suppressed"]


def _shutdown_persistence_executor() -> None:
    _PERSISTENCE_EXECUTOR.shutdown(wait=False, cancel_futures=True)


register_atexit(_shutdown_persistence_executor)


@dataclass(frozen=True)
class DistributionSummary:
    parcel_count: int
    trimmed_parcel_count: int
    excluded_parcel_count: int
    p10: float
    p25: float
    p50: float
    p75: float
    p90: float
    mean: float
    median: float
    stddev: float
    coefficient_of_variation: float | None


@dataclass(frozen=True)
class InstantQuoteRefreshSummary:
    subject_row_count: int = 0
    neighborhood_stats_count: int = 0
    segment_stats_count: int = 0
    excluded_subject_count: int = 0

    def as_log_extra(self) -> dict[str, int]:
        return {
            "subject_row_count": self.subject_row_count,
            "neighborhood_stats_count": self.neighborhood_stats_count,
            "segment_stats_count": self.segment_stats_count,
            "excluded_subject_count": self.excluded_subject_count,
        }


@dataclass(frozen=True)
class InstantQuoteStatsRow:
    parcel_count: int
    p10_assessed_psf: float | None
    p25_assessed_psf: float | None
    p50_assessed_psf: float | None
    p75_assessed_psf: float | None
    p90_assessed_psf: float | None
    mean_assessed_psf: float | None
    median_assessed_psf: float | None
    stddev_assessed_psf: float | None
    coefficient_of_variation: float | None
    support_level: str | None
    support_threshold_met: bool


def assign_size_bucket(living_area_sf: float | None) -> str | None:
    if living_area_sf is None or living_area_sf <= 0:
        return None
    if living_area_sf < 1400:
        return "lt_1400"
    if living_area_sf < 1700:
        return "1400_1699"
    if living_area_sf < 2000:
        return "1700_1999"
    if living_area_sf < 2400:
        return "2000_2399"
    if living_area_sf < 2900:
        return "2400_2899"
    if living_area_sf < 3500:
        return "2900_3499"
    return "3500_plus"


def assign_age_bucket(year_built: int | None) -> str:
    if year_built is None:
        return "unknown"
    if year_built < 1970:
        return "pre_1970"
    if year_built < 1990:
        return "1970_1989"
    if year_built < 2005:
        return "1990_2004"
    if year_built < 2015:
        return "2005_2014"
    return "2015_plus"


def calculate_distribution_stats(
    values: list[float],
    *,
    trim_lower: float = 0.05,
    trim_upper: float = 0.95,
) -> DistributionSummary | None:
    cleaned = sorted(value for value in values if value > 0)
    if not cleaned:
        return None

    lower_bound = percentile(cleaned, trim_lower)
    upper_bound = percentile(cleaned, trim_upper)
    trimmed = [value for value in cleaned if lower_bound <= value <= upper_bound]
    if not trimmed:
        trimmed = cleaned
    trimmed = sorted(trimmed)
    stddev = statistics.pstdev(trimmed) if len(trimmed) > 1 else 0.0
    mean_value = statistics.fmean(trimmed)
    cv = None if mean_value == 0 else stddev / mean_value

    return DistributionSummary(
        parcel_count=len(cleaned),
        trimmed_parcel_count=len(trimmed),
        excluded_parcel_count=max(len(cleaned) - len(trimmed), 0),
        p10=percentile(trimmed, 0.10),
        p25=percentile(trimmed, 0.25),
        p50=percentile(trimmed, 0.50),
        p75=percentile(trimmed, 0.75),
        p90=percentile(trimmed, 0.90),
        mean=mean_value,
        median=statistics.median(trimmed),
        stddev=stddev,
        coefficient_of_variation=cv,
    )


def percentile(sorted_values: list[float], quantile: float) -> float:
    if not sorted_values:
        raise ValueError("percentile requires at least one value")
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    position = (len(sorted_values) - 1) * quantile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return float(sorted_values[lower_index])
    lower_value = float(sorted_values[lower_index])
    upper_value = float(sorted_values[upper_index])
    weight = position - lower_index
    return lower_value + (upper_value - lower_value) * weight


class InstantQuoteRefreshService:
    def refresh(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
    ) -> InstantQuoteRefreshSummary:
        subject_rows = self._fetch_subject_rows(county_id=county_id, tax_year=tax_year)
        neighborhood_payloads = self._build_neighborhood_payloads(subject_rows)
        segment_payloads = self._build_segment_payloads(subject_rows)

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM instant_quote_segment_stats
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                cursor.execute(
                    """
                    DELETE FROM instant_quote_neighborhood_stats
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )

                for payload in neighborhood_payloads:
                    cursor.execute(
                        """
                        INSERT INTO instant_quote_neighborhood_stats (
                          county_id,
                          tax_year,
                          neighborhood_code,
                          property_type_code,
                          parcel_count,
                          trimmed_parcel_count,
                          excluded_parcel_count,
                          p10_assessed_psf,
                          p25_assessed_psf,
                          p50_assessed_psf,
                          p75_assessed_psf,
                          p90_assessed_psf,
                          mean_assessed_psf,
                          median_assessed_psf,
                          stddev_assessed_psf,
                          coefficient_of_variation,
                          support_level,
                          support_threshold_met,
                          trim_method_code
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        payload,
                    )

                for payload in segment_payloads:
                    cursor.execute(
                        """
                        INSERT INTO instant_quote_segment_stats (
                          county_id,
                          tax_year,
                          neighborhood_code,
                          property_type_code,
                          size_bucket,
                          age_bucket,
                          parcel_count,
                          trimmed_parcel_count,
                          excluded_parcel_count,
                          p10_assessed_psf,
                          p25_assessed_psf,
                          p50_assessed_psf,
                          p75_assessed_psf,
                          p90_assessed_psf,
                          mean_assessed_psf,
                          median_assessed_psf,
                          stddev_assessed_psf,
                          coefficient_of_variation,
                          support_level,
                          support_threshold_met,
                          trim_method_code
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        payload,
                    )
            connection.commit()

        excluded_subject_count = sum(
            1 for row in subject_rows if row.get("support_blocker_code") is not None
        )
        return InstantQuoteRefreshSummary(
            subject_row_count=len(subject_rows),
            neighborhood_stats_count=len(neighborhood_payloads),
            segment_stats_count=len(segment_payloads),
            excluded_subject_count=excluded_subject_count,
        )

    def _fetch_subject_rows(
        self,
        *,
        county_id: str | None,
        tax_year: int | None,
    ) -> list[dict[str, Any]]:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM instant_quote_subject_view
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                    ORDER BY county_id, tax_year, neighborhood_code, account_number
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                return list(cursor.fetchall())

    def _build_neighborhood_payloads(self, subject_rows: list[dict[str, Any]]) -> list[tuple[object, ...]]:
        grouped: dict[tuple[str, int, str], list[float]] = {}
        for row in subject_rows:
            if row.get("property_type_code") not in SUPPORTED_PROPERTY_TYPES:
                continue
            if row.get("support_blocker_code") is not None:
                continue
            neighborhood_code = row.get("neighborhood_code")
            subject_assessed_psf = _as_float(row.get("subject_assessed_psf"))
            if neighborhood_code is None or subject_assessed_psf is None or subject_assessed_psf <= 0:
                continue
            key = (str(row["county_id"]), int(row["tax_year"]), str(neighborhood_code))
            grouped.setdefault(key, []).append(subject_assessed_psf)

        payloads: list[tuple[object, ...]] = []
        for (county_id, tax_year, neighborhood_code), values in sorted(grouped.items()):
            summary = calculate_distribution_stats(values)
            if summary is None:
                continue
            payloads.append(
                (
                    county_id,
                    tax_year,
                    neighborhood_code,
                    "sfr",
                    summary.parcel_count,
                    summary.trimmed_parcel_count,
                    summary.excluded_parcel_count,
                    summary.p10,
                    summary.p25,
                    summary.p50,
                    summary.p75,
                    summary.p90,
                    summary.mean,
                    summary.median,
                    summary.stddev,
                    summary.coefficient_of_variation,
                    _support_level(summary.parcel_count, strong_threshold=NEIGHBORHOOD_MIN_COUNT),
                    summary.parcel_count >= NEIGHBORHOOD_MIN_COUNT,
                    "trim_p05_p95",
                )
            )
        return payloads

    def _build_segment_payloads(self, subject_rows: list[dict[str, Any]]) -> list[tuple[object, ...]]:
        grouped: dict[tuple[str, int, str, str, str], list[float]] = {}
        for row in subject_rows:
            if row.get("property_type_code") not in SUPPORTED_PROPERTY_TYPES:
                continue
            if row.get("support_blocker_code") is not None:
                continue
            neighborhood_code = row.get("neighborhood_code")
            size_bucket = row.get("size_bucket")
            age_bucket = row.get("age_bucket")
            subject_assessed_psf = _as_float(row.get("subject_assessed_psf"))
            if (
                neighborhood_code is None
                or size_bucket is None
                or age_bucket is None
                or subject_assessed_psf is None
                or subject_assessed_psf <= 0
            ):
                continue
            key = (
                str(row["county_id"]),
                int(row["tax_year"]),
                str(neighborhood_code),
                str(size_bucket),
                str(age_bucket),
            )
            grouped.setdefault(key, []).append(subject_assessed_psf)

        payloads: list[tuple[object, ...]] = []
        for (county_id, tax_year, neighborhood_code, size_bucket, age_bucket), values in sorted(
            grouped.items()
        ):
            summary = calculate_distribution_stats(values)
            if summary is None:
                continue
            payloads.append(
                (
                    county_id,
                    tax_year,
                    neighborhood_code,
                    "sfr",
                    size_bucket,
                    age_bucket,
                    summary.parcel_count,
                    summary.trimmed_parcel_count,
                    summary.excluded_parcel_count,
                    summary.p10,
                    summary.p25,
                    summary.p50,
                    summary.p75,
                    summary.p90,
                    summary.mean,
                    summary.median,
                    summary.stddev,
                    summary.coefficient_of_variation,
                    _support_level(summary.parcel_count, strong_threshold=STRONG_SEGMENT_COUNT),
                    summary.parcel_count >= SEGMENT_MIN_COUNT,
                    "trim_p05_p95",
                )
            )
        return payloads


class InstantQuoteService:
    def get_quote(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> InstantQuoteResponse:
        request_id = uuid4()
        started_at = perf_counter()
        subject_row = self._fetch_subject_row(
            county_id=county_id,
            requested_tax_year=tax_year,
            account_number=account_number,
        )
        if subject_row is None:
            raise LookupError(
                f"Instant quote not found for {county_id}/{tax_year}/{account_number}."
            )

        response, telemetry = self._build_response(
            request_id=request_id,
            subject_row=subject_row,
            requested_tax_year=tax_year,
        )
        latency_ms = int((perf_counter() - started_at) * 1000)
        telemetry["latency_ms"] = latency_ms
        self._emit_logs(response=response, telemetry=telemetry)
        self._enqueue_request_log_persistence(response=response, telemetry=telemetry)
        return response

    def _fetch_subject_row(
        self,
        *,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        ready_row = self._fetch_subject_row_with_ready_stats(
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            account_number=account_number,
        )
        if ready_row is not None:
            return ready_row
        return self._fetch_latest_subject_row(
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            account_number=account_number,
        )

    def _fetch_subject_row_with_ready_stats(
        self,
        *,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM instant_quote_subject_view subject
                    WHERE county_id = %s
                      AND account_number = %s
                      AND tax_year <= %s
                      AND EXISTS (
                        SELECT 1
                        FROM instant_quote_neighborhood_stats stats
                        WHERE stats.county_id = subject.county_id
                          AND stats.tax_year = subject.tax_year
                      )
                    ORDER BY tax_year DESC
                    LIMIT 1
                    """,
                    (county_id, account_number, requested_tax_year),
                )
                return cursor.fetchone()

    def _fetch_latest_subject_row(
        self,
        *,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM instant_quote_subject_view
                    WHERE county_id = %s
                      AND account_number = %s
                      AND tax_year <= %s
                    ORDER BY tax_year DESC
                    LIMIT 1
                    """,
                    (county_id, account_number, requested_tax_year),
                )
                return cursor.fetchone()

    def _fetch_neighborhood_stats(
        self,
        *,
        county_id: str,
        tax_year: int,
        neighborhood_code: str,
    ) -> InstantQuoteStatsRow | None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM instant_quote_neighborhood_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND neighborhood_code = %s
                      AND property_type_code = 'sfr'
                    LIMIT 1
                    """,
                    (county_id, tax_year, neighborhood_code),
                )
                row = cursor.fetchone()
        return _build_stats_row(row)

    def _fetch_segment_stats(
        self,
        *,
        county_id: str,
        tax_year: int,
        neighborhood_code: str,
        size_bucket: str | None,
        age_bucket: str | None,
    ) -> InstantQuoteStatsRow | None:
        if size_bucket is None or age_bucket is None:
            return None
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM instant_quote_segment_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND neighborhood_code = %s
                      AND property_type_code = 'sfr'
                      AND size_bucket = %s
                      AND age_bucket = %s
                    LIMIT 1
                    """,
                    (county_id, tax_year, neighborhood_code, size_bucket, age_bucket),
                )
                row = cursor.fetchone()
        return _build_stats_row(row)

    def _has_any_stats_for_year(self, *, county_id: str, tax_year: int) -> bool:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM instant_quote_neighborhood_stats
                      WHERE county_id = %s
                        AND tax_year = %s
                    ) AS has_stats
                    """,
                    (county_id, tax_year),
                )
                row = cursor.fetchone()
        return bool(row and row.get("has_stats"))

    def _build_response(
        self,
        *,
        request_id,
        subject_row: dict[str, Any],
        requested_tax_year: int,
    ) -> tuple[InstantQuoteResponse, dict[str, Any]]:
        served_tax_year = int(subject_row["tax_year"])
        tax_year_fallback_applied = served_tax_year != requested_tax_year
        telemetry: dict[str, Any] = {
            "request_id": request_id,
            "quote_version": QUOTE_VERSION,
            "parcel_id": subject_row.get("parcel_id"),
            "county_id": subject_row.get("county_id"),
            "tax_year": served_tax_year,
            "account_number": subject_row.get("account_number"),
        }

        blocker_code = subject_row.get("support_blocker_code")
        if blocker_code is not None:
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code="assessment_basis_unsupported",
                unsupported_reason=str(blocker_code),
                summary="This parcel is missing one or more inputs required for a safe instant quote.",
                bullets=[
                    "The property record resolved, but the instant quote inputs are incomplete.",
                    "A refined review can still inspect the parcel-year manually.",
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "support_blocker_code": blocker_code,
                    "unsupported_reason": blocker_code,
                    "fallback_tier": "unsupported",
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        neighborhood_code = str(subject_row["neighborhood_code"])
        county_id = str(subject_row["county_id"])
        neighborhood_stats = self._fetch_neighborhood_stats(
            county_id=county_id,
            tax_year=served_tax_year,
            neighborhood_code=neighborhood_code,
        )
        if neighborhood_stats is None:
            unsupported_reason = (
                "instant_quote_not_ready"
                if not self._has_any_stats_for_year(county_id=county_id, tax_year=served_tax_year)
                else "thin_market_support"
            )
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code="assessment_basis_unsupported",
                unsupported_reason=unsupported_reason,
                summary="Instant quote support is not available for this parcel-year yet.",
                bullets=[
                    "The parcel resolved, but the precomputed instant quote support tables are not ready for this county-year.",
                    "A refined review can still inspect the property with deeper support.",
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": unsupported_reason,
                    "fallback_tier": "unsupported",
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        if neighborhood_stats.parcel_count < NEIGHBORHOOD_MIN_COUNT:
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code="assessment_basis_unsupported",
                unsupported_reason="thin_market_support",
                summary="We found the parcel, but this neighborhood is too thin for a safe instant quote.",
                bullets=[
                    "The parcel-year resolved, but there are not enough precomputed neighborhood peers for a public estimate.",
                    "A refined review can use a broader evidence set instead of this consumer-safe fast estimate.",
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": "thin_market_support",
                    "fallback_tier": "unsupported",
                    "neighborhood_sample_count": neighborhood_stats.parcel_count,
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        segment_stats = self._fetch_segment_stats(
            county_id=county_id,
            tax_year=served_tax_year,
            neighborhood_code=neighborhood_code,
            size_bucket=subject_row.get("size_bucket"),
            age_bucket=subject_row.get("age_bucket"),
        )
        fallback_tier, segment_weight, neighborhood_weight, basis_code = choose_fallback(
            segment_stats=segment_stats,
            neighborhood_stats=neighborhood_stats,
        )
        if fallback_tier == "unsupported":
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code="assessment_basis_unsupported",
                unsupported_reason="thin_market_support",
                summary="We found the parcel, but support is too thin for a public instant quote.",
                bullets=[
                    "The parcel-year resolved, but the segment and neighborhood sample support is still too thin.",
                    "A refined review can examine the property with deeper evidence and manual review.",
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": "thin_market_support",
                    "fallback_tier": "unsupported",
                    "segment_sample_count": segment_stats.parcel_count if segment_stats else 0,
                    "neighborhood_sample_count": neighborhood_stats.parcel_count,
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        subject_basis_value = _as_float(subject_row.get("assessment_basis_value")) or 0.0
        living_area_sf = _as_float(subject_row.get("living_area_sf")) or 0.0
        subject_assessed_psf = _as_float(subject_row.get("subject_assessed_psf")) or 0.0
        tax_rate = _as_float(subject_row.get("effective_tax_rate")) or 0.0
        segment_component = (segment_stats.p50_assessed_psf or 0.0) * segment_weight
        neighborhood_component = (neighborhood_stats.p50_assessed_psf or 0.0) * neighborhood_weight
        size_adjustment = size_bucket_adjustment(
            living_area_sf=living_area_sf,
            size_bucket=str(subject_row.get("size_bucket") or ""),
        )
        age_adjustment = age_bucket_adjustment(
            year_built=_as_int(subject_row.get("year_built")),
            age_bucket=str(subject_row.get("age_bucket") or ""),
        )
        target_psf = max(
            (segment_component + neighborhood_component) * (1 + size_adjustment + age_adjustment),
            0.0,
        )
        if target_psf <= 0:
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code="assessment_basis_unsupported",
                unsupported_reason="thin_market_support",
                summary="Instant quote support for this parcel-year is incomplete.",
                bullets=[
                    "The parcel resolved, but the target assessed value basis could not be computed safely.",
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": "thin_market_support",
                    "fallback_tier": "unsupported",
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        equity_value_estimate = target_psf * living_area_sf
        reduction_estimate = max(subject_basis_value - equity_value_estimate, 0.0)
        savings_estimate = max(reduction_estimate * tax_rate, 0.0)
        subject_percentile = subject_percentile_from_distribution(
            subject_assessed_psf=subject_assessed_psf,
            neighborhood_stats=neighborhood_stats,
        )
        confidence_score = score_confidence(
            subject_row=subject_row,
            segment_stats=segment_stats,
            neighborhood_stats=neighborhood_stats,
            fallback_tier=fallback_tier,
            subject_assessed_psf=subject_assessed_psf,
            target_psf=target_psf,
        )
        confidence_label = confidence_label_for_score(confidence_score)
        tax_limitation_outcome = determine_tax_limitation_outcome(
            subject_row=subject_row,
            confidence_score=confidence_score,
        )

        if confidence_score < 45:
            unsupported_reason = (
                "tax_limitation_uncertain"
                if subject_row.get("freeze_flag")
                else "low_confidence_refined_review"
            )
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code=basis_code,
                unsupported_reason=unsupported_reason,
                summary="We found the parcel, but the instant quote is too uncertain for a safe public number.",
                bullets=[
                    "The parcel-year has some protest signal support, but the fast estimate confidence is still too low.",
                    REFINED_REVIEW_CTA,
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": unsupported_reason,
                    "fallback_tier": fallback_tier,
                    "confidence_score": confidence_score,
                    "confidence_label": confidence_label,
                    "neighborhood_sample_count": neighborhood_stats.parcel_count,
                    "segment_sample_count": segment_stats.parcel_count if segment_stats else 0,
                    "tax_rate_source_method": subject_row.get("effective_tax_rate_source_method"),
                    "subject_percentile": subject_percentile,
                    "target_psf": target_psf,
                    "subject_assessed_psf": subject_assessed_psf,
                    "target_psf_segment_component": segment_component,
                    "target_psf_neighborhood_component": neighborhood_component,
                    "equity_value_estimate": equity_value_estimate,
                    "reduction_estimate_raw": reduction_estimate,
                    "savings_estimate_raw": savings_estimate,
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        if tax_limitation_outcome == "suppressed":
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code=basis_code,
                unsupported_reason="tax_limitation_uncertain",
                summary="We found possible protest signals, but current tax-limitation mechanics make a public savings range too uncertain.",
                bullets=[
                    "The parcel appears to have tax-protection rules that can limit realized current-year savings.",
                    REFINED_REVIEW_CTA,
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": "tax_limitation_uncertain",
                    "fallback_tier": fallback_tier,
                    "confidence_score": confidence_score,
                    "confidence_label": confidence_label,
                    "neighborhood_sample_count": neighborhood_stats.parcel_count,
                    "segment_sample_count": segment_stats.parcel_count if segment_stats else 0,
                    "tax_rate_source_method": subject_row.get("effective_tax_rate_source_method"),
                    "subject_percentile": subject_percentile,
                    "target_psf": target_psf,
                    "subject_assessed_psf": subject_assessed_psf,
                    "target_psf_segment_component": segment_component,
                    "target_psf_neighborhood_component": neighborhood_component,
                    "equity_value_estimate": equity_value_estimate,
                    "reduction_estimate_raw": reduction_estimate,
                    "savings_estimate_raw": savings_estimate,
                    "explanation_payload": response.explanation.model_dump(),
                }
            )
            return response, telemetry

        estimate = build_public_estimate(
            savings_estimate=savings_estimate,
            confidence_label=confidence_label,
            tax_protection_limited=(tax_limitation_outcome == "constrained"),
        )
        methodology = (
            "segment_within_neighborhood"
            if fallback_tier == "segment_within_neighborhood"
            else "neighborhood_only"
        )
        limitation_note = (
            CONSTRAINED_SAVINGS_NOTE if estimate.tax_protection_limited else None
        )
        response = InstantQuoteResponse(
            supported=True,
            county_id=county_id,
            tax_year=served_tax_year,
            requested_tax_year=requested_tax_year,
            served_tax_year=served_tax_year,
            tax_year_fallback_applied=tax_year_fallback_applied,
            tax_year_fallback_reason=(
                "requested_year_unavailable" if tax_year_fallback_applied else None
            ),
            data_freshness_label=(
                "prior_year_fallback" if tax_year_fallback_applied else "current_year"
            ),
            account_number=str(subject_row["account_number"]),
            basis_code=basis_code,
            subject=InstantQuoteSubject(
                parcel_id=subject_row["parcel_id"],
                address=str(subject_row["address"]),
                neighborhood_code=subject_row.get("neighborhood_code"),
                school_district_name=subject_row.get("school_district_name"),
                property_type_code=subject_row.get("property_type_code"),
                property_class_code=subject_row.get("property_class_code"),
                living_area_sf=living_area_sf,
                year_built=_as_int(subject_row.get("year_built")),
                notice_value=_as_float(subject_row.get("notice_value")),
                homestead_flag=_as_bool(subject_row.get("homestead_flag")),
                freeze_flag=_as_bool(subject_row.get("freeze_flag")),
            ),
            estimate=estimate,
            explanation=InstantQuoteExplanation(
                methodology=methodology,
                estimate_strength_label=confidence_label,
                summary=build_public_summary(
                    fallback_tier=fallback_tier,
                    tax_protection_limited=estimate.tax_protection_limited,
                ),
                bullets=build_public_bullets(
                    fallback_tier=fallback_tier,
                    neighborhood_count=neighborhood_stats.parcel_count,
                    segment_count=segment_stats.parcel_count if segment_stats else 0,
                    tax_protection_limited=estimate.tax_protection_limited,
                ),
                limitation_note=limitation_note,
            ),
            disclaimers=[
                "Instant quote is a fast estimate for protest opportunity, not a final valuation.",
                "Savings ranges are rounded for public display and can change after a refined review.",
            ],
        )

        telemetry.update(
            {
                "basis_code": basis_code,
                "supported": True,
                "fallback_tier": fallback_tier,
                "confidence_score": confidence_score,
                "confidence_label": confidence_label,
                "neighborhood_sample_count": neighborhood_stats.parcel_count,
                "segment_sample_count": segment_stats.parcel_count if segment_stats else 0,
                "tax_rate_source_method": subject_row.get("effective_tax_rate_source_method"),
                "subject_percentile": subject_percentile,
                "target_psf": target_psf,
                "subject_assessed_psf": subject_assessed_psf,
                "target_psf_segment_component": segment_component,
                "target_psf_neighborhood_component": neighborhood_component,
                "equity_value_estimate": equity_value_estimate,
                "reduction_estimate_raw": reduction_estimate,
                "reduction_estimate_display": estimate.savings_midpoint_display / tax_rate
                if tax_rate > 0 and estimate.savings_midpoint_display is not None
                else None,
                "savings_estimate_raw": savings_estimate,
                "savings_estimate_display": estimate.savings_midpoint_display,
                "public_savings_range_low": estimate.savings_range_low,
                "public_savings_range_high": estimate.savings_range_high,
                "public_estimate_bucket": estimate.estimate_bucket,
                "explanation_payload": response.explanation.model_dump(),
            }
        )
        return response, telemetry

    def _enqueue_request_log_persistence(
        self,
        *,
        response: InstantQuoteResponse,
        telemetry: dict[str, Any],
    ) -> None:
        response_payload = response.model_dump()
        telemetry_payload = dict(telemetry)
        if not _PERSISTENCE_SLOTS.acquire(blocking=False):
            logger.warning(
                "instant quote telemetry dropped because the background queue is full",
                extra={
                    "request_id": str(telemetry_payload["request_id"]),
                    "county_id": response.county_id,
                    "tax_year": response.tax_year,
                    "account_number": response.account_number,
                },
            )
            return
        try:
            _PERSISTENCE_EXECUTOR.submit(
                self._persist_request_log_worker,
                response_payload=response_payload,
                telemetry=telemetry_payload,
            )
        except Exception:
            _PERSISTENCE_SLOTS.release()
            logger.exception(
                "instant quote telemetry enqueue failed",
                extra={
                    "request_id": str(telemetry_payload["request_id"]),
                    "county_id": response.county_id,
                    "tax_year": response.tax_year,
                    "account_number": response.account_number,
                },
            )

    def _persist_request_log_worker(
        self,
        *,
        response_payload: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> None:
        try:
            self._persist_request_log_best_effort(
                response_payload=response_payload,
                telemetry=telemetry,
            )
        finally:
            _PERSISTENCE_SLOTS.release()

    def _build_unsupported_response(
        self,
        *,
        subject_row: dict[str, Any],
        requested_tax_year: int,
        basis_code: str,
        unsupported_reason: str,
        summary: str,
        bullets: list[str],
        next_step_cta: str,
    ) -> InstantQuoteResponse:
        served_tax_year = int(subject_row["tax_year"])
        tax_year_fallback_applied = served_tax_year != requested_tax_year
        return InstantQuoteResponse(
            supported=False,
            county_id=str(subject_row["county_id"]),
            tax_year=served_tax_year,
            requested_tax_year=requested_tax_year,
            served_tax_year=served_tax_year,
            tax_year_fallback_applied=tax_year_fallback_applied,
            tax_year_fallback_reason=(
                "requested_year_unavailable" if tax_year_fallback_applied else None
            ),
            data_freshness_label=(
                "prior_year_fallback" if tax_year_fallback_applied else "current_year"
            ),
            account_number=str(subject_row["account_number"]),
            basis_code=basis_code,
            subject=InstantQuoteSubject(
                parcel_id=subject_row["parcel_id"],
                address=str(subject_row["address"]),
                neighborhood_code=subject_row.get("neighborhood_code"),
                school_district_name=subject_row.get("school_district_name"),
                property_type_code=subject_row.get("property_type_code"),
                property_class_code=subject_row.get("property_class_code"),
                living_area_sf=_as_float(subject_row.get("living_area_sf")),
                year_built=_as_int(subject_row.get("year_built")),
                notice_value=_as_float(subject_row.get("notice_value")),
                homestead_flag=_as_bool(subject_row.get("homestead_flag")),
                freeze_flag=_as_bool(subject_row.get("freeze_flag")),
            ),
            estimate=None,
            explanation=InstantQuoteExplanation(
                methodology="unsupported",
                estimate_strength_label="low",
                summary=summary,
                bullets=bullets,
            ),
            disclaimers=[
                "Instant quote is a public-safe fast estimate and can be withheld when support is too limited.",
            ],
            unsupported_reason=unsupported_reason,
            next_step_cta=next_step_cta,
        )

    def _emit_logs(self, *, response: InstantQuoteResponse, telemetry: dict[str, Any]) -> None:
        logger.info(
            "instant quote served",
            extra={
                "request_id": str(telemetry["request_id"]),
                "county_id": response.county_id,
                "tax_year": response.tax_year,
                "account_number": response.account_number,
                "supported": response.supported,
                "basis_code": response.basis_code,
                "fallback_tier": telemetry.get("fallback_tier"),
                "unsupported_reason": response.unsupported_reason,
                "confidence_label": telemetry.get("confidence_label"),
                "latency_ms": telemetry.get("latency_ms"),
            },
        )

    def _persist_request_log_best_effort(
        self,
        *,
        response_payload: dict[str, Any],
        telemetry: dict[str, Any],
    ) -> None:
        try:
            with get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO instant_quote_request_logs (
                          request_id,
                          quote_version,
                          parcel_id,
                          county_id,
                          tax_year,
                          account_number,
                          basis_code,
                          supported,
                          support_blocker_code,
                          target_psf,
                          subject_assessed_psf,
                          target_psf_segment_component,
                          target_psf_neighborhood_component,
                          equity_value_estimate,
                          reduction_estimate_raw,
                          reduction_estimate_display,
                          savings_estimate_raw,
                          savings_estimate_display,
                          public_savings_range_low,
                          public_savings_range_high,
                          public_estimate_bucket,
                          subject_percentile,
                          confidence_score,
                          confidence_label,
                          neighborhood_sample_count,
                          segment_sample_count,
                          tax_rate_source_method,
                          fallback_tier,
                          unsupported_reason,
                          explanation_payload,
                          latency_ms
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            telemetry["request_id"],
                            telemetry["quote_version"],
                            telemetry.get("parcel_id"),
                            response_payload["county_id"],
                            response_payload["tax_year"],
                            response_payload["account_number"],
                            response_payload["basis_code"],
                            response_payload["supported"],
                            telemetry.get("support_blocker_code"),
                            telemetry.get("target_psf"),
                            telemetry.get("subject_assessed_psf"),
                            telemetry.get("target_psf_segment_component"),
                            telemetry.get("target_psf_neighborhood_component"),
                            telemetry.get("equity_value_estimate"),
                            telemetry.get("reduction_estimate_raw"),
                            telemetry.get("reduction_estimate_display"),
                            telemetry.get("savings_estimate_raw"),
                            telemetry.get("savings_estimate_display"),
                            telemetry.get("public_savings_range_low"),
                            telemetry.get("public_savings_range_high"),
                            telemetry.get("public_estimate_bucket"),
                            telemetry.get("subject_percentile"),
                            telemetry.get("confidence_score"),
                            telemetry.get("confidence_label"),
                            telemetry.get("neighborhood_sample_count"),
                            telemetry.get("segment_sample_count"),
                            telemetry.get("tax_rate_source_method"),
                            telemetry.get("fallback_tier"),
                            response_payload.get("unsupported_reason"),
                            Jsonb(telemetry.get("explanation_payload") or {}),
                            telemetry.get("latency_ms"),
                        ),
                    )
                connection.commit()
        except Exception:
            logger.exception(
                "instant quote request log persistence failed",
                extra={
                    "request_id": str(telemetry["request_id"]),
                    "county_id": response_payload["county_id"],
                    "tax_year": response_payload["tax_year"],
                    "account_number": response_payload["account_number"],
                },
            )


def choose_fallback(
    *,
    segment_stats: InstantQuoteStatsRow | None,
    neighborhood_stats: InstantQuoteStatsRow | None,
) -> tuple[FallbackTier, float, float, str]:
    if neighborhood_stats is None or neighborhood_stats.parcel_count < NEIGHBORHOOD_MIN_COUNT:
        return ("unsupported", 0.0, 0.0, "assessment_basis_unsupported")
    if segment_stats is None or segment_stats.parcel_count < SEGMENT_MIN_COUNT:
        return ("neighborhood_only", 0.0, 1.0, "assessment_basis_neighborhood_only")
    if segment_stats.parcel_count >= STRONG_SEGMENT_COUNT:
        return ("segment_within_neighborhood", 0.70, 0.30, "assessment_basis_segment_blend")
    return ("segment_within_neighborhood", 0.55, 0.45, "assessment_basis_segment_blend")


def score_confidence(
    *,
    subject_row: dict[str, Any],
    segment_stats: InstantQuoteStatsRow | None,
    neighborhood_stats: InstantQuoteStatsRow,
    fallback_tier: FallbackTier,
    subject_assessed_psf: float,
    target_psf: float,
) -> float:
    score = 100.0
    if fallback_tier == "neighborhood_only":
        score -= 25.0
    elif segment_stats is not None and segment_stats.parcel_count < STRONG_SEGMENT_COUNT:
        score -= 10.0

    if neighborhood_stats.parcel_count < 30:
        score -= 8.0
    if segment_stats is not None and segment_stats.coefficient_of_variation is not None:
        if segment_stats.coefficient_of_variation > 0.40:
            score -= 15.0
        elif segment_stats.coefficient_of_variation > 0.25:
            score -= 8.0
    if neighborhood_stats.coefficient_of_variation is not None:
        if neighborhood_stats.coefficient_of_variation > 0.40:
            score -= 10.0
        elif neighborhood_stats.coefficient_of_variation > 0.25:
            score -= 5.0

    if _as_int(subject_row.get("year_built")) is None:
        score -= 8.0
    if not bool(subject_row.get("public_summary_ready_flag")):
        score -= 20.0
    if str(subject_row.get("effective_tax_rate_source_method") or "") == "component_rollup":
        score -= 4.0

    if bool(subject_row.get("freeze_flag")):
        score -= 15.0
    elif is_material_homestead_cap_limited(subject_row):
        score -= 8.0

    outlier_gap = abs(subject_assessed_psf - target_psf) / target_psf if target_psf > 0 else 1.0
    if outlier_gap > 0.35:
        score -= 10.0
    elif outlier_gap > 0.20:
        score -= 5.0

    return max(0.0, min(round(score, 2), 100.0))


def confidence_label_for_score(score: float) -> InstantQuoteEstimateStrengthLabel:
    if score >= 85:
        return "high"
    if score >= 65:
        return "medium"
    return "low"


def subject_percentile_from_distribution(
    *,
    subject_assessed_psf: float,
    neighborhood_stats: InstantQuoteStatsRow,
) -> float | None:
    thresholds = [
        neighborhood_stats.p10_assessed_psf,
        neighborhood_stats.p25_assessed_psf,
        neighborhood_stats.p50_assessed_psf,
        neighborhood_stats.p75_assessed_psf,
        neighborhood_stats.p90_assessed_psf,
    ]
    if any(value is None for value in thresholds):
        return None
    p10, p25, p50, p75, p90 = [float(value) for value in thresholds if value is not None]
    if subject_assessed_psf <= p10:
        return 10.0
    if subject_assessed_psf <= p25:
        return 25.0
    if subject_assessed_psf <= p50:
        return 50.0
    if subject_assessed_psf <= p75:
        return 75.0
    if subject_assessed_psf <= p90:
        return 90.0
    return 95.0


def build_public_estimate(
    *,
    savings_estimate: float,
    confidence_label: InstantQuoteEstimateStrengthLabel,
    tax_protection_limited: bool,
) -> InstantQuoteEstimate:
    midpoint = round_display_value(savings_estimate)
    if tax_protection_limited:
        high = round_up_for_display(max(savings_estimate * 0.60, 0.0))
        return InstantQuoteEstimate(
            savings_range_low=0.0,
            savings_range_high=high,
            savings_midpoint_display=round_display_value(high / 2 if high else 0.0),
            estimate_bucket=estimate_bucket(high),
            estimate_strength_label=confidence_label,
            tax_protection_limited=True,
            tax_protection_note=CONSTRAINED_SAVINGS_NOTE,
        )

    low = round_down_for_display(max(savings_estimate * 0.70, 0.0))
    high = round_up_for_display(max(savings_estimate * 1.30, 0.0))
    if high < low:
        high = low
    return InstantQuoteEstimate(
        savings_range_low=low,
        savings_range_high=high,
        savings_midpoint_display=midpoint,
        estimate_bucket=estimate_bucket(midpoint),
        estimate_strength_label=confidence_label,
        tax_protection_limited=False,
        tax_protection_note=None,
    )


def build_public_summary(*, fallback_tier: FallbackTier, tax_protection_limited: bool) -> str:
    if tax_protection_limited:
        return "This fast estimate found protest opportunity, but current tax protections may limit this year's realized savings."
    if fallback_tier == "neighborhood_only":
        return "This fast estimate is based on neighborhood assessment patterns because segment support is limited."
    return "This fast estimate blends neighborhood and segment assessment patterns for a public-safe savings range."


def build_public_bullets(
    *,
    fallback_tier: FallbackTier,
    neighborhood_count: int,
    segment_count: int,
    tax_protection_limited: bool,
) -> list[str]:
    bullets = [
        f"The estimate uses {neighborhood_count} nearby assessment peers from the same neighborhood.",
    ]
    if fallback_tier == "segment_within_neighborhood" and segment_count > 0:
        bullets.append(
            f"A same-size and age segment inside that neighborhood contributed {segment_count} peers."
        )
    else:
        bullets.append(
            "Segment support was limited, so the estimate leaned more heavily on the broader neighborhood pattern."
        )
    if tax_protection_limited:
        bullets.append(CONSTRAINED_SAVINGS_NOTE)
    return bullets


def round_display_value(value: float) -> float:
    increment = 25.0 if abs(value) < 1000 else 50.0
    return round(value / increment) * increment


def round_down_for_display(value: float) -> float:
    increment = 25.0 if abs(value) < 1000 else 50.0
    return math.floor(value / increment) * increment


def round_up_for_display(value: float) -> float:
    increment = 25.0 if abs(value) < 1000 else 50.0
    return math.ceil(value / increment) * increment


def estimate_bucket(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 500:
        return "under_500"
    if value < 1500:
        return "500_to_1499"
    return "1500_plus"


def size_bucket_adjustment(*, living_area_sf: float, size_bucket: str) -> float:
    ranges = {
        "lt_1400": (0.0, 1399.0),
        "1400_1699": (1400.0, 1699.0),
        "1700_1999": (1700.0, 1999.0),
        "2000_2399": (2000.0, 2399.0),
        "2400_2899": (2400.0, 2899.0),
        "2900_3499": (2900.0, 3499.0),
        "3500_plus": (3500.0, 4200.0),
    }
    lower_upper = ranges.get(size_bucket)
    if lower_upper is None:
        return 0.0
    lower, upper = lower_upper
    midpoint = (lower + upper) / 2.0
    span = max(upper - lower, 1.0)
    return max(min(((midpoint - living_area_sf) / span) * 0.03, 0.03), -0.03)


def age_bucket_adjustment(*, year_built: int | None, age_bucket: str) -> float:
    if year_built is None:
        return 0.0
    ranges = {
        "pre_1970": (1940, 1969),
        "1970_1989": (1970, 1989),
        "1990_2004": (1990, 2004),
        "2005_2014": (2005, 2014),
        "2015_plus": (2015, 2025),
    }
    lower_upper = ranges.get(age_bucket)
    if lower_upper is None:
        return 0.0
    lower, upper = lower_upper
    midpoint = (lower + upper) / 2.0
    span = max(upper - lower, 1.0)
    return max(min(((midpoint - year_built) / span) * 0.02, 0.02), -0.02)


def _support_level(count: int, *, strong_threshold: int) -> str:
    if count >= strong_threshold:
        return "strong"
    if count >= SEGMENT_MIN_COUNT:
        return "medium"
    return "thin"


def determine_tax_limitation_outcome(
    *,
    subject_row: dict[str, Any],
    confidence_score: float,
) -> TaxLimitationOutcome:
    if has_uncertain_tax_limitation_signal(subject_row):
        return "suppressed"
    if bool(subject_row.get("freeze_flag")):
        if confidence_score < 60:
            return "suppressed"
        return "constrained"
    if is_material_homestead_cap_limited(subject_row):
        return "constrained"
    return "normal"


def has_uncertain_tax_limitation_signal(subject_row: dict[str, Any]) -> bool:
    warning_codes = {str(code) for code in subject_row.get("warning_codes") or []}
    return bool(
        warning_codes.intersection(
            {
                "freeze_without_qualifying_exemption",
                "homestead_flag_mismatch",
                "missing_exemption_amount",
                "assessment_exemption_total_mismatch",
            }
        )
    )


def is_material_homestead_cap_limited(subject_row: dict[str, Any]) -> bool:
    if not bool(subject_row.get("homestead_flag")):
        return False
    capped_value = _as_float(subject_row.get("capped_value"))
    assessment_basis_value = _as_float(subject_row.get("assessment_basis_value"))
    if (
        capped_value is None
        or capped_value <= 0
        or assessment_basis_value is None
        or assessment_basis_value <= 0
        or capped_value >= assessment_basis_value
    ):
        return False
    gap_ratio = (assessment_basis_value - capped_value) / assessment_basis_value
    return gap_ratio >= MATERIAL_CAP_GAP_RATIO


def _build_stats_row(row: dict[str, Any] | None) -> InstantQuoteStatsRow | None:
    if row is None:
        return None
    return InstantQuoteStatsRow(
        parcel_count=int(row["parcel_count"]),
        p10_assessed_psf=_as_float(row.get("p10_assessed_psf")),
        p25_assessed_psf=_as_float(row.get("p25_assessed_psf")),
        p50_assessed_psf=_as_float(row.get("p50_assessed_psf")),
        p75_assessed_psf=_as_float(row.get("p75_assessed_psf")),
        p90_assessed_psf=_as_float(row.get("p90_assessed_psf")),
        mean_assessed_psf=_as_float(row.get("mean_assessed_psf")),
        median_assessed_psf=_as_float(row.get("median_assessed_psf")),
        stddev_assessed_psf=_as_float(row.get("stddev_assessed_psf")),
        coefficient_of_variation=_as_float(row.get("coefficient_of_variation")),
        support_level=row.get("support_level"),
        support_threshold_met=bool(row.get("support_threshold_met")),
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
