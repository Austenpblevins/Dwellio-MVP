from __future__ import annotations

import math
import statistics
from atexit import register as register_atexit
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
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
from app.services.instant_quote_tax_rate_basis import (
    SelectedTaxRateBasis,
    SameYearTaxRateAdoptionStatus,
    TaxRateBasisCandidate,
    TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES,
    choose_tax_rate_basis,
    assign_tax_rate_basis_status,
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

QUOTE_VERSION = "instant_quote_v5_stage0_baseline"
CURRENT_PUBLIC_SAVINGS_MODEL = "reduction_estimate_times_effective_tax_rate"
SUPPORTED_PROPERTY_TYPES = {"sfr"}
SEGMENT_MIN_COUNT = 6
NEIGHBORHOOD_MIN_COUNT = 20
STRONG_SEGMENT_COUNT = 20
MIN_TRIM_GROUP_SIZE = 3
TRIM_METHOD_P05_P95 = "trim_p05_p95"
TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3 = "trim_p05_p95_preserve_all_lt3"
MATERIAL_CAP_GAP_RATIO = 0.03
EXTREME_SAVINGS_REVIEW_RATIO = 0.25
CONSTRAINED_SAVINGS_NOTE = (
    "Your current tax protections may limit this year's savings even if your value is reduced."
)
REFINED_REVIEW_CTA = (
    "We found possible protest signals, but this property needs a refined review."
)

FallbackTier = Literal["segment_within_neighborhood", "neighborhood_only", "unsupported"]
TaxLimitationOutcome = Literal["normal", "constrained", "suppressed"]
WarningActionClass = Literal["suppress", "constrain", "disclose", "QA_only"]
WarningSeverity = Literal["low", "medium", "high"]
OpportunityVsSavingsState = Literal[
    "standard_quote",
    "strong_opportunity_high_cash",
    "strong_opportunity_low_cash",
    "supported_opportunity_low_cash",
    "opportunity_only_tax_profile_incomplete",
    "school_limited_non_school_possible",
    "near_total_exemption_low_cash",
    "total_exemption_low_cash",
    "tax_profile_low_quality",
    "suppressed_data_quality",
    "unsupported_value_signal",
    "unsupported_property_type",
    "no_opportunity_detected",
    "manual_review_recommended",
]

AssessmentBasisSourceValueType = Literal[
    "certified",
    "appraised",
    "assessed",
    "market",
    "notice",
]

AssessmentBasisQualityCode = Literal[
    "current_year_authoritative",
    "current_year_proxy",
    "prior_year_fallback",
    "missing",
]

LOW_CASH_SAVINGS_THRESHOLD = 250.0
HIGH_CASH_SAVINGS_THRESHOLD = 1000.0
STRONG_OPPORTUNITY_REDUCTION_RATIO = 0.10
WARNING_ACTION_CLASS_ORDER: tuple[WarningActionClass, ...] = (
    "suppress",
    "constrain",
    "disclose",
    "QA_only",
)
WARNING_TAXONOMY_RULES: dict[str, dict[str, str | None]] = {
    "unsupported_property_type": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "value_support",
        "affected_unit_mask": "all",
        "public_disclosure_code": "unsupported_property_type",
        "qa_note": "Property type remains out of instant-quote scope.",
    },
    "missing_living_area": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "data_completeness",
        "affected_unit_mask": "value_only",
        "public_disclosure_code": "missing_living_area",
        "qa_note": "Living area is required for PSF-based value support.",
    },
    "missing_assessment_basis": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "value_support",
        "affected_unit_mask": "value_only",
        "public_disclosure_code": "missing_assessment_basis",
        "qa_note": "Preserve the explicit missing assessment basis blocker for investigation.",
    },
    "missing_neighborhood_code": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "value_support",
        "affected_unit_mask": "value_only",
        "public_disclosure_code": "missing_neighborhood_support",
        "qa_note": "Neighborhood support remains unavailable for this parcel-year.",
    },
    "missing_effective_tax_rate": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "tax_rate",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": "missing_effective_tax_rate",
        "qa_note": "Tax-rate support is incomplete for the selected basis year.",
    },
    "freeze_without_qualifying_exemption": {
        "warning_action_class": "constrain",
        "warning_severity": "high",
        "affected_subsystem": "cap_limit",
        "affected_unit_mask": "school_tax",
        "public_disclosure_code": "tax_limitation_uncertain",
        "qa_note": "Freeze signal exists without a matching qualifying exemption.",
    },
    "assessment_exemption_total_mismatch": {
        "warning_action_class": "QA_only",
        "warning_severity": "medium",
        "affected_subsystem": "exemption_profile",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": None,
        "qa_note": "Assessment and exemption totals disagree and need operator review.",
    },
    "missing_exemption_amount": {
        "warning_action_class": "QA_only",
        "warning_severity": "medium",
        "affected_subsystem": "exemption_profile",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": None,
        "qa_note": "Exemption amount detail is incomplete but not a direct public blocker.",
    },
    "homestead_flag_mismatch": {
        "warning_action_class": "QA_only",
        "warning_severity": "medium",
        "affected_subsystem": "exemption_profile",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": None,
        "qa_note": "Assessment and exemption layers disagree on homestead status.",
    },
    "prior_year_assessment_basis_fallback": {
        "warning_action_class": "disclose",
        "warning_severity": "medium",
        "affected_subsystem": "value_support",
        "affected_unit_mask": "value_only",
        "public_disclosure_code": "prior_year_assessment_basis_fallback",
        "qa_note": "Current-year assessed basis fell back to prior-year values.",
    },
    "prior_year_living_area_fallback": {
        "warning_action_class": "disclose",
        "warning_severity": "low",
        "affected_subsystem": "data_completeness",
        "affected_unit_mask": "value_only",
        "public_disclosure_code": None,
        "qa_note": "Living area was sourced from the prior year.",
    },
    "tax_rate_basis_fallback_applied": {
        "warning_action_class": "constrain",
        "warning_severity": "medium",
        "affected_subsystem": "tax_rate",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": "tax_rate_basis_fallback_applied",
        "qa_note": "Tax-rate basis fell back off the requested year.",
    },
    "tax_rate_basis_current_year_unofficial_or_proposed": {
        "warning_action_class": "disclose",
        "warning_severity": "medium",
        "affected_subsystem": "tax_rate",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": "tax_rate_basis_current_year_unofficial_or_proposed",
        "qa_note": "Current-year rates are not fully adopted yet.",
    },
    "low_confidence_refined_review": {
        "warning_action_class": "suppress",
        "warning_severity": "medium",
        "affected_subsystem": "runtime_guardrail",
        "affected_unit_mask": "all",
        "public_disclosure_code": "low_confidence_refined_review",
        "qa_note": "Confidence threshold suppressed the public quote.",
    },
    "tax_limitation_uncertain": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "cap_limit",
        "affected_unit_mask": "tax_only",
        "public_disclosure_code": "tax_limitation_uncertain",
        "qa_note": "Runtime guardrail suppressed savings due to uncertain tax limitations.",
    },
    "implausible_savings_outlier": {
        "warning_action_class": "suppress",
        "warning_severity": "high",
        "affected_subsystem": "runtime_guardrail",
        "affected_unit_mask": "all",
        "public_disclosure_code": "implausible_savings_outlier",
        "qa_note": "Savings estimate exceeded the public-safe outlier threshold.",
    },
}


def _shutdown_persistence_executor() -> None:
    _PERSISTENCE_EXECUTOR.shutdown(wait=False, cancel_futures=True)


register_atexit(_shutdown_persistence_executor)


def extract_assessment_basis_contract(subject_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "assessment_basis_value": _as_float(subject_row.get("assessment_basis_value")),
        "assessment_basis_source_value_type": subject_row.get(
            "assessment_basis_source_value_type"
        ),
        "assessment_basis_source_year": _as_int(subject_row.get("assessment_basis_source_year")),
        "assessment_basis_source_reason": subject_row.get("assessment_basis_source_reason"),
        "assessment_basis_quality_code": subject_row.get("assessment_basis_quality_code"),
    }


@dataclass(frozen=True)
class DistributionSummary:
    parcel_count: int
    trimmed_parcel_count: int
    excluded_parcel_count: int
    trim_method_code: str
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
    supportable_subject_row_count: int = 0
    neighborhood_stats_count: int = 0
    supported_neighborhood_stats_count: int = 0
    segment_stats_count: int = 0
    supported_segment_stats_count: int = 0
    excluded_subject_count: int = 0
    cache_rebuild_duration_ms: int = 0
    neighborhood_stats_refresh_duration_ms: int = 0
    segment_stats_refresh_duration_ms: int = 0
    total_refresh_duration_ms: int = 0
    cache_view_row_delta: int = 0
    tax_rate_basis_year: int | None = None
    tax_rate_basis_reason: str | None = None
    tax_rate_basis_fallback_applied: bool = False
    tax_rate_basis_status: str | None = None
    tax_rate_basis_status_reason: str | None = None
    requested_tax_rate_supportable_subject_row_count: int = 0
    tax_rate_basis_supportable_subject_row_count: int = 0
    tax_rate_quoteable_subject_row_count: int = 0
    requested_tax_rate_effective_tax_rate_coverage_ratio: float = 0.0
    requested_tax_rate_assignment_coverage_ratio: float = 0.0
    tax_rate_basis_effective_tax_rate_coverage_ratio: float = 0.0
    tax_rate_basis_assignment_coverage_ratio: float = 0.0
    tax_rate_basis_continuity_parcel_match_row_count: int = 0
    tax_rate_basis_continuity_parcel_gap_row_count: int = 0
    tax_rate_basis_continuity_parcel_match_ratio: float = 0.0
    tax_rate_basis_continuity_account_number_match_row_count: int = 0
    tax_rate_basis_warning_codes: tuple[str, ...] = ()

    def as_log_extra(self) -> dict[str, int | float | str | bool | list[str] | None]:
        return {
            "subject_row_count": self.subject_row_count,
            "supportable_subject_row_count": self.supportable_subject_row_count,
            "neighborhood_stats_count": self.neighborhood_stats_count,
            "supported_neighborhood_stats_count": self.supported_neighborhood_stats_count,
            "segment_stats_count": self.segment_stats_count,
            "supported_segment_stats_count": self.supported_segment_stats_count,
            "excluded_subject_count": self.excluded_subject_count,
            "cache_rebuild_duration_ms": self.cache_rebuild_duration_ms,
            "neighborhood_stats_refresh_duration_ms": self.neighborhood_stats_refresh_duration_ms,
            "segment_stats_refresh_duration_ms": self.segment_stats_refresh_duration_ms,
            "total_refresh_duration_ms": self.total_refresh_duration_ms,
            "cache_view_row_delta": self.cache_view_row_delta,
            "tax_rate_basis_year": self.tax_rate_basis_year,
            "tax_rate_basis_reason": self.tax_rate_basis_reason,
            "tax_rate_basis_fallback_applied": self.tax_rate_basis_fallback_applied,
            "tax_rate_basis_status": self.tax_rate_basis_status,
            "tax_rate_basis_status_reason": self.tax_rate_basis_status_reason,
            "requested_tax_rate_supportable_subject_row_count": (
                self.requested_tax_rate_supportable_subject_row_count
            ),
            "tax_rate_basis_supportable_subject_row_count": (
                self.tax_rate_basis_supportable_subject_row_count
            ),
            "tax_rate_quoteable_subject_row_count": self.tax_rate_quoteable_subject_row_count,
            "requested_tax_rate_effective_tax_rate_coverage_ratio": (
                self.requested_tax_rate_effective_tax_rate_coverage_ratio
            ),
            "requested_tax_rate_assignment_coverage_ratio": (
                self.requested_tax_rate_assignment_coverage_ratio
            ),
            "tax_rate_basis_effective_tax_rate_coverage_ratio": (
                self.tax_rate_basis_effective_tax_rate_coverage_ratio
            ),
            "tax_rate_basis_assignment_coverage_ratio": (
                self.tax_rate_basis_assignment_coverage_ratio
            ),
            "tax_rate_basis_continuity_parcel_match_row_count": (
                self.tax_rate_basis_continuity_parcel_match_row_count
            ),
            "tax_rate_basis_continuity_parcel_gap_row_count": (
                self.tax_rate_basis_continuity_parcel_gap_row_count
            ),
            "tax_rate_basis_continuity_parcel_match_ratio": (
                self.tax_rate_basis_continuity_parcel_match_ratio
            ),
            "tax_rate_basis_continuity_account_number_match_row_count": (
                self.tax_rate_basis_continuity_account_number_match_row_count
            ),
            "tax_rate_basis_warning_codes": list(self.tax_rate_basis_warning_codes),
        }


@dataclass(frozen=True)
class _SubjectCacheRefreshMetrics:
    source_view_row_count: int
    subject_cache_row_count: int
    supportable_subject_row_count: int
    cache_view_row_delta: int
    selected_tax_rate_basis: SelectedTaxRateBasis | None = None


@dataclass(frozen=True)
class _ScopedTaxRateBasisSelection:
    county_id: str
    quote_tax_year: int
    selection: SelectedTaxRateBasis


@dataclass(frozen=True)
class _StatsRefreshMetrics:
    total_row_count: int
    supported_row_count: int


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
    trim_method_code = TRIM_METHOD_P05_P95
    if len(cleaned) < MIN_TRIM_GROUP_SIZE:
        trimmed = cleaned
        trim_method_code = TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3
    else:
        trimmed = [value for value in cleaned if lower_bound <= value <= upper_bound]
    if not trimmed:
        trimmed = cleaned
        trim_method_code = TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3
    trimmed = sorted(trimmed)
    stddev = statistics.pstdev(trimmed) if len(trimmed) > 1 else 0.0
    mean_value = statistics.fmean(trimmed)
    cv = None if mean_value == 0 else stddev / mean_value

    return DistributionSummary(
        parcel_count=len(cleaned),
        trimmed_parcel_count=len(trimmed),
        excluded_parcel_count=max(len(cleaned) - len(trimmed), 0),
        trim_method_code=trim_method_code,
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
        refresh_started_at = datetime.now(timezone.utc)
        refresh_run_id = self._start_refresh_run(
            county_id=county_id,
            tax_year=tax_year,
            refresh_started_at=refresh_started_at,
        )
        total_started_at = perf_counter()

        try:
            cache_started_at = perf_counter()
            cache_metrics = self._refresh_subject_cache(county_id=county_id, tax_year=tax_year)
            cache_duration_ms = int((perf_counter() - cache_started_at) * 1000)

            neighborhood_started_at = perf_counter()
            neighborhood_metrics = self._refresh_neighborhood_stats(
                county_id=county_id,
                tax_year=tax_year,
            )
            neighborhood_duration_ms = int((perf_counter() - neighborhood_started_at) * 1000)

            segment_started_at = perf_counter()
            segment_metrics = self._refresh_segment_stats(
                county_id=county_id,
                tax_year=tax_year,
            )
            segment_duration_ms = int((perf_counter() - segment_started_at) * 1000)

            summary = InstantQuoteRefreshSummary(
                subject_row_count=cache_metrics.subject_cache_row_count,
                supportable_subject_row_count=cache_metrics.supportable_subject_row_count,
                neighborhood_stats_count=neighborhood_metrics.total_row_count,
                supported_neighborhood_stats_count=neighborhood_metrics.supported_row_count,
                segment_stats_count=segment_metrics.total_row_count,
                supported_segment_stats_count=segment_metrics.supported_row_count,
                excluded_subject_count=max(
                    cache_metrics.subject_cache_row_count - cache_metrics.supportable_subject_row_count,
                    0,
                ),
                cache_rebuild_duration_ms=cache_duration_ms,
                neighborhood_stats_refresh_duration_ms=neighborhood_duration_ms,
                segment_stats_refresh_duration_ms=segment_duration_ms,
                total_refresh_duration_ms=int((perf_counter() - total_started_at) * 1000),
                cache_view_row_delta=cache_metrics.cache_view_row_delta,
                tax_rate_basis_year=(
                    None
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.basis_tax_year
                ),
                tax_rate_basis_reason=(
                    None
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.reason_code
                ),
                tax_rate_basis_fallback_applied=bool(
                    cache_metrics.selected_tax_rate_basis
                    and cache_metrics.selected_tax_rate_basis.fallback_applied
                ),
                tax_rate_basis_status=(
                    None
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.basis_status
                ),
                tax_rate_basis_status_reason=(
                    None
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.basis_status_reason
                ),
                requested_tax_rate_supportable_subject_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.requested_year_supportable_subject_row_count
                    )
                ),
                tax_rate_basis_supportable_subject_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.selected_basis_supportable_subject_row_count
                ),
                tax_rate_quoteable_subject_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.quoteable_subject_row_count
                ),
                requested_tax_rate_effective_tax_rate_coverage_ratio=(
                    0.0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.requested_year_effective_tax_rate_coverage_ratio
                    )
                ),
                requested_tax_rate_assignment_coverage_ratio=(
                    0.0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.requested_year_assignment_coverage_ratio
                    )
                ),
                tax_rate_basis_effective_tax_rate_coverage_ratio=(
                    0.0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.selected_basis_effective_tax_rate_coverage_ratio
                    )
                ),
                tax_rate_basis_assignment_coverage_ratio=(
                    0.0
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.selected_basis_assignment_coverage_ratio
                ),
                tax_rate_basis_continuity_parcel_match_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.selected_basis_continuity_parcel_match_row_count
                    )
                ),
                tax_rate_basis_continuity_parcel_gap_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.selected_basis_continuity_parcel_gap_row_count
                    )
                ),
                tax_rate_basis_continuity_parcel_match_ratio=(
                    0.0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.selected_basis_continuity_parcel_match_ratio
                    )
                ),
                tax_rate_basis_continuity_account_number_match_row_count=(
                    0
                    if cache_metrics.selected_tax_rate_basis is None
                    else (
                        cache_metrics.selected_tax_rate_basis.selected_basis_continuity_account_number_match_row_count
                    )
                ),
                tax_rate_basis_warning_codes=(
                    ()
                    if cache_metrics.selected_tax_rate_basis is None
                    else cache_metrics.selected_tax_rate_basis.selected_basis_warning_codes
                ),
            )
            self._complete_refresh_run(
                refresh_run_id=refresh_run_id,
                summary=summary,
                source_view_row_count=cache_metrics.source_view_row_count,
            )
            return summary
        except Exception as exc:
            self._fail_refresh_run(refresh_run_id=refresh_run_id, error_message=str(exc))
            raise

    def _prepare_refresh_session(self, connection: object) -> None:
        # Local Postgres environments used for county-scale refreshes can exhaust
        # shared memory when the planner chooses parallel workers on these wide
        # derived queries, so keep the refresh path single-gather and predictable.
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")

    def _refresh_subject_cache(
        self,
        *,
        county_id: str | None,
        tax_year: int | None,
    ) -> _SubjectCacheRefreshMetrics:
        with get_connection() as connection:
            self._prepare_refresh_session(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS tmp_instant_quote_subject_scope
                    """
                )
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS tmp_instant_quote_subject_refresh
                    """
                )
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_instant_quote_subject_scope ON COMMIT DROP AS
                    SELECT
                      pys.parcel_year_snapshot_id,
                      pys.parcel_id,
                      pys.county_id,
                      pys.tax_year,
                      pys.account_number,
                      pys.cad_owner_name,
                      pys.cad_owner_name_normalized
                    FROM parcel_year_snapshots pys
                    JOIN parcels p
                      ON p.parcel_id = pys.parcel_id
	                    LEFT JOIN property_characteristics pc
	                      ON pc.parcel_year_snapshot_id = pys.parcel_year_snapshot_id
	                    WHERE pys.is_current = true
	                      AND CASE
	                        WHEN pc.property_characteristic_id IS NOT NULL THEN pc.property_type_code
	                        ELSE p.property_type_code
	                      END = 'sfr'
                      AND (%s::text IS NULL OR pys.county_id = %s)
                      AND (%s::integer IS NULL OR pys.tax_year = %s)
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX idx_tmp_instant_quote_subject_scope_key
                      ON tmp_instant_quote_subject_scope(parcel_id, tax_year)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX idx_tmp_instant_quote_subject_scope_snapshot
                      ON tmp_instant_quote_subject_scope(parcel_year_snapshot_id)
                    """
                )
                cursor.execute("ANALYZE tmp_instant_quote_subject_scope")
                cursor.execute(
                    """
                    DROP TABLE IF EXISTS tmp_instant_quote_tax_rate_basis_selection
                    """
                )
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_instant_quote_tax_rate_basis_selection (
                      county_id text NOT NULL,
                      tax_year integer NOT NULL,
                      effective_tax_rate_basis_year integer,
                      effective_tax_rate_basis_reason text NOT NULL,
                      effective_tax_rate_basis_fallback_applied boolean NOT NULL,
                      effective_tax_rate_basis_status text,
                      effective_tax_rate_basis_status_reason text,
                      requested_tax_rate_supportable_subject_row_count integer NOT NULL,
                      effective_tax_rate_basis_supportable_subject_row_count integer NOT NULL,
                      PRIMARY KEY (county_id, tax_year)
                    ) ON COMMIT DROP
                    """
                )
                basis_selections = [
                    self._select_tax_rate_basis_for_scope(
                        cursor,
                        county_id=row["county_id"],
                        tax_year=int(row["tax_year"]),
                    )
                    for row in self._list_scoped_county_tax_years(cursor)
                ]
                if (
                    county_id is not None
                    and tax_year is not None
                    and not any(
                        selection.county_id == county_id and selection.quote_tax_year == tax_year
                        for selection in basis_selections
                    )
                ):
                    basis_selections.append(
                        self._select_tax_rate_basis_for_scope(
                            cursor,
                            county_id=county_id,
                            tax_year=tax_year,
                        )
                    )
                for basis_selection in basis_selections:
                    cursor.execute(
                        """
                        INSERT INTO tmp_instant_quote_tax_rate_basis_selection (
                          county_id,
                          tax_year,
                          effective_tax_rate_basis_year,
                          effective_tax_rate_basis_reason,
                          effective_tax_rate_basis_fallback_applied,
                          effective_tax_rate_basis_status,
                          effective_tax_rate_basis_status_reason,
                          requested_tax_rate_supportable_subject_row_count,
                          effective_tax_rate_basis_supportable_subject_row_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (county_id, tax_year) DO UPDATE
                        SET effective_tax_rate_basis_year = EXCLUDED.effective_tax_rate_basis_year,
                            effective_tax_rate_basis_reason = EXCLUDED.effective_tax_rate_basis_reason,
                            effective_tax_rate_basis_fallback_applied = EXCLUDED.effective_tax_rate_basis_fallback_applied,
                            effective_tax_rate_basis_status = EXCLUDED.effective_tax_rate_basis_status,
                            effective_tax_rate_basis_status_reason = EXCLUDED.effective_tax_rate_basis_status_reason,
                            requested_tax_rate_supportable_subject_row_count = EXCLUDED.requested_tax_rate_supportable_subject_row_count,
                            effective_tax_rate_basis_supportable_subject_row_count = EXCLUDED.effective_tax_rate_basis_supportable_subject_row_count
                        """,
                        (
                            basis_selection.county_id,
                            basis_selection.quote_tax_year,
                            basis_selection.selection.basis_tax_year,
                            basis_selection.selection.reason_code,
                            basis_selection.selection.fallback_applied,
                            basis_selection.selection.basis_status,
                            basis_selection.selection.basis_status_reason,
                            basis_selection.selection.requested_year_supportable_subject_row_count,
                            basis_selection.selection.selected_basis_supportable_subject_row_count,
                        ),
                    )
                cursor.execute("ANALYZE tmp_instant_quote_tax_rate_basis_selection")
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_instant_quote_subject_refresh ON COMMIT DROP AS
                    WITH current_addresses AS (
                      SELECT DISTINCT ON (pa.parcel_id)
                        pa.parcel_id,
                        pa.situs_address,
                        pa.situs_city,
                        COALESCE(pa.situs_state, 'TX') AS situs_state,
                        pa.situs_zip,
                        pa.normalized_address
                      FROM parcel_addresses pa
                      JOIN tmp_instant_quote_subject_scope scope
                        ON scope.parcel_id = pa.parcel_id
                      WHERE pa.is_current = true
                      ORDER BY
                        pa.parcel_id,
                        pa.updated_at DESC,
                        pa.created_at DESC,
                        pa.parcel_address_id DESC
                    ),
                    raw_codes AS (
                      SELECT
                        pe.parcel_id,
                        pe.tax_year,
                        array_agg(DISTINCT raw_code ORDER BY raw_code) AS raw_exemption_codes
                      FROM parcel_exemptions pe
                      JOIN tmp_instant_quote_subject_scope scope
                        ON scope.parcel_id = pe.parcel_id
                       AND scope.tax_year = pe.tax_year
                      CROSS JOIN LATERAL unnest(
                        CASE
                          WHEN pe.raw_exemption_codes IS NULL
                            OR cardinality(pe.raw_exemption_codes) = 0
                            THEN ARRAY[COALESCE(pe.exemption_type_code, '')]
                          ELSE pe.raw_exemption_codes
                        END
                      ) AS raw_code
                      WHERE btrim(raw_code) <> ''
                      GROUP BY pe.parcel_id, pe.tax_year
                    ),
                    exemption_rollup AS (
                      SELECT
                        pe.parcel_id,
                        pe.tax_year,
                        COUNT(*) AS exemption_record_count,
                        COALESCE(
                          SUM(pe.exemption_amount)
                          FILTER (WHERE pe.granted_flag AND pe.exemption_amount IS NOT NULL),
                          0::numeric
                        ) AS granted_exemption_amount_total,
                        COALESCE(
                          array_agg(DISTINCT pe.exemption_type_code ORDER BY pe.exemption_type_code)
                          FILTER (WHERE pe.exemption_type_code IS NOT NULL),
                          ARRAY[]::text[]
                        ) AS exemption_type_codes,
                        COALESCE(rc.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
                        COALESCE(
                          BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'homestead'),
                          false
                        ) AS homestead_flag,
                        COALESCE(
                          BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'over65'),
                          false
                        ) AS over65_flag,
                        COALESCE(
                          BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled'),
                          false
                        ) AS disabled_flag,
                        COALESCE(
                          BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'disabled_veteran'),
                          false
                        ) AS disabled_veteran_flag,
                        COALESCE(
                          BOOL_OR((COALESCE(et.metadata_json, '{}'::jsonb) -> 'summary_flags') ? 'freeze'),
                          false
                        ) AS freeze_flag,
                        COALESCE(
                          BOOL_OR(pe.amount_missing_flag OR (pe.granted_flag AND pe.exemption_amount IS NULL)),
                          false
                        ) AS missing_exemption_amount_flag
                      FROM parcel_exemptions pe
                      JOIN tmp_instant_quote_subject_scope scope
                        ON scope.parcel_id = pe.parcel_id
                       AND scope.tax_year = pe.tax_year
                      LEFT JOIN exemption_types et
                        ON et.exemption_type_code = pe.exemption_type_code
                      LEFT JOIN raw_codes rc
                        ON rc.parcel_id = pe.parcel_id
                       AND rc.tax_year = pe.tax_year
                      GROUP BY pe.parcel_id, pe.tax_year, rc.raw_exemption_codes
                    ),
                    tax_assignment_rollup AS (
                      SELECT
                        scope.parcel_id,
                        scope.tax_year,
                        COUNT(*) FILTER (WHERE tu.unit_type_code = 'county')::integer AS county_assignment_count,
                        COUNT(*) FILTER (WHERE tu.unit_type_code = 'school')::integer AS school_assignment_count
                      FROM tmp_instant_quote_subject_scope scope
                      JOIN tmp_instant_quote_tax_rate_basis_selection tax_basis
                        ON tax_basis.county_id = scope.county_id
                       AND tax_basis.tax_year = scope.tax_year
                      JOIN parcel_taxing_units ptu
                        ON ptu.parcel_id = scope.parcel_id
                       AND ptu.tax_year = tax_basis.effective_tax_rate_basis_year
                      JOIN taxing_units tu
                        ON tu.taxing_unit_id = ptu.taxing_unit_id
                      GROUP BY scope.parcel_id, scope.tax_year
                    ),
                    tax_assignment_requirements AS (
                      SELECT
                        tax_basis.county_id,
                        tax_basis.tax_year,
                        COALESCE(BOOL_OR(tu.unit_type_code = 'county'), false) AS requires_county_assignment,
                        COALESCE(BOOL_OR(tu.unit_type_code = 'school'), false) AS requires_school_assignment
                      FROM tmp_instant_quote_tax_rate_basis_selection tax_basis
                      LEFT JOIN parcel_taxing_units ptu
                        ON ptu.tax_year = tax_basis.effective_tax_rate_basis_year
                      LEFT JOIN parcels basis_parcel
                        ON basis_parcel.parcel_id = ptu.parcel_id
                       AND basis_parcel.county_id = tax_basis.county_id
                      LEFT JOIN taxing_units tu
                        ON tu.taxing_unit_id = ptu.taxing_unit_id
                       AND tu.unit_type_code IN ('county', 'school')
                      GROUP BY tax_basis.county_id, tax_basis.tax_year
                    ),
                    geometry_flags AS (
                      SELECT
                        pg.parcel_id,
                        pg.tax_year,
                        BOOL_OR(pg.geometry_role = 'parcel_polygon' AND pg.is_current) AS has_parcel_polygon,
                        BOOL_OR(pg.geometry_role = 'parcel_centroid' AND pg.is_current) AS has_parcel_centroid
                      FROM parcel_geometries pg
                      JOIN tmp_instant_quote_subject_scope scope
                        ON scope.parcel_id = pg.parcel_id
                       AND scope.tax_year = pg.tax_year
                      GROUP BY pg.parcel_id, pg.tax_year
                    ),
                    source_basis AS (
                      SELECT
                        scope.parcel_id,
                        scope.county_id,
                        scope.tax_year,
                        scope.account_number,
                        p.cad_property_id,
                        COALESCE(ca.situs_address, p.situs_address) AS situs_address,
                        COALESCE(ca.situs_city, p.situs_city) AS situs_city,
                        COALESCE(ca.situs_state, COALESCE(p.situs_state, 'TX')) AS situs_state,
                        COALESCE(ca.situs_zip, p.situs_zip) AS situs_zip,
                        COALESCE(
                          ca.normalized_address,
                          upper(regexp_replace(COALESCE(ca.situs_address, p.situs_address, ''), '[^A-Za-z0-9 ]', '', 'g'))
                        ) AS normalized_address,
                        COALESCE(cor.owner_name, scope.cad_owner_name, p.owner_name) AS owner_name,
                        COALESCE(cor.owner_name_normalized, scope.cad_owner_name_normalized) AS owner_name_normalized,
                        COALESCE(cor.override_flag, false) AS owner_override_flag,
                        scope.cad_owner_name,
                        scope.cad_owner_name_normalized,
	                        CASE
	                          WHEN pc.property_characteristic_id IS NOT NULL THEN pc.property_type_code
	                          ELSE p.property_type_code
	                        END AS property_type_code,
                        COALESCE(pc.property_class_code, p.property_class_code) AS property_class_code,
                        COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code,
                        COALESCE(pc.school_district_name, p.school_district_name) AS school_district_name,
                        COALESCE(NULLIF(pi.living_area_sf, 0), pi_prior.living_area_sf) AS living_area_sf,
                        CASE
                          WHEN COALESCE(pi.living_area_sf, 0) <= 0
                            AND COALESCE(pi_prior.living_area_sf, 0) > 0
                            THEN COALESCE(pi_prior.year_built, pi.year_built)
                          ELSE COALESCE(pi.year_built, pi_prior.year_built)
                        END AS year_built,
                        pl.parcel_land_id,
                        COALESCE(pa.parcel_assessment_id, pa_prior.parcel_assessment_id) AS parcel_assessment_id,
                        COALESCE(NULLIF(pa.market_value, 0), NULLIF(pa_prior.market_value, 0)) AS market_value,
                        COALESCE(NULLIF(pa.assessed_value, 0), NULLIF(pa_prior.assessed_value, 0)) AS assessed_value,
                        COALESCE(NULLIF(pa.capped_value, 0), NULLIF(pa_prior.capped_value, 0)) AS capped_value,
                        COALESCE(NULLIF(pa.appraised_value, 0), NULLIF(pa_prior.appraised_value, 0)) AS appraised_value,
                        COALESCE(NULLIF(pa.certified_value, 0), NULLIF(pa_prior.certified_value, 0)) AS certified_value,
                        COALESCE(NULLIF(pa.notice_value, 0), NULLIF(pa_prior.notice_value, 0)) AS notice_value,
                        COALESCE(pa.land_value, pa_prior.land_value) AS land_value,
                        COALESCE(pa.improvement_value, pa_prior.improvement_value) AS improvement_value,
                        COALESCE(pa.exemption_value_total, pa_prior.exemption_value_total) AS exemption_value_total,
                        CASE
                          WHEN NULLIF(pa.certified_value, 0) IS NOT NULL THEN 'certified'
                          WHEN NULLIF(pa.appraised_value, 0) IS NOT NULL THEN 'appraised'
                          WHEN NULLIF(pa.assessed_value, 0) IS NOT NULL THEN 'assessed'
                          WHEN NULLIF(pa.market_value, 0) IS NOT NULL THEN 'market'
                          WHEN NULLIF(pa.notice_value, 0) IS NOT NULL THEN 'notice'
                          WHEN NULLIF(pa_prior.certified_value, 0) IS NOT NULL THEN 'certified'
                          WHEN NULLIF(pa_prior.appraised_value, 0) IS NOT NULL THEN 'appraised'
                          WHEN NULLIF(pa_prior.assessed_value, 0) IS NOT NULL THEN 'assessed'
                          WHEN NULLIF(pa_prior.market_value, 0) IS NOT NULL THEN 'market'
                          WHEN NULLIF(pa_prior.notice_value, 0) IS NOT NULL THEN 'notice'
                          ELSE NULL
                        END AS assessment_basis_source_value_type,
                        CASE
                          WHEN NULLIF(pa.certified_value, 0) IS NOT NULL THEN scope.tax_year
                          WHEN NULLIF(pa.appraised_value, 0) IS NOT NULL THEN scope.tax_year
                          WHEN NULLIF(pa.assessed_value, 0) IS NOT NULL THEN scope.tax_year
                          WHEN NULLIF(pa.market_value, 0) IS NOT NULL THEN scope.tax_year
                          WHEN NULLIF(pa.notice_value, 0) IS NOT NULL THEN scope.tax_year
                          WHEN NULLIF(pa_prior.certified_value, 0) IS NOT NULL THEN scope.tax_year - 1
                          WHEN NULLIF(pa_prior.appraised_value, 0) IS NOT NULL THEN scope.tax_year - 1
                          WHEN NULLIF(pa_prior.assessed_value, 0) IS NOT NULL THEN scope.tax_year - 1
                          WHEN NULLIF(pa_prior.market_value, 0) IS NOT NULL THEN scope.tax_year - 1
                          WHEN NULLIF(pa_prior.notice_value, 0) IS NOT NULL THEN scope.tax_year - 1
                          ELSE NULL
                        END AS assessment_basis_source_year,
                        CASE
                          WHEN NULLIF(pa.certified_value, 0) IS NOT NULL THEN 'current_year_certified'
                          WHEN NULLIF(pa.appraised_value, 0) IS NOT NULL THEN 'current_year_appraised'
                          WHEN NULLIF(pa.assessed_value, 0) IS NOT NULL THEN 'current_year_assessed'
                          WHEN NULLIF(pa.market_value, 0) IS NOT NULL THEN 'current_year_market'
                          WHEN NULLIF(pa.notice_value, 0) IS NOT NULL THEN 'current_year_notice'
                          WHEN NULLIF(pa_prior.certified_value, 0) IS NOT NULL THEN 'prior_year_certified_fallback'
                          WHEN NULLIF(pa_prior.appraised_value, 0) IS NOT NULL THEN 'prior_year_appraised_fallback'
                          WHEN NULLIF(pa_prior.assessed_value, 0) IS NOT NULL THEN 'prior_year_assessed_fallback'
                          WHEN NULLIF(pa_prior.market_value, 0) IS NOT NULL THEN 'prior_year_market_fallback'
                          WHEN NULLIF(pa_prior.notice_value, 0) IS NOT NULL THEN 'prior_year_notice_fallback'
                          ELSE 'missing'
                        END AS assessment_basis_source_reason,
                        CASE
                          WHEN NULLIF(pa.certified_value, 0) IS NOT NULL THEN 'current_year_authoritative'
                          WHEN NULLIF(pa.appraised_value, 0) IS NOT NULL THEN 'current_year_authoritative'
                          WHEN NULLIF(pa.assessed_value, 0) IS NOT NULL THEN 'current_year_authoritative'
                          WHEN NULLIF(pa.market_value, 0) IS NOT NULL THEN 'current_year_proxy'
                          WHEN NULLIF(pa.notice_value, 0) IS NOT NULL THEN 'current_year_proxy'
                          WHEN NULLIF(pa_prior.certified_value, 0) IS NOT NULL THEN 'prior_year_fallback'
                          WHEN NULLIF(pa_prior.appraised_value, 0) IS NOT NULL THEN 'prior_year_fallback'
                          WHEN NULLIF(pa_prior.assessed_value, 0) IS NOT NULL THEN 'prior_year_fallback'
                          WHEN NULLIF(pa_prior.market_value, 0) IS NOT NULL THEN 'prior_year_fallback'
                          WHEN NULLIF(pa_prior.notice_value, 0) IS NOT NULL THEN 'prior_year_fallback'
                          ELSE 'missing'
                        END AS assessment_basis_quality_code,
                        COALESCE(pa.homestead_flag, pa_prior.homestead_flag) AS assessment_homestead_flag,
                        COALESCE(er.exemption_record_count, 0) AS exemption_record_count,
                        COALESCE(er.granted_exemption_amount_total, 0::numeric) AS granted_exemption_amount_total,
                        COALESCE(er.exemption_type_codes, ARRAY[]::text[]) AS exemption_type_codes,
                        COALESCE(er.raw_exemption_codes, ARRAY[]::text[]) AS raw_exemption_codes,
                        COALESCE(er.homestead_flag, false) AS homestead_flag,
                        COALESCE(er.over65_flag, false) AS over65_flag,
                        COALESCE(er.disabled_flag, false) AS disabled_flag,
                        COALESCE(er.disabled_veteran_flag, false) AS disabled_veteran_flag,
                        COALESCE(er.freeze_flag, false) AS freeze_flag,
                        COALESCE(er.missing_exemption_amount_flag, false) AS missing_exemption_amount_flag,
                        COALESCE(etr.effective_tax_rate, NULL) AS effective_tax_rate,
                        etr.source_method AS effective_tax_rate_source_method,
                        tax_basis.effective_tax_rate_basis_year,
                        tax_basis.effective_tax_rate_basis_reason,
                        tax_basis.effective_tax_rate_basis_fallback_applied,
                        tax_basis.effective_tax_rate_basis_status,
                        tax_basis.effective_tax_rate_basis_status_reason,
                        COALESCE(tar.county_assignment_count, 0) AS county_assignment_count,
                        COALESCE(tar.school_assignment_count, 0) AS school_assignment_count,
                        COALESCE(tarq.requires_county_assignment, false) AS requires_county_assignment,
                        COALESCE(tarq.requires_school_assignment, false) AS requires_school_assignment,
                        COALESCE(gf.has_parcel_polygon, false) AS has_parcel_polygon,
                        COALESCE(gf.has_parcel_centroid, false) AS has_parcel_centroid,
                        pc.property_characteristic_id IS NOT NULL AS has_characteristics,
                        (
                          pi.parcel_improvement_id IS NOT NULL
                          OR pi_prior.parcel_improvement_id IS NOT NULL
                        ) AS has_improvement,
                        (
                          COALESCE(pi.living_area_sf, 0) <= 0
                          AND COALESCE(pi_prior.living_area_sf, 0) > 0
                        ) AS used_prior_year_living_area_fallback,
                        (
                          COALESCE(
                            NULLIF(pa.certified_value, 0),
                            NULLIF(pa.appraised_value, 0),
                            NULLIF(pa.assessed_value, 0),
                            NULLIF(pa.market_value, 0),
                            NULLIF(pa.notice_value, 0),
                            0
                          ) <= 0
                          AND COALESCE(
                            NULLIF(pa_prior.certified_value, 0),
                            NULLIF(pa_prior.appraised_value, 0),
                            NULLIF(pa_prior.assessed_value, 0),
                            NULLIF(pa_prior.market_value, 0),
                            NULLIF(pa_prior.notice_value, 0),
                            0
                          ) > 0
                        ) AS used_prior_year_assessment_basis_fallback,
                        pl.parcel_land_id IS NOT NULL AS has_land,
                        (
                          pa.parcel_assessment_id IS NOT NULL
                          OR pa_prior.parcel_assessment_id IS NOT NULL
                        ) AS has_assessment,
                        cor.current_owner_rollup_id IS NOT NULL AS has_owner_rollup,
                        etr.effective_tax_rate IS NOT NULL AS has_effective_tax_rate
                      FROM tmp_instant_quote_subject_scope scope
                      JOIN parcels p
                        ON p.parcel_id = scope.parcel_id
                      LEFT JOIN current_addresses ca
                        ON ca.parcel_id = scope.parcel_id
                      LEFT JOIN property_characteristics pc
                        ON pc.parcel_year_snapshot_id = scope.parcel_year_snapshot_id
                      LEFT JOIN parcel_improvements pi
                        ON pi.parcel_id = scope.parcel_id
                       AND pi.tax_year = scope.tax_year
                      LEFT JOIN parcel_improvements pi_prior
                        ON pi_prior.parcel_id = scope.parcel_id
                       AND pi_prior.tax_year = scope.tax_year - 1
                      LEFT JOIN parcel_lands pl
                        ON pl.parcel_id = scope.parcel_id
                       AND pl.tax_year = scope.tax_year
                      LEFT JOIN parcel_assessments pa
                        ON pa.parcel_id = scope.parcel_id
                       AND pa.tax_year = scope.tax_year
                      LEFT JOIN parcel_assessments pa_prior
                        ON pa_prior.parcel_id = scope.parcel_id
                       AND pa_prior.tax_year = scope.tax_year - 1
                      LEFT JOIN exemption_rollup er
                        ON er.parcel_id = scope.parcel_id
                       AND er.tax_year = scope.tax_year
                      LEFT JOIN tmp_instant_quote_tax_rate_basis_selection tax_basis
                        ON tax_basis.county_id = scope.county_id
                       AND tax_basis.tax_year = scope.tax_year
                      LEFT JOIN effective_tax_rates etr
                        ON etr.parcel_id = scope.parcel_id
                       AND etr.tax_year = tax_basis.effective_tax_rate_basis_year
                      LEFT JOIN tax_assignment_rollup tar
                        ON tar.parcel_id = scope.parcel_id
                       AND tar.tax_year = scope.tax_year
                      LEFT JOIN tax_assignment_requirements tarq
                        ON tarq.county_id = scope.county_id
                       AND tarq.tax_year = scope.tax_year
                      LEFT JOIN current_owner_rollups cor
                        ON cor.parcel_id = scope.parcel_id
                       AND cor.tax_year = scope.tax_year
                      LEFT JOIN geometry_flags gf
                        ON gf.parcel_id = scope.parcel_id
                       AND gf.tax_year = scope.tax_year
                    )
                    SELECT
                      sb.parcel_id,
                      sb.county_id,
                      sb.tax_year,
                      sb.account_number,
                      concat_ws(
                        ', ',
                        sb.situs_address,
                        sb.situs_city,
                        concat_ws(' ', sb.situs_state, sb.situs_zip)
                      ) AS address,
                      sb.situs_address,
                      sb.situs_city,
                      sb.situs_state,
                      sb.situs_zip,
                      sb.neighborhood_code,
                      sb.school_district_name,
                      sb.property_type_code,
                      sb.property_class_code,
                      sb.living_area_sf,
                      sb.year_built,
                      sb.assessed_value,
                      sb.capped_value,
                      sb.notice_value,
                      sb.land_value,
                      sb.improvement_value,
                      COALESCE(
                        sb.certified_value,
                        sb.appraised_value,
                        sb.assessed_value,
                        sb.market_value,
                        sb.notice_value
                      ) AS assessment_basis_value,
                      sb.assessment_basis_source_value_type,
                      sb.assessment_basis_source_year,
                      sb.assessment_basis_source_reason,
                      sb.assessment_basis_quality_code,
                      sb.effective_tax_rate,
                      sb.effective_tax_rate_source_method,
                      sb.effective_tax_rate_basis_year,
                      sb.effective_tax_rate_basis_reason,
                      sb.effective_tax_rate_basis_fallback_applied,
                      sb.effective_tax_rate_basis_status,
                      sb.effective_tax_rate_basis_status_reason,
                      ROUND(
                        (
                          (
                            (CASE WHEN sb.situs_address IS NOT NULL THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_characteristics THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_improvement THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_land THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_assessment THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.exemption_record_count > 0 OR sb.exemption_value_total IS NOT NULL THEN 1 ELSE 0 END) +
                            (
                              CASE
                                WHEN (
                                  (
                                    NOT sb.requires_county_assignment
                                    OR sb.county_assignment_count > 0
                                  )
                                  AND (
                                    NOT sb.requires_school_assignment
                                    OR sb.school_assignment_count > 0
                                  )
                                ) THEN 1
                                ELSE 0
                              END
                            ) +
                            (CASE WHEN sb.has_effective_tax_rate THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_owner_rollup THEN 1 ELSE 0 END) +
                            (CASE WHEN sb.has_parcel_polygon OR sb.has_parcel_centroid THEN 1 ELSE 0 END)
                          )::numeric / 10::numeric
                        ) * 100.0,
                        2
                      ) AS completeness_score,
                      (
                        sb.situs_address IS NOT NULL
                        AND sb.has_assessment
                        AND sb.has_effective_tax_rate
                        AND sb.has_owner_rollup
                      ) AS public_summary_ready_flag,
                      sb.homestead_flag,
                      sb.over65_flag,
                      sb.disabled_flag,
                      sb.disabled_veteran_flag,
                      sb.freeze_flag,
                      sb.exemption_type_codes,
                      ARRAY_REMOVE(
                        ARRAY[
                          CASE WHEN sb.situs_address IS NULL THEN 'missing_address' END,
                          CASE WHEN NOT sb.has_characteristics THEN 'missing_characteristics' END,
                          CASE WHEN NOT sb.has_improvement THEN 'missing_improvement' END,
                          CASE WHEN NOT sb.has_land THEN 'missing_land' END,
                          CASE WHEN NOT sb.has_assessment THEN 'missing_assessment' END,
                          CASE WHEN sb.exemption_record_count = 0 AND sb.exemption_value_total IS NULL THEN 'missing_exemption_data' END,
                          CASE
                            WHEN sb.requires_county_assignment
                              AND sb.county_assignment_count = 0
                            THEN 'missing_county_assignment'
                          END,
                          CASE
                            WHEN sb.requires_school_assignment
                              AND sb.school_assignment_count = 0
                            THEN 'missing_school_assignment'
                          END,
                          CASE WHEN NOT sb.has_effective_tax_rate THEN 'missing_effective_tax_rate' END,
                          CASE WHEN NOT sb.has_owner_rollup THEN 'missing_owner_rollup' END,
                          CASE
                            WHEN sb.owner_name IS NOT NULL
                              AND sb.cad_owner_name IS NOT NULL
                              AND sb.owner_name IS DISTINCT FROM sb.cad_owner_name
                              AND COALESCE(sb.owner_override_flag, false) = false
                            THEN 'cad_owner_mismatch'
                          END,
                          CASE WHEN sb.missing_exemption_amount_flag THEN 'missing_exemption_amount' END,
                          CASE
                            WHEN sb.exemption_value_total IS NOT NULL
                              AND ABS(sb.granted_exemption_amount_total - sb.exemption_value_total) > 0.01
                            THEN 'assessment_exemption_total_mismatch'
                          END,
                          CASE
                            WHEN sb.assessment_homestead_flag IS NOT NULL
                              AND sb.assessment_homestead_flag IS DISTINCT FROM sb.homestead_flag
                            THEN 'homestead_flag_mismatch'
                          END,
                          CASE
                            WHEN sb.freeze_flag
                              AND NOT (sb.over65_flag OR sb.disabled_flag OR sb.disabled_veteran_flag)
                            THEN 'freeze_without_qualifying_exemption'
                          END,
                          CASE
                            WHEN NOT (sb.has_parcel_polygon OR sb.has_parcel_centroid)
                            THEN 'missing_geometry'
                          END,
                          CASE
                            WHEN sb.used_prior_year_living_area_fallback
                            THEN 'prior_year_living_area_fallback'
                          END,
                          CASE
                            WHEN sb.used_prior_year_assessment_basis_fallback
                            THEN 'prior_year_assessment_basis_fallback'
                          END
                          ,
                          CASE
                            WHEN sb.used_prior_year_assessment_basis_fallback
                            THEN 'prior_year_assessment_basis_fallback'
                          END
                        ],
                        NULL
                      ) AS warning_codes,
                      CASE
                        WHEN sb.living_area_sf IS NULL OR sb.living_area_sf <= 0 THEN NULL
                        WHEN sb.living_area_sf < 1400 THEN 'lt_1400'
                        WHEN sb.living_area_sf < 1700 THEN '1400_1699'
                        WHEN sb.living_area_sf < 2000 THEN '1700_1999'
                        WHEN sb.living_area_sf < 2400 THEN '2000_2399'
                        WHEN sb.living_area_sf < 2900 THEN '2400_2899'
                        WHEN sb.living_area_sf < 3500 THEN '2900_3499'
                        ELSE '3500_plus'
                      END AS size_bucket,
                      CASE
                        WHEN sb.year_built IS NULL THEN 'unknown'
                        WHEN sb.year_built < 1970 THEN 'pre_1970'
                        WHEN sb.year_built < 1990 THEN '1970_1989'
                        WHEN sb.year_built < 2005 THEN '1990_2004'
                        WHEN sb.year_built < 2015 THEN '2005_2014'
                        ELSE '2015_plus'
                      END AS age_bucket,
                      CASE
                        WHEN sb.property_type_code IS DISTINCT FROM 'sfr' THEN 'unsupported_property_type'
                        WHEN sb.living_area_sf IS NULL OR sb.living_area_sf <= 0 THEN 'missing_living_area'
                        WHEN COALESCE(
                          sb.certified_value,
                          sb.appraised_value,
                          sb.assessed_value,
                          sb.market_value,
                          sb.notice_value
                        ) IS NULL
                          OR COALESCE(
                            sb.certified_value,
                            sb.appraised_value,
                            sb.assessed_value,
                            sb.market_value,
                            sb.notice_value
                          ) <= 0
                          THEN 'missing_assessment_basis'
                        WHEN sb.neighborhood_code IS NULL OR btrim(sb.neighborhood_code) = '' THEN 'missing_neighborhood_code'
                        WHEN sb.effective_tax_rate IS NULL OR sb.effective_tax_rate <= 0 THEN 'missing_effective_tax_rate'
                        ELSE NULL
                      END AS support_blocker_code,
                      CASE
                        WHEN sb.living_area_sf IS NULL OR sb.living_area_sf <= 0 THEN NULL
                        WHEN COALESCE(
                          sb.certified_value,
                          sb.appraised_value,
                          sb.assessed_value,
                          sb.market_value,
                          sb.notice_value
                        ) IS NULL
                          OR COALESCE(
                            sb.certified_value,
                            sb.appraised_value,
                            sb.assessed_value,
                            sb.market_value,
                            sb.notice_value
                          ) <= 0
                          THEN NULL
                        ELSE COALESCE(
                          sb.certified_value,
                          sb.appraised_value,
                          sb.assessed_value,
                          sb.market_value,
                          sb.notice_value
                        ) / sb.living_area_sf
                      END AS subject_assessed_psf
                    FROM source_basis sb
                    """
                )
                cursor.execute(
                    """
                    CREATE UNIQUE INDEX idx_tmp_instant_quote_subject_refresh_key
                      ON tmp_instant_quote_subject_refresh(parcel_id, tax_year)
                    """
                )
                cursor.execute("ANALYZE tmp_instant_quote_subject_refresh")
                cursor.execute(
                    """
                    SELECT COUNT(*)::integer AS count
                    FROM tmp_instant_quote_subject_refresh
                    """
                )
                source_view_row_count = int((cursor.fetchone() or {}).get("count") or 0)
                cursor.execute(
                    """
                    INSERT INTO instant_quote_subject_cache (
                      parcel_id,
                      county_id,
                      tax_year,
                      account_number,
                      address,
                      situs_address,
                      situs_city,
                      situs_state,
                      situs_zip,
                      neighborhood_code,
                      school_district_name,
                      property_type_code,
                      property_class_code,
                      living_area_sf,
                      year_built,
                      assessed_value,
                      capped_value,
                      notice_value,
                      land_value,
                      improvement_value,
                      assessment_basis_value,
                      assessment_basis_source_value_type,
                      assessment_basis_source_year,
                      assessment_basis_source_reason,
                      assessment_basis_quality_code,
                      effective_tax_rate,
                      effective_tax_rate_source_method,
                      effective_tax_rate_basis_year,
                      effective_tax_rate_basis_reason,
                      effective_tax_rate_basis_fallback_applied,
                      effective_tax_rate_basis_status,
                      effective_tax_rate_basis_status_reason,
                      completeness_score,
                      public_summary_ready_flag,
                      homestead_flag,
                      over65_flag,
                      disabled_flag,
                      disabled_veteran_flag,
                      freeze_flag,
                      exemption_type_codes,
                      warning_codes,
                      size_bucket,
                      age_bucket,
                      support_blocker_code,
                      subject_assessed_psf,
                      refreshed_at
                    )
                    SELECT
                      parcel_id,
                      county_id,
                      tax_year,
                      account_number,
                      address,
                      situs_address,
                      situs_city,
                      situs_state,
                      situs_zip,
                      neighborhood_code,
                      school_district_name,
                      property_type_code,
                      property_class_code,
                      living_area_sf,
                      year_built,
                      assessed_value,
                      capped_value,
                      notice_value,
                      land_value,
                      improvement_value,
                      assessment_basis_value,
                      assessment_basis_source_value_type,
                      assessment_basis_source_year,
                      assessment_basis_source_reason,
                      assessment_basis_quality_code,
                      effective_tax_rate,
                      effective_tax_rate_source_method,
                      effective_tax_rate_basis_year,
                      effective_tax_rate_basis_reason,
                      effective_tax_rate_basis_fallback_applied,
                      effective_tax_rate_basis_status,
                      effective_tax_rate_basis_status_reason,
                      completeness_score,
                      public_summary_ready_flag,
                      homestead_flag,
                      over65_flag,
                      disabled_flag,
                      disabled_veteran_flag,
                      freeze_flag,
                      exemption_type_codes,
                      warning_codes,
                      size_bucket,
                      age_bucket,
                      support_blocker_code,
                      subject_assessed_psf,
                      now()
                    FROM tmp_instant_quote_subject_refresh
                    ON CONFLICT (parcel_id, tax_year) DO UPDATE
                    SET county_id = EXCLUDED.county_id,
                        account_number = EXCLUDED.account_number,
                        address = EXCLUDED.address,
                        situs_address = EXCLUDED.situs_address,
                        situs_city = EXCLUDED.situs_city,
                        situs_state = EXCLUDED.situs_state,
                        situs_zip = EXCLUDED.situs_zip,
                        neighborhood_code = EXCLUDED.neighborhood_code,
                        school_district_name = EXCLUDED.school_district_name,
                        property_type_code = EXCLUDED.property_type_code,
                        property_class_code = EXCLUDED.property_class_code,
                        living_area_sf = EXCLUDED.living_area_sf,
                        year_built = EXCLUDED.year_built,
                        assessed_value = EXCLUDED.assessed_value,
                        capped_value = EXCLUDED.capped_value,
                        notice_value = EXCLUDED.notice_value,
                        land_value = EXCLUDED.land_value,
                        improvement_value = EXCLUDED.improvement_value,
                        assessment_basis_value = EXCLUDED.assessment_basis_value,
                        assessment_basis_source_value_type = EXCLUDED.assessment_basis_source_value_type,
                        assessment_basis_source_year = EXCLUDED.assessment_basis_source_year,
                        assessment_basis_source_reason = EXCLUDED.assessment_basis_source_reason,
                        assessment_basis_quality_code = EXCLUDED.assessment_basis_quality_code,
                        effective_tax_rate = EXCLUDED.effective_tax_rate,
                        effective_tax_rate_source_method = EXCLUDED.effective_tax_rate_source_method,
                        effective_tax_rate_basis_year = EXCLUDED.effective_tax_rate_basis_year,
                        effective_tax_rate_basis_reason = EXCLUDED.effective_tax_rate_basis_reason,
                        effective_tax_rate_basis_fallback_applied = EXCLUDED.effective_tax_rate_basis_fallback_applied,
                        effective_tax_rate_basis_status = EXCLUDED.effective_tax_rate_basis_status,
                        effective_tax_rate_basis_status_reason = EXCLUDED.effective_tax_rate_basis_status_reason,
                        completeness_score = EXCLUDED.completeness_score,
                        public_summary_ready_flag = EXCLUDED.public_summary_ready_flag,
                        homestead_flag = EXCLUDED.homestead_flag,
                        over65_flag = EXCLUDED.over65_flag,
                        disabled_flag = EXCLUDED.disabled_flag,
                        disabled_veteran_flag = EXCLUDED.disabled_veteran_flag,
                        freeze_flag = EXCLUDED.freeze_flag,
                        exemption_type_codes = EXCLUDED.exemption_type_codes,
                        warning_codes = EXCLUDED.warning_codes,
                        size_bucket = EXCLUDED.size_bucket,
                        age_bucket = EXCLUDED.age_bucket,
                        support_blocker_code = EXCLUDED.support_blocker_code,
                        subject_assessed_psf = EXCLUDED.subject_assessed_psf,
                        refreshed_at = now()
                    WHERE (
                      instant_quote_subject_cache.county_id,
                      instant_quote_subject_cache.account_number,
                      instant_quote_subject_cache.address,
                      instant_quote_subject_cache.situs_address,
                      instant_quote_subject_cache.situs_city,
                      instant_quote_subject_cache.situs_state,
                      instant_quote_subject_cache.situs_zip,
                      instant_quote_subject_cache.neighborhood_code,
                      instant_quote_subject_cache.school_district_name,
                      instant_quote_subject_cache.property_type_code,
                      instant_quote_subject_cache.property_class_code,
                      instant_quote_subject_cache.living_area_sf,
                      instant_quote_subject_cache.year_built,
                      instant_quote_subject_cache.assessed_value,
                      instant_quote_subject_cache.capped_value,
                      instant_quote_subject_cache.notice_value,
                      instant_quote_subject_cache.land_value,
                      instant_quote_subject_cache.improvement_value,
                      instant_quote_subject_cache.assessment_basis_value,
                      instant_quote_subject_cache.assessment_basis_source_value_type,
                      instant_quote_subject_cache.assessment_basis_source_year,
                      instant_quote_subject_cache.assessment_basis_source_reason,
                      instant_quote_subject_cache.assessment_basis_quality_code,
                      instant_quote_subject_cache.effective_tax_rate,
                      instant_quote_subject_cache.effective_tax_rate_source_method,
                      instant_quote_subject_cache.effective_tax_rate_basis_year,
                      instant_quote_subject_cache.effective_tax_rate_basis_reason,
                      instant_quote_subject_cache.effective_tax_rate_basis_fallback_applied,
                      instant_quote_subject_cache.effective_tax_rate_basis_status,
                      instant_quote_subject_cache.effective_tax_rate_basis_status_reason,
                      instant_quote_subject_cache.completeness_score,
                      instant_quote_subject_cache.public_summary_ready_flag,
                      instant_quote_subject_cache.homestead_flag,
                      instant_quote_subject_cache.over65_flag,
                      instant_quote_subject_cache.disabled_flag,
                      instant_quote_subject_cache.disabled_veteran_flag,
                      instant_quote_subject_cache.freeze_flag,
                      instant_quote_subject_cache.exemption_type_codes,
                      instant_quote_subject_cache.warning_codes,
                      instant_quote_subject_cache.size_bucket,
                      instant_quote_subject_cache.age_bucket,
                      instant_quote_subject_cache.support_blocker_code,
                      instant_quote_subject_cache.subject_assessed_psf
                    ) IS DISTINCT FROM (
                      EXCLUDED.county_id,
                      EXCLUDED.account_number,
                      EXCLUDED.address,
                      EXCLUDED.situs_address,
                      EXCLUDED.situs_city,
                      EXCLUDED.situs_state,
                      EXCLUDED.situs_zip,
                      EXCLUDED.neighborhood_code,
                      EXCLUDED.school_district_name,
                      EXCLUDED.property_type_code,
                      EXCLUDED.property_class_code,
                      EXCLUDED.living_area_sf,
                      EXCLUDED.year_built,
                      EXCLUDED.assessed_value,
                      EXCLUDED.capped_value,
                      EXCLUDED.notice_value,
                      EXCLUDED.land_value,
                      EXCLUDED.improvement_value,
                      EXCLUDED.assessment_basis_value,
                      EXCLUDED.assessment_basis_source_value_type,
                      EXCLUDED.assessment_basis_source_year,
                      EXCLUDED.assessment_basis_source_reason,
                      EXCLUDED.assessment_basis_quality_code,
                      EXCLUDED.effective_tax_rate,
                      EXCLUDED.effective_tax_rate_source_method,
                      EXCLUDED.effective_tax_rate_basis_year,
                      EXCLUDED.effective_tax_rate_basis_reason,
                      EXCLUDED.effective_tax_rate_basis_fallback_applied,
                      EXCLUDED.effective_tax_rate_basis_status,
                      EXCLUDED.effective_tax_rate_basis_status_reason,
                      EXCLUDED.completeness_score,
                      EXCLUDED.public_summary_ready_flag,
                      EXCLUDED.homestead_flag,
                      EXCLUDED.over65_flag,
                      EXCLUDED.disabled_flag,
                      EXCLUDED.disabled_veteran_flag,
                      EXCLUDED.freeze_flag,
                      EXCLUDED.exemption_type_codes,
                      EXCLUDED.warning_codes,
                      EXCLUDED.size_bucket,
                      EXCLUDED.age_bucket,
                      EXCLUDED.support_blocker_code,
                      EXCLUDED.subject_assessed_psf
                    )
                    """
                )
                cursor.execute(
                    """
                    DELETE FROM instant_quote_subject_cache cache
                    WHERE (%s::text IS NULL OR cache.county_id = %s)
                      AND (%s::integer IS NULL OR cache.tax_year = %s)
                      AND NOT EXISTS (
                        SELECT 1
                        FROM tmp_instant_quote_subject_refresh source
                        WHERE source.parcel_id = cache.parcel_id
                          AND source.tax_year = cache.tax_year
                      )
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                cursor.execute(
                    """
                    SELECT
                      COUNT(*)::integer AS subject_cache_row_count,
                      COUNT(*) FILTER (WHERE support_blocker_code IS NULL)::integer AS supportable_subject_row_count
                    FROM instant_quote_subject_cache
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                counts = cursor.fetchone() or {}
                selected_tax_rate_basis = next(
                    (
                        selection.selection
                        for selection in basis_selections
                        if selection.county_id == county_id and selection.quote_tax_year == tax_year
                    ),
                    None,
                )
            connection.commit()
        subject_cache_row_count = int(counts.get("subject_cache_row_count") or 0)
        supportable_subject_row_count = int(counts.get("supportable_subject_row_count") or 0)
        return _SubjectCacheRefreshMetrics(
            source_view_row_count=int(source_view_row_count or 0),
            subject_cache_row_count=subject_cache_row_count,
            supportable_subject_row_count=supportable_subject_row_count,
            cache_view_row_delta=subject_cache_row_count - int(source_view_row_count or 0),
            selected_tax_rate_basis=selected_tax_rate_basis,
        )

    def _list_scoped_county_tax_years(self, cursor: object) -> list[dict[str, object]]:
        cursor.execute(
            """
            SELECT DISTINCT county_id, tax_year
            FROM tmp_instant_quote_subject_scope
            ORDER BY county_id, tax_year
            """
        )
        return list(cursor.fetchall())

    def _select_tax_rate_basis_for_scope(
        self,
        cursor: object,
        *,
        county_id: str,
        tax_year: int,
    ) -> _ScopedTaxRateBasisSelection:
        cursor.execute(
            """
            WITH scope_base AS (
              SELECT
                scope.parcel_id,
                scope.account_number,
                COALESCE(pc.property_type_code, p.property_type_code) AS property_type_code,
                COALESCE(pc.neighborhood_code, p.neighborhood_code) AS neighborhood_code,
                pi.living_area_sf,
                COALESCE(
                  pa.certified_value,
                  pa.appraised_value,
                  pa.assessed_value,
                  pa.market_value,
                  pa.notice_value
                ) AS assessment_basis_value
              FROM tmp_instant_quote_subject_scope scope
              JOIN parcels p
                ON p.parcel_id = scope.parcel_id
              LEFT JOIN property_characteristics pc
                ON pc.parcel_year_snapshot_id = scope.parcel_year_snapshot_id
              LEFT JOIN parcel_improvements pi
                ON pi.parcel_id = scope.parcel_id
               AND pi.tax_year = scope.tax_year
              LEFT JOIN parcel_assessments pa
                ON pa.parcel_id = scope.parcel_id
               AND pa.tax_year = scope.tax_year
              WHERE scope.county_id = %s
                AND scope.tax_year = %s
            ),
            quoteable_cohort AS (
              SELECT *
              FROM scope_base
              WHERE property_type_code = 'sfr'
                AND COALESCE(living_area_sf, 0) > 0
                AND COALESCE(assessment_basis_value, 0) > 0
                AND neighborhood_code IS NOT NULL
                AND btrim(neighborhood_code) <> ''
            ),
            candidate_basis_years AS (
              SELECT %s::integer AS basis_year
              UNION
              SELECT DISTINCT etr.tax_year AS basis_year
              FROM effective_tax_rates etr
              JOIN quoteable_cohort base
                ON base.parcel_id = etr.parcel_id
              WHERE etr.tax_year < %s
            ),
            basis_assignment_requirements AS (
              SELECT
                candidate.basis_year,
                COALESCE(BOOL_OR(tu.unit_type_code = 'county'), false) AS requires_county_assignment,
                COALESCE(BOOL_OR(tu.unit_type_code = 'school'), false) AS requires_school_assignment
              FROM candidate_basis_years candidate
              LEFT JOIN parcel_taxing_units ptu
                ON ptu.tax_year = candidate.basis_year
              LEFT JOIN parcels basis_parcel
                ON basis_parcel.parcel_id = ptu.parcel_id
               AND basis_parcel.county_id = %s
              LEFT JOIN taxing_units tu
                ON tu.taxing_unit_id = ptu.taxing_unit_id
               AND tu.unit_type_code IN ('county', 'school')
              GROUP BY candidate.basis_year
            ),
            basis_assignments AS (
              SELECT
                ptu.parcel_id,
                ptu.tax_year,
                COUNT(*) FILTER (WHERE tu.unit_type_code = 'county')::integer AS county_assignment_count,
                COUNT(*) FILTER (WHERE tu.unit_type_code = 'school')::integer AS school_assignment_count
              FROM parcel_taxing_units ptu
              JOIN taxing_units tu
                ON tu.taxing_unit_id = ptu.taxing_unit_id
              JOIN candidate_basis_years candidate
                ON candidate.basis_year = ptu.tax_year
              JOIN quoteable_cohort cohort
                ON cohort.parcel_id = ptu.parcel_id
              GROUP BY ptu.parcel_id, ptu.tax_year
            ),
            basis_parcel_continuity AS (
              SELECT
                pys.parcel_id,
                pys.tax_year
              FROM parcel_year_snapshots pys
              JOIN candidate_basis_years candidate
                ON candidate.basis_year = pys.tax_year
              JOIN quoteable_cohort cohort
                ON cohort.parcel_id = pys.parcel_id
              WHERE pys.is_current = true
            ),
            basis_account_diagnostic AS (
              SELECT
                cohort.parcel_id,
                candidate.basis_year,
                BOOL_OR(snapshot.account_number = cohort.account_number) AS account_number_match
              FROM quoteable_cohort cohort
              CROSS JOIN candidate_basis_years candidate
              LEFT JOIN parcel_year_snapshots snapshot
                ON snapshot.county_id = %s
               AND snapshot.tax_year = candidate.basis_year
               AND snapshot.is_current = true
               AND snapshot.account_number = cohort.account_number
              GROUP BY cohort.parcel_id, candidate.basis_year
            )
            SELECT
              candidate_basis_years.basis_year AS tax_year,
              COUNT(*)::integer AS quoteable_subject_row_count,
              COUNT(*) FILTER (
                WHERE COALESCE(etr.effective_tax_rate, 0) > 0
              )::integer AS supportable_subject_row_count
              ,
              COUNT(*) FILTER (
                WHERE (
                  NOT COALESCE(requirements.requires_county_assignment, false)
                  OR COALESCE(assignments.county_assignment_count, 0) > 0
                )
                  AND (
                    NOT COALESCE(requirements.requires_school_assignment, false)
                    OR COALESCE(assignments.school_assignment_count, 0) > 0
                  )
              )::integer AS assignment_complete_row_count,
              COUNT(*) FILTER (
                WHERE candidate_basis_years.basis_year = %s
                   OR continuity.parcel_id IS NOT NULL
              )::integer AS continuity_parcel_match_row_count,
              COUNT(*) FILTER (
                WHERE candidate_basis_years.basis_year < %s
                  AND continuity.parcel_id IS NULL
                  AND COALESCE(account_diagnostic.account_number_match, false)
              )::integer AS continuity_account_number_match_row_count
            FROM quoteable_cohort
            CROSS JOIN candidate_basis_years
            LEFT JOIN effective_tax_rates etr
              ON etr.parcel_id = quoteable_cohort.parcel_id
             AND etr.tax_year = candidate_basis_years.basis_year
            LEFT JOIN basis_assignments assignments
              ON assignments.parcel_id = quoteable_cohort.parcel_id
             AND assignments.tax_year = candidate_basis_years.basis_year
            LEFT JOIN basis_assignment_requirements requirements
              ON requirements.basis_year = candidate_basis_years.basis_year
            LEFT JOIN basis_parcel_continuity continuity
              ON continuity.parcel_id = quoteable_cohort.parcel_id
             AND continuity.tax_year = candidate_basis_years.basis_year
            LEFT JOIN basis_account_diagnostic account_diagnostic
              ON account_diagnostic.parcel_id = quoteable_cohort.parcel_id
             AND account_diagnostic.basis_year = candidate_basis_years.basis_year
            GROUP BY candidate_basis_years.basis_year
            ORDER BY candidate_basis_years.basis_year DESC
            """,
            (
                county_id,
                tax_year,
                tax_year,
                tax_year,
                county_id,
                county_id,
                tax_year,
                tax_year,
            ),
        )
        candidates = [
            TaxRateBasisCandidate(
                tax_year=int(row["tax_year"]),
                quoteable_subject_row_count=int(row["quoteable_subject_row_count"] or 0),
                supportable_subject_row_count=int(row["supportable_subject_row_count"] or 0),
                assignment_complete_row_count=int(row["assignment_complete_row_count"] or 0),
                continuity_parcel_match_row_count=int(
                    row["continuity_parcel_match_row_count"] or 0
                ),
                continuity_account_number_match_row_count=int(
                    row["continuity_account_number_match_row_count"] or 0
                ),
            )
            for row in cursor.fetchall()
        ]
        selection = choose_tax_rate_basis(
            quote_tax_year=tax_year,
            candidates=candidates,
        )
        same_year_adoption_status = self._load_same_year_tax_rate_adoption_status(
            cursor,
            county_id=county_id,
            tax_year=selection.basis_tax_year,
            quote_tax_year=tax_year,
        )
        return _ScopedTaxRateBasisSelection(
            county_id=county_id,
            quote_tax_year=tax_year,
            selection=assign_tax_rate_basis_status(
                selection=selection,
                same_year_adoption_status=same_year_adoption_status,
            ),
        )

    def _load_same_year_tax_rate_adoption_status(
        self,
        cursor: object,
        *,
        county_id: str,
        tax_year: int | None,
        quote_tax_year: int,
    ) -> SameYearTaxRateAdoptionStatus | None:
        if tax_year is None or tax_year != quote_tax_year:
            return None
        cursor.execute(
            """
            SELECT
              adoption_status,
              adoption_status_reason,
              status_source,
              source_note
            FROM instant_quote_tax_rate_adoption_statuses
            WHERE county_id = %s
              AND tax_year = %s
            """,
            (county_id, tax_year),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return SameYearTaxRateAdoptionStatus(
            county_id=county_id,
            tax_year=tax_year,
            adoption_status=str(row["adoption_status"]),
            adoption_status_reason=(
                None
                if row.get("adoption_status_reason") is None
                else str(row["adoption_status_reason"])
            ),
            status_source=(
                None if row.get("status_source") is None else str(row["status_source"])
            ),
            source_note=(
                None if row.get("source_note") is None else str(row["source_note"])
            ),
        )

    def _refresh_neighborhood_stats(
        self,
        *,
        county_id: str | None,
        tax_year: int | None,
    ) -> _StatsRefreshMetrics:
        with get_connection() as connection:
            self._prepare_refresh_session(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM instant_quote_neighborhood_stats
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                cursor.execute(
                    f"""
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
                      trim_method_code,
                      last_refresh_at
                    )
                    WITH eligible AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        subject_assessed_psf
                      FROM instant_quote_subject_cache
                      WHERE (%s::text IS NULL OR county_id = %s)
                        AND (%s::integer IS NULL OR tax_year = %s)
                        AND property_type_code = 'sfr'
                        AND support_blocker_code IS NULL
                        AND neighborhood_code IS NOT NULL
                        AND subject_assessed_psf IS NOT NULL
                        AND subject_assessed_psf > 0
                    ),
                    bounds AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        COUNT(*)::integer AS parcel_count,
                        percentile_cont(0.05) WITHIN GROUP (ORDER BY subject_assessed_psf) AS lower_bound,
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY subject_assessed_psf) AS upper_bound
                      FROM eligible
                      GROUP BY county_id, tax_year, neighborhood_code, property_type_code
                    ),
                    trimmed AS (
                      SELECT
                        eligible.county_id,
                        eligible.tax_year,
                        eligible.neighborhood_code,
                        eligible.property_type_code,
                        eligible.subject_assessed_psf,
                        bounds.parcel_count
                      FROM eligible
                      JOIN bounds
                        ON bounds.county_id = eligible.county_id
                       AND bounds.tax_year = eligible.tax_year
                       AND bounds.neighborhood_code = eligible.neighborhood_code
                       AND bounds.property_type_code = eligible.property_type_code
                      WHERE eligible.subject_assessed_psf BETWEEN bounds.lower_bound AND bounds.upper_bound
                    ),
                    aggregated AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        MAX(parcel_count)::integer AS parcel_count,
                        COUNT(*)::integer AS trimmed_parcel_count,
                        GREATEST(MAX(parcel_count)::integer - COUNT(*)::integer, 0)::integer AS excluded_parcel_count,
                        percentile_cont(0.10) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p10_assessed_psf,
                        percentile_cont(0.25) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p25_assessed_psf,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p50_assessed_psf,
                        percentile_cont(0.75) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p75_assessed_psf,
                        percentile_cont(0.90) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p90_assessed_psf,
                        AVG(subject_assessed_psf) AS mean_assessed_psf,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY subject_assessed_psf) AS median_assessed_psf,
                        stddev_pop(subject_assessed_psf) AS stddev_assessed_psf
                      FROM trimmed
                      GROUP BY county_id, tax_year, neighborhood_code, property_type_code
                    )
                    SELECT
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
                      CASE
                        WHEN mean_assessed_psf IS NULL OR mean_assessed_psf = 0 THEN NULL
                        ELSE stddev_assessed_psf / mean_assessed_psf
                      END AS coefficient_of_variation,
                      CASE
                        WHEN parcel_count >= {NEIGHBORHOOD_MIN_COUNT} THEN 'strong'
                        WHEN parcel_count >= {SEGMENT_MIN_COUNT} THEN 'medium'
                        ELSE 'thin'
                      END AS support_level,
                      (parcel_count >= {NEIGHBORHOOD_MIN_COUNT}) AS support_threshold_met,
                      'trim_p05_p95',
                      now()
                    FROM aggregated
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                total_row_count = int(cursor.rowcount or 0)
                cursor.execute(
                    """
                    SELECT COUNT(*)::integer AS count
                    FROM instant_quote_neighborhood_stats
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                row = cursor.fetchone() or {}
            connection.commit()
        return _StatsRefreshMetrics(
            total_row_count=total_row_count,
            supported_row_count=int(row.get("count") or 0),
        )

    def _refresh_segment_stats(
        self,
        *,
        county_id: str | None,
        tax_year: int | None,
    ) -> _StatsRefreshMetrics:
        with get_connection() as connection:
            self._prepare_refresh_session(connection)
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
                    f"""
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
                      trim_method_code,
                      last_refresh_at
                    )
                    WITH eligible AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        size_bucket,
                        age_bucket,
                        subject_assessed_psf
                      FROM instant_quote_subject_cache
                      WHERE (%s::text IS NULL OR county_id = %s)
                        AND (%s::integer IS NULL OR tax_year = %s)
                        AND property_type_code = 'sfr'
                        AND support_blocker_code IS NULL
                        AND neighborhood_code IS NOT NULL
                        AND size_bucket IS NOT NULL
                        AND age_bucket IS NOT NULL
                        AND subject_assessed_psf IS NOT NULL
                        AND subject_assessed_psf > 0
                    ),
                    bounds AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        size_bucket,
                        age_bucket,
                        COUNT(*)::integer AS parcel_count,
                        percentile_cont(0.05) WITHIN GROUP (ORDER BY subject_assessed_psf) AS lower_bound,
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY subject_assessed_psf) AS upper_bound
                      FROM eligible
                      GROUP BY county_id, tax_year, neighborhood_code, property_type_code, size_bucket, age_bucket
                    ),
                    trimmed_preferred AS (
                      SELECT
                        eligible.county_id,
                        eligible.tax_year,
                        eligible.neighborhood_code,
                        eligible.property_type_code,
                        eligible.size_bucket,
                        eligible.age_bucket,
                        eligible.subject_assessed_psf,
                        bounds.parcel_count,
                        FALSE AS used_trim_fallback
                      FROM eligible
                      JOIN bounds
                        ON bounds.county_id = eligible.county_id
                       AND bounds.tax_year = eligible.tax_year
                       AND bounds.neighborhood_code = eligible.neighborhood_code
                       AND bounds.property_type_code = eligible.property_type_code
                       AND bounds.size_bucket = eligible.size_bucket
                       AND bounds.age_bucket = eligible.age_bucket
                      WHERE bounds.parcel_count < {MIN_TRIM_GROUP_SIZE}
                         OR eligible.subject_assessed_psf BETWEEN bounds.lower_bound AND bounds.upper_bound
                    ),
                    trimmed AS (
                      SELECT *
                      FROM trimmed_preferred
                    ),
                    aggregated AS (
                      SELECT
                        county_id,
                        tax_year,
                        neighborhood_code,
                        property_type_code,
                        size_bucket,
                        age_bucket,
                        MAX(parcel_count)::integer AS parcel_count,
                        COUNT(*)::integer AS trimmed_parcel_count,
                        GREATEST(MAX(parcel_count)::integer - COUNT(*)::integer, 0)::integer AS excluded_parcel_count,
                        percentile_cont(0.10) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p10_assessed_psf,
                        percentile_cont(0.25) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p25_assessed_psf,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p50_assessed_psf,
                        percentile_cont(0.75) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p75_assessed_psf,
                        percentile_cont(0.90) WITHIN GROUP (ORDER BY subject_assessed_psf) AS p90_assessed_psf,
                        AVG(subject_assessed_psf) AS mean_assessed_psf,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY subject_assessed_psf) AS median_assessed_psf,
                        stddev_pop(subject_assessed_psf) AS stddev_assessed_psf,
                        BOOL_OR(used_trim_fallback) AS used_trim_fallback,
                        MAX(parcel_count)::integer < {MIN_TRIM_GROUP_SIZE} AS preserve_small_group
                      FROM trimmed
                      GROUP BY county_id, tax_year, neighborhood_code, property_type_code, size_bucket, age_bucket
                    )
                    SELECT
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
                      CASE
                        WHEN mean_assessed_psf IS NULL OR mean_assessed_psf = 0 THEN NULL
                        ELSE stddev_assessed_psf / mean_assessed_psf
                      END AS coefficient_of_variation,
                      CASE
                        WHEN parcel_count >= {STRONG_SEGMENT_COUNT} THEN 'strong'
                        WHEN parcel_count >= {SEGMENT_MIN_COUNT} THEN 'medium'
                        ELSE 'thin'
                      END AS support_level,
                      (parcel_count >= {SEGMENT_MIN_COUNT}) AS support_threshold_met,
                      CASE
                        WHEN preserve_small_group OR used_trim_fallback THEN '{TRIM_METHOD_P05_P95_PRESERVE_ALL_LT3}'
                        ELSE '{TRIM_METHOD_P05_P95}'
                      END,
                      now()
                    FROM aggregated
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                total_row_count = int(cursor.rowcount or 0)
                cursor.execute(
                    """
                    SELECT COUNT(*)::integer AS count
                    FROM instant_quote_segment_stats
                    WHERE (%s::text IS NULL OR county_id = %s)
                      AND (%s::integer IS NULL OR tax_year = %s)
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, county_id, tax_year, tax_year),
                )
                row = cursor.fetchone() or {}
            connection.commit()
        return _StatsRefreshMetrics(
            total_row_count=total_row_count,
            supported_row_count=int(row.get("count") or 0),
        )

    def _start_refresh_run(
        self,
        *,
        county_id: str | None,
        tax_year: int | None,
        refresh_started_at: datetime,
    ) -> str | None:
        if county_id is None or tax_year is None:
            return None
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO instant_quote_refresh_runs (
                      county_id,
                      tax_year,
                      refresh_status,
                      refresh_started_at
                    )
                    VALUES (%s, %s, 'running', %s)
                    RETURNING instant_quote_refresh_run_id
                    """,
                    (county_id, tax_year, refresh_started_at),
                )
                row = cursor.fetchone()
            connection.commit()
        return None if row is None else str(row["instant_quote_refresh_run_id"])

    def _complete_refresh_run(
        self,
        *,
        refresh_run_id: str | None,
        summary: InstantQuoteRefreshSummary,
        source_view_row_count: int,
    ) -> None:
        if refresh_run_id is None:
            return
        warning_codes: list[str] = []
        if summary.cache_view_row_delta != 0:
            warning_codes.append("subject_cache_row_mismatch")
        if summary.supported_neighborhood_stats_count == 0:
            warning_codes.append("no_supported_neighborhood_stats")
        if summary.supportable_subject_row_count == 0:
            warning_codes.append("no_supportable_subjects")
        if summary.tax_rate_basis_fallback_applied:
            warning_codes.append("tax_rate_basis_fallback_applied")
        if summary.tax_rate_basis_year is None:
            warning_codes.append("no_usable_tax_rate_basis")
        if (
            summary.tax_rate_basis_status
            == TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES
        ):
            warning_codes.append("tax_rate_basis_current_year_unofficial_or_proposed")
        warning_codes.extend(summary.tax_rate_basis_warning_codes)
        warning_codes = list(dict.fromkeys(warning_codes))

        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE instant_quote_refresh_runs
                    SET refresh_status = 'completed',
                        refresh_finished_at = now(),
                        cache_rebuild_duration_ms = %s,
                        neighborhood_stats_refresh_duration_ms = %s,
                        segment_stats_refresh_duration_ms = %s,
                        total_refresh_duration_ms = %s,
                        source_view_row_count = %s,
                        subject_cache_row_count = %s,
                        supportable_subject_row_count = %s,
                        neighborhood_stats_row_count = %s,
                        supported_neighborhood_stats_row_count = %s,
                        segment_stats_row_count = %s,
                        supported_segment_stats_row_count = %s,
                        cache_view_row_delta = %s,
                        tax_rate_basis_year = %s,
                        tax_rate_basis_reason = %s,
                        tax_rate_basis_fallback_applied = %s,
                        tax_rate_basis_status = %s,
                        tax_rate_basis_status_reason = %s,
                        requested_tax_rate_supportable_subject_row_count = %s,
                        tax_rate_basis_supportable_subject_row_count = %s,
                        tax_rate_quoteable_subject_row_count = %s,
                        requested_tax_rate_effective_tax_rate_coverage_ratio = %s,
                        requested_tax_rate_assignment_coverage_ratio = %s,
                        tax_rate_basis_effective_tax_rate_coverage_ratio = %s,
                        tax_rate_basis_assignment_coverage_ratio = %s,
                        tax_rate_basis_continuity_parcel_match_row_count = %s,
                        tax_rate_basis_continuity_parcel_gap_row_count = %s,
                        tax_rate_basis_continuity_parcel_match_ratio = %s,
                        tax_rate_basis_continuity_account_number_match_row_count = %s,
                        tax_rate_basis_warning_codes = %s,
                        warning_codes = %s
                    WHERE instant_quote_refresh_run_id = %s::uuid
                    """,
                    (
                        summary.cache_rebuild_duration_ms,
                        summary.neighborhood_stats_refresh_duration_ms,
                        summary.segment_stats_refresh_duration_ms,
                        summary.total_refresh_duration_ms,
                        source_view_row_count,
                        summary.subject_row_count,
                        summary.supportable_subject_row_count,
                        summary.neighborhood_stats_count,
                        summary.supported_neighborhood_stats_count,
                        summary.segment_stats_count,
                        summary.supported_segment_stats_count,
                        summary.cache_view_row_delta,
                        summary.tax_rate_basis_year,
                        summary.tax_rate_basis_reason,
                        summary.tax_rate_basis_fallback_applied,
                        summary.tax_rate_basis_status,
                        summary.tax_rate_basis_status_reason,
                        summary.requested_tax_rate_supportable_subject_row_count,
                        summary.tax_rate_basis_supportable_subject_row_count,
                        summary.tax_rate_quoteable_subject_row_count,
                        summary.requested_tax_rate_effective_tax_rate_coverage_ratio,
                        summary.requested_tax_rate_assignment_coverage_ratio,
                        summary.tax_rate_basis_effective_tax_rate_coverage_ratio,
                        summary.tax_rate_basis_assignment_coverage_ratio,
                        summary.tax_rate_basis_continuity_parcel_match_row_count,
                        summary.tax_rate_basis_continuity_parcel_gap_row_count,
                        summary.tax_rate_basis_continuity_parcel_match_ratio,
                        summary.tax_rate_basis_continuity_account_number_match_row_count,
                        list(summary.tax_rate_basis_warning_codes),
                        warning_codes,
                        refresh_run_id,
                    ),
                )
            connection.commit()

    def _fail_refresh_run(self, *, refresh_run_id: str | None, error_message: str) -> None:
        if refresh_run_id is None:
            return
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE instant_quote_refresh_runs
                    SET refresh_status = 'failed',
                        refresh_finished_at = now(),
                        error_message = %s
                    WHERE instant_quote_refresh_run_id = %s::uuid
                    """,
                    (error_message[:4000], refresh_run_id),
                )
            connection.commit()


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
        with get_connection() as connection:
            self._prepare_request_session(connection)
            subject_row = self._fetch_subject_row(
                connection=connection,
                county_id=county_id,
                requested_tax_year=tax_year,
                account_number=account_number,
            )
            if subject_row is None:
                raise LookupError(
                    f"Instant quote not found for {county_id}/{tax_year}/{account_number}."
                )

            response, telemetry = self._build_response(
                connection=connection,
                request_id=request_id,
                subject_row=subject_row,
                requested_tax_year=tax_year,
            )
        latency_ms = int((perf_counter() - started_at) * 1000)
        telemetry["latency_ms"] = latency_ms
        self._emit_logs(response=response, telemetry=telemetry)
        self._enqueue_request_log_persistence(response=response, telemetry=telemetry)
        return response

    def _prepare_request_session(self, connection: object) -> None:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")

    def _fetch_subject_row(
        self,
        *,
        connection: object | None = None,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        if connection is None:
            with get_connection() as managed_connection:
                self._prepare_request_session(managed_connection)
                return self._fetch_subject_row(
                    connection=managed_connection,
                    county_id=county_id,
                    requested_tax_year=requested_tax_year,
                    account_number=account_number,
                )
        ready_row = self._fetch_subject_row_with_ready_stats(
            connection=connection,
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            account_number=account_number,
        )
        if ready_row is not None:
            return ready_row
        return self._fetch_latest_subject_row(
            connection=connection,
            county_id=county_id,
            requested_tax_year=requested_tax_year,
            account_number=account_number,
        )

    def _fetch_subject_row_with_ready_stats(
        self,
        *,
        connection: object,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM instant_quote_subject_cache subject
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
        connection: object,
        county_id: str,
        requested_tax_year: int,
        account_number: str,
    ) -> dict[str, Any] | None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM instant_quote_subject_cache
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
        connection: object,
        county_id: str,
        tax_year: int,
        neighborhood_code: str,
    ) -> InstantQuoteStatsRow | None:
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
        connection: object,
        county_id: str,
        tax_year: int,
        neighborhood_code: str,
        size_bucket: str | None,
        age_bucket: str | None,
    ) -> InstantQuoteStatsRow | None:
        if size_bucket is None or age_bucket is None:
            return None
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
            self._prepare_request_session(connection)
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
        connection: object,
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
            **extract_assessment_basis_contract(subject_row),
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
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason=str(blocker_code),
                        reduction_estimate=None,
                        savings_estimate=None,
                        confidence_score=None,
                        tax_limitation_outcome=None,
                    ),
                }
            )
            return response, telemetry

        neighborhood_code = str(subject_row["neighborhood_code"])
        county_id = str(subject_row["county_id"])
        neighborhood_stats = self._fetch_neighborhood_stats(
            connection=connection,
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
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason=unsupported_reason,
                        reduction_estimate=None,
                        savings_estimate=None,
                        confidence_score=None,
                        tax_limitation_outcome=None,
                    ),
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
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason="thin_market_support",
                        reduction_estimate=None,
                        savings_estimate=None,
                        confidence_score=None,
                        tax_limitation_outcome=None,
                    ),
                }
            )
            return response, telemetry

        segment_stats = self._fetch_segment_stats(
            connection=connection,
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
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason="thin_market_support",
                        reduction_estimate=None,
                        savings_estimate=None,
                        confidence_score=None,
                        tax_limitation_outcome=None,
                    ),
                }
            )
            return response, telemetry

        subject_basis_value = _as_float(subject_row.get("assessment_basis_value")) or 0.0
        living_area_sf = _as_float(subject_row.get("living_area_sf")) or 0.0
        subject_assessed_psf = _as_float(subject_row.get("subject_assessed_psf")) or 0.0
        tax_rate = _as_float(subject_row.get("effective_tax_rate")) or 0.0
        segment_component = (
            ((segment_stats.p50_assessed_psf or 0.0) if segment_stats else 0.0) * segment_weight
        )
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
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason="thin_market_support",
                        reduction_estimate=None,
                        savings_estimate=None,
                        confidence_score=None,
                        tax_limitation_outcome=None,
                    ),
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
                    "tax_rate_basis_year": subject_row.get("effective_tax_rate_basis_year"),
                    "tax_rate_basis_reason": subject_row.get("effective_tax_rate_basis_reason"),
                    "tax_rate_basis_status": subject_row.get("effective_tax_rate_basis_status"),
                    "tax_rate_basis_status_reason": subject_row.get(
                        "effective_tax_rate_basis_status_reason"
                    ),
                    "subject_percentile": subject_percentile,
                    "target_psf": target_psf,
                    "subject_assessed_psf": subject_assessed_psf,
                    "target_psf_segment_component": segment_component,
                    "target_psf_neighborhood_component": neighborhood_component,
                    "equity_value_estimate": equity_value_estimate,
                    "reduction_estimate_raw": reduction_estimate,
                    "savings_estimate_raw": savings_estimate,
                    "explanation_payload": response.explanation.model_dump(),
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason=unsupported_reason,
                        reduction_estimate=reduction_estimate,
                        savings_estimate=savings_estimate,
                        confidence_score=confidence_score,
                        tax_limitation_outcome=tax_limitation_outcome,
                    ),
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
                    "tax_rate_basis_year": subject_row.get("effective_tax_rate_basis_year"),
                    "tax_rate_basis_reason": subject_row.get("effective_tax_rate_basis_reason"),
                    "tax_rate_basis_status": subject_row.get("effective_tax_rate_basis_status"),
                    "tax_rate_basis_status_reason": subject_row.get(
                        "effective_tax_rate_basis_status_reason"
                    ),
                    "subject_percentile": subject_percentile,
                    "target_psf": target_psf,
                    "subject_assessed_psf": subject_assessed_psf,
                    "target_psf_segment_component": segment_component,
                    "target_psf_neighborhood_component": neighborhood_component,
                    "equity_value_estimate": equity_value_estimate,
                    "reduction_estimate_raw": reduction_estimate,
                    "savings_estimate_raw": savings_estimate,
                    "explanation_payload": response.explanation.model_dump(),
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason="tax_limitation_uncertain",
                        reduction_estimate=reduction_estimate,
                        savings_estimate=savings_estimate,
                        confidence_score=confidence_score,
                        tax_limitation_outcome=tax_limitation_outcome,
                    ),
                }
            )
            return response, telemetry

        if is_implausible_savings_outlier(
            savings_estimate=savings_estimate,
            assessment_basis_value=subject_basis_value,
        ):
            response = self._build_unsupported_response(
                subject_row=subject_row,
                requested_tax_year=requested_tax_year,
                basis_code=basis_code,
                unsupported_reason="implausible_savings_outlier",
                summary="We found the parcel, but the projected savings is too large for a safe public quote.",
                bullets=[
                    "The estimate exceeded the public-safe savings threshold relative to the parcel's assessed basis.",
                    REFINED_REVIEW_CTA,
                ],
                next_step_cta=REFINED_REVIEW_CTA,
            )
            telemetry.update(
                {
                    "basis_code": response.basis_code,
                    "supported": False,
                    "unsupported_reason": "implausible_savings_outlier",
                    "fallback_tier": fallback_tier,
                    "confidence_score": confidence_score,
                    "confidence_label": confidence_label,
                    "neighborhood_sample_count": neighborhood_stats.parcel_count,
                    "segment_sample_count": segment_stats.parcel_count if segment_stats else 0,
                    "tax_rate_source_method": subject_row.get("effective_tax_rate_source_method"),
                    "tax_rate_basis_year": subject_row.get("effective_tax_rate_basis_year"),
                    "tax_rate_basis_reason": subject_row.get("effective_tax_rate_basis_reason"),
                    "tax_rate_basis_status": subject_row.get("effective_tax_rate_basis_status"),
                    "tax_rate_basis_status_reason": subject_row.get(
                        "effective_tax_rate_basis_status_reason"
                    ),
                    "subject_percentile": subject_percentile,
                    "target_psf": target_psf,
                    "subject_assessed_psf": subject_assessed_psf,
                    "target_psf_segment_component": segment_component,
                    "target_psf_neighborhood_component": neighborhood_component,
                    "equity_value_estimate": equity_value_estimate,
                    "reduction_estimate_raw": reduction_estimate,
                    "savings_estimate_raw": savings_estimate,
                    "explanation_payload": response.explanation.model_dump(),
                    **build_internal_classification_payload(
                        subject_row=subject_row,
                        unsupported_reason="implausible_savings_outlier",
                        reduction_estimate=reduction_estimate,
                        savings_estimate=savings_estimate,
                        confidence_score=confidence_score,
                        tax_limitation_outcome=tax_limitation_outcome,
                    ),
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
                *(
                    [
                        "Current-year assessed basis was unavailable, so this estimate uses the prior year's assessed basis as a fallback."
                    ]
                    if "prior_year_assessment_basis_fallback"
                    in {str(code) for code in subject_row.get("warning_codes") or []}
                    else []
                ),
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
                "tax_rate_basis_year": subject_row.get("effective_tax_rate_basis_year"),
                "tax_rate_basis_reason": subject_row.get("effective_tax_rate_basis_reason"),
                "tax_rate_basis_status": subject_row.get("effective_tax_rate_basis_status"),
                "tax_rate_basis_status_reason": subject_row.get(
                    "effective_tax_rate_basis_status_reason"
                ),
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
                **build_internal_classification_payload(
                    subject_row=subject_row,
                    unsupported_reason=None,
                    reduction_estimate=reduction_estimate,
                    savings_estimate=savings_estimate,
                    confidence_score=confidence_score,
                    tax_limitation_outcome=tax_limitation_outcome,
                ),
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
                "opportunity_vs_savings_state": telemetry.get("opportunity_vs_savings_state"),
                "dominant_warning_action_class": telemetry.get("dominant_warning_action_class"),
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
                          warning_action_classes,
                          dominant_warning_action_class,
                          warning_taxonomy_json,
                          opportunity_vs_savings_state,
                          product_state_reason_code,
                          unsupported_reason,
                          explanation_payload,
                          latency_ms
                        )
                        VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                            telemetry.get("warning_action_classes") or [],
                            telemetry.get("dominant_warning_action_class"),
                            Jsonb(telemetry.get("warning_taxonomy_json") or []),
                            telemetry.get("opportunity_vs_savings_state"),
                            telemetry.get("product_state_reason_code"),
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
    if (
        neighborhood_stats is None
        or neighborhood_stats.parcel_count < NEIGHBORHOOD_MIN_COUNT
        or neighborhood_stats.p50_assessed_psf is None
    ):
        return ("unsupported", 0.0, 0.0, "assessment_basis_unsupported")
    if (
        segment_stats is None
        or segment_stats.parcel_count < SEGMENT_MIN_COUNT
        or segment_stats.p50_assessed_psf is None
    ):
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
        score -= 20.0
    elif segment_stats is not None and segment_stats.parcel_count < STRONG_SEGMENT_COUNT:
        score -= 10.0

    if neighborhood_stats.parcel_count < 30:
        score -= 8.0
    if segment_stats is not None and segment_stats.coefficient_of_variation is not None:
        if segment_stats.coefficient_of_variation > 0.40:
            score -= 10.0
        elif segment_stats.coefficient_of_variation > 0.25:
            score -= 5.0
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
    warning_codes = {str(code) for code in subject_row.get("warning_codes") or []}
    if "prior_year_assessment_basis_fallback" in warning_codes:
        score -= 8.0

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


def _warning_taxonomy_entry(warning_code: str) -> dict[str, str | None] | None:
    rule = WARNING_TAXONOMY_RULES.get(warning_code)
    if rule is None:
        return None
    return {
        "warning_code": warning_code,
        "warning_action_class": rule["warning_action_class"],
        "warning_severity": rule["warning_severity"],
        "affected_subsystem": rule["affected_subsystem"],
        "affected_unit_mask": rule["affected_unit_mask"],
        "public_disclosure_code": rule["public_disclosure_code"],
        "qa_note": rule["qa_note"],
    }


def build_internal_warning_taxonomy(
    *,
    subject_row: dict[str, Any],
    unsupported_reason: str | None = None,
) -> list[dict[str, str | None]]:
    ordered_codes: list[str] = []
    blocker_code = str(subject_row.get("support_blocker_code") or "").strip()
    if blocker_code:
        ordered_codes.append(blocker_code)

    for warning_code in subject_row.get("warning_codes") or []:
        code = str(warning_code or "").strip()
        if code:
            ordered_codes.append(code)

    if bool(subject_row.get("effective_tax_rate_basis_fallback_applied")):
        ordered_codes.append("tax_rate_basis_fallback_applied")
    if (
        str(subject_row.get("effective_tax_rate_basis_status") or "")
        == TAX_RATE_BASIS_STATUS_CURRENT_YEAR_UNOFFICIAL_OR_PROPOSED_RATES
    ):
        ordered_codes.append("tax_rate_basis_current_year_unofficial_or_proposed")
    if unsupported_reason:
        ordered_codes.append(unsupported_reason)

    entries: list[dict[str, str | None]] = []
    seen_codes: set[str] = set()
    for warning_code in ordered_codes:
        if warning_code in seen_codes:
            continue
        seen_codes.add(warning_code)
        entry = _warning_taxonomy_entry(warning_code)
        if entry is not None:
            entries.append(entry)
    return entries


def summarize_warning_action_classes(
    warning_taxonomy: list[dict[str, str | None]],
) -> tuple[list[str], str | None]:
    action_classes: list[str] = []
    for action_class in WARNING_ACTION_CLASS_ORDER:
        if any(entry.get("warning_action_class") == action_class for entry in warning_taxonomy):
            action_classes.append(action_class)
    dominant_action_class = action_classes[0] if action_classes else None
    return action_classes, dominant_action_class


def classify_opportunity_vs_savings_state(
    *,
    subject_row: dict[str, Any],
    unsupported_reason: str | None,
    reduction_estimate: float | None,
    savings_estimate: float | None,
    confidence_score: float | None,
    tax_limitation_outcome: TaxLimitationOutcome | None,
) -> tuple[OpportunityVsSavingsState, str]:
    blocker_code = str(subject_row.get("support_blocker_code") or "").strip()
    if blocker_code == "unsupported_property_type":
        return ("unsupported_property_type", "support_blocker_unsupported_property_type")
    if blocker_code:
        return ("suppressed_data_quality", f"support_blocker_{blocker_code}")

    if unsupported_reason == "instant_quote_not_ready":
        return ("unsupported_value_signal", "unsupported_reason_instant_quote_not_ready")
    if unsupported_reason == "thin_market_support":
        return ("unsupported_value_signal", "unsupported_reason_thin_market_support")
    if unsupported_reason == "low_confidence_refined_review":
        return ("manual_review_recommended", "unsupported_reason_low_confidence_refined_review")
    if unsupported_reason == "implausible_savings_outlier":
        return ("manual_review_recommended", "unsupported_reason_implausible_savings_outlier")
    if unsupported_reason == "tax_limitation_uncertain":
        return ("opportunity_only_tax_profile_incomplete", "unsupported_reason_tax_limitation_uncertain")

    reduction_estimate_value = max(reduction_estimate or 0.0, 0.0)
    savings_estimate_value = max(savings_estimate or 0.0, 0.0)
    basis_value = max(_as_float(subject_row.get("assessment_basis_value")) or 0.0, 0.0)
    reduction_ratio = (
        reduction_estimate_value / basis_value if basis_value > 0 and reduction_estimate_value > 0 else 0.0
    )

    if reduction_estimate_value <= 0:
        return ("no_opportunity_detected", "reduction_estimate_zero")

    if tax_limitation_outcome == "suppressed":
        return ("opportunity_only_tax_profile_incomplete", "tax_limitation_suppressed")

    if savings_estimate_value <= 0:
        if bool(subject_row.get("disabled_veteran_flag")):
            return ("total_exemption_low_cash", "disabled_veteran_low_cash")
        if bool(subject_row.get("homestead_flag")) and bool(subject_row.get("freeze_flag")):
            return ("school_limited_non_school_possible", "freeze_low_cash")
        if reduction_ratio >= STRONG_OPPORTUNITY_REDUCTION_RATIO:
            return ("strong_opportunity_low_cash", "strong_value_gap_low_cash")
        return ("supported_opportunity_low_cash", "supported_value_gap_low_cash")

    if tax_limitation_outcome == "constrained":
        if bool(subject_row.get("freeze_flag")):
            return ("school_limited_non_school_possible", "freeze_constrained")
        if bool(subject_row.get("disabled_veteran_flag")):
            return ("near_total_exemption_low_cash", "disabled_veteran_constrained")
        if reduction_ratio >= STRONG_OPPORTUNITY_REDUCTION_RATIO:
            return ("strong_opportunity_low_cash", "constrained_strong_value_gap")
        return ("supported_opportunity_low_cash", "constrained_supported_value_gap")

    if confidence_score is not None and confidence_score < 65:
        return ("tax_profile_low_quality", "confidence_below_public_high_threshold")

    if savings_estimate_value < LOW_CASH_SAVINGS_THRESHOLD:
        if reduction_ratio >= STRONG_OPPORTUNITY_REDUCTION_RATIO:
            return ("strong_opportunity_low_cash", "strong_value_gap_low_cash")
        return ("supported_opportunity_low_cash", "supported_value_gap_low_cash")

    if (
        reduction_ratio >= STRONG_OPPORTUNITY_REDUCTION_RATIO
        and savings_estimate_value >= HIGH_CASH_SAVINGS_THRESHOLD
    ):
        return ("strong_opportunity_high_cash", "strong_value_gap_high_cash")

    return ("standard_quote", "public_safe_standard_quote")


def build_internal_classification_payload(
    *,
    subject_row: dict[str, Any],
    unsupported_reason: str | None,
    reduction_estimate: float | None,
    savings_estimate: float | None,
    confidence_score: float | None,
    tax_limitation_outcome: TaxLimitationOutcome | None,
) -> dict[str, Any]:
    warning_taxonomy = build_internal_warning_taxonomy(
        subject_row=subject_row,
        unsupported_reason=unsupported_reason,
    )
    warning_action_classes, dominant_warning_action_class = summarize_warning_action_classes(
        warning_taxonomy
    )
    opportunity_vs_savings_state, product_state_reason_code = (
        classify_opportunity_vs_savings_state(
            subject_row=subject_row,
            unsupported_reason=unsupported_reason,
            reduction_estimate=reduction_estimate,
            savings_estimate=savings_estimate,
            confidence_score=confidence_score,
            tax_limitation_outcome=tax_limitation_outcome,
        )
    )
    return {
        "warning_action_classes": warning_action_classes,
        "dominant_warning_action_class": dominant_warning_action_class,
        "warning_taxonomy_json": warning_taxonomy,
        "opportunity_vs_savings_state": opportunity_vs_savings_state,
        "product_state_reason_code": product_state_reason_code,
    }


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


def is_implausible_savings_outlier(
    *,
    savings_estimate: float,
    assessment_basis_value: float,
) -> bool:
    if savings_estimate <= 0 or assessment_basis_value <= 0:
        return False
    return (savings_estimate / assessment_basis_value) > EXTREME_SAVINGS_REVIEW_RATIO


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
