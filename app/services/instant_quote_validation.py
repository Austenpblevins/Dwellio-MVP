from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from psycopg.types.json import Jsonb

from app.county_adapters.common.config_loader import load_county_adapter_config
from app.db.connection import get_connection
from app.services.instant_quote import (
    EXTREME_SAVINGS_REVIEW_RATIO,
    InstantQuoteService,
)
from app.services.instant_quote_tax_completeness import (
    classify_instant_quote_tax_completeness,
)
from app.services.instant_quote_tax_rate_basis import (
    INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS,
)


@dataclass(frozen=True)
class InstantQuoteExampleResult:
    account_number: str
    served_tax_year: int
    supported: bool
    unsupported_reason: str | None = None
    estimate_bucket: str | None = None
    estimate_strength_label: str | None = None


@dataclass(frozen=True)
class InstantQuoteValidationReport:
    county_id: str
    tax_year: int
    parcel_rows_with_living_area: int
    parcel_rows_with_effective_tax_rate: int
    subject_cache_row_count: int
    instant_quote_supportable_rows: int
    supported_neighborhood_stats_rows: int
    supported_segment_stats_rows: int
    tax_rate_basis_year: int | None = None
    tax_rate_basis_reason: str | None = None
    tax_rate_basis_fallback_applied: bool = False
    tax_rate_basis_status: str | None = None
    tax_rate_basis_status_reason: str | None = None
    tax_completeness_status: str | None = None
    tax_completeness_reason: str | None = None
    tax_completeness_internal_note: str | None = None
    tax_completeness_warning_codes: list[str] = field(default_factory=list)
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
    tax_rate_basis_warning_codes: list[str] = field(default_factory=list)
    subject_rows_without_usable_neighborhood_stats: int = 0
    subject_rows_without_usable_segment_stats: int = 0
    subject_rows_missing_segment_row: int = 0
    subject_rows_thin_segment_support: int = 0
    subject_rows_unusable_segment_basis: int = 0
    served_neighborhood_only_quote_count: int = 0
    served_supported_neighborhood_only_quote_count: int = 0
    served_unsupported_neighborhood_only_quote_count: int = 0
    latest_refresh_status: str | None = None
    latest_refresh_finished_at: str | None = None
    latest_validation_at: str | None = None
    cache_view_row_delta: int | None = None
    blocker_distribution: dict[str, int] = field(default_factory=dict)
    supported_public_quote_exists: bool = False
    supportable_row_rate: float = 0.0
    support_rate_all_sfr_flagged_denominator_count: int = 0
    support_rate_all_sfr_flagged_supportable_count: int = 0
    support_rate_all_sfr_flagged: float = 0.0
    support_rate_strict_sfr_eligible_denominator_count: int = 0
    support_rate_strict_sfr_eligible_supportable_count: int = 0
    support_rate_strict_sfr_eligible: float = 0.0
    high_value_subject_row_count: int = 0
    high_value_supportable_subject_row_count: int = 0
    high_value_support_rate: float = 0.0
    special_district_heavy_subject_row_count: int = 0
    special_district_heavy_supportable_subject_row_count: int = 0
    special_district_heavy_support_rate: float = 0.0
    monitored_zero_savings_sample_row_count: int = 0
    monitored_zero_savings_supported_quote_count: int = 0
    monitored_zero_savings_quote_count: int = 0
    monitored_zero_savings_quote_share: float = 0.0
    monitored_extreme_savings_watchlist_count: int = 0
    monitored_extreme_savings_flagged_count: int = 0
    monitored_extreme_savings_watchlist: list[dict[str, Any]] = field(default_factory=list)
    examples: list[InstantQuoteExampleResult] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["examples"] = [asdict(example) for example in self.examples]
        return payload


class InstantQuoteValidationService:
    def __init__(self, *, quote_service: InstantQuoteService | None = None) -> None:
        self.quote_service = quote_service or InstantQuoteService()

    def build_report(self, *, county_id: str, tax_year: int) -> InstantQuoteValidationReport:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                parcel_rows_with_living_area = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND COALESCE(living_area_sf, 0) > 0
                    """,
                    (county_id, tax_year),
                )
                parcel_rows_with_effective_tax_rate = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND COALESCE(effective_tax_rate, 0) > 0
                    """,
                    (county_id, tax_year),
                )
                instant_quote_supportable_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_blocker_code IS NULL
                    """,
                    (county_id, tax_year),
                )
                subject_cache_row_count = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_subject_cache
                    WHERE county_id = %s
                      AND tax_year = %s
                    """,
                    (county_id, tax_year),
                )
                supported_neighborhood_stats_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_neighborhood_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, tax_year),
                )
                supported_segment_stats_rows = self._count(
                    cursor,
                    """
                    SELECT COUNT(*) AS count
                    FROM instant_quote_segment_stats
                    WHERE county_id = %s
                      AND tax_year = %s
                      AND support_threshold_met IS TRUE
                    """,
                    (county_id, tax_year),
                )
                fallback_metrics = self._fallback_metrics(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                served_fallback_metrics = self._served_fallback_metrics(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                blocker_distribution = self._blocker_distribution(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                latest_refresh_run = self._latest_refresh_run(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                high_value_support_metrics = self._high_value_support_metrics(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                denominator_quality_metrics = self._denominator_quality_metrics(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
                special_district_heavy_support_metrics = (
                    self._special_district_heavy_support_metrics(
                        cursor,
                        county_id=county_id,
                        tax_year=tax_year,
                        basis_tax_year=(
                            tax_year
                            if latest_refresh_run is None
                            else int(latest_refresh_run.get("tax_rate_basis_year") or tax_year)
                        ),
                    )
                )
                zero_savings_sample_accounts = self._monitoring_sample_accounts(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                    limit=50,
                )
                extreme_savings_candidate_rows = self._extreme_savings_candidate_rows(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                    limit=40,
                )
                candidate_accounts = self._candidate_accounts(
                    cursor,
                    county_id=county_id,
                    tax_year=tax_year,
                )
        instant_quote_ready = (
            latest_refresh_run is not None
            and str(latest_refresh_run.get("refresh_status")) == "completed"
            and subject_cache_row_count > 0
            and supported_neighborhood_stats_rows > 0
            and supported_segment_stats_rows > 0
            and int(latest_refresh_run.get("cache_view_row_delta") or 0) == 0
            and instant_quote_supportable_rows >= INSTANT_QUOTE_TAX_RATE_BASIS_MIN_SUPPORTABLE_SUBJECTS
        )
        tax_completeness_posture = classify_instant_quote_tax_completeness(
            county_id=county_id,
            tax_year=tax_year,
            instant_quote_ready=instant_quote_ready,
            basis_tax_year=(
                None if latest_refresh_run is None else latest_refresh_run.get("tax_rate_basis_year")
            ),
            basis_status=(
                None
                if latest_refresh_run is None
                else latest_refresh_run.get("tax_rate_basis_status")
            ),
            basis_effective_tax_rate_coverage_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_effective_tax_rate_coverage_ratio")
                or 0.0
            ),
            basis_assignment_coverage_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_assignment_coverage_ratio")
                or 0.0
            ),
            continuity_parcel_gap_row_count=int(
                (latest_refresh_run or {}).get("tax_rate_basis_continuity_parcel_gap_row_count")
                or 0
            ),
            continuity_parcel_match_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_continuity_parcel_match_ratio")
                or 0.0
            ),
        )
        supportable_row_rate = (
            float(instant_quote_supportable_rows) / float(subject_cache_row_count)
            if subject_cache_row_count > 0
            else 0.0
        )
        zero_savings_sample_results = self._run_quote_monitoring_accounts(
            county_id=county_id,
            tax_year=tax_year,
            account_numbers=zero_savings_sample_accounts,
        )
        monitored_zero_savings_supported_quote_count = sum(
            1 for _, response in zero_savings_sample_results if response.supported
        )
        monitored_zero_savings_quote_count = sum(
            1
            for _, response in zero_savings_sample_results
            if response.supported
            and response.estimate is not None
            and float(response.estimate.savings_midpoint_display or 0.0) <= 0.0
        )
        monitored_zero_savings_quote_share = (
            float(monitored_zero_savings_quote_count)
            / float(monitored_zero_savings_supported_quote_count)
            if monitored_zero_savings_supported_quote_count > 0
            else 0.0
        )
        extreme_savings_results = self._run_quote_monitoring_accounts(
            county_id=county_id,
            tax_year=tax_year,
            account_numbers=[
                str(row["account_number"])
                for row in extreme_savings_candidate_rows
            ],
        )
        responses_by_account = {
            account_number: response
            for account_number, response in extreme_savings_results
        }
        monitored_extreme_savings_watchlist = sorted(
            [
                {
                    "account_number": str(row["account_number"]),
                    "assessment_basis_value": float(row.get("assessment_basis_value") or 0.0),
                    "effective_tax_rate": float(row.get("effective_tax_rate") or 0.0),
                    "projected_savings": float(
                        responses_by_account[str(row["account_number"])].estimate.savings_midpoint_display
                        or 0.0
                    ),
                    "projected_savings_ratio": (
                        0.0
                        if float(row.get("assessment_basis_value") or 0.0) <= 0.0
                        else float(
                            responses_by_account[str(row["account_number"])].estimate.savings_midpoint_display
                            or 0.0
                        )
                        / float(row.get("assessment_basis_value") or 0.0)
                    ),
                    "flagged_by_ratio_threshold": (
                        float(row.get("assessment_basis_value") or 0.0) > 0.0
                        and float(
                            responses_by_account[str(row["account_number"])].estimate.savings_midpoint_display
                            or 0.0
                        )
                        / float(row.get("assessment_basis_value") or 0.0)
                        > EXTREME_SAVINGS_REVIEW_RATIO
                    ),
                    "basis_code": responses_by_account[str(row["account_number"])].basis_code,
                    "estimate_bucket": (
                        None
                        if responses_by_account[str(row["account_number"])].estimate is None
                        else responses_by_account[str(row["account_number"])].estimate.estimate_bucket
                    ),
                }
                for row in extreme_savings_candidate_rows
                if str(row["account_number"]) in responses_by_account
                and responses_by_account[str(row["account_number"])].supported
                and responses_by_account[str(row["account_number"])].estimate is not None
            ],
            key=lambda item: (
                float(item["projected_savings"]),
                float(item["assessment_basis_value"]),
                str(item["account_number"]),
            ),
            reverse=True,
        )[:10]
        monitored_extreme_savings_flagged_count = sum(
            1
            for item in monitored_extreme_savings_watchlist
            if bool(item.get("flagged_by_ratio_threshold"))
        )

        examples: list[InstantQuoteExampleResult] = []
        supported_public_quote_exists = False
        for account_number in candidate_accounts:
            try:
                response = self.quote_service.get_quote(
                    county_id=county_id,
                    tax_year=tax_year,
                    account_number=account_number,
                )
            except LookupError:
                continue

            if response.supported:
                supported_public_quote_exists = True
            examples.append(
                InstantQuoteExampleResult(
                    account_number=account_number,
                    served_tax_year=response.served_tax_year,
                    supported=response.supported,
                    unsupported_reason=response.unsupported_reason,
                    estimate_bucket=(
                        None
                        if response.estimate is None
                        else response.estimate.estimate_bucket
                    ),
                    estimate_strength_label=(
                        None
                        if response.estimate is None
                        else response.estimate.estimate_strength_label
                    ),
                )
            )
            if len(examples) >= 6 and supported_public_quote_exists:
                break

        self._persist_validation_report(
            county_id=county_id,
            tax_year=tax_year,
            report_payload={
                "county_id": county_id,
                "tax_year": tax_year,
                "tax_rate_basis_year": (
                    None
                    if latest_refresh_run is None
                    else latest_refresh_run.get("tax_rate_basis_year")
                ),
                "tax_rate_basis_reason": (
                    None
                    if latest_refresh_run is None
                    else latest_refresh_run.get("tax_rate_basis_reason")
                ),
                "tax_rate_basis_fallback_applied": bool(
                    latest_refresh_run
                    and latest_refresh_run.get("tax_rate_basis_fallback_applied")
                ),
                "tax_rate_basis_status": (
                    None
                    if latest_refresh_run is None
                    else latest_refresh_run.get("tax_rate_basis_status")
                ),
                "tax_rate_basis_status_reason": (
                    None
                    if latest_refresh_run is None
                    else latest_refresh_run.get("tax_rate_basis_status_reason")
                ),
                "tax_completeness_status": tax_completeness_posture.status,
                "tax_completeness_reason": tax_completeness_posture.reason,
                "tax_completeness_internal_note": tax_completeness_posture.internal_note,
                "tax_completeness_warning_codes": list(tax_completeness_posture.warning_codes),
                "requested_tax_rate_supportable_subject_row_count": int(
                    (latest_refresh_run or {}).get(
                        "requested_tax_rate_supportable_subject_row_count"
                    )
                    or 0
                ),
                "tax_rate_basis_supportable_subject_row_count": int(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_supportable_subject_row_count"
                    )
                    or 0
                ),
                "tax_rate_quoteable_subject_row_count": int(
                    (latest_refresh_run or {}).get("tax_rate_quoteable_subject_row_count") or 0
                ),
                "requested_tax_rate_effective_tax_rate_coverage_ratio": float(
                    (latest_refresh_run or {}).get(
                        "requested_tax_rate_effective_tax_rate_coverage_ratio"
                    )
                    or 0.0
                ),
                "requested_tax_rate_assignment_coverage_ratio": float(
                    (latest_refresh_run or {}).get(
                        "requested_tax_rate_assignment_coverage_ratio"
                    )
                    or 0.0
                ),
                "tax_rate_basis_effective_tax_rate_coverage_ratio": float(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_effective_tax_rate_coverage_ratio"
                    )
                    or 0.0
                ),
                "tax_rate_basis_assignment_coverage_ratio": float(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_assignment_coverage_ratio"
                    )
                    or 0.0
                ),
                "tax_rate_basis_continuity_parcel_match_row_count": int(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_continuity_parcel_match_row_count"
                    )
                    or 0
                ),
                "tax_rate_basis_continuity_parcel_gap_row_count": int(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_continuity_parcel_gap_row_count"
                    )
                    or 0
                ),
                "tax_rate_basis_continuity_parcel_match_ratio": float(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_continuity_parcel_match_ratio"
                    )
                    or 0.0
                ),
                "tax_rate_basis_continuity_account_number_match_row_count": int(
                    (latest_refresh_run or {}).get(
                        "tax_rate_basis_continuity_account_number_match_row_count"
                    )
                    or 0
                ),
                "tax_rate_basis_warning_codes": list(
                    (latest_refresh_run or {}).get("tax_rate_basis_warning_codes") or []
                ),
                "parcel_rows_with_living_area": parcel_rows_with_living_area,
                "parcel_rows_with_effective_tax_rate": parcel_rows_with_effective_tax_rate,
                "subject_cache_row_count": subject_cache_row_count,
                "instant_quote_supportable_rows": instant_quote_supportable_rows,
                "supported_neighborhood_stats_rows": supported_neighborhood_stats_rows,
                "supported_segment_stats_rows": supported_segment_stats_rows,
                "subject_rows_without_usable_neighborhood_stats": fallback_metrics[
                    "subject_rows_without_usable_neighborhood_stats"
                ],
                "subject_rows_without_usable_segment_stats": fallback_metrics[
                    "subject_rows_without_usable_segment_stats"
                ],
                "subject_rows_missing_segment_row": fallback_metrics[
                    "subject_rows_missing_segment_row"
                ],
                "subject_rows_thin_segment_support": fallback_metrics[
                    "subject_rows_thin_segment_support"
                ],
                "subject_rows_unusable_segment_basis": fallback_metrics[
                    "subject_rows_unusable_segment_basis"
                ],
                "served_neighborhood_only_quote_count": served_fallback_metrics[
                    "served_neighborhood_only_quote_count"
                ],
                "served_supported_neighborhood_only_quote_count": served_fallback_metrics[
                    "served_supported_neighborhood_only_quote_count"
                ],
                "served_unsupported_neighborhood_only_quote_count": served_fallback_metrics[
                    "served_unsupported_neighborhood_only_quote_count"
                ],
                "blocker_distribution": blocker_distribution,
                "supported_public_quote_exists": supported_public_quote_exists,
                "supportable_row_rate": supportable_row_rate,
                "support_rate_all_sfr_flagged_denominator_count": int(
                    denominator_quality_metrics["all_sfr_flagged_denominator_count"]
                ),
                "support_rate_all_sfr_flagged_supportable_count": int(
                    denominator_quality_metrics["all_sfr_flagged_supportable_count"]
                ),
                "support_rate_all_sfr_flagged": float(
                    denominator_quality_metrics["all_sfr_flagged_support_rate"]
                ),
                "support_rate_strict_sfr_eligible_denominator_count": int(
                    denominator_quality_metrics["strict_sfr_eligible_denominator_count"]
                ),
                "support_rate_strict_sfr_eligible_supportable_count": int(
                    denominator_quality_metrics["strict_sfr_eligible_supportable_count"]
                ),
                "support_rate_strict_sfr_eligible": float(
                    denominator_quality_metrics["strict_sfr_eligible_support_rate"]
                ),
                "high_value_subject_row_count": int(
                    high_value_support_metrics["subject_row_count"]
                ),
                "high_value_supportable_subject_row_count": int(
                    high_value_support_metrics["supportable_row_count"]
                ),
                "high_value_support_rate": float(high_value_support_metrics["support_rate"]),
                "special_district_heavy_subject_row_count": int(
                    special_district_heavy_support_metrics["subject_row_count"]
                ),
                "special_district_heavy_supportable_subject_row_count": int(
                    special_district_heavy_support_metrics["supportable_row_count"]
                ),
                "special_district_heavy_support_rate": float(
                    special_district_heavy_support_metrics["support_rate"]
                ),
                "monitored_zero_savings_sample_row_count": len(zero_savings_sample_accounts),
                "monitored_zero_savings_supported_quote_count": (
                    monitored_zero_savings_supported_quote_count
                ),
                "monitored_zero_savings_quote_count": monitored_zero_savings_quote_count,
                "monitored_zero_savings_quote_share": monitored_zero_savings_quote_share,
                "monitored_extreme_savings_watchlist_count": len(
                    monitored_extreme_savings_watchlist
                ),
                "monitored_extreme_savings_flagged_count": (
                    monitored_extreme_savings_flagged_count
                ),
                "monitored_extreme_savings_watchlist": monitored_extreme_savings_watchlist,
                "examples": [asdict(example) for example in examples],
            },
        )

        return InstantQuoteValidationReport(
            county_id=county_id,
            tax_year=tax_year,
            tax_rate_basis_year=(
                None
                if latest_refresh_run is None
                else (
                    None
                    if latest_refresh_run.get("tax_rate_basis_year") is None
                    else int(latest_refresh_run["tax_rate_basis_year"])
                )
            ),
            tax_rate_basis_reason=(
                None
                if latest_refresh_run is None
                else (
                    None
                    if latest_refresh_run.get("tax_rate_basis_reason") is None
                    else str(latest_refresh_run["tax_rate_basis_reason"])
                )
            ),
            tax_rate_basis_fallback_applied=bool(
                latest_refresh_run
                and latest_refresh_run.get("tax_rate_basis_fallback_applied")
            ),
            tax_rate_basis_status=(
                None
                if latest_refresh_run is None
                else (
                    None
                    if latest_refresh_run.get("tax_rate_basis_status") is None
                    else str(latest_refresh_run["tax_rate_basis_status"])
                )
            ),
            tax_rate_basis_status_reason=(
                None
                if latest_refresh_run is None
                else (
                    None
                    if latest_refresh_run.get("tax_rate_basis_status_reason") is None
                    else str(latest_refresh_run["tax_rate_basis_status_reason"])
                )
            ),
            tax_completeness_status=tax_completeness_posture.status,
            tax_completeness_reason=tax_completeness_posture.reason,
            tax_completeness_internal_note=tax_completeness_posture.internal_note,
            tax_completeness_warning_codes=list(tax_completeness_posture.warning_codes),
            requested_tax_rate_supportable_subject_row_count=int(
                (latest_refresh_run or {}).get("requested_tax_rate_supportable_subject_row_count")
                or 0
            ),
            tax_rate_basis_supportable_subject_row_count=int(
                (latest_refresh_run or {}).get("tax_rate_basis_supportable_subject_row_count")
                or 0
            ),
            tax_rate_quoteable_subject_row_count=int(
                (latest_refresh_run or {}).get("tax_rate_quoteable_subject_row_count") or 0
            ),
            requested_tax_rate_effective_tax_rate_coverage_ratio=float(
                (latest_refresh_run or {}).get(
                    "requested_tax_rate_effective_tax_rate_coverage_ratio"
                )
                or 0.0
            ),
            requested_tax_rate_assignment_coverage_ratio=float(
                (latest_refresh_run or {}).get("requested_tax_rate_assignment_coverage_ratio")
                or 0.0
            ),
            tax_rate_basis_effective_tax_rate_coverage_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_effective_tax_rate_coverage_ratio")
                or 0.0
            ),
            tax_rate_basis_assignment_coverage_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_assignment_coverage_ratio")
                or 0.0
            ),
            tax_rate_basis_continuity_parcel_match_row_count=int(
                (latest_refresh_run or {}).get(
                    "tax_rate_basis_continuity_parcel_match_row_count"
                )
                or 0
            ),
            tax_rate_basis_continuity_parcel_gap_row_count=int(
                (latest_refresh_run or {}).get("tax_rate_basis_continuity_parcel_gap_row_count")
                or 0
            ),
            tax_rate_basis_continuity_parcel_match_ratio=float(
                (latest_refresh_run or {}).get("tax_rate_basis_continuity_parcel_match_ratio")
                or 0.0
            ),
            tax_rate_basis_continuity_account_number_match_row_count=int(
                (latest_refresh_run or {}).get(
                    "tax_rate_basis_continuity_account_number_match_row_count"
                )
                or 0
            ),
            tax_rate_basis_warning_codes=list(
                (latest_refresh_run or {}).get("tax_rate_basis_warning_codes") or []
            ),
            parcel_rows_with_living_area=parcel_rows_with_living_area,
            parcel_rows_with_effective_tax_rate=parcel_rows_with_effective_tax_rate,
            subject_cache_row_count=subject_cache_row_count,
            instant_quote_supportable_rows=instant_quote_supportable_rows,
            supported_neighborhood_stats_rows=supported_neighborhood_stats_rows,
            supported_segment_stats_rows=supported_segment_stats_rows,
            subject_rows_without_usable_neighborhood_stats=int(
                fallback_metrics["subject_rows_without_usable_neighborhood_stats"]
            ),
            subject_rows_without_usable_segment_stats=int(
                fallback_metrics["subject_rows_without_usable_segment_stats"]
            ),
            subject_rows_missing_segment_row=int(
                fallback_metrics["subject_rows_missing_segment_row"]
            ),
            subject_rows_thin_segment_support=int(
                fallback_metrics["subject_rows_thin_segment_support"]
            ),
            subject_rows_unusable_segment_basis=int(
                fallback_metrics["subject_rows_unusable_segment_basis"]
            ),
            served_neighborhood_only_quote_count=int(
                served_fallback_metrics["served_neighborhood_only_quote_count"]
            ),
            served_supported_neighborhood_only_quote_count=int(
                served_fallback_metrics["served_supported_neighborhood_only_quote_count"]
            ),
            served_unsupported_neighborhood_only_quote_count=int(
                served_fallback_metrics["served_unsupported_neighborhood_only_quote_count"]
            ),
            latest_refresh_status=(
                None if latest_refresh_run is None else str(latest_refresh_run["refresh_status"])
            ),
            latest_refresh_finished_at=(
                None
                if latest_refresh_run is None or latest_refresh_run.get("refresh_finished_at") is None
                else latest_refresh_run["refresh_finished_at"].isoformat()
            ),
            latest_validation_at=(
                None
                if latest_refresh_run is None or latest_refresh_run.get("validated_at") is None
                else latest_refresh_run["validated_at"].isoformat()
            ),
            cache_view_row_delta=(
                None if latest_refresh_run is None else int(latest_refresh_run["cache_view_row_delta"] or 0)
            ),
            blocker_distribution=blocker_distribution,
            supported_public_quote_exists=supported_public_quote_exists,
            supportable_row_rate=supportable_row_rate,
            support_rate_all_sfr_flagged_denominator_count=int(
                denominator_quality_metrics["all_sfr_flagged_denominator_count"]
            ),
            support_rate_all_sfr_flagged_supportable_count=int(
                denominator_quality_metrics["all_sfr_flagged_supportable_count"]
            ),
            support_rate_all_sfr_flagged=float(
                denominator_quality_metrics["all_sfr_flagged_support_rate"]
            ),
            support_rate_strict_sfr_eligible_denominator_count=int(
                denominator_quality_metrics["strict_sfr_eligible_denominator_count"]
            ),
            support_rate_strict_sfr_eligible_supportable_count=int(
                denominator_quality_metrics["strict_sfr_eligible_supportable_count"]
            ),
            support_rate_strict_sfr_eligible=float(
                denominator_quality_metrics["strict_sfr_eligible_support_rate"]
            ),
            high_value_subject_row_count=int(high_value_support_metrics["subject_row_count"]),
            high_value_supportable_subject_row_count=int(
                high_value_support_metrics["supportable_row_count"]
            ),
            high_value_support_rate=float(high_value_support_metrics["support_rate"]),
            special_district_heavy_subject_row_count=int(
                special_district_heavy_support_metrics["subject_row_count"]
            ),
            special_district_heavy_supportable_subject_row_count=int(
                special_district_heavy_support_metrics["supportable_row_count"]
            ),
            special_district_heavy_support_rate=float(
                special_district_heavy_support_metrics["support_rate"]
            ),
            monitored_zero_savings_sample_row_count=len(zero_savings_sample_accounts),
            monitored_zero_savings_supported_quote_count=(
                monitored_zero_savings_supported_quote_count
            ),
            monitored_zero_savings_quote_count=monitored_zero_savings_quote_count,
            monitored_zero_savings_quote_share=monitored_zero_savings_quote_share,
            monitored_extreme_savings_watchlist_count=len(
                monitored_extreme_savings_watchlist
            ),
            monitored_extreme_savings_flagged_count=monitored_extreme_savings_flagged_count,
            monitored_extreme_savings_watchlist=monitored_extreme_savings_watchlist,
            examples=examples,
        )

    def _count(self, cursor: Any, sql: str, params: tuple[object, ...]) -> int:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row["count"] if row is not None else 0)

    def _blocker_distribution(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, int]:
        cursor.execute(
            """
            SELECT
              COALESCE(support_blocker_code, 'supportable') AS blocker_code,
              COUNT(*) AS count
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            GROUP BY COALESCE(support_blocker_code, 'supportable')
            ORDER BY count DESC, blocker_code ASC
            """,
            (county_id, tax_year),
        )
        return {str(row["blocker_code"]): int(row["count"]) for row in cursor.fetchall()}

    def _fallback_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, int]:
        cursor.execute(
            """
            WITH supportable_subjects AS (
              SELECT
                county_id,
                tax_year,
                neighborhood_code,
                size_bucket,
                age_bucket
              FROM instant_quote_subject_cache
              WHERE county_id = %s
                AND tax_year = %s
                AND support_blocker_code IS NULL
            )
            SELECT
              COUNT(*) FILTER (
                WHERE neighborhood.neighborhood_code IS NULL
                   OR neighborhood.parcel_count < 20
                   OR neighborhood.p50_assessed_psf IS NULL
              ) AS subject_rows_without_usable_neighborhood_stats,
              COUNT(*) FILTER (
                WHERE neighborhood.neighborhood_code IS NOT NULL
                  AND neighborhood.parcel_count >= 20
                  AND neighborhood.p50_assessed_psf IS NOT NULL
                  AND (
                    segment.neighborhood_code IS NULL
                    OR segment.parcel_count < 8
                    OR segment.p50_assessed_psf IS NULL
                  )
              ) AS subject_rows_without_usable_segment_stats,
              COUNT(*) FILTER (
                WHERE neighborhood.neighborhood_code IS NOT NULL
                  AND neighborhood.parcel_count >= 20
                  AND neighborhood.p50_assessed_psf IS NOT NULL
                  AND segment.neighborhood_code IS NULL
              ) AS subject_rows_missing_segment_row,
              COUNT(*) FILTER (
                WHERE neighborhood.neighborhood_code IS NOT NULL
                  AND neighborhood.parcel_count >= 20
                  AND neighborhood.p50_assessed_psf IS NOT NULL
                  AND segment.neighborhood_code IS NOT NULL
                  AND segment.parcel_count < 8
              ) AS subject_rows_thin_segment_support,
              COUNT(*) FILTER (
                WHERE neighborhood.neighborhood_code IS NOT NULL
                  AND neighborhood.parcel_count >= 20
                  AND neighborhood.p50_assessed_psf IS NOT NULL
                  AND segment.neighborhood_code IS NOT NULL
                  AND segment.p50_assessed_psf IS NULL
              ) AS subject_rows_unusable_segment_basis
            FROM supportable_subjects subjects
            LEFT JOIN instant_quote_neighborhood_stats neighborhood
              ON neighborhood.county_id = subjects.county_id
             AND neighborhood.tax_year = subjects.tax_year
             AND neighborhood.neighborhood_code = subjects.neighborhood_code
             AND neighborhood.property_type_code = 'sfr'
            LEFT JOIN instant_quote_segment_stats segment
              ON segment.county_id = subjects.county_id
             AND segment.tax_year = subjects.tax_year
             AND segment.neighborhood_code = subjects.neighborhood_code
             AND segment.property_type_code = 'sfr'
             AND segment.size_bucket = subjects.size_bucket
             AND segment.age_bucket = subjects.age_bucket
            """,
            (county_id, tax_year),
        )
        row = cursor.fetchone() or {}
        return {
            "subject_rows_without_usable_neighborhood_stats": int(
                row.get("subject_rows_without_usable_neighborhood_stats") or 0
            ),
            "subject_rows_without_usable_segment_stats": int(
                row.get("subject_rows_without_usable_segment_stats") or 0
            ),
            "subject_rows_missing_segment_row": int(
                row.get("subject_rows_missing_segment_row") or 0
            ),
            "subject_rows_thin_segment_support": int(
                row.get("subject_rows_thin_segment_support") or 0
            ),
            "subject_rows_unusable_segment_basis": int(
                row.get("subject_rows_unusable_segment_basis") or 0
            ),
        }

    def _served_fallback_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, int]:
        cursor.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE fallback_tier = 'neighborhood_only') AS served_neighborhood_only_quote_count,
              COUNT(*) FILTER (
                WHERE fallback_tier = 'neighborhood_only'
                  AND supported IS TRUE
              ) AS served_supported_neighborhood_only_quote_count,
              COUNT(*) FILTER (
                WHERE fallback_tier = 'neighborhood_only'
                  AND supported IS FALSE
              ) AS served_unsupported_neighborhood_only_quote_count
            FROM instant_quote_request_logs
            WHERE county_id = %s
              AND tax_year = %s
            """,
            (county_id, tax_year),
        )
        row = cursor.fetchone() or {}
        return {
            "served_neighborhood_only_quote_count": int(
                row.get("served_neighborhood_only_quote_count") or 0
            ),
            "served_supported_neighborhood_only_quote_count": int(
                row.get("served_supported_neighborhood_only_quote_count") or 0
            ),
            "served_unsupported_neighborhood_only_quote_count": int(
                row.get("served_unsupported_neighborhood_only_quote_count") or 0
            ),
        }

    def _latest_refresh_run(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT
              refresh_status,
              refresh_finished_at,
              validated_at,
              cache_view_row_delta,
              tax_rate_basis_year,
              tax_rate_basis_reason,
              tax_rate_basis_fallback_applied,
              tax_rate_basis_status,
              tax_rate_basis_status_reason,
              requested_tax_rate_supportable_subject_row_count,
              tax_rate_basis_supportable_subject_row_count,
              tax_rate_quoteable_subject_row_count,
              requested_tax_rate_effective_tax_rate_coverage_ratio,
              requested_tax_rate_assignment_coverage_ratio,
              tax_rate_basis_effective_tax_rate_coverage_ratio,
              tax_rate_basis_assignment_coverage_ratio,
              tax_rate_basis_continuity_parcel_match_row_count,
              tax_rate_basis_continuity_parcel_gap_row_count,
              tax_rate_basis_continuity_parcel_match_ratio,
              tax_rate_basis_continuity_account_number_match_row_count,
              tax_rate_basis_warning_codes
            FROM instant_quote_refresh_runs
            WHERE county_id = %s
              AND tax_year = %s
            ORDER BY refresh_started_at DESC
            LIMIT 1
            """,
            (county_id, tax_year),
        )
        return cursor.fetchone()

    def _candidate_accounts(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> list[str]:
        account_numbers: list[str] = []
        seen: set[str] = set()
        preferred_blockers: tuple[str | None, ...] = (
            None,
            "missing_effective_tax_rate",
            "missing_living_area",
            "missing_assessment_basis",
        )
        for blocker_code in preferred_blockers:
            cursor.execute(
                """
                SELECT account_number
                FROM instant_quote_subject_cache
                WHERE county_id = %s
                  AND tax_year = %s
                  AND (
                    (%s::text IS NULL AND support_blocker_code IS NULL)
                    OR support_blocker_code = %s
                  )
                ORDER BY account_number ASC
                LIMIT 2
                """,
                (county_id, tax_year, blocker_code, blocker_code),
            )
            for row in cursor.fetchall():
                account_number = str(row["account_number"])
                if account_number in seen:
                    continue
                seen.add(account_number)
                account_numbers.append(account_number)

        if len(account_numbers) >= 12:
            return account_numbers[:12]

        cursor.execute(
            """
            SELECT account_number
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            ORDER BY account_number ASC
            LIMIT 12
            """,
            (county_id, tax_year),
        )
        for row in cursor.fetchall():
            account_number = str(row["account_number"])
            if account_number in seen:
                continue
            seen.add(account_number)
            account_numbers.append(account_number)
            if len(account_numbers) >= 12:
                break
        return account_numbers

    def _high_value_support_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, float | int]:
        cursor.execute(
            """
            WITH scoped AS (
              SELECT assessment_basis_value, support_blocker_code
              FROM instant_quote_subject_cache
              WHERE county_id = %s
                AND tax_year = %s
                AND assessment_basis_value IS NOT NULL
            ),
            threshold AS (
              SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY assessment_basis_value) AS p95
              FROM scoped
            )
            SELECT
              COUNT(*) FILTER (
                WHERE threshold.p95 IS NOT NULL
                  AND scoped.assessment_basis_value >= threshold.p95
              ) AS subject_row_count,
              COUNT(*) FILTER (
                WHERE threshold.p95 IS NOT NULL
                  AND scoped.assessment_basis_value >= threshold.p95
                  AND scoped.support_blocker_code IS NULL
              ) AS supportable_row_count
            FROM scoped
            CROSS JOIN threshold
            """,
            (county_id, tax_year),
        )
        row = cursor.fetchone() or {}
        subject_row_count = int(row.get("subject_row_count") or 0)
        supportable_row_count = int(row.get("supportable_row_count") or 0)
        return {
            "subject_row_count": subject_row_count,
            "supportable_row_count": supportable_row_count,
            "support_rate": (
                float(supportable_row_count) / float(subject_row_count)
                if subject_row_count > 0
                else 0.0
            ),
        }

    def _denominator_quality_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
    ) -> dict[str, float | int]:
        strict_sfr_class_codes = list(self._strict_sfr_class_codes(county_id=county_id))
        cursor.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE property_type_code = 'sfr')
                AS all_sfr_flagged_denominator_count,
              COUNT(*) FILTER (
                WHERE property_type_code = 'sfr'
                  AND support_blocker_code IS NULL
              ) AS all_sfr_flagged_supportable_count,
              COUNT(*) FILTER (
                WHERE property_type_code = 'sfr'
                  AND UPPER(BTRIM(COALESCE(property_class_code, ''))) = ANY(%s::text[])
              ) AS strict_sfr_eligible_denominator_count,
              COUNT(*) FILTER (
                WHERE property_type_code = 'sfr'
                  AND UPPER(BTRIM(COALESCE(property_class_code, ''))) = ANY(%s::text[])
                  AND support_blocker_code IS NULL
              ) AS strict_sfr_eligible_supportable_count
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
            """,
            (strict_sfr_class_codes, strict_sfr_class_codes, county_id, tax_year),
        )
        row = cursor.fetchone() or {}
        all_denominator = int(row.get("all_sfr_flagged_denominator_count") or 0)
        all_supportable = int(row.get("all_sfr_flagged_supportable_count") or 0)
        strict_denominator = int(row.get("strict_sfr_eligible_denominator_count") or 0)
        strict_supportable = int(row.get("strict_sfr_eligible_supportable_count") or 0)
        return {
            "all_sfr_flagged_denominator_count": all_denominator,
            "all_sfr_flagged_supportable_count": all_supportable,
            "all_sfr_flagged_support_rate": (
                float(all_supportable) / float(all_denominator)
                if all_denominator > 0
                else 0.0
            ),
            "strict_sfr_eligible_denominator_count": strict_denominator,
            "strict_sfr_eligible_supportable_count": strict_supportable,
            "strict_sfr_eligible_support_rate": (
                float(strict_supportable) / float(strict_denominator)
                if strict_denominator > 0
                else 0.0
            ),
        }

    def _strict_sfr_class_codes(self, *, county_id: str) -> tuple[str, ...]:
        config = load_county_adapter_config(county_id)
        dataset_mapping = config.field_mappings.get("property_roll")
        if dataset_mapping is None:
            return ()

        for section_name in ("characteristics", "parcel"):
            section = dataset_mapping.sections.get(section_name)
            if section is None:
                continue
            for field_mapping in section.fields:
                if (
                    field_mapping.target_field == "property_type_code"
                    and field_mapping.transform == "property_class_to_sfr"
                ):
                    return tuple(
                        sorted(
                            {
                                str(code).strip().upper()
                                for code in field_mapping.transform_options.get(
                                    "sfr_class_codes", []
                                )
                                if str(code).strip()
                            }
                        )
                    )
        return ()

    def _special_district_heavy_support_metrics(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
        basis_tax_year: int,
    ) -> dict[str, float | int]:
        cursor.execute(
            """
            WITH scoped AS (
              SELECT
                sc.support_blocker_code,
                COALESCE(petr.mud_assignment_count, 0) + COALESCE(petr.special_assignment_count, 0)
                  AS special_stack_count
              FROM instant_quote_subject_cache sc
              LEFT JOIN parcel_effective_tax_rate_view petr
                ON petr.parcel_id = sc.parcel_id
               AND petr.tax_year = %s
              WHERE sc.county_id = %s
                AND sc.tax_year = %s
            )
            SELECT
              COUNT(*) FILTER (WHERE special_stack_count > 0) AS subject_row_count,
              COUNT(*) FILTER (
                WHERE special_stack_count > 0
                  AND support_blocker_code IS NULL
              ) AS supportable_row_count
            FROM scoped
            """,
            (basis_tax_year, county_id, tax_year),
        )
        row = cursor.fetchone() or {}
        subject_row_count = int(row.get("subject_row_count") or 0)
        supportable_row_count = int(row.get("supportable_row_count") or 0)
        return {
            "subject_row_count": subject_row_count,
            "supportable_row_count": supportable_row_count,
            "support_rate": (
                float(supportable_row_count) / float(subject_row_count)
                if subject_row_count > 0
                else 0.0
            ),
        }

    def _monitoring_sample_accounts(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
        limit: int,
    ) -> list[str]:
        cursor.execute(
            """
            SELECT account_number
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
              AND support_blocker_code IS NULL
            ORDER BY md5(account_number), account_number
            LIMIT %s
            """,
            (county_id, tax_year, limit),
        )
        return [str(row["account_number"]) for row in cursor.fetchall()]

    def _extreme_savings_candidate_rows(
        self,
        cursor: Any,
        *,
        county_id: str,
        tax_year: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              account_number,
              assessment_basis_value,
              effective_tax_rate
            FROM instant_quote_subject_cache
            WHERE county_id = %s
              AND tax_year = %s
              AND support_blocker_code IS NULL
            ORDER BY assessment_basis_value DESC, effective_tax_rate DESC, account_number ASC
            LIMIT %s
            """,
            (county_id, tax_year, limit),
        )
        return cursor.fetchall()

    def _run_quote_monitoring_accounts(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_numbers: list[str],
    ) -> list[tuple[str, Any]]:
        results: list[tuple[str, Any]] = []
        for account_number in account_numbers:
            try:
                response = self.quote_service.get_quote(
                    county_id=county_id,
                    tax_year=tax_year,
                    account_number=account_number,
                )
            except LookupError:
                continue
            results.append((account_number, response))
        return results

    def _persist_validation_report(
        self,
        *,
        county_id: str,
        tax_year: int,
        report_payload: dict[str, Any],
    ) -> None:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE instant_quote_refresh_runs
                    SET validated_at = now(),
                        validation_report = %s::jsonb
                    WHERE instant_quote_refresh_run_id = (
                      SELECT instant_quote_refresh_run_id
                      FROM instant_quote_refresh_runs
                      WHERE county_id = %s
                        AND tax_year = %s
                      ORDER BY refresh_started_at DESC
                      LIMIT 1
                    )
                    """,
                    (Jsonb(report_payload), county_id, tax_year),
                )
            connection.commit()
