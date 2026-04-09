from __future__ import annotations

from dataclasses import dataclass

from app.services.instant_quote_tax_rate_basis import (
    TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES,
)

INSTANT_QUOTE_TAX_COMPLETENESS_STATUS_OPERATIONAL_WITH_CAVEATS = "operational_with_caveats"
INSTANT_QUOTE_TAX_COMPLETENESS_REASON_FORT_BEND_REVALIDATION_RESIDUAL_RISK = (
    "fort_bend_revalidation_residual_risk"
)
INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_ACCEPTABLE_CAUTION_OPERATIONAL = (
    "acceptable_caution_rows_operational"
)
INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RISKY_CAUTION_MONITORED = (
    "risky_caution_rows_monitored"
)
INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CONTINUITY_GAP_MONITORED = (
    "continuity_gap_rows_monitored"
)


@dataclass(frozen=True)
class InstantQuoteTaxCompletenessPosture:
    status: str | None = None
    reason: str | None = None
    internal_note: str | None = None
    warning_codes: tuple[str, ...] = ()


def classify_instant_quote_tax_completeness(
    *,
    county_id: str,
    tax_year: int,
    instant_quote_ready: bool,
    basis_tax_year: int | None,
    basis_status: str | None,
    basis_effective_tax_rate_coverage_ratio: float,
    basis_assignment_coverage_ratio: float,
    continuity_parcel_gap_row_count: int,
    continuity_parcel_match_ratio: float,
) -> InstantQuoteTaxCompletenessPosture:
    if (
        county_id != "fort_bend"
        or tax_year != 2026
        or not instant_quote_ready
        or basis_tax_year != 2025
        or basis_status != TAX_RATE_BASIS_STATUS_PRIOR_YEAR_ADOPTED_RATES
    ):
        return InstantQuoteTaxCompletenessPosture()

    if (
        basis_effective_tax_rate_coverage_ratio >= 0.995
        and basis_assignment_coverage_ratio >= 0.995
        and continuity_parcel_match_ratio >= 0.995
        and continuity_parcel_gap_row_count > 0
    ):
        return InstantQuoteTaxCompletenessPosture(
            status=INSTANT_QUOTE_TAX_COMPLETENESS_STATUS_OPERATIONAL_WITH_CAVEATS,
            reason=INSTANT_QUOTE_TAX_COMPLETENESS_REASON_FORT_BEND_REVALIDATION_RESIDUAL_RISK,
            internal_note=(
                "Fort Bend 2026 parcel tax completeness is operational with caveats. "
                "Most remaining caution rows appear acceptable because assigned billable "
                "components tie cleanly and leftover unresolved codes behave like "
                "overlay/non-rate extras. A smaller risky unresolved special-family "
                "slice and a small continuity-gap-only slice remain under monitoring."
            ),
            warning_codes=(
                INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_ACCEPTABLE_CAUTION_OPERATIONAL,
                INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RISKY_CAUTION_MONITORED,
                INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CONTINUITY_GAP_MONITORED,
            ),
        )

    return InstantQuoteTaxCompletenessPosture()
