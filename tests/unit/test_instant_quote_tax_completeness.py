from __future__ import annotations

from app.services.instant_quote_tax_completeness import (
    INSTANT_QUOTE_TAX_COMPLETENESS_REASON_FORT_BEND_REVALIDATION_RESIDUAL_RISK,
    INSTANT_QUOTE_TAX_COMPLETENESS_REASON_HARRIS_REFRESHED_SPECIAL_FAMILY_RECOVERY,
    INSTANT_QUOTE_TAX_COMPLETENESS_STATUS_OPERATIONAL_WITH_CAVEATS,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_ACCEPTABLE_CAUTION_OPERATIONAL,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CAVEATED_SPECIAL_FAMILY_MONITORED,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CONTINUITY_GAP_MONITORED,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_MISSING_SCHOOL_ASSIGNMENT_MONITORED,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RECOVERED_SPECIAL_FAMILY_OPERATIONAL,
    INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RISKY_CAUTION_MONITORED,
    classify_instant_quote_tax_completeness,
)


def test_classify_fort_bend_2026_tax_completeness_as_operational_with_caveats() -> None:
    posture = classify_instant_quote_tax_completeness(
        county_id="fort_bend",
        tax_year=2026,
        instant_quote_ready=True,
        basis_tax_year=2025,
        basis_status="prior_year_adopted_rates",
        basis_effective_tax_rate_coverage_ratio=0.997,
        basis_assignment_coverage_ratio=0.9969,
        continuity_parcel_gap_row_count=858,
        continuity_parcel_match_ratio=0.997,
    )

    assert posture.status == INSTANT_QUOTE_TAX_COMPLETENESS_STATUS_OPERATIONAL_WITH_CAVEATS
    assert posture.reason == INSTANT_QUOTE_TAX_COMPLETENESS_REASON_FORT_BEND_REVALIDATION_RESIDUAL_RISK
    assert posture.warning_codes == (
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_ACCEPTABLE_CAUTION_OPERATIONAL,
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RISKY_CAUTION_MONITORED,
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CONTINUITY_GAP_MONITORED,
    )
    assert "Most remaining caution rows appear acceptable" in (posture.internal_note or "")


def test_classify_harris_2026_tax_completeness_as_operational_with_caveats() -> None:
    posture = classify_instant_quote_tax_completeness(
        county_id="harris",
        tax_year=2026,
        instant_quote_ready=True,
        basis_tax_year=2025,
        basis_status="prior_year_adopted_rates",
        basis_effective_tax_rate_coverage_ratio=0.9999,
        basis_assignment_coverage_ratio=0.9999,
        continuity_parcel_gap_row_count=88,
        continuity_parcel_match_ratio=0.9999,
    )

    assert posture.status == INSTANT_QUOTE_TAX_COMPLETENESS_STATUS_OPERATIONAL_WITH_CAVEATS
    assert posture.reason == INSTANT_QUOTE_TAX_COMPLETENESS_REASON_HARRIS_REFRESHED_SPECIAL_FAMILY_RECOVERY
    assert posture.warning_codes == (
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_RECOVERED_SPECIAL_FAMILY_OPERATIONAL,
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CAVEATED_SPECIAL_FAMILY_MONITORED,
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_MISSING_SCHOOL_ASSIGNMENT_MONITORED,
        INSTANT_QUOTE_TAX_COMPLETENESS_WARNING_CONTINUITY_GAP_MONITORED,
    )
    assert "Newport MUD DA 2" in (posture.internal_note or "")
    assert "HC MUD 568 remains a monitored caveated identity" in (posture.internal_note or "")
    assert "residual school-assignment slice" in (posture.internal_note or "")


def test_classify_other_counties_as_no_special_posture() -> None:
    posture = classify_instant_quote_tax_completeness(
        county_id="galveston",
        tax_year=2026,
        instant_quote_ready=True,
        basis_tax_year=2025,
        basis_status="prior_year_adopted_rates",
        basis_effective_tax_rate_coverage_ratio=0.999,
        basis_assignment_coverage_ratio=0.999,
        continuity_parcel_gap_row_count=10,
        continuity_parcel_match_ratio=0.999,
    )

    assert posture.status is None
    assert posture.reason is None
    assert posture.internal_note is None
    assert posture.warning_codes == ()
