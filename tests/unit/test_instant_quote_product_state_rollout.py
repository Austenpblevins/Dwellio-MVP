from uuid import uuid4

from app.models.quote import (
    InstantQuoteEstimate,
    InstantQuoteExplanation,
    InstantQuoteResponse,
    InstantQuoteSubject,
)
from app.services.instant_quote import REFINED_REVIEW_CTA
from app.services.instant_quote_product_state_rollout import (
    apply_public_product_state_rollout,
    decide_public_product_state_rollout,
)


def _supported_response() -> InstantQuoteResponse:
    return InstantQuoteResponse(
        supported=True,
        county_id="fort_bend",
        tax_year=2026,
        requested_tax_year=2026,
        served_tax_year=2026,
        tax_year_fallback_applied=False,
        tax_year_fallback_reason=None,
        data_freshness_label="current_year",
        account_number="123",
        basis_code="assessment_basis_segment_blend",
        subject=InstantQuoteSubject(
            parcel_id=uuid4(),
            address="123 Main St",
            neighborhood_code="1001",
            school_district_name="Fort Bend ISD",
            property_type_code="sfr",
            property_class_code="A1",
            living_area_sf=2000.0,
            year_built=1995,
            notice_value=350000.0,
            homestead_flag=True,
            freeze_flag=False,
        ),
        estimate=InstantQuoteEstimate(
            savings_range_low=950.0,
            savings_range_high=1450.0,
            savings_midpoint_display=1200.0,
            estimate_bucket="500_to_1499",
            estimate_strength_label="medium",
            tax_protection_limited=False,
            tax_protection_note=None,
        ),
        explanation=InstantQuoteExplanation(
            methodology="segment_within_neighborhood",
            estimate_strength_label="medium",
            summary="Base summary",
            bullets=["Base bullet"],
            limitation_note=None,
        ),
        disclaimers=["Base disclaimer"],
        unsupported_reason=None,
        next_step_cta=None,
    )


def _unsupported_response() -> InstantQuoteResponse:
    return InstantQuoteResponse(
        supported=False,
        county_id="fort_bend",
        tax_year=2026,
        requested_tax_year=2026,
        served_tax_year=2026,
        tax_year_fallback_applied=False,
        tax_year_fallback_reason=None,
        data_freshness_label="current_year",
        account_number="123",
        basis_code="assessment_basis_neighborhood_only",
        subject=InstantQuoteSubject(
            parcel_id=uuid4(),
            address="123 Main St",
            neighborhood_code="1001",
            school_district_name="Fort Bend ISD",
            property_type_code="sfr",
            property_class_code="A1",
            living_area_sf=2000.0,
            year_built=1995,
            notice_value=350000.0,
            homestead_flag=True,
            freeze_flag=False,
        ),
        estimate=None,
        explanation=InstantQuoteExplanation(
            methodology="neighborhood_only",
            estimate_strength_label="low",
            summary="Base unsupported summary",
            bullets=["Base unsupported bullet"],
            limitation_note=None,
        ),
        disclaimers=["Base disclaimer"],
        unsupported_reason="low_confidence_refined_review",
        next_step_cta=REFINED_REVIEW_CTA,
    )


def test_total_exemption_rollout_keeps_public_estimate_but_changes_presentation() -> None:
    rollout = decide_public_product_state_rollout(
        response_supported=True,
        unsupported_reason=None,
        internal_opportunity_state="standard_quote",
        internal_product_state_reason_code="public_safe_standard_quote",
        shadow_tax_profile_status="supported_with_disclosure",
        shadow_opportunity_vs_savings_state="total_exemption_low_cash",
        shadow_limiting_reason_codes=[
            "profile_support_level_summary_only",
            "tax_rate_basis_fallback_applied",
            "total_exemption_likely",
        ],
        shadow_savings_estimate_raw=0.0,
        shadow_fallback_tax_profile_used_flag=True,
        refined_review_cta=REFINED_REVIEW_CTA,
    )

    updated = apply_public_product_state_rollout(
        response=_supported_response(),
        rollout=rollout,
    )

    assert rollout.public_rollout_state == "total_exemption_low_cash"
    assert updated.estimate is not None
    assert updated.estimate.savings_midpoint_display == 1200.0
    assert updated.next_step_cta == REFINED_REVIEW_CTA
    assert "exemptions likely already absorb" in updated.explanation.summary
    assert "current-year cash savings" in (updated.explanation.limitation_note or "")
    assert "fallback summary tax profile" in " ".join(updated.disclaimers)


def test_manual_review_shadow_quoteable_rollout_keeps_request_unsupported() -> None:
    rollout = decide_public_product_state_rollout(
        response_supported=False,
        unsupported_reason="low_confidence_refined_review",
        internal_opportunity_state="manual_review_recommended",
        internal_product_state_reason_code="unsupported_reason_low_confidence_refined_review",
        shadow_tax_profile_status="supported_with_disclosure",
        shadow_opportunity_vs_savings_state="standard_quote",
        shadow_limiting_reason_codes=["tax_rate_basis_fallback_applied"],
        shadow_savings_estimate_raw=93244.0,
        shadow_fallback_tax_profile_used_flag=True,
        refined_review_cta=REFINED_REVIEW_CTA,
    )

    updated = apply_public_product_state_rollout(
        response=_unsupported_response(),
        rollout=rollout,
    )

    assert rollout.public_rollout_state == "manual_review_shadow_quoteable"
    assert updated.supported is False
    assert updated.unsupported_reason == "low_confidence_refined_review"
    assert "needs a refined review" in updated.explanation.summary
    assert updated.next_step_cta == REFINED_REVIEW_CTA


def test_missing_assessment_basis_stays_explicit_in_rollout_mapping() -> None:
    rollout = decide_public_product_state_rollout(
        response_supported=False,
        unsupported_reason="missing_assessment_basis",
        internal_opportunity_state="suppressed_data_quality",
        internal_product_state_reason_code="support_blocker_missing_assessment_basis",
        shadow_tax_profile_status="opportunity_only",
        shadow_opportunity_vs_savings_state="opportunity_only_tax_profile_incomplete",
        shadow_limiting_reason_codes=["missing_assessment_basis"],
        shadow_savings_estimate_raw=None,
        shadow_fallback_tax_profile_used_flag=True,
        refined_review_cta=REFINED_REVIEW_CTA,
    )

    assert rollout.public_rollout_state == "suppressed_data_quality"
    assert rollout.public_rollout_reason_code == "support_blocker_missing_assessment_basis"
    assert rollout.applied_public_change is False
