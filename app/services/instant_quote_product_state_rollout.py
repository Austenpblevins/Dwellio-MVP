from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.models.quote import InstantQuoteResponse


@dataclass(frozen=True)
class PublicProductStateRollout:
    public_rollout_state: str
    public_rollout_reason_code: str
    summary_override: str | None = None
    limitation_note_override: str | None = None
    tax_protection_note_override: str | None = None
    next_step_cta_override: str | None = None
    extra_bullets: tuple[str, ...] = ()
    extra_disclaimers: tuple[str, ...] = ()
    applied_public_change: bool = False


def decide_public_product_state_rollout(
    *,
    response_supported: bool,
    unsupported_reason: str | None,
    internal_opportunity_state: str | None,
    internal_product_state_reason_code: str | None,
    shadow_tax_profile_status: str | None,
    shadow_opportunity_vs_savings_state: str | None,
    shadow_limiting_reason_codes: list[str] | tuple[str, ...] | None,
    shadow_savings_estimate_raw: float | None,
    shadow_fallback_tax_profile_used_flag: bool | None,
    refined_review_cta: str,
) -> PublicProductStateRollout:
    limiting_reason_codes = tuple(
        sorted({str(code) for code in shadow_limiting_reason_codes or [] if str(code).strip()})
    )
    using_shadow_fallback = bool(shadow_fallback_tax_profile_used_flag) or (
        "tax_rate_basis_fallback_applied" in limiting_reason_codes
    )

    if internal_product_state_reason_code == "support_blocker_missing_assessment_basis":
        return PublicProductStateRollout(
            public_rollout_state="suppressed_data_quality",
            public_rollout_reason_code="support_blocker_missing_assessment_basis",
        )

    if unsupported_reason == "low_confidence_refined_review" and shadow_savings_estimate_raw is not None:
        return PublicProductStateRollout(
            public_rollout_state="manual_review_shadow_quoteable",
            public_rollout_reason_code="low_confidence_public_quote_shadow_quoteable",
            summary_override=(
                "We found protest signal, but this parcel still needs a refined review "
                "before we show a safe public savings range."
            ),
            limitation_note_override=(
                "The summary tax profile still shows possible savings, but the public quote "
                "confidence is too low to publish a safe instant number."
            ),
            next_step_cta_override=refined_review_cta,
            extra_bullets=(
                "The parcel shows analytical protest signal under the internal tax-profile path.",
                "Public quote confidence is still too low for a consumer-safe instant range.",
            ),
            extra_disclaimers=_fallback_rollout_disclaimers(using_shadow_fallback),
            applied_public_change=True,
        )

    if shadow_opportunity_vs_savings_state == "total_exemption_low_cash":
        return PublicProductStateRollout(
            public_rollout_state="total_exemption_low_cash",
            public_rollout_reason_code="shadow_state_total_exemption_low_cash",
            summary_override=(
                "We found protest signal, but exemptions likely already absorb most or all "
                "current-year cash savings."
            ),
            limitation_note_override=(
                "This looks more like an opportunity signal than a likely current-year cash "
                "savings event because exemptions appear to dominate the tax profile."
            ),
            tax_protection_note_override=(
                "Exemptions likely already absorb most or all current-year cash savings."
            ),
            next_step_cta_override=refined_review_cta,
            extra_bullets=(
                "The value signal can still matter, but realized near-term cash savings may stay near zero.",
            ),
            extra_disclaimers=_fallback_rollout_disclaimers(using_shadow_fallback),
            applied_public_change=True,
        )

    if shadow_opportunity_vs_savings_state == "near_total_exemption_low_cash":
        return PublicProductStateRollout(
            public_rollout_state="near_total_exemption_low_cash",
            public_rollout_reason_code="shadow_state_near_total_exemption_low_cash",
            summary_override=(
                "We found protest signal, but this parcel already appears close to full tax "
                "protection, so current-year cash savings may stay low."
            ),
            limitation_note_override=(
                "The tax profile suggests near-total exemption coverage, so this estimate "
                "should be treated as an opportunity signal instead of a confident cash outcome."
            ),
            tax_protection_note_override=(
                "This parcel appears close to full tax protection, which can keep current-year savings low."
            ),
            next_step_cta_override=refined_review_cta,
            extra_disclaimers=_fallback_rollout_disclaimers(using_shadow_fallback),
            applied_public_change=True,
        )

    if shadow_opportunity_vs_savings_state == "school_limited_non_school_possible":
        return PublicProductStateRollout(
            public_rollout_state="school_limited_non_school_possible",
            public_rollout_reason_code="shadow_state_school_limited_non_school_possible",
            summary_override=(
                "We found protest opportunity, but school-tax protections may limit current-year "
                "cash savings more than non-school taxes."
            ),
            limitation_note_override=(
                "School-related tax effects appear limited, so the public savings range should "
                "be treated as directional rather than fully realized current-year cash."
            ),
            tax_protection_note_override=(
                "School-tax protections may limit current-year savings more than non-school taxes."
            ),
            next_step_cta_override=refined_review_cta,
            extra_disclaimers=_fallback_rollout_disclaimers(using_shadow_fallback),
            applied_public_change=True,
        )

    if (
        shadow_opportunity_vs_savings_state == "opportunity_only_tax_profile_incomplete"
        or internal_opportunity_state == "opportunity_only_tax_profile_incomplete"
    ):
        summary_override = (
            "We found protest signal, but the current tax profile is too limited to treat this "
            "as a reliable cash-savings quote."
            if response_supported
            else "We found protest signal, but the current tax profile is too limited for a safe public savings range."
        )
        return PublicProductStateRollout(
            public_rollout_state="opportunity_only_tax_profile_incomplete",
            public_rollout_reason_code=(
                "shadow_state_opportunity_only_tax_profile_incomplete"
                if shadow_opportunity_vs_savings_state == "opportunity_only_tax_profile_incomplete"
                else "internal_state_opportunity_only_tax_profile_incomplete"
            ),
            summary_override=summary_override,
            limitation_note_override=(
                "Treat this parcel as an opportunity signal, not a confident promise of current-year cash savings."
            ),
            next_step_cta_override=refined_review_cta,
            extra_bullets=(
                "The current tax profile is incomplete enough that a refined review is safer than a public cash promise.",
            ),
            extra_disclaimers=_fallback_rollout_disclaimers(using_shadow_fallback),
            applied_public_change=True,
        )

    if internal_opportunity_state in {"strong_opportunity_low_cash", "supported_opportunity_low_cash"}:
        return PublicProductStateRollout(
            public_rollout_state="high_opportunity_low_cash",
            public_rollout_reason_code=internal_opportunity_state,
            summary_override=(
                "We found protest opportunity, but the likely current-year cash savings still looks modest."
            ),
            limitation_note_override=(
                "The value gap may still be worth pursuing even when the near-term public cash range stays low."
            ),
            next_step_cta_override=refined_review_cta,
            applied_public_change=True,
        )

    if internal_product_state_reason_code:
        return PublicProductStateRollout(
            public_rollout_state=internal_opportunity_state or "standard_quote",
            public_rollout_reason_code=internal_product_state_reason_code,
        )

    return PublicProductStateRollout(
        public_rollout_state=internal_opportunity_state or "standard_quote",
        public_rollout_reason_code="no_state_rollout_override",
    )


def apply_public_product_state_rollout(
    *,
    response: InstantQuoteResponse,
    rollout: PublicProductStateRollout,
) -> InstantQuoteResponse:
    explanation = response.explanation
    estimate = response.estimate

    explanation_update: dict[str, Any] = {}
    if rollout.summary_override is not None:
        explanation_update["summary"] = rollout.summary_override
    if rollout.limitation_note_override is not None:
        explanation_update["limitation_note"] = rollout.limitation_note_override
    if rollout.extra_bullets:
        explanation_update["bullets"] = _merge_unique_strings(
            explanation.bullets,
            rollout.extra_bullets,
        )
    if explanation_update:
        explanation = explanation.model_copy(update=explanation_update)

    if estimate is not None and rollout.tax_protection_note_override is not None:
        estimate = estimate.model_copy(
            update={"tax_protection_note": rollout.tax_protection_note_override}
        )

    next_step_cta = response.next_step_cta
    if rollout.next_step_cta_override is not None:
        next_step_cta = rollout.next_step_cta_override

    disclaimers = _merge_unique_strings(response.disclaimers, rollout.extra_disclaimers)

    return response.model_copy(
        update={
            "estimate": estimate,
            "explanation": explanation,
            "next_step_cta": next_step_cta,
            "disclaimers": disclaimers,
        }
    )


def _merge_unique_strings(existing: list[str], additions: tuple[str, ...]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*existing, *additions]:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(text)
    return merged


def _fallback_rollout_disclaimers(using_shadow_fallback: bool) -> tuple[str, ...]:
    if not using_shadow_fallback:
        return ()
    return (
        "State-aware notes rely on a fallback summary tax profile and may change after refined review.",
    )
