from __future__ import annotations

from dataclasses import dataclass

from app.core.config import get_settings
from app.models.quote import InstantQuoteResponse


@dataclass(frozen=True)
class SavingsTranslationRolloutDecision:
    savings_translation_mode: str
    savings_translation_reason_code: str
    savings_translation_applied_flag: bool
    selected_public_savings_estimate_raw: float | None
    extra_disclaimers: tuple[str, ...] = ()


def decide_savings_translation_rollout(
    *,
    county_id: str,
    response_supported: bool,
    unsupported_reason: str | None,
    public_rollout_state: str | None,
    current_savings_estimate_raw: float | None,
    shadow_savings_estimate_raw: float | None,
    shadow_tax_profile_status: str | None,
    shadow_limiting_reason_codes: list[str] | tuple[str, ...] | None,
    shadow_fallback_tax_profile_used_flag: bool | None,
) -> SavingsTranslationRolloutDecision:
    settings = get_settings()
    limiting_reason_codes = tuple(
        sorted({str(code) for code in shadow_limiting_reason_codes or [] if str(code).strip()})
    )
    current_savings_value = _coerce_float(current_savings_estimate_raw)
    shadow_savings_value = _coerce_float(shadow_savings_estimate_raw)

    if not settings.instant_quote_v5_savings_translation_enabled:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="flag_disabled",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    if not response_supported or unsupported_reason is not None:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="unsupported_or_manual_review_state",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    allowed_counties = _parse_csv(settings.instant_quote_v5_savings_translation_county_ids)
    if allowed_counties and county_id not in allowed_counties:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="county_not_enabled",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    allowed_states = _parse_csv(settings.instant_quote_v5_savings_translation_rollout_states)
    rollout_state = str(public_rollout_state or "").strip()
    if rollout_state not in allowed_states:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="rollout_state_not_enabled",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    if shadow_savings_value is None:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="missing_shadow_savings_estimate",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    if shadow_tax_profile_status not in {"supported_with_disclosure", "constrained"}:
        return SavingsTranslationRolloutDecision(
            savings_translation_mode="current_public_formula",
            savings_translation_reason_code="shadow_tax_profile_not_quoteable",
            savings_translation_applied_flag=False,
            selected_public_savings_estimate_raw=current_savings_value,
        )

    extra_disclaimers = (
        "This public savings range uses the summary tax profile for this cohort because the standard fast estimate can overstate current-year cash savings.",
    )
    if bool(shadow_fallback_tax_profile_used_flag) or "tax_rate_basis_fallback_applied" in limiting_reason_codes:
        extra_disclaimers = extra_disclaimers + (
            "The translated savings range still relies on a fallback summary tax profile and can change after refined review.",
        )

    return SavingsTranslationRolloutDecision(
        savings_translation_mode="v5_shadow_tax_profile_rollout",
        savings_translation_reason_code=f"rollout_state_{rollout_state}",
        savings_translation_applied_flag=True,
        selected_public_savings_estimate_raw=shadow_savings_value,
        extra_disclaimers=extra_disclaimers,
    )


def apply_savings_translation_rollout(
    *,
    response: InstantQuoteResponse,
    translated_estimate,
    decision: SavingsTranslationRolloutDecision,
) -> InstantQuoteResponse:
    if not decision.savings_translation_applied_flag:
        return response

    disclaimers = _merge_unique_strings(response.disclaimers, decision.extra_disclaimers)
    return response.model_copy(
        update={
            "estimate": translated_estimate,
            "disclaimers": disclaimers,
        }
    )


def _parse_csv(value: str) -> set[str]:
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


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


def _coerce_float(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value)
