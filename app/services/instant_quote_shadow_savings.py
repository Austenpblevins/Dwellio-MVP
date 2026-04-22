from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

INSTANT_QUOTE_TAX_PROFILE_VERSION = "v5_summary_profile_v1"
SHADOW_LOW_CASH_CAP = 250.0


@dataclass(frozen=True)
class InstantQuoteShadowComparison:
    profile_version: str | None = None
    current_savings_estimate_raw: float | None = None
    shadow_savings_estimate_raw: float | None = None
    shadow_savings_delta_raw: float | None = None
    tax_profile_status: str | None = None
    tax_profile_quality_score: int | None = None
    marginal_model_type: str | None = None
    marginal_tax_rate_total: float | None = None
    opportunity_vs_savings_state: str | None = None
    limiting_reason_codes: tuple[str, ...] = ()
    fallback_tax_profile_used_flag: bool | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["limiting_reason_codes"] = list(self.limiting_reason_codes)
        return payload


def build_shadow_savings_comparison(
    *,
    tax_profile: dict[str, Any] | None,
    reduction_estimate: float | None,
    current_savings_estimate: float | None,
) -> InstantQuoteShadowComparison:
    if tax_profile is None:
        return InstantQuoteShadowComparison(
            profile_version=None,
            current_savings_estimate_raw=_coerce_float(current_savings_estimate),
            shadow_savings_estimate_raw=None,
            shadow_savings_delta_raw=None,
            tax_profile_status=None,
            tax_profile_quality_score=None,
            marginal_model_type=None,
            marginal_tax_rate_total=None,
            opportunity_vs_savings_state=None,
            limiting_reason_codes=("missing_tax_profile",),
            fallback_tax_profile_used_flag=None,
        )

    profile_version = _coerce_text(tax_profile.get("profile_version"))
    tax_profile_status = _coerce_text(tax_profile.get("tax_profile_status"))
    tax_profile_quality_score = _coerce_int(tax_profile.get("tax_profile_quality_score"))
    marginal_model_type = _coerce_text(tax_profile.get("marginal_model_type"))
    marginal_tax_rate_total = _coerce_float(tax_profile.get("marginal_tax_rate_total"))
    marginal_tax_rate_non_school = _coerce_float(tax_profile.get("marginal_tax_rate_non_school"))
    opportunity_vs_savings_state = _coerce_text(tax_profile.get("opportunity_vs_savings_state"))
    fallback_tax_profile_used_flag = _coerce_bool(tax_profile.get("fallback_tax_profile_used_flag"))
    limiting_reason_codes = tuple(
        sorted({str(code) for code in tax_profile.get("savings_limited_by_codes") or [] if str(code)})
    )
    reduction_estimate_value = _coerce_float(reduction_estimate)
    current_savings_estimate_value = _coerce_float(current_savings_estimate)

    shadow_savings_estimate_raw: float | None = None
    if reduction_estimate_value is not None:
        reduction_estimate_value = max(reduction_estimate_value, 0.0)
        if _coerce_bool(tax_profile.get("total_exemption_flag")):
            shadow_savings_estimate_raw = 0.0
        elif tax_profile_status not in {"unsupported", "opportunity_only"}:
            shadow_rate = marginal_tax_rate_total
            if (
                _coerce_bool(tax_profile.get("freeze_flag"))
                and "school_ceiling_amount_unavailable" in limiting_reason_codes
                and marginal_tax_rate_non_school is not None
                and marginal_tax_rate_non_school > 0
            ):
                shadow_rate = marginal_tax_rate_non_school
            if shadow_rate is not None and shadow_rate > 0:
                shadow_savings_estimate_raw = max(reduction_estimate_value * shadow_rate, 0.0)
                if _coerce_bool(tax_profile.get("near_total_exemption_flag")):
                    shadow_savings_estimate_raw = min(
                        shadow_savings_estimate_raw,
                        SHADOW_LOW_CASH_CAP,
                    )

    shadow_savings_delta_raw = (
        None
        if shadow_savings_estimate_raw is None or current_savings_estimate_value is None
        else shadow_savings_estimate_raw - current_savings_estimate_value
    )
    return InstantQuoteShadowComparison(
        profile_version=profile_version,
        current_savings_estimate_raw=current_savings_estimate_value,
        shadow_savings_estimate_raw=shadow_savings_estimate_raw,
        shadow_savings_delta_raw=shadow_savings_delta_raw,
        tax_profile_status=tax_profile_status,
        tax_profile_quality_score=tax_profile_quality_score,
        marginal_model_type=marginal_model_type,
        marginal_tax_rate_total=marginal_tax_rate_total,
        opportunity_vs_savings_state=opportunity_vs_savings_state,
        limiting_reason_codes=limiting_reason_codes,
        fallback_tax_profile_used_flag=fallback_tax_profile_used_flag,
    )


def fetch_shadow_tax_profile(
    connection: object,
    *,
    parcel_id: Any,
    county_id: str,
    tax_year: int,
    profile_version: str = INSTANT_QUOTE_TAX_PROFILE_VERSION,
) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              profile_version,
              parcel_id,
              county_id,
              tax_year,
              assessment_basis_value,
              total_exemption_flag,
              near_total_exemption_flag,
              freeze_flag,
              tax_profile_status,
              tax_profile_quality_score,
              marginal_model_type,
              marginal_tax_rate_total,
              marginal_tax_rate_non_school,
              opportunity_vs_savings_state,
              savings_limited_by_codes,
              fallback_tax_profile_used_flag
            FROM instant_quote_tax_profile
            WHERE parcel_id = %s
              AND county_id = %s
              AND tax_year = %s
              AND profile_version = %s
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (parcel_id, county_id, tax_year, profile_version),
        )
        row = cursor.fetchone()
    return None if row is None else dict(row)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: Any) -> bool:
    return bool(value)
