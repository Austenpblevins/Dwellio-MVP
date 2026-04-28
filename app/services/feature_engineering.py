from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_parcel_feature_payload(
    *,
    current_summary: Mapping[str, Any],
    prior_summary: Mapping[str, Any] | None = None,
    neighborhood_trend: Mapping[str, Any] | None = None,
    valuation_bathroom_features: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    current_tax_year = int(current_summary["tax_year"])
    prior_tax_year = int(prior_summary["tax_year"]) if prior_summary is not None else None

    payload = {
        "parcel_id": current_summary.get("parcel_id"),
        "county_id": current_summary.get("county_id"),
        "tax_year": current_tax_year,
        "subject": {
            "property_type_code": current_summary.get("property_type_code"),
            "property_class_code": current_summary.get("property_class_code"),
            "neighborhood_code": current_summary.get("neighborhood_code"),
            "subdivision_name": current_summary.get("subdivision_name"),
            "school_district_name": current_summary.get("school_district_name"),
            "living_area_sf": current_summary.get("living_area_sf"),
            "year_built": current_summary.get("year_built"),
            "effective_age": current_summary.get("effective_age"),
            "bedrooms": current_summary.get("bedrooms"),
            "full_baths": current_summary.get("full_baths"),
            "half_baths": current_summary.get("half_baths"),
            "total_rooms": current_summary.get("total_rooms"),
            "land_sf": current_summary.get("land_sf"),
            "land_acres": current_summary.get("land_acres"),
        },
        "current_values": {
            "market_value": current_summary.get("market_value"),
            "appraised_value": current_summary.get("appraised_value"),
            "assessed_value": current_summary.get("assessed_value"),
            "certified_value": current_summary.get("certified_value"),
            "notice_value": current_summary.get("notice_value"),
            "effective_tax_rate": current_summary.get("effective_tax_rate"),
            "estimated_annual_tax": current_summary.get("estimated_annual_tax"),
        },
        "exemptions": {
            "exemption_value_total": current_summary.get("exemption_value_total"),
            "homestead_flag": current_summary.get("homestead_flag"),
            "over65_flag": current_summary.get("over65_flag"),
            "disabled_flag": current_summary.get("disabled_flag"),
            "disabled_veteran_flag": current_summary.get("disabled_veteran_flag"),
            "freeze_flag": current_summary.get("freeze_flag"),
        },
        "ratios": {
            "appraised_to_market_ratio": _safe_ratio(
                current_summary.get("appraised_value"),
                current_summary.get("market_value"),
            ),
            "assessed_to_market_ratio": _safe_ratio(
                current_summary.get("assessed_value"),
                current_summary.get("market_value"),
            ),
            "exemption_share_of_notice": _safe_ratio(
                current_summary.get("exemption_value_total"),
                current_summary.get("notice_value"),
            ),
        },
        "history": {
            "prior_tax_year": prior_tax_year,
            "has_prior_year": prior_summary is not None,
            "appraised_value_change": _change_metrics(
                current_summary.get("appraised_value"),
                prior_summary.get("appraised_value") if prior_summary is not None else None,
            ),
            "assessed_value_change": _change_metrics(
                current_summary.get("assessed_value"),
                prior_summary.get("assessed_value") if prior_summary is not None else None,
            ),
            "notice_value_change": _change_metrics(
                current_summary.get("notice_value"),
                prior_summary.get("notice_value") if prior_summary is not None else None,
            ),
            "effective_tax_rate_change": _change_metrics(
                current_summary.get("effective_tax_rate"),
                prior_summary.get("effective_tax_rate") if prior_summary is not None else None,
            ),
            "estimated_annual_tax_change": _change_metrics(
                current_summary.get("estimated_annual_tax"),
                prior_summary.get("estimated_annual_tax") if prior_summary is not None else None,
            ),
            "exemption_value_change": _change_metrics(
                current_summary.get("exemption_value_total"),
                prior_summary.get("exemption_value_total") if prior_summary is not None else None,
            ),
            "homestead_changed_flag": (
                current_summary.get("homestead_flag")
                is not prior_summary.get("homestead_flag")
                if prior_summary is not None
                else False
            ),
            "exemption_changed_flag": (
                _coerce_number(current_summary.get("exemption_value_total"))
                != _coerce_number(prior_summary.get("exemption_value_total"))
                if prior_summary is not None
                else False
            ),
            "trend_support_weak_flag": prior_summary is None,
        },
        "warning_codes": list(current_summary.get("warning_codes", [])),
        "public_summary_ready_flag": bool(current_summary.get("public_summary_ready_flag", False)),
    }

    if neighborhood_trend is not None:
        payload["neighborhood_trend"] = {
            "period_months": neighborhood_trend.get("period_months"),
            "sale_count": neighborhood_trend.get("sale_count"),
            "prior_sale_count": neighborhood_trend.get("prior_sale_count"),
            "median_sale_psf": neighborhood_trend.get("median_sale_psf"),
            "prior_median_sale_psf": neighborhood_trend.get("prior_median_sale_psf"),
            "median_sale_psf_change": neighborhood_trend.get("median_sale_psf_change"),
            "median_sale_psf_change_pct": neighborhood_trend.get("median_sale_psf_change_pct"),
            "median_sale_price": neighborhood_trend.get("median_sale_price"),
            "prior_median_sale_price": neighborhood_trend.get("prior_median_sale_price"),
            "median_sale_price_change": neighborhood_trend.get("median_sale_price_change"),
            "median_sale_price_change_pct": neighborhood_trend.get("median_sale_price_change_pct"),
            "price_std_dev": neighborhood_trend.get("price_std_dev"),
            "prior_price_std_dev": neighborhood_trend.get("prior_price_std_dev"),
            "price_std_dev_change": neighborhood_trend.get("price_std_dev_change"),
            "weak_sample_support_flag": bool(
                neighborhood_trend.get("weak_sample_support_flag", False)
            ),
        }

    if valuation_bathroom_features is not None:
        payload["valuation_bathroom_features"] = {
            "quick_ref_id": valuation_bathroom_features.get("quick_ref_id"),
            "selected_improvement_number": valuation_bathroom_features.get("selected_improvement_number"),
            "selected_improvement_rule_version": valuation_bathroom_features.get(
                "selected_improvement_rule_version"
            ),
            "normalization_rule_version": valuation_bathroom_features.get(
                "normalization_rule_version"
            ),
            "source_file_version": valuation_bathroom_features.get("source_file_version"),
            "plumbing_raw": valuation_bathroom_features.get("plumbing_raw"),
            "half_baths_raw": valuation_bathroom_features.get("half_baths_raw"),
            "quarter_baths_raw": valuation_bathroom_features.get("quarter_baths_raw"),
            "full_baths_derived": valuation_bathroom_features.get("full_baths_derived"),
            "half_baths_derived": valuation_bathroom_features.get("half_baths_derived"),
            "quarter_baths_derived": valuation_bathroom_features.get("quarter_baths_derived"),
            "bathroom_equivalent_derived": valuation_bathroom_features.get(
                "bathroom_equivalent_derived"
            ),
            "bathroom_count_status": valuation_bathroom_features.get("bathroom_count_status"),
            "bathroom_count_confidence": valuation_bathroom_features.get(
                "bathroom_count_confidence"
            ),
            "bathroom_flags": list(valuation_bathroom_features.get("bathroom_flags", [])),
        }

    return payload


def build_neighborhood_trend_payload(
    *,
    current_stats: Mapping[str, Any],
    prior_stats: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "county_id": current_stats.get("county_id"),
        "tax_year": current_stats.get("tax_year"),
        "prior_tax_year": prior_stats.get("tax_year") if prior_stats is not None else None,
        "neighborhood_code": current_stats.get("neighborhood_code"),
        "property_type_code": current_stats.get("property_type_code"),
        "period_months": current_stats.get("period_months"),
        "sale_count": current_stats.get("sale_count"),
        "prior_sale_count": prior_stats.get("sale_count") if prior_stats is not None else None,
        "median_sale_psf": current_stats.get("median_sale_psf"),
        "prior_median_sale_psf": (
            prior_stats.get("median_sale_psf") if prior_stats is not None else None
        ),
        "median_sale_psf_change": _change_metrics(
            current_stats.get("median_sale_psf"),
            prior_stats.get("median_sale_psf") if prior_stats is not None else None,
        )["amount"],
        "median_sale_psf_change_pct": _change_metrics(
            current_stats.get("median_sale_psf"),
            prior_stats.get("median_sale_psf") if prior_stats is not None else None,
        )["pct"],
        "median_sale_price": current_stats.get("median_sale_price"),
        "prior_median_sale_price": (
            prior_stats.get("median_sale_price") if prior_stats is not None else None
        ),
        "median_sale_price_change": _change_metrics(
            current_stats.get("median_sale_price"),
            prior_stats.get("median_sale_price") if prior_stats is not None else None,
        )["amount"],
        "median_sale_price_change_pct": _change_metrics(
            current_stats.get("median_sale_price"),
            prior_stats.get("median_sale_price") if prior_stats is not None else None,
        )["pct"],
        "price_std_dev": current_stats.get("price_std_dev"),
        "prior_price_std_dev": prior_stats.get("price_std_dev") if prior_stats is not None else None,
        "price_std_dev_change": _change_metrics(
            current_stats.get("price_std_dev"),
            prior_stats.get("price_std_dev") if prior_stats is not None else None,
        )["amount"],
        "weak_sample_support_flag": (
            _coerce_number(current_stats.get("sale_count"), 0.0) < 5
            or _coerce_number(
                prior_stats.get("sale_count") if prior_stats is not None else None,
                0.0,
            )
            < 5
        ),
    }


def _change_metrics(current_value: Any, prior_value: Any) -> dict[str, float | None]:
    current_number = _coerce_number(current_value)
    prior_number = _coerce_number(prior_value)
    if current_number is None or prior_number is None:
        return {"amount": None, "pct": None}

    amount = current_number - prior_number
    pct = None if prior_number == 0 else amount / prior_number
    return {"amount": amount, "pct": pct}


def _safe_ratio(numerator: Any, denominator: Any) -> float | None:
    numerator_value = _coerce_number(numerator)
    denominator_value = _coerce_number(denominator)
    if numerator_value is None or denominator_value in {None, 0.0}:
        return None
    return numerator_value / denominator_value


def _coerce_number(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    return float(value)
