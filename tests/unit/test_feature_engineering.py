from __future__ import annotations

import pytest

from app.services.feature_engineering import (
    build_neighborhood_trend_payload,
    build_parcel_feature_payload,
)


def test_build_parcel_feature_payload_includes_yoy_changes() -> None:
    current_summary = {
        "parcel_id": "parcel-1",
        "county_id": "harris",
        "tax_year": 2025,
        "property_type_code": "sfr",
        "property_class_code": "A1",
        "neighborhood_code": "HOU-001",
        "subdivision_name": "Heights",
        "school_district_name": "Houston ISD",
        "living_area_sf": 2000,
        "year_built": 1995,
        "effective_age": 12,
        "bedrooms": 3,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "total_rooms": 7,
        "land_sf": 7200,
        "land_acres": 0.1653,
        "market_value": 420000,
        "appraised_value": 405000,
        "assessed_value": 390000,
        "certified_value": 392000,
        "notice_value": 410000,
        "effective_tax_rate": 0.0201,
        "estimated_annual_tax": 7600,
        "exemption_value_total": 100000,
        "homestead_flag": True,
        "over65_flag": False,
        "disabled_flag": False,
        "disabled_veteran_flag": False,
        "freeze_flag": False,
        "warning_codes": ["missing_geometry"],
        "public_summary_ready_flag": True,
    }
    prior_summary = {
        "tax_year": 2024,
        "appraised_value": 385000,
        "assessed_value": 372000,
        "notice_value": 390000,
        "effective_tax_rate": 0.0196,
        "estimated_annual_tax": 7100,
        "exemption_value_total": 90000,
        "homestead_flag": True,
    }
    neighborhood_trend = {
        "period_months": 12,
        "sale_count": 18,
        "prior_sale_count": 16,
        "median_sale_psf": 210,
        "prior_median_sale_psf": 198,
        "median_sale_psf_change": 12,
        "median_sale_psf_change_pct": 12 / 198,
        "median_sale_price": 435000,
        "prior_median_sale_price": 410000,
        "median_sale_price_change": 25000,
        "median_sale_price_change_pct": 25000 / 410000,
        "price_std_dev": 18.5,
        "prior_price_std_dev": 17.0,
        "price_std_dev_change": 1.5,
        "weak_sample_support_flag": False,
    }

    payload = build_parcel_feature_payload(
        current_summary=current_summary,
        prior_summary=prior_summary,
        neighborhood_trend=neighborhood_trend,
    )

    assert payload["history"]["has_prior_year"] is True
    assert payload["history"]["prior_tax_year"] == 2024
    assert payload["history"]["appraised_value_change"]["amount"] == 20000.0
    assert payload["history"]["assessed_value_change"]["pct"] == 18000.0 / 372000.0
    assert payload["history"]["effective_tax_rate_change"]["amount"] == pytest.approx(0.0005)
    assert payload["history"]["exemption_changed_flag"] is True
    assert payload["ratios"]["appraised_to_market_ratio"] == 405000.0 / 420000.0
    assert payload["subject"]["total_rooms"] == 7
    assert payload["neighborhood_trend"]["median_sale_psf_change"] == 12


def test_build_parcel_feature_payload_marks_weak_history_without_prior_year() -> None:
    payload = build_parcel_feature_payload(
        current_summary={"parcel_id": "parcel-2", "county_id": "fort_bend", "tax_year": 2026},
    )

    assert payload["history"]["has_prior_year"] is False
    assert payload["history"]["trend_support_weak_flag"] is True
    assert payload["history"]["notice_value_change"]["amount"] is None


def test_build_neighborhood_trend_payload_flags_weak_samples() -> None:
    payload = build_neighborhood_trend_payload(
        current_stats={
            "county_id": "harris",
            "tax_year": 2025,
            "neighborhood_code": "HOU-001",
            "property_type_code": "sfr",
            "period_months": 12,
            "sale_count": 4,
            "median_sale_psf": 205,
            "median_sale_price": 420000,
            "price_std_dev": 19,
        },
        prior_stats={
            "tax_year": 2024,
            "sale_count": 7,
            "median_sale_psf": 198,
            "median_sale_price": 405000,
            "price_std_dev": 17,
        },
    )

    assert payload["median_sale_psf_change"] == 7.0
    assert payload["median_sale_price_change_pct"] == 15000.0 / 405000.0
    assert payload["weak_sample_support_flag"] is True


def test_build_parcel_feature_payload_includes_additive_valuation_bathroom_features() -> None:
    payload = build_parcel_feature_payload(
        current_summary={"parcel_id": "parcel-3", "county_id": "fort_bend", "tax_year": 2026},
        valuation_bathroom_features={
            "quick_ref_id": "R100",
            "selected_improvement_number": "1",
            "selected_improvement_rule_version": "fort_bend_primary_residential_improvement_v1",
            "normalization_rule_version": "fort_bend_bathroom_features_v1",
            "source_file_version": "WebsiteResidentialSegs.csv:sha256:test",
            "plumbing_raw": 2.5,
            "half_baths_raw": None,
            "quarter_baths_raw": 0,
            "full_baths_derived": 2.0,
            "half_baths_derived": 1.0,
            "quarter_baths_derived": 0.0,
            "bathroom_equivalent_derived": 2.5,
            "bathroom_count_status": "reconciled_fractional_plumbing",
            "bathroom_count_confidence": "medium",
            "bathroom_flags": ["fractional_plumbing_source"],
        },
    )

    assert payload["valuation_bathroom_features"]["quick_ref_id"] == "R100"
    assert payload["valuation_bathroom_features"]["full_baths_derived"] == 2.0
    assert payload["valuation_bathroom_features"]["bathroom_count_status"] == (
        "reconciled_fractional_plumbing"
    )
