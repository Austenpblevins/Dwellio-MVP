from __future__ import annotations

from app.services.quote_generation import (
    QuoteGenerationInputs,
    QuoteGenerationService,
    QuotePeerStats,
)


def test_quote_generation_builds_defensible_value_from_real_peer_stats() -> None:
    service = QuoteGenerationService()

    inputs = QuoteGenerationInputs(
        parcel_id="11111111-1111-1111-1111-111111111111",
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
        neighborhood_code="HOU-001",
        size_bucket="2000_2399",
        age_bucket="1990_2004",
        living_area_sf=2150,
        notice_value=360000,
        market_value=350000,
        assessed_value=330000,
        assessment_basis_value=330000,
        subject_assessed_psf=153.4884,
        effective_tax_rate=0.0201,
    )
    valuation = service._build_valuation(
        inputs,
        segment_stats=QuotePeerStats(
            peer_count=11,
            market_p25_psf=142,
            market_p50_psf=150,
            market_p75_psf=158,
            market_cv=0.18,
            assessed_p25_psf=130,
            assessed_p50_psf=138,
            assessed_p75_psf=145,
            assessed_cv=0.16,
        ),
        neighborhood_stats=QuotePeerStats(
            peer_count=48,
            market_p25_psf=140,
            market_p50_psf=148,
            market_p75_psf=156,
            market_cv=0.14,
            assessed_p25_psf=128,
            assessed_p50_psf=136,
            assessed_p75_psf=143,
            assessed_cv=0.12,
        ),
    )

    assert valuation["defensible_value_point"] == min(
        valuation["market_value_point"],
        valuation["equity_value_point"],
    )
    assert valuation["model_inputs_json"]["generation_mode"] == "stage17_reasonableness_benchmark"
    assert valuation["model_inputs_json"]["support_scope"] == "segment_within_neighborhood"
    assert valuation["model_inputs_json"]["valid_comp_count"] == 11


def test_quote_generation_falls_back_to_neighborhood_only_when_segment_support_is_thin() -> None:
    service = QuoteGenerationService()

    valuation = service._build_valuation(
        QuoteGenerationInputs(
            parcel_id="11111111-1111-1111-1111-111111111111",
            county_id="harris",
            tax_year=2026,
            account_number="1001001001001",
            neighborhood_code="HOU-001",
            size_bucket="2000_2399",
            age_bucket="1990_2004",
            living_area_sf=2150,
            notice_value=360000,
            market_value=350000,
            assessed_value=330000,
            assessment_basis_value=330000,
            subject_assessed_psf=153.4884,
            effective_tax_rate=0.0201,
        ),
        segment_stats=QuotePeerStats(
            peer_count=4,
            market_p25_psf=142,
            market_p50_psf=150,
            market_p75_psf=158,
            market_cv=0.18,
            assessed_p25_psf=130,
            assessed_p50_psf=138,
            assessed_p75_psf=145,
            assessed_cv=0.16,
        ),
        neighborhood_stats=QuotePeerStats(
            peer_count=48,
            market_p25_psf=140,
            market_p50_psf=148,
            market_p75_psf=156,
            market_cv=0.14,
            assessed_p25_psf=128,
            assessed_p50_psf=136,
            assessed_p75_psf=143,
            assessed_cv=0.12,
        ),
    )

    assert valuation is not None
    assert valuation["model_inputs_json"]["support_scope"] == "neighborhood_only"
    assert valuation["model_inputs_json"]["valid_comp_count"] == 48


def test_quote_generation_returns_none_when_neighborhood_support_is_too_thin() -> None:
    service = QuoteGenerationService()

    valuation = service._build_valuation(
        QuoteGenerationInputs(
            parcel_id="11111111-1111-1111-1111-111111111111",
            county_id="harris",
            tax_year=2026,
            account_number="1001001001001",
            neighborhood_code="HOU-001",
            size_bucket="2000_2399",
            age_bucket="1990_2004",
            living_area_sf=2150,
            notice_value=360000,
            market_value=350000,
            assessed_value=330000,
            assessment_basis_value=330000,
            subject_assessed_psf=153.4884,
            effective_tax_rate=0.0201,
        ),
        segment_stats=None,
        neighborhood_stats=QuotePeerStats(
            peer_count=12,
            market_p25_psf=140,
            market_p50_psf=148,
            market_p75_psf=156,
            market_cv=0.14,
            assessed_p25_psf=128,
            assessed_p50_psf=136,
            assessed_p75_psf=143,
            assessed_cv=0.12,
        ),
    )

    assert valuation is None


def test_quote_generation_recommendation_mapping_prefers_canonical_404_safe_states() -> None:
    service = QuoteGenerationService()

    assert service.derive_recommendation_code(
        [{"rule_result": "pass"}, {"rule_result": "pass"}]
    ) == "file_protest"
    assert service.derive_recommendation_code(
        [{"rule_result": "manual_review"}, {"rule_result": "pass"}]
    ) == "manual_review"
    assert service.derive_recommendation_code(
        [{"rule_result": "reject"}]
    ) == "reject"
