from __future__ import annotations

from app.services.quote_generation import QuoteGenerationInputs, QuoteGenerationService


def test_quote_generation_builds_defensible_value_from_market_and_equity_minimum() -> None:
    service = QuoteGenerationService()

    valuation = service._build_valuation(
        QuoteGenerationInputs(
            parcel_id="11111111-1111-1111-1111-111111111111",
            county_id="harris",
            tax_year=2026,
            account_number="1001001001001",
            neighborhood_code="HOU-001",
            living_area_sf=2150,
            notice_value=360000,
            market_value=350000,
            assessed_value=330000,
            effective_tax_rate=0.0201,
        )
    )

    assert valuation["defensible_value_point"] == min(
        valuation["market_value_point"],
        valuation["equity_value_point"],
    )
    assert valuation["model_inputs_json"]["generation_mode"] == "stage13_public_summary_proxy"


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
