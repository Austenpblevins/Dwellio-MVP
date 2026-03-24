from __future__ import annotations

from app.services.decision_tree import ProtestDecisionTreeService
from app.services.equity_model import EquityModelService
from app.services.market_model import MarketModelInputs, MarketModelService
from app.services.savings_engine import SavingsEngineService


def test_historical_market_equity_savings_and_decision_services_are_deterministic() -> None:
    market_result = MarketModelService().run(
        MarketModelInputs(
            subject_living_area_sf=2100,
            neighborhood_median_sale_psf_12m=205,
            adjusted_comp_value_psf=[198, 202, 210],
            comp_rank_weights=[0.5, 0.3, 0.2],
        )
    )
    equity_result = EquityModelService().run(
        subject_living_area_sf=2100,
        adjusted_equity_comp_psf=[186, 190, 194, 192],
    )

    defensible_point = min(
        market_result["market_value_point"],
        equity_result["equity_value_point"],
    )
    savings_result = SavingsEngineService().run(
        current_notice_value=430000,
        defensible_value_low=defensible_point * 0.95,
        defensible_value_point=defensible_point,
        defensible_value_high=defensible_point * 1.05,
        effective_tax_rate=0.0202,
        success_probability=0.65,
        contingency_rate=0.35,
    )
    decision_tree_result = ProtestDecisionTreeService().evaluate(
        current_notice_value=430000,
        defensible_value_point=defensible_point,
        valid_comp_count=4,
        expected_tax_savings_point=savings_result["expected_tax_savings_point"],
        confidence_score=0.7,
    )

    assert round(market_result["market_value_point"], 2) == round(
        MarketModelService().run(
            MarketModelInputs(
                subject_living_area_sf=2100,
                neighborhood_median_sale_psf_12m=205,
                adjusted_comp_value_psf=[198, 202, 210],
                comp_rank_weights=[0.5, 0.3, 0.2],
            )
        )["market_value_point"],
        2,
    )
    assert equity_result["equity_value_point"] == 401100.0
    assert savings_result["gross_tax_savings_point"] >= 0
    assert any(rule["rule_code"] == "value_gap_detection" for rule in decision_tree_result)

