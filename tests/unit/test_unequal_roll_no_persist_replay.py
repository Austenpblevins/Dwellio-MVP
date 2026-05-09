from __future__ import annotations

import json
from unittest.mock import MagicMock

from app.services.unequal_roll_no_persist_replay import (
    UnequalRollNoPersistReplayService,
    subject_requests_from_runtime_artifact,
)
from app.services.unequal_roll_smart_harvest import SIMILARITY_TOP_100, SameNeighborhoodHarvestSelection


def test_subject_requests_from_runtime_artifact_extracts_requests(tmp_path) -> None:
    artifact_path = tmp_path / "runtime.json"
    artifact_path.write_text(
        json.dumps(
            {
                "subjects": [
                    {"subject_identifier": "A1", "county": "harris"},
                    {"subject_identifier": "B2", "county": "fort_bend"},
                    {"subject_identifier": "", "county": "harris"},
                ]
            }
        )
    )

    requests = subject_requests_from_runtime_artifact(
        artifact_path,
        requested_tax_year=2025,
    )

    assert [(item.county_id, item.account_number) for item in requests] == [
        ("harris", "A1"),
        ("fort_bend", "B2"),
    ]


def test_build_full_final_value_detail_preserves_similarity_and_line_items() -> None:
    service = UnequalRollNoPersistReplayService()
    candidates = [
        {
            "unequal_roll_candidate_id": "c1",
            "normalized_similarity_score": 0.9812,
        }
    ]
    adjustment_math_plan = {
        "c1": {
            "line_items": [
                {"adjustment_type": "gla", "signed_adjustment_amount": 1200.0},
                {"adjustment_type": "full_bath", "signed_adjustment_amount": -500.0},
            ]
        }
    }
    final_value_output = {
        "final_value_status": "supported",
        "requested_roll_value": 200000.0,
        "requested_reduction_amount": 10000.0,
        "requested_reduction_pct": 0.05,
        "final_value_set_summary": {"included_count": 1},
        "ordered_adjusted_values": [
            {
                "final_value_position": 1,
                "unequal_roll_candidate_id": "c1",
                "candidate_parcel_id": "p1",
                "address": "123 Main",
                "chosen_comp_status": "chosen_comp",
                "review_visible_flag": False,
                "acceptable_zone_admitted_flag": False,
                "adjusted_appraised_value": 200000.0,
                "adjusted_appraised_value_per_sf": 150.0,
            }
        ],
        "median_calculation": {"count": 1},
        "stability_metrics": {"median_all": 200000.0},
        "qa_flags": {},
        "included_comp_rows": [
            {
                "unequal_roll_candidate_id": "c1",
                "candidate_parcel_id": "p1",
                "address": "123 Main",
                "final_value_status": "included_in_final_value",
                "chosen_comp_status": "chosen_comp",
                "chosen_comp_position": 1,
                "review_visible_flag": False,
                "acceptable_zone_governance": {},
                "adjusted_appraised_value": 200000.0,
                "adjusted_appraised_value_per_sf": 150.0,
                "raw_appraised_value": 198000.0,
                "raw_appraised_value_per_sf": 148.0,
                "adjustment_math_status": "adjusted",
                "adjusted_set_governance_status": "usable_adjusted_comp",
                "adjusted_set_governance_reason_codes": [],
                "burden_governance_status": "within_thresholds",
                "burden_governance_reason_codes": [],
                "source_governance_status": "fully_supported",
                "review_carry_forward_flag": False,
                "hybrid_supported_source_flag": False,
                "unresolved_review_only_channel_count": 0,
                "material_adjustment_count": 2,
                "adjustment_pct_of_raw_value": 0.01,
                "dominant_adjustment_channel": "gla",
                "conflict_divergence_governance": {},
                "bathroom_boundary_context": {},
            }
        ],
        "excluded_comp_rows": [],
        "methodology_guardrails": {},
        "carried_forward_governance": {},
    }

    detail = service._build_full_final_value_detail(
        final_value_output=final_value_output,
        candidates=candidates,
        adjustment_math_plan=adjustment_math_plan,
        run_context={"selection_log_json": {}},
    )

    included_row = detail["included_comp_rows"][0]
    ordered_row = detail["ordered_adjusted_values"][0]

    assert included_row["similarity_score"] == 0.9812
    assert len(included_row["line_items"]) == 2
    assert ordered_row["similarity_score"] == 0.9812


def test_select_same_neighborhood_rows_uses_unbounded_fetch_for_smart_strategy() -> None:
    service = UnequalRollNoPersistReplayService()
    subject_snapshot = {"county_id": "harris", "neighborhood_code": "N1"}
    cursor = object()
    service._discovery_service = MagicMock()
    service._discovery_service._fetch_same_neighborhood_candidates.return_value = [
        {"account_number": "100"},
        {"account_number": "101"},
    ]

    selection = service._select_same_neighborhood_rows(
        cursor=cursor,
        subject_snapshot=subject_snapshot,
        same_neighborhood_harvest_strategy=SIMILARITY_TOP_100,
    )

    assert selection.strategy == SIMILARITY_TOP_100
    service._discovery_service._fetch_same_neighborhood_candidates.assert_called_once_with(
        cursor,
        subject_snapshot=subject_snapshot,
        limit=None,
    )


def test_discover_candidates_uses_selection_override_without_fetching_strategy_rows() -> None:
    service = UnequalRollNoPersistReplayService()
    subject_snapshot = {"county_id": "harris", "tax_year": 2026, "neighborhood_code": "N1"}
    cursor = object()
    service._discovery_service = MagicMock()
    service._discovery_service._fetch_county_sfr_fallback_candidates.return_value = []
    service._discovery_service._fetch_candidate_valuation_bathroom_features_json.return_value = {}
    service._discovery_service._evaluate_candidate_eligibility.return_value = (
        "eligible",
        None,
        {},
    )
    service._discovery_service._build_source_provenance_json.return_value = {}
    service._discovery_service._build_candidate_snapshot_json.return_value = {"bathroom_support": {}}

    selection_override = SameNeighborhoodHarvestSelection(
        strategy="experimental_full_reranking_v1",
        universe_count=2,
        selected_count=1,
        cap_used=1,
        excluded_by_cap=1,
        scored_universe=[{"account_number": "100", "experimental_score": 91.2}],
        selected_rows=[
            {
                "parcel_id": "p1",
                "county_id": "harris",
                "tax_year": 2026,
                "account_number": "100",
                "address": "123 Main",
                "neighborhood_code": "N1",
                "subdivision_name": "Oak",
                "property_type_code": "sfr",
                "property_class_code": "A1",
                "living_area_sf": 2000.0,
                "year_built": 2000,
                "effective_age": 10.0,
                "bedrooms": 4,
                "full_baths": 2.0,
                "half_baths": 1.0,
                "total_rooms": 8,
                "stories": 2.0,
                "quality_code": "AVG",
                "condition_code": "AVG",
                "pool_flag": False,
                "land_sf": 7000.0,
                "land_acres": 7000.0 / 43560.0,
                "frontage_sf": None,
                "depth_sf": None,
                "market_value": 310000.0,
                "assessed_value": 310000.0,
                "appraised_value": 310000.0,
                "certified_value": 310000.0,
                "notice_value": 310000.0,
            }
        ],
    )

    candidates, discovery_summary = service._discover_candidates(
        cursor,
        subject_snapshot=subject_snapshot,
        same_neighborhood_harvest_strategy=SIMILARITY_TOP_100,
        same_neighborhood_selection_override=selection_override,
    )

    assert discovery_summary["same_neighborhood_harvest_strategy"] == "experimental_full_reranking_v1"
    assert candidates[0]["account_number"] == "100"
    service._discovery_service._fetch_same_neighborhood_candidates.assert_not_called()


def test_summarize_taxpayer_favorable_tiebreak_review_preserves_not_evaluated() -> None:
    service = UnequalRollNoPersistReplayService()
    result = service._build_taxpayer_favorable_tiebreak_review(
        cursor=object(),
        request=type("Req", (), {"county_id": "harris", "account_number": "A1", "requested_tax_year": 2026})(),
        smart_result={"replay_status": "completed", "final_value_detail_json": {"included_comp_rows": []}},
        same_neighborhood_harvest_strategy="current_order_cap_100",
        statement_timeout="120s",
        max_parallel_workers_per_gather=0,
        include_discovery_debug=False,
    )

    assert result["taxpayer_favorable_tiebreak_class"] == "not_evaluated"
    assert result["taxpayer_favorable_tiebreak_primary_reason"] == "current_strategy_not_similarity_top_100"


def test_summarize_taxpayer_favorable_tiebreak_review_is_null_safe_and_uses_primary_one_swap() -> None:
    service = UnequalRollNoPersistReplayService()
    review = service._summarize_taxpayer_favorable_tiebreak_review(
        current_result={"requested_reduction_amount": 10000.0},
        smart_result={"requested_reduction_amount": 2000.0},
        one_swap_result={
            "requested_reduction_amount": 2600.0,
            "accepted_swaps": [{"swapped_in_candidate_parcel_id": "p1", "swapped_out_candidate_parcel_id": "p2"}],
            "rejected_alternatives": [{"rejection_reasons": ["similarity_below_equal_credibility_band"]}],
            "automation_assessment": {
                "automation_status": "manual_review_only",
                "automation_reasons": ["accepted_swap_requires_review_visible_comp"],
            },
        },
        two_swap_result={
            "requested_reduction_amount": 3200.0,
            "accepted_swaps": [{"swapped_in_candidate_parcel_id": "p1", "swapped_out_candidate_parcel_id": "p2"}],
            "rejected_alternatives": [],
            "automation_assessment": {
                "automation_status": "safe_automated_candidate",
                "automation_reasons": [],
            },
        },
        parcel_account_map={"p1": "A-IN", "p2": "A-OUT"},
    )

    assert review["taxpayer_favorable_tiebreak_class"] == "manual_review_only"
    assert review["taxpayer_favorable_tiebreak_primary_reason"] == "accepted_swap_requires_review_visible_comp"
    assert review["taxpayer_favorable_tiebreak_swapped_in_accounts"] == ["A-IN"]
    assert review["taxpayer_favorable_tiebreak_rejected_reason_counts"] == {
        "similarity_below_equal_credibility_band": 1
    }
    assert review["taxpayer_favorable_tiebreak_two_swap_comparison"]["taxpayer_favorable_tiebreak_class"] == "safe_automated_candidate"
