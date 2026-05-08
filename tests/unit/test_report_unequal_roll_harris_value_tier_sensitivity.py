from __future__ import annotations

import json

from infra.scripts.report_unequal_roll_harris_value_tier_sensitivity import (
    HIGH_VALUE_PER_SF_DELTA_THRESHOLD,
    INPUT_CONTRACT,
    MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD,
    MARGINAL_SIMILARITY_IMPROVEMENT_THRESHOLD,
    build_payload,
    build_threshold_metadata,
    is_marginal_similarity_high_value_tradeoff,
    is_value_per_sf_outlier_risk,
    lower_value_alternative_class,
)


def _candidate_row(account: str, *, subdivision: str = "SUB A") -> dict[str, object]:
    return {
        "account_number": account,
        "similarity_score": 0.955,
        "raw_appraised_value_per_sf": 205.0,
        "adjusted_appraised_value_per_sf": 214.0,
        "source_features": {
            "subdivision_name": subdivision,
        },
        "neighborhood_value_context": {
            "appraised_value_per_sf": 205.0,
            "neighborhood_median_appraised_value_per_sf": 192.0,
            "distance_from_neighborhood_median_appraised_value_per_sf": 13.0,
            "neighborhood_value_percentile": 0.88,
        },
    }


def _case(
    account: str,
    neighborhood: str,
    role: str,
    *,
    similarity_delta: float,
    adjusted_delta: float,
    appraised_psf_delta: float,
    adjusted_psf_delta: float,
    alt_class: str,
    smart_subdivision: str = "SUB B",
) -> dict[str, object]:
    return {
        "county": "harris",
        "account": account,
        "neighborhood_code": neighborhood,
        "cohort_role": role,
        "artifact_reduction_change_amount": -2500.0 if role == "priority_taxpayer_loss" else 1200.0,
        "subject_features": {
            "subdivision_name": "SUB A",
        },
        "comparison_summary": {
            "adjusted_median_change": adjusted_delta,
        },
        "value_fairness_outlier_report": {
            "delta_smart_minus_current": {
                "avg_similarity_score": similarity_delta,
                "median_adjusted_value": adjusted_delta,
                "median_appraised_value_per_sf": appraised_psf_delta,
                "median_adjusted_value_per_sf": adjusted_psf_delta,
            },
            "subject_value_context": {
                "subject_appraised_value_per_sf": 210.0,
                "same_neighborhood_median_appraised_value_per_sf": 192.0,
            },
            "current_included": {
                "median_appraised_value_per_sf": 201.0,
                "median_adjusted_value_per_sf": 206.0,
                "median_neighborhood_value_percentile": 0.77,
                "median_distance_from_neighborhood_median_value_per_sf": 14.0,
            },
            "smart_included": {
                "median_appraised_value_per_sf": 209.0,
                "median_adjusted_value_per_sf": 215.0,
                "median_neighborhood_value_percentile": 0.89,
                "median_distance_from_neighborhood_median_value_per_sf": 25.0,
                "high_value_outlier_comp_count": 3 if appraised_psf_delta >= 5.0 else 0,
                "high_adjusted_value_outlier_count": 2 if adjusted_psf_delta >= 5.0 else 0,
                "high_value_per_sf_outlier_count": 1 if appraised_psf_delta >= 5.0 else 0,
            },
        },
        "equally_credible_lower_value_alternative_report": {
            "safe_manual_or_no_safe": alt_class,
            "count_lower_value_equally_credible_alternatives": 1 if alt_class != "no_safe_opportunity" else 0,
            "estimated_reduction_impact": 1800.0 if alt_class != "no_safe_opportunity" else 0.0,
            "top_candidate_account_ids": ["cand-1"] if alt_class != "no_safe_opportunity" else [],
        },
        "smart_harvest_harm_explanation": {
            "primary_explanation_category": "lost_lower_value_but_still_credible_comps",
        },
        "smart_included_comp_rows": [
            _candidate_row("smart-1", subdivision=smart_subdivision),
            _candidate_row("smart-2", subdivision=smart_subdivision),
        ],
    }


def _artifact() -> dict[str, object]:
    return {
        "generated_at": "2026-05-08T13:41:32",
        "source_artifact": "/private/tmp/unequal_roll_smart_harvest_harris_focus_20260507T235803.json",
        "cases": [
            _case(
                "A",
                "229.60",
                "priority_taxpayer_loss",
                similarity_delta=0.01,
                adjusted_delta=8000.0,
                appraised_psf_delta=7.0,
                adjusted_psf_delta=8.0,
                alt_class="safe_automated_candidate",
            ),
            _case(
                "B",
                "222.02",
                "priority_taxpayer_loss",
                similarity_delta=0.08,
                adjusted_delta=2500.0,
                appraised_psf_delta=1.0,
                adjusted_psf_delta=1.5,
                alt_class="manual_review_only",
                smart_subdivision="SUB A",
            ),
            _case(
                "C",
                "8309.06",
                "positive_control",
                similarity_delta=0.03,
                adjusted_delta=-1500.0,
                appraised_psf_delta=-2.0,
                adjusted_psf_delta=-2.5,
                alt_class="no_safe_opportunity",
                smart_subdivision="SUB A",
            ),
        ],
    }


def test_build_payload_emits_required_sections_and_guardrails() -> None:
    payload = build_payload(diagnostic_artifact=_artifact())

    assert INPUT_CONTRACT["script_mode"] == "enriched_artifact_sensitivity_reporter"
    assert set(payload.keys()) >= {
        "threshold_metadata",
        "cohort_summary",
        "per_subject_sensitivity_table",
        "per_neighborhood_summary",
        "value_per_sf_outlier_table",
        "marginal_similarity_high_value_tradeoff_table",
        "lower_value_equally_credible_alternative_summary",
        "subdivision_micro_location_proxy_summary",
        "recommended_no_persist_scoring_sensitivity_experiments",
        "finding_buckets",
        "guardrails",
    }
    assert payload["guardrails"]["db_writes_occurred"] is False
    assert payload["guardrails"]["runtime_defaults_changed"] is False
    assert payload["guardrails"]["smart_harvest_became_default"] is False
    assert payload["guardrails"]["tie_break_automation_enabled"] is False
    assert payload["guardrails"]["scoring_or_adjustment_formulas_changed"] is False
    assert payload["guardrails"]["final_values_changed"] is False


def test_threshold_metadata_is_exposed() -> None:
    metadata = build_threshold_metadata()
    assert metadata["marginal_similarity_improvement_threshold"] == MARGINAL_SIMILARITY_IMPROVEMENT_THRESHOLD
    assert metadata["material_adjusted_median_increase_threshold"] == MATERIAL_ADJUSTED_MEDIAN_INCREASE_THRESHOLD
    assert metadata["high_value_per_sf_delta_threshold"] == HIGH_VALUE_PER_SF_DELTA_THRESHOLD


def test_value_per_sf_outlier_and_tradeoff_classification() -> None:
    cases = _artifact()["cases"]
    assert is_value_per_sf_outlier_risk(cases[0]) is True
    assert is_marginal_similarity_high_value_tradeoff(cases[0]) is True
    assert is_value_per_sf_outlier_risk(cases[1]) is False
    assert is_marginal_similarity_high_value_tradeoff(cases[1]) is False


def test_lower_value_alternative_classification_and_micro_location_labels() -> None:
    payload = build_payload(diagnostic_artifact=_artifact())
    row_by_account = {
        row["subject_account"]: row for row in payload["per_subject_sensitivity_table"]
    }

    assert lower_value_alternative_class(_artifact()["cases"][0]) == "safe_automated_candidate"
    assert "lower_value_equally_credible_available" in row_by_account["A"]["heuristic_labels"]
    assert "possible_micro_location_proxy" in row_by_account["A"]["heuristic_labels"]
    assert "manual_review_lower_value_candidate" in row_by_account["B"]["heuristic_labels"]
    assert "no_safe_lower_value_alternative" in row_by_account["C"]["heuristic_labels"]


def test_wrapper_resolution_metadata_can_be_merged() -> None:
    payload = build_payload(
        diagnostic_artifact=_artifact(),
        input_resolution={"input_artifact_kind": "clarification_wrapper"},
    )
    assert payload["input_contract"]["input_artifact_kind"] == "clarification_wrapper"
    assert json.loads(json.dumps(payload["recommended_no_persist_scoring_sensitivity_experiments"]))
