from __future__ import annotations

from copy import deepcopy
import json

from infra.scripts.report_unequal_roll_smart_harvest_harris_diagnostic import (
    FEATURE_USAGE_POSTURE,
    INPUT_CONTRACT,
    build_guardrail_summary,
    build_parser,
    build_payload,
    format_lower_value_alternative_row,
    resolve_diagnostic_artifact,
)


def _candidate_row(account: str, *, include_payload_gaps: bool = False) -> dict[str, object]:
    row: dict[str, object] = {
        "account_number": account,
        "subdivision_name": "SUBDIVISION A",
        "property_class_code": "A1",
        "living_area_sf": 2100.0,
        "year_built": 2005,
        "effective_age": 9.0,
        "bedrooms": 4,
        "full_baths": 2.0,
        "half_baths": 1.0,
        "stories": 2.0,
        "quality_code": "Q1",
        "condition_code": "C1",
        "pool_flag": False,
        "land_sf": 7200.0,
        "land_acres": 0.1653,
        "frontage_sf": 64.0,
        "depth_sf": 112.0,
        "raw_appraised_value_per_sf": 102.5,
        "adjusted_appraised_value_per_sf": 109.25,
        "land_sf_delta": -150.0,
        "land_acres_delta": -0.004,
    }
    if include_payload_gaps:
        row["garage_spaces"] = None
    return row


def _case(
    account: str,
    neighborhood: str,
    change: float,
    *,
    alt_class: str = "manual_review_only",
    missing_subject_land: bool = False,
) -> dict[str, object]:
    return {
        "account": account,
        "neighborhood_code": neighborhood,
        "cohort_role": "priority_taxpayer_loss",
        "artifact_reduction_change_amount": change,
        "subject_features": {
            "subdivision_name": "SUBDIVISION A",
            "property_class_code": "A1",
            "year_built": 2003,
            "effective_age": 11.0,
            "bedrooms": 4,
            "full_baths": 2.0,
            "half_baths": 1.0,
            "stories": 2.0,
            "quality_code": "Q1",
            "condition_code": "C1",
            "pool_flag": False,
            "garage_spaces": 2.0,
            "frontage_sf": 65.0,
            "depth_sf": 110.0,
            "land_sf": None if missing_subject_land else 7350.0,
            "land_acres": None if missing_subject_land else 0.1687,
            "has_parcel_polygon": True,
            "has_parcel_centroid": True,
        },
        "current_result": {
            "included_comp_count": 12,
            "final_value_status": "manual_review_required",
            "included_comp_rows": [_candidate_row("cur-1"), _candidate_row("cur-2")],
        },
        "smart_result": {
            "included_comp_count": 18,
            "final_value_status": "supported_with_review",
            "included_comp_rows": [
                _candidate_row("smart-1", include_payload_gaps=True),
                _candidate_row("smart-2"),
            ],
        },
        "comparison_summary": {
            "comp_overlap_count": 4,
            "comps_removed_by_smart_harvest": ["r1", "r2"],
            "comps_added_by_smart_harvest": ["a1", "a2", "a3"],
            "included_comp_count_change": 6,
            "review_heavy_count_change": -1,
            "likely_exclude_count_change": 0,
            "status_change": {
                "current": "manual_review_required",
                "smart": "supported_with_review",
                "changed": True,
            },
            "support_status_change": {
                "current": "supported_with_review",
                "smart": "supported_with_review",
                "changed": False,
            },
            "requested_reduction_change": change,
            "adjusted_median_change": 5000.0,
        },
        "value_fairness_outlier_report": {
            "delta_smart_minus_current": {
                "avg_similarity_score": 0.05,
                "median_adjusted_value": 5000.0,
                "median_appraised_value_per_sf": 7.5,
                "median_adjusted_value_per_sf": 8.25,
            },
            "smart_included": {"median_adjusted_value": 210000.0},
        },
        "feature_mismatch_report": {
            "summary": {
                "features_where_smart_is_closer": [
                    "land_sf",
                    "bedrooms",
                    "stories",
                ],
                "features_where_smart_is_farther": ["pool_flag"],
            }
        },
        "smart_harvest_harm_explanation": {
            "primary_explanation_category": "lost_lower_value_but_still_credible_comps",
            "which_unadjusted_features_most_explain_loss": ["land_sf", "bedrooms"],
            "should_case_remain_gated_manual_review_only": True,
        },
        "equally_credible_lower_value_alternative_report": {
            "opportunity_class": alt_class,
            "count_lower_value_equally_credible_alternatives": 1,
            "top_candidate_account_ids": ["cand-1"],
            "accepted_alternatives": [
                {
                    "account_number": "cand-1",
                    "similarity_score": 0.962,
                    "adjusted_value": 198500.0,
                    "value_difference_vs_selected_smart_comp_median": -11500.0,
                    "reason_accepted_as_equally_credible": "accepted_by_tiebreak_prototype_screen",
                }
            ],
            "rejected_alternatives_sample": [],
        },
    }


def _diagnostic_artifact() -> dict[str, object]:
    return {
        "generated_at": "2026-05-08T11:33:14",
        "source_artifact": "/tmp/harris_focus.json",
        "summary": {
            "cases_reviewed": 3,
            "priority_taxpayer_loss_cases": 2,
            "positive_control_cases": 1,
            "stable_control_cases": 0,
            "harm_category_counts": {
                "lost_lower_value_but_still_credible_comps": 2,
            },
            "tiebreak_class_counts": {
                "manual_review_only": 1,
                "safe_automated_candidate": 1,
            },
        },
        "cases": [
            _case("A", "229.60", -1000.0, alt_class="manual_review_only"),
            _case("B", "222.02", -2000.0, alt_class="safe_automated_candidate", missing_subject_land=True),
            {
                **_case("C", "8309.06", 1500.0),
                "cohort_role": "positive_control",
            },
        ],
    }


def _harris_reference_artifact() -> dict[str, object]:
    return {
        "summary": {
            "neighborhood_summary": {
                "229.60": {"net": -800.0, "loss": -1000.0},
                "222.02": {"net": -1500.0, "loss": -2000.0},
            }
        }
    }


def test_build_payload_emits_required_report_sections() -> None:
    payload = build_payload(
        diagnostic_artifact=_diagnostic_artifact(),
        harris_reference_artifact=_harris_reference_artifact(),
    )

    assert set(payload.keys()) >= {
        "cohort_summary",
        "neighborhood_total_reconciliation",
        "per_subject_compact_evidence_table",
        "current_vs_similarity_top_100_included_comp_comparison",
        "feature_mismatch_summary",
        "value_fairness_value_per_sf_outlier_summary",
        "lower_value_equally_credible_alternative_table",
        "feature_usage_posture_table",
        "data_availability_payload_propagation_audit",
        "coverage_missingness_summary",
        "finding_buckets",
        "recommendation_summary",
        "guardrails",
        "input_contract",
    }
    assert payload["cohort_summary"]["priority_taxpayer_loss_cases"] == 2
    assert len(payload["per_subject_compact_evidence_table"]) == 2


def test_input_contract_and_cli_help_are_honest_about_enriched_artifact_mode() -> None:
    parser = build_parser()
    help_text = parser.format_help()

    assert INPUT_CONTRACT["script_mode"] == "enriched_artifact_reporter"
    assert INPUT_CONTRACT["supports_original_harris_artifact_directly"] is False
    assert "clarification wrapper" in help_text
    assert "reconciliation" in help_text


def test_clarification_wrapper_can_resolve_case_level_source_artifact(tmp_path) -> None:
    case_level_path = tmp_path / "case_level.json"
    case_level_path.write_text(json.dumps(_diagnostic_artifact()))
    wrapper = {
        "generated_at": "2026-05-08T11:51:09",
        "source_artifact": str(case_level_path),
        "compact_per_subject_evidence": [],
    }

    resolved, metadata = resolve_diagnostic_artifact(wrapper)

    assert len(resolved["cases"]) == 3
    assert metadata["input_artifact_kind"] == "clarification_wrapper"
    assert metadata["resolved_case_level_artifact_via_source_pointer"] is True


def test_feature_usage_posture_and_audit_schema_cover_payload_gap_fields() -> None:
    payload = build_payload(
        diagnostic_artifact=_diagnostic_artifact(),
        harris_reference_artifact=_harris_reference_artifact(),
    )
    posture_by_feature = {row["feature"]: row["posture"] for row in FEATURE_USAGE_POSTURE}
    audit_by_feature = {
        row["feature"]: row for row in payload["data_availability_payload_propagation_audit"]
    }

    assert posture_by_feature["effective_age"] == "monetized_adjustment_and_scoring"
    assert posture_by_feature["garage_spaces"] == "unavailable_missing_from_candidate_payload"
    assert posture_by_feature["land_sf"] == "scoring_and_non_monetized_guardrail"
    assert audit_by_feature["frontage_sf"]["snapshot_json_available"] is True
    assert audit_by_feature["frontage_sf"]["candidate_discovery_payload_available"] is True
    assert audit_by_feature["frontage_sf"]["final_value_review_evidence_available"] is True
    assert audit_by_feature["garage_spaces"]["candidate_discovery_payload_available"] is False
    assert audit_by_feature["garage_spaces"]["present_in_subject_snapshot"] is False
    assert audit_by_feature["garage_spaces"]["final_value_review_evidence_available"] is True
    assert audit_by_feature["frontage_sf"]["not_usable_yet"] is False
    assert audit_by_feature["adjusted_value_per_sf"]["enough_for_review_only_use"] is True


def test_missing_field_handling_and_payload_gap_labels_are_emitted() -> None:
    payload = build_payload(
        diagnostic_artifact=_diagnostic_artifact(),
        harris_reference_artifact=_harris_reference_artifact(),
    )
    compact_row = payload["per_subject_compact_evidence_table"][0]
    audit_by_feature = {
        row["feature"]: row for row in payload["data_availability_payload_propagation_audit"]
    }

    assert "possible_price_tier_drift" in compact_row["attribution_risk_labels"]
    assert "land_signal_not_causal" in compact_row["attribution_risk_labels"]
    assert "bedroom_signal_not_causal" in compact_row["attribution_risk_labels"]
    assert "payload_gap_limits_explanation" in compact_row["attribution_risk_labels"]
    assert audit_by_feature["land_sf"]["enriched_subject_missing_count"] == 1
    assert audit_by_feature["geometry_availability"]["candidate_discovery_payload_available"] is False


def test_lower_value_alternative_row_and_guardrails_preserve_review_only_behavior() -> None:
    diagnostic = _diagnostic_artifact()
    diagnostic_before = deepcopy(diagnostic)

    payload = build_payload(
        diagnostic_artifact=diagnostic,
        harris_reference_artifact=_harris_reference_artifact(),
    )
    row = format_lower_value_alternative_row(
        _case("A", "229.60", -1000.0, alt_class="safe_automated_candidate")
    )

    assert diagnostic == diagnostic_before
    assert row["subject_account"] == "A"
    assert row["classification"] == "safe_automated_candidate"
    assert row["candidate_account_id"] == "cand-1"
    assert row["accepted_or_rejected_reason"] == "accepted_by_tiebreak_prototype_screen"
    assert payload["guardrails"] == build_guardrail_summary()
    assert payload["guardrails"]["db_writes_occurred"] is False
    assert payload["guardrails"]["runtime_defaults_changed"] is False
    assert payload["guardrails"]["smart_harvest_became_default"] is False
    assert payload["guardrails"]["tie_break_automation_enabled"] is False
    assert payload["guardrails"]["scoring_or_adjustment_formulas_changed"] is False
