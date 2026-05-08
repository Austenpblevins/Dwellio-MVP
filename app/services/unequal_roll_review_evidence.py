from __future__ import annotations

from collections import Counter
from typing import Any


MODEL_OUTCOME_STATUSES = {
    "supported",
    "supported_with_review",
    "manual_review_required",
    "unsupported",
}

TAXPAYER_FAVORABLE_TIEBREAK_CLASSES = {
    "safe_automated_candidate",
    "manual_review_only",
    "no_safe_opportunity",
    "not_evaluated",
}


def reconcile_outcome_row(
    *,
    runtime_row: dict[str, Any],
    classified_row: dict[str, Any] | None,
    run_state_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    runtime_status = runtime_row.get("final_value_status")
    classified_row = classified_row or {}
    recovered_status = classified_row.get("final_value_status")
    completeness_status_family = classified_row.get("completeness_status_family")
    defect_code = classified_row.get("completeness_status_code")
    recovery_source = classified_row.get("downstream_payload_attachment_status")

    final_reconciled_status = runtime_status or recovered_status
    outcome_complete = final_reconciled_status in MODEL_OUTCOME_STATUSES

    if runtime_status is None:
        if recovered_status in MODEL_OUTCOME_STATUSES:
            none_origin = "payload_gap_recovered_from_downstream_payload"
        elif run_state_payload is not None:
            none_origin = "payload_gap_run_state_available_but_final_outcome_missing"
        else:
            none_origin = "payload_gap_unrecovered"
    else:
        none_origin = "not_applicable"

    if runtime_status is None and recovered_status in MODEL_OUTCOME_STATUSES:
        recommended_interpretation = (
            "runtime row omitted final outcome but downstream payload recovered it"
        )
    elif runtime_status is None:
        recommended_interpretation = (
            "runtime completed but final outcome is not recoverable from available payload sources"
        )
    elif final_reconciled_status == "unsupported":
        recommended_interpretation = (
            "unsupported outcome; requested values must be interpreted using unsupported-value semantics"
        )
    else:
        recommended_interpretation = "final model outcome observable"

    return {
        "runtime_completed_flag": bool(
            runtime_row.get("discovery_completion_status") == "completed"
            and runtime_row.get("probe_error") is None
        ),
        "runtime_final_value_status": runtime_status,
        "recovered_v14_status": recovered_status,
        "final_reconciled_status": final_reconciled_status,
        "model_outcome_complete": outcome_complete,
        "none_origin": none_origin,
        "defect_code": defect_code,
        "recovery_source": recovery_source,
        "recommended_interpretation": recommended_interpretation,
    }


def classify_unsupported_value_semantics(
    *,
    current_appraised_value: float | None,
    final_value_status: str | None,
    exposed_requested_roll_value: float | None,
    exposed_requested_reduction_amount: float | None,
    exposed_requested_reduction_pct: float | None,
) -> dict[str, Any]:
    if final_value_status != "unsupported":
        return {
            "safe_requested_roll_value": exposed_requested_roll_value,
            "safe_requested_reduction_amount": exposed_requested_reduction_amount,
            "safe_requested_reduction_pct": exposed_requested_reduction_pct,
            "value_interpretation": "final_model_value",
            "value_interpretation_reason": "row is not unsupported",
        }

    amount = _as_float(exposed_requested_reduction_amount)
    roll = _as_float(exposed_requested_roll_value)
    pct = _as_float(exposed_requested_reduction_pct)
    current_value = _as_float(current_appraised_value)

    if amount is None and roll is None and pct is None:
        return {
            "safe_requested_roll_value": None,
            "safe_requested_reduction_amount": None,
            "safe_requested_reduction_pct": None,
            "value_interpretation": "unavailable",
            "value_interpretation_reason": (
                "unsupported row does not expose requested values in available artifacts"
            ),
        }

    if amount is not None and amount > 0:
        return {
            "safe_requested_roll_value": None,
            "safe_requested_reduction_amount": None,
            "safe_requested_reduction_pct": None,
            "value_interpretation": "diagnostic_only",
            "value_interpretation_reason": (
                "unsupported row exposes a positive reduction; treat exposed values as diagnostic/provisional, not final requested values"
            ),
        }

    if (
        current_value is not None
        and roll is not None
        and abs(current_value - roll) < 0.01
        and (amount in {None, 0.0})
        and (pct in {None, 0.0})
    ):
        return {
            "safe_requested_roll_value": current_value,
            "safe_requested_reduction_amount": 0.0,
            "safe_requested_reduction_pct": 0.0,
            "value_interpretation": "suppressed_identity_value",
            "value_interpretation_reason": (
                "unsupported row exposes identity/zero values consistent with suppressed no-reduction posture"
            ),
        }

    return {
        "safe_requested_roll_value": None,
        "safe_requested_reduction_amount": None,
        "safe_requested_reduction_pct": None,
        "value_interpretation": "provisional_or_ambiguous",
        "value_interpretation_reason": (
            "unsupported row exposes values that cannot be trusted as final requested values from current artifacts"
        ),
    }


def evidence_completeness_grade(
    *,
    final_reconciled_status: str | None,
    model_outcome_complete: bool,
    subject_context_present: bool,
    comp_evidence_present: bool,
    stability_metrics_present: bool,
    none_origin: str,
) -> str:
    if not model_outcome_complete and none_origin == "payload_gap_unrecovered":
        return "not_reviewable"
    if model_outcome_complete and subject_context_present and comp_evidence_present:
        if stability_metrics_present:
            return "complete_review_evidence"
        return "usable_with_minor_gaps"
    if model_outcome_complete and subject_context_present:
        return "limited_review_evidence"
    return "not_reviewable"


def summarize_run_state_candidates(run_state_payload: dict[str, Any]) -> dict[str, Any]:
    candidates = list(run_state_payload.get("candidates") or [])
    included = [
        row
        for row in candidates
        if row.get("final_value_status")
        in {"included_in_final_value", "included_in_final_value_with_review"}
    ]
    excluded = [
        row
        for row in candidates
        if row.get("final_value_status")
        in {
            "excluded_review_heavy",
            "excluded_likely_exclude",
            "excluded_from_final_value",
        }
    ]
    reason_counter: Counter[str] = Counter(
        reason
        for row in candidates
        for reason in (row.get("reason_codes") or [])
    )
    burden_counter: Counter[str] = Counter(row.get("burden_status") for row in candidates)
    source_counter: Counter[str] = Counter(row.get("source_status") for row in candidates)
    adjusted_set_counter: Counter[str] = Counter(
        row.get("adjusted_set_status") for row in candidates
    )

    channel_counter: Counter[str] = Counter()
    land_site_flag = False
    bedroom_best_signal: dict[str, Any] | None = None
    fort_bend_bathroom_posture: list[dict[str, Any]] = []

    for row in candidates:
        channels = dict(row.get("adjustment_support_channels") or {})
        for channel_name, detail in channels.items():
            if detail.get("potential_adjustment_flag"):
                channel_counter[channel_name] += 1

        land_site_detail = channels.get("land_site") or {}
        if land_site_detail.get("readiness_status") == "review_required":
            land_site_flag = True

        bedroom_detail = channels.get("bedroom") or {}
        gla_detail = channels.get("gla") or {}
        if bedroom_detail.get("potential_adjustment_flag"):
            signal = {
                "bedroom_difference": bedroom_detail.get("difference_value"),
                "gla_difference_pct": gla_detail.get("difference_pct"),
                "final_comp_status": row.get("final_value_status"),
                "burden_status": row.get("burden_status"),
            }
            signal_rank = (
                abs(_as_float(signal["gla_difference_pct"]) or 999.0),
                -abs(_as_float(signal["bedroom_difference"]) or 0.0),
            )
            if bedroom_best_signal is None or signal_rank < bedroom_best_signal["_rank"]:
                bedroom_best_signal = {"_rank": signal_rank, **signal}

        full_bath_detail = channels.get("full_bath") or {}
        if (
            full_bath_detail.get("readiness_status") == "review_required"
            or row.get("source_status") == "mixed_with_unresolved_review_only"
        ):
            fort_bend_bathroom_posture.append(
                {
                    "final_comp_status": row.get("final_value_status"),
                    "source_status": row.get("source_status"),
                    "bath_readiness": full_bath_detail.get("readiness_status"),
                    "bath_reason": full_bath_detail.get("basis_source_reason_code"),
                    "valuation_attachment": full_bath_detail.get(
                        "valuation_support_attachment_status"
                    ),
                    "valuation_basis_status": full_bath_detail.get(
                        "valuation_support_basis_status"
                    ),
                    "reason_codes": list(row.get("reason_codes") or []),
                }
            )

    return {
        "candidate_count": len(candidates),
        "included_count": len(included),
        "excluded_count": len(excluded),
        "reason_code_counts": dict(reason_counter),
        "burden_status_counts": dict(burden_counter),
        "source_status_counts": dict(source_counter),
        "adjusted_set_status_counts": dict(adjusted_set_counter),
        "dominant_adjustment_channels": [
            {"channel": channel, "count": count}
            for channel, count in channel_counter.most_common(5)
        ],
        "land_site_signal_present": land_site_flag,
        "bedroom_signal": (
            {
                key: value
                for key, value in bedroom_best_signal.items()
                if key != "_rank"
            }
            if bedroom_best_signal is not None
            else None
        ),
        "fort_bend_bathroom_source_posture": fort_bend_bathroom_posture,
        "included_comp_rows": [
            _candidate_summary_row(row) for row in included[:10]
        ],
        "excluded_comp_rows": [
            _candidate_summary_row(row) for row in excluded[:10]
        ],
    }


def normalize_taxpayer_favorable_tiebreak_review(
    review_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    review_payload = dict(review_payload or {})
    review_class = str(review_payload.get("taxpayer_favorable_tiebreak_class") or "").strip()
    if review_class not in TAXPAYER_FAVORABLE_TIEBREAK_CLASSES:
        review_class = "not_evaluated"

    benefit_flags = dict(
        review_payload.get("taxpayer_favorable_tiebreak_benefit_threshold_flags") or {}
    )
    normalized = {
        "taxpayer_favorable_tiebreak_opportunity_flag": bool(
            review_payload.get("taxpayer_favorable_tiebreak_opportunity_flag")
        )
        if review_class != "not_evaluated"
        else False,
        "taxpayer_favorable_tiebreak_class": review_class,
        "taxpayer_favorable_tiebreak_primary_reason": review_payload.get(
            "taxpayer_favorable_tiebreak_primary_reason"
        ),
        "taxpayer_favorable_tiebreak_secondary_reasons": list(
            review_payload.get("taxpayer_favorable_tiebreak_secondary_reasons") or []
        ),
        "taxpayer_favorable_tiebreak_swap_count": review_payload.get(
            "taxpayer_favorable_tiebreak_swap_count"
        ),
        "taxpayer_favorable_tiebreak_estimated_reduction_impact": review_payload.get(
            "taxpayer_favorable_tiebreak_estimated_reduction_impact"
        ),
        "taxpayer_favorable_tiebreak_benefit_threshold_flags": {
            "lt_500": bool(benefit_flags.get("lt_500")),
            "lt_1000": bool(benefit_flags.get("lt_1000")),
            "lt_2500": bool(benefit_flags.get("lt_2500")),
        },
        "taxpayer_favorable_tiebreak_swapped_in_accounts": list(
            review_payload.get("taxpayer_favorable_tiebreak_swapped_in_accounts") or []
        ),
        "taxpayer_favorable_tiebreak_swapped_out_accounts": list(
            review_payload.get("taxpayer_favorable_tiebreak_swapped_out_accounts") or []
        ),
        "taxpayer_favorable_tiebreak_rejected_reason_counts": dict(
            review_payload.get("taxpayer_favorable_tiebreak_rejected_reason_counts") or {}
        ),
        "taxpayer_favorable_tiebreak_review_note": review_payload.get(
            "taxpayer_favorable_tiebreak_review_note"
        ),
        "taxpayer_favorable_tiebreak_two_swap_comparison": dict(
            review_payload.get("taxpayer_favorable_tiebreak_two_swap_comparison") or {}
        ),
    }
    if (
        review_class == "not_evaluated"
        and normalized["taxpayer_favorable_tiebreak_primary_reason"] is None
    ):
        normalized["taxpayer_favorable_tiebreak_primary_reason"] = "not_evaluated"
    return normalized


def _candidate_summary_row(row: dict[str, Any]) -> dict[str, Any]:
    channels = dict(row.get("adjustment_support_channels") or {})
    return {
        "final_comp_status": row.get("final_value_status"),
        "chosen_comp_status": row.get("chosen_comp_status"),
        "year_built": row.get("year_built"),
        "effective_age": row.get("effective_age"),
        "frontage_sf": row.get("frontage_sf"),
        "depth_sf": row.get("depth_sf"),
        "source_status": row.get("source_status"),
        "burden_status": row.get("burden_status"),
        "adjusted_set_status": row.get("adjusted_set_status"),
        "reason_codes": list(row.get("reason_codes") or []),
        "review_carry_forward_flag": bool(row.get("review_carry_forward_flag")),
        "gla_signal": _channel_projection(channels.get("gla")),
        "bedroom_signal": _channel_projection(channels.get("bedroom")),
        "full_bath_signal": _channel_projection(channels.get("full_bath")),
        "half_bath_signal": _channel_projection(channels.get("half_bath")),
        "land_site_signal": _channel_projection(channels.get("land_site")),
    }


def _channel_projection(detail: dict[str, Any] | None) -> dict[str, Any] | None:
    if not detail:
        return None
    return {
        "readiness_status": detail.get("readiness_status"),
        "difference_value": detail.get("difference_value"),
        "difference_pct": detail.get("difference_pct"),
        "potential_adjustment_flag": bool(detail.get("potential_adjustment_flag")),
        "basis_source_code": detail.get("basis_source_code"),
        "basis_source_reason_code": detail.get("basis_source_reason_code"),
        "valuation_support_attachment_status": detail.get(
            "valuation_support_attachment_status"
        ),
        "valuation_support_basis_status": detail.get("valuation_support_basis_status"),
    }


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
