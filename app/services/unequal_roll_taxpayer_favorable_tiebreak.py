from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from statistics import mean, median
from typing import Any

from app.services.unequal_roll_final_value import UnequalRollFinalValueService


@dataclass(frozen=True)
class TaxpayerFavorableTieBreakConfig:
    max_swaps: int = 1
    similarity_tolerance: float = 0.02
    median_movement_cap_ratio: float = 0.02
    minimum_included_comp_count: int = 12
    max_adjustment_pct: float = 0.125
    max_material_adjustment_count: int = 4
    max_avg_similarity_drop: float = 0.01
    minimum_reduction_gain_for_automation: float = 0.01


class UnequalRollTaxpayerFavorableTieBreakService:
    def __init__(self) -> None:
        self._final_value_service = UnequalRollFinalValueService()

    def simulate(
        self,
        *,
        current_result: dict[str, Any],
        smart_result: dict[str, Any],
        config: TaxpayerFavorableTieBreakConfig,
    ) -> dict[str, Any]:
        run_context = self._build_run_context(smart_result)
        smart_detail = dict(smart_result.get("final_value_detail_json") or {})
        current_detail = dict(current_result.get("final_value_detail_json") or {})
        smart_included = deepcopy(list(smart_detail.get("included_comp_rows") or []))
        smart_excluded = deepcopy(list(smart_detail.get("excluded_comp_rows") or []))
        smart_included_ids = {str(row.get("candidate_parcel_id") or "") for row in smart_included}
        current_only_included = [
            deepcopy(row)
            for row in list(current_detail.get("included_comp_rows") or [])
            if str(row.get("candidate_parcel_id") or "") not in smart_included_ids
        ]

        baseline = self._recompute(
            run_context=run_context,
            included_rows=smart_included,
            excluded_rows=smart_excluded,
            subject_appraised_value=_as_float(smart_result.get("current_appraised_value")),
        )

        weakest_similarity = _min_similarity(smart_included)
        baseline_avg_similarity = _avg_similarity(smart_included)
        included_rows = deepcopy(smart_included)
        excluded_rows = deepcopy(smart_excluded)
        accepted_swaps: list[dict[str, Any]] = []
        rejected_alternatives: list[dict[str, Any]] = []

        for alternative in sorted(
            current_only_included,
            key=lambda row: (
                _as_float(row.get("adjusted_appraised_value")) if _as_float(row.get("adjusted_appraised_value")) is not None else float("inf"),
                str(row.get("candidate_parcel_id") or ""),
            ),
        ):
            rejection_reasons = self._global_rejection_reasons(
                alternative=alternative,
                weakest_similarity=weakest_similarity,
                smart_median=_as_float(baseline["requested_roll_value"]),
                config=config,
            )
            if rejection_reasons:
                rejected_alternatives.append(self._rejected_record(alternative, rejection_reasons))
                continue

            candidate_to_displace = self._find_displaced_candidate(
                alternative=alternative,
                included_rows=included_rows,
            )
            if candidate_to_displace is None:
                rejected_alternatives.append(
                    self._rejected_record(alternative, ["no_compatible_displaced_comp"])
                )
                continue

            proposed_included = [
                deepcopy(row)
                for row in included_rows
                if str(row.get("candidate_parcel_id") or "")
                != str(candidate_to_displace.get("candidate_parcel_id") or "")
            ]
            proposed_included.append(deepcopy(alternative))
            proposed_excluded = deepcopy(excluded_rows)
            displaced_row = deepcopy(candidate_to_displace)
            displaced_row["final_value_status"] = "excluded_from_final_value"
            displaced_row["exclusion_reason_code"] = "taxpayer_favorable_tiebreak_displaced"
            displaced_row["tiebreak_displaced_flag"] = True
            proposed_excluded.append(displaced_row)

            recomputed = self._recompute(
                run_context=run_context,
                included_rows=proposed_included,
                excluded_rows=proposed_excluded,
                subject_appraised_value=_as_float(smart_result.get("current_appraised_value")),
            )
            safety_reasons = self._safety_rejection_reasons(
                baseline=baseline,
                recomputed=recomputed,
                candidate_to_displace=candidate_to_displace,
                alternative=alternative,
                config=config,
                baseline_avg_similarity=baseline_avg_similarity,
            )
            if safety_reasons:
                rejected_alternatives.append(self._rejected_record(alternative, safety_reasons))
                continue

            accepted_swaps.append(
                {
                    "swapped_out_candidate_parcel_id": candidate_to_displace.get("candidate_parcel_id"),
                    "swapped_out_address": candidate_to_displace.get("address"),
                    "swapped_in_candidate_parcel_id": alternative.get("candidate_parcel_id"),
                    "swapped_in_address": alternative.get("address"),
                    "swapped_out_similarity_score": candidate_to_displace.get("similarity_score"),
                    "swapped_in_similarity_score": alternative.get("similarity_score"),
                    "swapped_out_adjusted_appraised_value": candidate_to_displace.get("adjusted_appraised_value"),
                    "swapped_in_adjusted_appraised_value": alternative.get("adjusted_appraised_value"),
                    "post_swap_requested_reduction_amount": recomputed["requested_reduction_amount"],
                    "post_swap_requested_roll_value": recomputed["requested_roll_value"],
                }
            )
            included_rows = proposed_included
            excluded_rows = proposed_excluded
            baseline = recomputed
            baseline_avg_similarity = _avg_similarity(included_rows)
            if len(accepted_swaps) >= config.max_swaps:
                break

        final_result = self._recompute(
            run_context=run_context,
            included_rows=included_rows,
            excluded_rows=excluded_rows,
            subject_appraised_value=_as_float(smart_result.get("current_appraised_value")),
        )
        final_result["accepted_swaps"] = accepted_swaps
        final_result["rejected_alternatives"] = rejected_alternatives
        final_result["alternatives_considered_count"] = len(current_only_included)
        final_result["swapped_comp_count"] = len(accepted_swaps)
        final_result["remains_defensible"] = self._remains_defensible(
            baseline_included_rows=smart_included,
            final_included_rows=included_rows,
            baseline=smart_result,
            final_result=final_result,
            config=config,
        )
        final_result["simulation_metadata"] = {
            "strategy": "similarity_top_100_taxpayer_favorable_tiebreak",
            "max_swaps": config.max_swaps,
            "similarity_tolerance": config.similarity_tolerance,
            "median_movement_cap_ratio": config.median_movement_cap_ratio,
            "minimum_included_comp_count": config.minimum_included_comp_count,
            "max_adjustment_pct": config.max_adjustment_pct,
            "max_material_adjustment_count": config.max_material_adjustment_count,
            "minimum_reduction_gain_for_automation": config.minimum_reduction_gain_for_automation,
        }
        final_result["automation_assessment"] = self.assess_automation(
            current_result=current_result,
            smart_result=smart_result,
            simulated_result=final_result,
            config=config,
        )
        return final_result

    def assess_automation(
        self,
        *,
        current_result: dict[str, Any],
        smart_result: dict[str, Any],
        simulated_result: dict[str, Any],
        config: TaxpayerFavorableTieBreakConfig,
    ) -> dict[str, Any]:
        current_detail = dict(current_result.get("final_value_detail_json") or {})
        current_rows = {
            str(row.get("candidate_parcel_id") or ""): row
            for row in list(current_detail.get("included_comp_rows") or [])
        }
        accepted_swaps = list(simulated_result.get("accepted_swaps") or [])
        reasons: list[str] = []
        automation_status = "safe_automated_candidate"
        reduction_gain = round(
            (_as_float(simulated_result.get("requested_reduction_amount")) or 0.0)
            - (_as_float(smart_result.get("requested_reduction_amount")) or 0.0),
            2,
        )

        if not accepted_swaps:
            automation_status = "no_safe_opportunity"
            reasons.append("no_accepted_swaps")
        elif reduction_gain < config.minimum_reduction_gain_for_automation:
            automation_status = "no_safe_opportunity"
            reasons.append("reduction_gain_below_minimum")
        elif (
            str(current_result.get("final_value_status") or "")
            != str(smart_result.get("final_value_status") or "")
            or str(current_result.get("support_status") or "")
            != str(smart_result.get("support_status") or "")
        ):
            automation_status = "manual_review_only"
            reasons.append("baseline_governance_or_support_drift")

        if automation_status == "safe_automated_candidate":
            for swap in accepted_swaps:
                candidate = current_rows.get(str(swap.get("swapped_in_candidate_parcel_id") or ""))
                if candidate is None:
                    automation_status = "manual_review_only"
                    reasons.append("missing_candidate_detail_for_assessment")
                    break
                if bool(candidate.get("review_visible_flag")):
                    automation_status = "manual_review_only"
                    reasons.append("accepted_swap_requires_review_visible_comp")
                    break
                if str(candidate.get("burden_governance_status") or "") != "within_thresholds":
                    automation_status = "manual_review_only"
                    reasons.append("accepted_swap_has_nonclean_burden")
                    break
                if str(candidate.get("adjusted_set_governance_status") or "") != "usable_adjusted_comp":
                    automation_status = "manual_review_only"
                    reasons.append("accepted_swap_not_clean_adjusted_comp")
                    break
                if str(candidate.get("source_governance_status") or "") not in {
                    "fallback_only",
                    "hybrid_supported",
                    "supported",
                }:
                    automation_status = "manual_review_only"
                    reasons.append("accepted_swap_source_not_supported")
                    break

        return {
            "automation_status": automation_status,
            "automation_reasons": reasons,
            "reduction_gain_vs_smart": reduction_gain,
        }

    def _build_run_context(self, smart_result: dict[str, Any]) -> dict[str, Any]:
        detail = dict(smart_result.get("final_value_detail_json") or {})
        carried = dict(detail.get("carried_forward_governance") or {})
        subject_snapshot_json = dict(smart_result.get("subject_snapshot_json") or {})
        return {
            "support_status": smart_result.get("support_status"),
            "selection_governance_status": carried.get("selection_governance_status"),
            "final_comp_count_status": carried.get("final_comp_count_status"),
            "summary_json": smart_result.get("summary_json"),
            "selection_log_json": smart_result.get("selection_log_json"),
            "appraised_value": _as_float(smart_result.get("current_appraised_value")),
            "land_sf": _as_float(subject_snapshot_json.get("land_sf")),
            "land_acres": _as_float(subject_snapshot_json.get("land_acres")),
        }

    def _global_rejection_reasons(
        self,
        *,
        alternative: dict[str, Any],
        weakest_similarity: float | None,
        smart_median: float | None,
        config: TaxpayerFavorableTieBreakConfig,
    ) -> list[str]:
        reasons: list[str] = []
        similarity = _as_float(alternative.get("similarity_score"))
        adjusted_value = _as_float(alternative.get("adjusted_appraised_value"))
        if weakest_similarity is None or similarity is None:
            reasons.append("missing_similarity_score")
        elif similarity < weakest_similarity - config.similarity_tolerance:
            reasons.append("similarity_below_equal_credibility_band")
        if adjusted_value is None:
            reasons.append("missing_adjusted_appraised_value")
        elif smart_median is not None and adjusted_value >= smart_median:
            reasons.append("not_taxpayer_favorable_vs_smart_median")
        if str(alternative.get("source_governance_status") or "") not in {
            "fallback_only",
            "hybrid_supported",
            "supported",
        }:
            reasons.append("unsupported_source_posture")
        if int(alternative.get("unresolved_review_only_channel_count") or 0) > 0:
            reasons.append("unresolved_review_only_channel_present")
        if _bathroom_unresolved(alternative):
            reasons.append("unresolved_dirty_bath_issue")
        if _land_unresolved(alternative):
            reasons.append("unresolved_dirty_land_issue")
        if (_as_float(alternative.get("adjustment_pct_of_raw_value")) or 0.0) > config.max_adjustment_pct:
            reasons.append("adjustment_pct_above_cap")
        if int(alternative.get("material_adjustment_count") or 0) > config.max_material_adjustment_count:
            reasons.append("material_adjustment_count_above_cap")
        return reasons

    def _find_displaced_candidate(
        self,
        *,
        alternative: dict[str, Any],
        included_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        candidates = sorted(
            included_rows,
            key=lambda row: (
                -(_as_float(row.get("adjusted_appraised_value")) or 0.0),
                str(row.get("candidate_parcel_id") or ""),
            ),
        )
        for displaced in candidates:
            if (_as_float(displaced.get("adjusted_appraised_value")) or 0.0) <= (
                _as_float(alternative.get("adjusted_appraised_value")) or 0.0
            ):
                continue
            if _support_rank(alternative) < _support_rank(displaced):
                continue
            if _burden_rank(alternative) < _burden_rank(displaced):
                continue
            if _source_rank(alternative) < _source_rank(displaced):
                continue
            if int(alternative.get("unresolved_review_only_channel_count") or 0) > int(
                displaced.get("unresolved_review_only_channel_count") or 0
            ):
                continue
            return displaced
        return None

    def _safety_rejection_reasons(
        self,
        *,
        baseline: dict[str, Any],
        recomputed: dict[str, Any],
        candidate_to_displace: dict[str, Any],
        alternative: dict[str, Any],
        config: TaxpayerFavorableTieBreakConfig,
        baseline_avg_similarity: float | None,
    ) -> list[str]:
        reasons: list[str] = []
        baseline_status = str(baseline.get("final_value_status") or "")
        recomputed_status = str(recomputed.get("final_value_status") or "")
        if _status_rank(recomputed_status) < _status_rank(baseline_status):
            reasons.append("status_worsened")
        if (recomputed.get("included_comp_count") or 0) < config.minimum_included_comp_count:
            reasons.append("included_comp_count_below_minimum")
        if (recomputed.get("excluded_review_heavy_count") or 0) > (
            baseline.get("excluded_review_heavy_count") or 0
        ):
            reasons.append("review_heavy_increased")
        if (recomputed.get("excluded_likely_exclude_count") or 0) > (
            baseline.get("excluded_likely_exclude_count") or 0
        ):
            reasons.append("likely_exclude_increased")
        baseline_median = _as_float(baseline.get("requested_roll_value"))
        recomputed_median = _as_float(recomputed.get("requested_roll_value"))
        if (
            baseline_median not in {None, 0.0}
            and recomputed_median is not None
            and abs(recomputed_median - baseline_median) / baseline_median
            > config.median_movement_cap_ratio
        ):
            reasons.append("median_movement_exceeded_cap")
        baseline_qa = dict(baseline.get("qa_flags") or {})
        recomputed_qa = dict(recomputed.get("qa_flags") or {})
        for key in (
            "leave_one_out_review_flag",
            "high_low_removal_review_flag",
            "adjusted_value_iqr_review_flag",
        ):
            if bool(recomputed_qa.get(key)) and not bool(baseline_qa.get(key)):
                reasons.append(f"{key}_triggered")
        recomputed_avg_similarity = _avg_similarity(recomputed.get("included_comp_rows") or [])
        if (
            baseline_avg_similarity is not None
            and recomputed_avg_similarity is not None
            and baseline_avg_similarity - recomputed_avg_similarity > config.max_avg_similarity_drop
        ):
            reasons.append("average_similarity_drop_exceeded")
        if _support_rank(alternative) < _support_rank(candidate_to_displace):
            reasons.append("support_posture_worsened")
        if _burden_rank(alternative) < _burden_rank(candidate_to_displace):
            reasons.append("burden_posture_worsened")
        if _source_rank(alternative) < _source_rank(candidate_to_displace):
            reasons.append("source_posture_worsened")
        return reasons

    def _recompute(
        self,
        *,
        run_context: dict[str, Any],
        included_rows: list[dict[str, Any]],
        excluded_rows: list[dict[str, Any]],
        subject_appraised_value: float | None,
    ) -> dict[str, Any]:
        included_sorted = sorted(
            [deepcopy(row) for row in included_rows if _as_float(row.get("adjusted_appraised_value")) is not None],
            key=lambda row: (
                _as_float(row.get("adjusted_appraised_value")) or 0.0,
                str(row.get("candidate_parcel_id") or ""),
            ),
        )
        for index, row in enumerate(included_sorted, start=1):
            row["final_value_position"] = index
        excluded_copy = [deepcopy(row) for row in excluded_rows]

        requested_roll_value = (
            round(float(median([row["adjusted_appraised_value"] for row in included_sorted])), 2)
            if included_sorted
            else None
        )
        requested_reduction_amount = _reduction_amount(
            subject_appraised_value,
            requested_roll_value,
        )
        requested_reduction_pct = _reduction_pct(
            subject_appraised_value,
            requested_reduction_amount,
        )
        stability_metrics = self._final_value_service._final_value_stability_metrics(
            included_rows=included_sorted,
        )
        qa_flags = self._final_value_service._final_value_qa_flags(
            run_context=run_context,
            included_rows=included_sorted,
            excluded_rows=excluded_copy,
            stability_metrics=stability_metrics,
        )
        final_value_status, governance_refinement_detail = (
            self._final_value_service._run_final_value_status(
                run_context=run_context,
                requested_roll_value=requested_roll_value,
                requested_reduction_amount=requested_reduction_amount,
                included_rows=included_sorted,
                excluded_rows=excluded_copy,
                qa_flags=qa_flags,
                stability_metrics=stability_metrics,
            )
        )
        return {
            "final_value_status": final_value_status,
            "support_status": run_context.get("support_status"),
            "requested_roll_value": requested_roll_value,
            "requested_reduction_amount": requested_reduction_amount,
            "requested_reduction_pct": requested_reduction_pct,
            "included_comp_count": len(included_sorted),
            "excluded_review_heavy_count": sum(
                1 for row in excluded_copy if row.get("final_value_status") == "excluded_review_heavy"
            ),
            "excluded_likely_exclude_count": sum(
                1 for row in excluded_copy if row.get("final_value_status") == "excluded_likely_exclude"
            ),
            "included_comp_rows": included_sorted,
            "excluded_comp_rows": excluded_copy,
            "stability_metrics": stability_metrics,
            "qa_flags": qa_flags,
            "governance_refinement_detail": governance_refinement_detail,
        }

    def _remains_defensible(
        self,
        *,
        baseline_included_rows: list[dict[str, Any]],
        final_included_rows: list[dict[str, Any]],
        baseline: dict[str, Any],
        final_result: dict[str, Any],
        config: TaxpayerFavorableTieBreakConfig,
    ) -> bool:
        baseline_avg_similarity = _avg_similarity(baseline_included_rows)
        final_avg_similarity = _avg_similarity(final_included_rows)
        if (
            baseline_avg_similarity is not None
            and final_avg_similarity is not None
            and baseline_avg_similarity - final_avg_similarity > config.max_avg_similarity_drop
        ):
            return False
        if _status_rank(str(final_result.get("final_value_status") or "")) < _status_rank(
            str(baseline.get("final_value_status") or "")
        ):
            return False
        return True

    def _rejected_record(
        self,
        alternative: dict[str, Any],
        reasons: list[str],
    ) -> dict[str, Any]:
        return {
            "candidate_parcel_id": alternative.get("candidate_parcel_id"),
            "address": alternative.get("address"),
            "similarity_score": alternative.get("similarity_score"),
            "adjusted_appraised_value": alternative.get("adjusted_appraised_value"),
            "review_visible_flag": alternative.get("review_visible_flag"),
            "source_governance_status": alternative.get("source_governance_status"),
            "burden_governance_status": alternative.get("burden_governance_status"),
            "adjusted_set_governance_status": alternative.get("adjusted_set_governance_status"),
            "material_adjustment_count": alternative.get("material_adjustment_count"),
            "adjustment_pct_of_raw_value": alternative.get("adjustment_pct_of_raw_value"),
            "rejection_reasons": reasons,
        }


def _avg_similarity(rows: list[dict[str, Any]]) -> float | None:
    values = [
        _as_float(row.get("similarity_score"))
        for row in rows
        if _as_float(row.get("similarity_score")) is not None
    ]
    return round(mean(values), 4) if values else None


def _min_similarity(rows: list[dict[str, Any]]) -> float | None:
    values = [
        _as_float(row.get("similarity_score"))
        for row in rows
        if _as_float(row.get("similarity_score")) is not None
    ]
    return min(values) if values else None


def _reduction_amount(subject_appraised_value: float | None, requested_roll_value: float | None) -> float | None:
    if subject_appraised_value is None or requested_roll_value is None:
        return None
    return round(max(0.0, subject_appraised_value - requested_roll_value), 2)


def _reduction_pct(subject_appraised_value: float | None, requested_reduction_amount: float | None) -> float | None:
    if subject_appraised_value in {None, 0.0} or requested_reduction_amount is None:
        return None
    return round(requested_reduction_amount / subject_appraised_value, 6)


def _support_rank(row: dict[str, Any]) -> int:
    return 1 if bool(row.get("review_visible_flag")) else 2


def _burden_rank(row: dict[str, Any]) -> int:
    status = str(row.get("burden_governance_status") or "")
    if status == "within_thresholds":
        return 2
    if status == "warning":
        return 1
    return 0


def _source_rank(row: dict[str, Any]) -> int:
    status = str(row.get("source_governance_status") or "")
    if status in {"hybrid_supported", "supported"}:
        return 3
    if status == "fallback_only":
        return 2
    return 0


def _status_rank(status: str) -> int:
    order = {
        "unsupported": 0,
        "manual_review_required": 1,
        "supported_with_review": 2,
        "supported": 3,
    }
    return order.get(status, -1)


def _bathroom_unresolved(row: dict[str, Any]) -> bool:
    context = dict(row.get("bathroom_boundary_context") or {})
    support = dict(context.get("candidate_bathroom_support") or {})
    return bool(support.get("unresolved_bathroom_support_flag"))


def _land_unresolved(row: dict[str, Any]) -> bool:
    return int(row.get("unresolved_review_only_channel_count") or 0) > 0


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
