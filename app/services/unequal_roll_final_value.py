from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import median
from typing import Any

from psycopg.types.json import Jsonb

from app.db.connection import get_connection

FINAL_VALUE_VERSION = "unequal_roll_final_value_v1"
FINAL_VALUE_CONFIG_VERSION = "unequal_roll_final_value_v1"

FINAL_VALUE_UNSUPPORTED_MIN_COMP_COUNT = 6
FINAL_VALUE_AUTO_SUPPORTED_MIN_COMP_COUNT = 8
FINAL_VALUE_LEAVE_ONE_OUT_REVIEW_DELTA = 25000.0
FINAL_VALUE_HIGH_LOW_REVIEW_DELTA = 15000.0
FINAL_VALUE_ADJUSTED_VALUE_IQR_REVIEW_THRESHOLD = 60000.0


@dataclass(frozen=True)
class UnequalRollFinalValueResult:
    unequal_roll_run_id: str
    final_value_status: str
    included_count: int
    included_with_review_count: int
    excluded_review_heavy_count: int
    excluded_likely_exclude_count: int
    requested_roll_value: float | None
    requested_reduction_amount: float | None
    requested_reduction_pct: float | None


class UnequalRollFinalValueService:
    def build_final_value_for_run(
        self,
        *,
        unequal_roll_run_id: str,
    ) -> UnequalRollFinalValueResult:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                run_context = self._fetch_run_context(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                )
                if run_context is None:
                    raise LookupError(
                        "Unequal-roll run context not found for final value "
                        f"{unequal_roll_run_id}."
                    )

                candidates = self._fetch_candidates(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                )
                if not candidates:
                    raise LookupError(
                        "Unequal-roll adjustment candidates not found for final value "
                        f"{unequal_roll_run_id}."
                    )

                adjustment_lines = self._fetch_adjustment_lines(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                )
                final_value_plan, final_value_output = self._build_final_value_plan(
                    candidates=candidates,
                    run_context=run_context,
                    adjustment_lines=adjustment_lines,
                )
                selection_log_json = self._build_final_value_selection_log(
                    run_context=run_context,
                    final_value_output=final_value_output,
                )
                summary_json = self._build_run_summary_json(
                    run_context=run_context,
                    final_value_output=final_value_output,
                )

                for candidate in candidates:
                    candidate_id = str(candidate["unequal_roll_candidate_id"])
                    self._persist_candidate_final_value(
                        cursor,
                        unequal_roll_candidate_id=candidate_id,
                        final_value_assignment=final_value_plan[candidate_id],
                    )

                self._persist_run_final_value(
                    cursor,
                    unequal_roll_run_id=unequal_roll_run_id,
                    final_value_output=final_value_output,
                    selection_log_json=selection_log_json,
                    summary_json=summary_json,
                )
            connection.commit()

        return UnequalRollFinalValueResult(
            unequal_roll_run_id=unequal_roll_run_id,
            final_value_status=final_value_output["final_value_status"],
            included_count=final_value_output["final_value_set_summary"]["included_count"],
            included_with_review_count=final_value_output["final_value_set_summary"][
                "included_with_review_count"
            ],
            excluded_review_heavy_count=final_value_output["final_value_set_summary"][
                "excluded_review_heavy_count"
            ],
            excluded_likely_exclude_count=final_value_output["final_value_set_summary"][
                "excluded_likely_exclude_count"
            ],
            requested_roll_value=final_value_output["requested_roll_value"],
            requested_reduction_amount=final_value_output["requested_reduction_amount"],
            requested_reduction_pct=final_value_output["requested_reduction_pct"],
        )

    def _fetch_run_context(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT
              urr.unequal_roll_run_id,
              urr.support_status,
              urr.selection_governance_status,
              urr.final_comp_count_status,
              urr.summary_json,
              urr.selection_log_json,
              urss.county_id,
              urss.tax_year,
              urss.appraised_value,
              urss.living_area_sf,
              urss.full_baths,
              urss.half_baths
            FROM unequal_roll_runs AS urr
            JOIN unequal_roll_subject_snapshots AS urss
              ON urss.unequal_roll_run_id = urr.unequal_roll_run_id
            WHERE urr.unequal_roll_run_id = %s
            LIMIT 1
            """,
            (unequal_roll_run_id,),
        )
        return cursor.fetchone()

    def _fetch_candidates(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              unequal_roll_candidate_id,
              unequal_roll_run_id,
              candidate_parcel_id,
              address,
              county_id,
              tax_year,
              living_area_sf,
              full_baths,
              half_baths,
              appraised_value,
              chosen_comp_status,
              chosen_comp_position,
              adjustment_math_status,
              adjusted_appraised_value,
              total_absolute_adjustment,
              adjustment_pct_of_raw_value,
              material_adjustment_count,
              nontrivial_adjustment_sources_count,
              adjustment_summary_json
            FROM unequal_roll_candidates
            WHERE unequal_roll_run_id = %s
            ORDER BY
              chosen_comp_position NULLS LAST,
              adjustment_support_position NULLS LAST,
              candidate_parcel_id
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _fetch_adjustment_lines(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
    ) -> list[dict[str, Any]]:
        cursor.execute(
            """
            SELECT
              unequal_roll_candidate_id,
              adjustment_type,
              signed_adjustment_amount
            FROM unequal_roll_adjustments
            WHERE unequal_roll_run_id = %s
            ORDER BY unequal_roll_candidate_id, adjustment_line_order
            """,
            (unequal_roll_run_id,),
        )
        return list(cursor.fetchall())

    def _build_final_value_plan(
        self,
        *,
        candidates: list[dict[str, Any]],
        run_context: dict[str, Any],
        adjustment_lines: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        line_items_by_candidate_id: dict[str, list[dict[str, Any]]] = {}
        for line in adjustment_lines:
            candidate_id = str(line["unequal_roll_candidate_id"])
            line_items_by_candidate_id.setdefault(candidate_id, []).append(line)

        plan: dict[str, dict[str, Any]] = {}
        included_rows: list[dict[str, Any]] = []
        excluded_rows: list[dict[str, Any]] = []

        for candidate in candidates:
            candidate_id = str(candidate["unequal_roll_candidate_id"])
            summary_json = dict(candidate.get("adjustment_summary_json") or {})
            adjusted_set_governance = dict(summary_json.get("adjusted_set_governance") or {})
            burden_governance = dict(summary_json.get("burden_governance") or {})
            source_governance = dict(summary_json.get("source_governance") or {})
            acceptable_zone_governance = dict(
                summary_json.get("acceptable_zone_governance") or {}
            )
            bathroom_boundary_context = dict(
                summary_json.get("bathroom_boundary_context") or {}
            )
            adjustment_conflict_support = dict(
                summary_json.get("adjustment_conflict_support") or {}
            )
            dispersion_scaffolding = dict(summary_json.get("dispersion_scaffolding") or {})

            final_value_status, exclusion_reason_code = self._candidate_final_value_status(
                candidate=candidate,
                adjusted_set_governance=adjusted_set_governance,
            )
            review_visible_flag = final_value_status == "included_in_final_value_with_review"
            included_flag = final_value_status in {
                "included_in_final_value",
                "included_in_final_value_with_review",
            }
            adjusted_appraised_value = _as_float(candidate.get("adjusted_appraised_value"))
            raw_appraised_value = _as_float(candidate.get("appraised_value"))
            living_area_sf = _as_float(candidate.get("living_area_sf"))
            adjusted_appraised_value_per_sf = _value_per_sf(
                adjusted_appraised_value,
                living_area_sf,
            )
            raw_appraised_value_per_sf = _value_per_sf(raw_appraised_value, living_area_sf)

            row = {
                "unequal_roll_candidate_id": candidate_id,
                "candidate_parcel_id": str(candidate.get("candidate_parcel_id") or ""),
                "address": candidate.get("address"),
                "final_value_status": final_value_status,
                "chosen_comp_status": candidate.get("chosen_comp_status"),
                "chosen_comp_position": _as_int(candidate.get("chosen_comp_position")),
                "review_visible_flag": review_visible_flag,
                "adjusted_appraised_value": adjusted_appraised_value,
                "adjusted_appraised_value_per_sf": adjusted_appraised_value_per_sf,
                "raw_appraised_value": raw_appraised_value,
                "raw_appraised_value_per_sf": raw_appraised_value_per_sf,
                "adjustment_math_status": candidate.get("adjustment_math_status"),
                "adjusted_set_governance_status": adjusted_set_governance.get("status"),
                "adjusted_set_governance_reason_codes": list(
                    adjusted_set_governance.get("reason_codes") or []
                ),
                "burden_governance_status": burden_governance.get("status"),
                "burden_governance_reason_codes": list(
                    burden_governance.get("reason_codes") or []
                ),
                "source_governance_status": source_governance.get("source_governance_status"),
                "review_carry_forward_flag": bool(summary_json.get("review_carry_forward_flag")),
                "acceptable_zone_governance": acceptable_zone_governance,
                "hybrid_supported_source_flag": bool(
                    adjusted_set_governance.get("hybrid_supported_source_flag")
                ),
                "unresolved_review_only_channel_count": _as_int(
                    adjusted_set_governance.get("unresolved_review_only_channel_count")
                )
                or 0,
                "bathroom_boundary_context": bathroom_boundary_context,
                "adjustment_conflict_support": adjustment_conflict_support,
                "conflict_divergence_governance": dict(
                    adjusted_set_governance.get("conflict_divergence_governance") or {}
                ),
                "dispersion_scaffolding": dispersion_scaffolding,
                "material_adjustment_count": _as_int(candidate.get("material_adjustment_count"))
                or 0,
                "adjustment_pct_of_raw_value": _as_float(
                    candidate.get("adjustment_pct_of_raw_value")
                ),
                "dominant_adjustment_channel": adjustment_conflict_support.get(
                    "dominant_adjustment_channel"
                ),
                "line_items": line_items_by_candidate_id.get(candidate_id, []),
            }

            detail_json = {
                "final_value_version": FINAL_VALUE_VERSION,
                "final_value_config_version": FINAL_VALUE_CONFIG_VERSION,
                "final_value_status": final_value_status,
                "final_value_position": None,
                "final_value_context": {
                    "included_in_median_flag": included_flag,
                    "review_visible_flag": review_visible_flag,
                    "exclusion_reason_code": exclusion_reason_code,
                    "inclusion_basis": "adjusted_set_governance_status",
                },
                "adjusted_value_context": {
                    "adjusted_appraised_value": adjusted_appraised_value,
                    "adjusted_appraised_value_per_sf": adjusted_appraised_value_per_sf,
                    "raw_appraised_value": raw_appraised_value,
                    "raw_appraised_value_per_sf": raw_appraised_value_per_sf,
                },
                "governance_carry_forward": {
                    "chosen_comp_status": candidate.get("chosen_comp_status"),
                    "chosen_comp_position": _as_int(candidate.get("chosen_comp_position")),
                    "adjustment_math_status": candidate.get("adjustment_math_status"),
                    "adjusted_set_governance_status": adjusted_set_governance.get("status"),
                    "adjusted_set_governance_reason_codes": list(
                        adjusted_set_governance.get("reason_codes") or []
                    ),
                    "burden_governance_status": burden_governance.get("status"),
                    "burden_governance_reason_codes": list(
                        burden_governance.get("reason_codes") or []
                    ),
                    "source_governance_status": source_governance.get(
                        "source_governance_status"
                    ),
                    "review_carry_forward_flag": bool(
                        summary_json.get("review_carry_forward_flag")
                    ),
                    "acceptable_zone_governance": acceptable_zone_governance,
                    "hybrid_supported_source_flag": bool(
                        adjusted_set_governance.get("hybrid_supported_source_flag")
                    ),
                    "unresolved_review_only_channel_count": _as_int(
                        adjusted_set_governance.get(
                            "unresolved_review_only_channel_count"
                        )
                    )
                    or 0,
                },
                "conflict_divergence_governance": dict(
                    adjusted_set_governance.get("conflict_divergence_governance") or {}
                ),
                "bathroom_boundary_context": bathroom_boundary_context,
            }

            plan[candidate_id] = {
                "final_value_position": None,
                "final_value_status": final_value_status,
                "final_value_detail_json": detail_json,
            }

            if included_flag and adjusted_appraised_value is not None:
                included_rows.append(row)
            else:
                row["exclusion_reason_code"] = exclusion_reason_code
                excluded_rows.append(row)

        included_rows.sort(
            key=lambda row: (
                row["adjusted_appraised_value"],
                row["candidate_parcel_id"],
            )
        )
        for position, row in enumerate(included_rows, start=1):
            candidate_id = row["unequal_roll_candidate_id"]
            plan[candidate_id]["final_value_position"] = position
            plan[candidate_id]["final_value_detail_json"]["final_value_position"] = position

        included_values = [
            row["adjusted_appraised_value"]
            for row in included_rows
            if row["adjusted_appraised_value"] is not None
        ]
        requested_roll_value = (
            round(float(median(included_values)), 2) if included_values else None
        )
        subject_appraised_value = _as_float(run_context.get("appraised_value"))
        requested_reduction_amount = _reduction_amount(
            subject_appraised_value,
            requested_roll_value,
        )
        requested_reduction_pct = _reduction_pct(
            subject_appraised_value,
            requested_reduction_amount,
        )
        stability_metrics = self._final_value_stability_metrics(
            included_rows=included_rows,
        )
        qa_flags = self._final_value_qa_flags(
            run_context=run_context,
            included_rows=included_rows,
            excluded_rows=excluded_rows,
            stability_metrics=stability_metrics,
        )
        final_value_status = self._run_final_value_status(
            run_context=run_context,
            requested_roll_value=requested_roll_value,
            included_rows=included_rows,
            excluded_rows=excluded_rows,
            qa_flags=qa_flags,
        )

        ordered_adjusted_values = [
            {
                "final_value_position": position,
                "unequal_roll_candidate_id": row["unequal_roll_candidate_id"],
                "candidate_parcel_id": row["candidate_parcel_id"],
                "address": row["address"],
                "chosen_comp_status": row["chosen_comp_status"],
                "review_visible_flag": row["review_visible_flag"],
                "acceptable_zone_admitted_flag": bool(
                    row["acceptable_zone_governance"].get("acceptable_zone_admitted_flag")
                ),
                "adjusted_appraised_value": row["adjusted_appraised_value"],
                "adjusted_appraised_value_per_sf": row["adjusted_appraised_value_per_sf"],
            }
            for position, row in enumerate(included_rows, start=1)
        ]
        median_calculation = _median_calculation_detail(ordered_adjusted_values)
        final_value_set_summary = {
            "included_count": len(included_rows),
            "included_with_review_count": sum(
                1 for row in included_rows if row["review_visible_flag"]
            ),
            "excluded_review_heavy_count": sum(
                1
                for row in excluded_rows
                if row["final_value_status"] == "excluded_review_heavy"
            ),
            "excluded_likely_exclude_count": sum(
                1
                for row in excluded_rows
                if row["final_value_status"] == "excluded_likely_exclude"
            ),
            "all_included_review_visible_flag": bool(included_rows)
            and all(row["review_visible_flag"] for row in included_rows),
            "accepted_from_adjusted_set_status_counts": dict(
                Counter(row["final_value_status"] for row in included_rows)
            ),
            "excluded_status_counts": dict(
                Counter(row["final_value_status"] for row in excluded_rows)
            ),
        }

        final_value_output = {
            "final_value_status": final_value_status,
            "requested_roll_value": requested_roll_value,
            "requested_reduction_amount": requested_reduction_amount,
            "requested_reduction_pct": requested_reduction_pct,
            "final_value_set_summary": final_value_set_summary,
            "ordered_adjusted_values": ordered_adjusted_values,
            "median_calculation": median_calculation,
            "stability_metrics": stability_metrics,
            "qa_flags": qa_flags,
            "included_comp_rows": [
                self._final_value_row_without_line_items(row) for row in included_rows
            ],
            "excluded_comp_rows": [
                self._final_value_row_without_line_items(row) for row in excluded_rows
            ],
            "methodology_guardrails": {
                "final_requested_value_formula": "median_of_adjusted_appraised_values",
                "similarity_score_selection_only_flag": True,
                "raw_psf_diagnostic_only_flag": True,
                "weighted_psf_shortcut_used_flag": False,
            },
            "carried_forward_governance": {
                "selection_governance_status": run_context.get(
                    "selection_governance_status"
                ),
                "final_comp_count_status": run_context.get("final_comp_count_status"),
                "support_status": run_context.get("support_status"),
                "adjustment_math": dict(
                    (run_context.get("selection_log_json") or {}).get("adjustment_math")
                    or {}
                ),
            },
        }
        return plan, final_value_output

    def _final_value_row_without_line_items(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if key != "line_items"
        }

    def _candidate_final_value_status(
        self,
        *,
        candidate: dict[str, Any],
        adjusted_set_governance: dict[str, Any],
    ) -> tuple[str, str | None]:
        adjustment_math_status = str(candidate.get("adjustment_math_status") or "")
        if adjustment_math_status == "excluded_from_adjustment_math":
            return "excluded_from_final_value", "excluded_from_adjustment_math"
        if _as_float(candidate.get("adjusted_appraised_value")) is None:
            return "excluded_from_final_value", "missing_adjusted_appraised_value"

        adjusted_set_status = str(adjusted_set_governance.get("status") or "")
        if adjusted_set_status == "usable_adjusted_comp":
            return "included_in_final_value", None
        if adjusted_set_status == "usable_with_review_adjusted_comp":
            return "included_in_final_value_with_review", None
        if adjusted_set_status == "review_heavy_adjusted_comp":
            return "excluded_review_heavy", "adjusted_set_review_heavy"
        if adjusted_set_status == "likely_exclude_adjusted_comp":
            return "excluded_likely_exclude", "adjusted_set_likely_exclude"
        return "excluded_from_final_value", "unsupported_adjusted_set_status"

    def _final_value_stability_metrics(
        self,
        *,
        included_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        adjusted_values = [
            row["adjusted_appraised_value"]
            for row in included_rows
            if row["adjusted_appraised_value"] is not None
        ]
        sorted_values = sorted(adjusted_values)
        median_all = round(float(median(sorted_values)), 2) if sorted_values else None
        minus_high_low_values = (
            sorted_values[1:-1] if len(sorted_values) > 2 else list(sorted_values)
        )
        median_minus_high_low = (
            round(float(median(minus_high_low_values)), 2)
            if minus_high_low_values
            else median_all
        )
        trimmed_values = (
            sorted_values[1:-1] if len(sorted_values) > 2 else list(sorted_values)
        )
        trimmed_median_adjusted_value = (
            round(float(median(trimmed_values)), 2)
            if trimmed_values
            else median_all
        )
        average_adjustment_pct = _avg(
            [
                row["adjustment_pct_of_raw_value"]
                for row in included_rows
                if row["adjustment_pct_of_raw_value"] is not None
            ]
        )
        max_adjustment_pct = _max_or_none(
            [
                row["adjustment_pct_of_raw_value"]
                for row in included_rows
                if row["adjustment_pct_of_raw_value"] is not None
            ]
        )
        material_adjustment_count_max = _max_or_none(
            [row["material_adjustment_count"] for row in included_rows]
        )
        dominant_adjustment_channel = _dominant_adjustment_channel(
            [
                line_item
                for row in included_rows
                for line_item in row["line_items"]
            ]
        )
        adjusted_value_stats = _iqr_stats(adjusted_values)

        return {
            "median_all": median_all,
            "median_minus_high_low": median_minus_high_low,
            "trimmed_median_adjusted_value": trimmed_median_adjusted_value,
            "max_leave_one_out_delta": _max_leave_one_out_delta(adjusted_values),
            "median_absolute_deviation_adjusted_values": _median_absolute_deviation(
                adjusted_values
            ),
            "adjusted_value_iqr": adjusted_value_stats.get("iqr"),
            "adjusted_value_stats": adjusted_value_stats,
            "average_adjustment_pct": average_adjustment_pct,
            "max_adjustment_pct": max_adjustment_pct,
            "material_adjustment_count_max": material_adjustment_count_max,
            "dominant_adjustment_channel": dominant_adjustment_channel,
        }

    def _final_value_qa_flags(
        self,
        *,
        run_context: dict[str, Any],
        included_rows: list[dict[str, Any]],
        excluded_rows: list[dict[str, Any]],
        stability_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        summary_json = dict(run_context.get("summary_json") or {})
        discovery_summary = dict(summary_json.get("candidate_discovery_summary") or {})
        selection_log_json = dict(run_context.get("selection_log_json") or {})
        adjustment_math_log = dict(selection_log_json.get("adjustment_math") or {})
        run_dispersion = dict(adjustment_math_log.get("dispersion_scaffolding") or {})
        median_all = _as_float(stability_metrics.get("median_all"))
        median_minus_high_low = _as_float(stability_metrics.get("median_minus_high_low"))

        leave_one_out_review_flag = bool(
            _as_float(stability_metrics.get("max_leave_one_out_delta")) is not None
            and _as_float(stability_metrics.get("max_leave_one_out_delta"))
            > FINAL_VALUE_LEAVE_ONE_OUT_REVIEW_DELTA
        )
        high_low_review_flag = bool(
            median_all is not None
            and median_minus_high_low is not None
            and abs(median_all - median_minus_high_low)
            > FINAL_VALUE_HIGH_LOW_REVIEW_DELTA
        )
        adjusted_value_iqr_review_flag = bool(
            _as_float(stability_metrics.get("adjusted_value_iqr")) is not None
            and _as_float(stability_metrics.get("adjusted_value_iqr"))
            > FINAL_VALUE_ADJUSTED_VALUE_IQR_REVIEW_THRESHOLD
        )
        included_with_review_count = sum(
            1 for row in included_rows if row["review_visible_flag"]
        )

        return {
            "final_value_set_under_manual_review_minimum_flag": len(included_rows)
            < FINAL_VALUE_UNSUPPORTED_MIN_COMP_COUNT,
            "final_value_set_under_auto_supported_minimum_flag": len(included_rows)
            < FINAL_VALUE_AUTO_SUPPORTED_MIN_COMP_COUNT,
            "leave_one_out_review_flag": leave_one_out_review_flag,
            "high_low_removal_review_flag": high_low_review_flag,
            "adjusted_value_iqr_review_flag": adjusted_value_iqr_review_flag,
            "fallback_geography_used_flag": bool(discovery_summary.get("fallback_used")),
            "raw_sf_divergence_check_flag": bool(
                run_dispersion.get("raw_sf_divergence_check_flag")
            ),
            "all_included_review_visible_flag": bool(included_rows)
            and included_with_review_count == len(included_rows),
            "review_heavy_comp_excluded_flag": any(
                row["final_value_status"] == "excluded_review_heavy"
                for row in excluded_rows
            ),
            "likely_exclude_comp_excluded_flag": any(
                row["final_value_status"] == "excluded_likely_exclude"
                for row in excluded_rows
            ),
        }

    def _run_final_value_status(
        self,
        *,
        run_context: dict[str, Any],
        requested_roll_value: float | None,
        included_rows: list[dict[str, Any]],
        excluded_rows: list[dict[str, Any]],
        qa_flags: dict[str, Any],
    ) -> str:
        if run_context.get("selection_governance_status") == "unsupported":
            return "unsupported"
        if requested_roll_value is None or len(included_rows) < FINAL_VALUE_UNSUPPORTED_MIN_COMP_COUNT:
            return "unsupported"
        if (
            run_context.get("selection_governance_status") == "manual_review_required"
            or qa_flags["final_value_set_under_auto_supported_minimum_flag"]
            or qa_flags["leave_one_out_review_flag"]
            or qa_flags["high_low_removal_review_flag"]
            or qa_flags["adjusted_value_iqr_review_flag"]
            or qa_flags["all_included_review_visible_flag"]
        ):
            return "manual_review_required"
        if (
            any(row["review_visible_flag"] for row in included_rows)
            or qa_flags["fallback_geography_used_flag"]
            or qa_flags["review_heavy_comp_excluded_flag"]
            or qa_flags["likely_exclude_comp_excluded_flag"]
            or run_context.get("selection_governance_status") == "supported_with_warnings"
            or run_context.get("support_status") == "supported_with_review"
        ):
            return "supported_with_review"
        return "supported"

    def _build_final_value_selection_log(
        self,
        *,
        run_context: dict[str, Any],
        final_value_output: dict[str, Any],
    ) -> dict[str, Any]:
        selection_log_json = dict(run_context.get("selection_log_json") or {})
        selection_log_json["final_value"] = {
            "final_value_version": FINAL_VALUE_VERSION,
            "final_value_config_version": FINAL_VALUE_CONFIG_VERSION,
            "final_value_status": final_value_output["final_value_status"],
            "requested_roll_value": final_value_output["requested_roll_value"],
            "requested_reduction_amount": final_value_output[
                "requested_reduction_amount"
            ],
            "requested_reduction_pct": final_value_output["requested_reduction_pct"],
            "final_value_set_summary": final_value_output["final_value_set_summary"],
            "median_calculation": final_value_output["median_calculation"],
            "stability_metrics": final_value_output["stability_metrics"],
            "qa_flags": final_value_output["qa_flags"],
            "methodology_guardrails": final_value_output["methodology_guardrails"],
        }
        return selection_log_json

    def _build_run_summary_json(
        self,
        *,
        run_context: dict[str, Any],
        final_value_output: dict[str, Any],
    ) -> dict[str, Any]:
        summary_json = dict(run_context.get("summary_json") or {})
        summary_json["final_value_summary"] = {
            "final_value_status": final_value_output["final_value_status"],
            "requested_roll_value": final_value_output["requested_roll_value"],
            "requested_reduction_amount": final_value_output["requested_reduction_amount"],
            "requested_reduction_pct": final_value_output["requested_reduction_pct"],
            "included_count": final_value_output["final_value_set_summary"][
                "included_count"
            ],
            "included_with_review_count": final_value_output["final_value_set_summary"][
                "included_with_review_count"
            ],
        }
        return summary_json

    def _persist_candidate_final_value(
        self,
        cursor: Any,
        *,
        unequal_roll_candidate_id: str,
        final_value_assignment: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_candidates
            SET final_value_position = %s,
                final_value_status = %s,
                final_value_detail_json = %s,
                updated_at = now()
            WHERE unequal_roll_candidate_id = %s
            """,
            (
                final_value_assignment["final_value_position"],
                final_value_assignment["final_value_status"],
                Jsonb(final_value_assignment["final_value_detail_json"]),
                unequal_roll_candidate_id,
            ),
        )

    def _persist_run_final_value(
        self,
        cursor: Any,
        *,
        unequal_roll_run_id: str,
        final_value_output: dict[str, Any],
        selection_log_json: dict[str, Any],
        summary_json: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            UPDATE unequal_roll_runs
            SET final_value_status = %s,
                final_value_version = %s,
                final_value_config_version = %s,
                requested_roll_value = %s,
                requested_reduction_amount = %s,
                requested_reduction_pct = %s,
                final_value_detail_json = %s,
                selection_log_json = %s,
                summary_json = %s,
                updated_at = now()
            WHERE unequal_roll_run_id = %s
            """,
            (
                final_value_output["final_value_status"],
                FINAL_VALUE_VERSION,
                FINAL_VALUE_CONFIG_VERSION,
                final_value_output["requested_roll_value"],
                final_value_output["requested_reduction_amount"],
                final_value_output["requested_reduction_pct"],
                Jsonb(final_value_output),
                Jsonb(selection_log_json),
                Jsonb(summary_json),
                unequal_roll_run_id,
            ),
        )


def _median_calculation_detail(
    ordered_adjusted_values: list[dict[str, Any]],
) -> dict[str, Any]:
    values = [row["adjusted_appraised_value"] for row in ordered_adjusted_values]
    if not values:
        return {
            "count": 0,
            "ordered_adjusted_values": [],
            "requested_roll_value": None,
        }

    midpoint = len(values) // 2
    if len(values) % 2 == 1:
        lower_idx = upper_idx = midpoint
    else:
        lower_idx = midpoint - 1
        upper_idx = midpoint

    requested_roll_value = round(float(median(values)), 2)
    return {
        "count": len(values),
        "ordered_adjusted_values": values,
        "requested_roll_value": requested_roll_value,
        "odd_count_flag": len(values) % 2 == 1,
        "middle_position_lower": lower_idx + 1,
        "middle_position_upper": upper_idx + 1,
        "middle_value_lower": values[lower_idx],
        "middle_value_upper": values[upper_idx],
    }


def _reduction_amount(
    subject_appraised_value: float | None,
    requested_roll_value: float | None,
) -> float | None:
    if subject_appraised_value is None or requested_roll_value is None:
        return None
    return round(max(0.0, subject_appraised_value - requested_roll_value), 2)


def _reduction_pct(
    subject_appraised_value: float | None,
    requested_reduction_amount: float | None,
) -> float | None:
    if (
        subject_appraised_value in {None, 0.0}
        or requested_reduction_amount is None
    ):
        return None
    return round(requested_reduction_amount / subject_appraised_value, 6)


def _dominant_adjustment_channel(line_items: list[dict[str, Any]]) -> str | None:
    totals: dict[str, float] = {}
    for line_item in line_items:
        amount = _as_float(line_item.get("signed_adjustment_amount"))
        if amount is None:
            continue
        adjustment_type = str(line_item.get("adjustment_type") or "")
        totals[adjustment_type] = totals.get(adjustment_type, 0.0) + abs(amount)
    if not totals:
        return None
    return max(totals.items(), key=lambda item: item[1])[0]


def _value_per_sf(value: float | None, living_area_sf: float | None) -> float | None:
    if value is None or living_area_sf in {None, 0.0}:
        return None
    return round(value / living_area_sf, 2)


def _iqr_stats(values: list[float]) -> dict[str, float | None]:
    ordered = sorted(float(value) for value in values if value is not None)
    if not ordered:
        return {
            "min": None,
            "q1": None,
            "median": None,
            "q3": None,
            "max": None,
            "iqr": None,
        }
    if len(ordered) == 1:
        only = round(ordered[0], 2)
        return {
            "min": only,
            "q1": only,
            "median": only,
            "q3": only,
            "max": only,
            "iqr": 0.0,
        }
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 0:
        lower = ordered[:midpoint]
        upper = ordered[midpoint:]
    else:
        lower = ordered[:midpoint]
        upper = ordered[midpoint + 1 :]
    q1 = median(lower) if lower else ordered[0]
    q3 = median(upper) if upper else ordered[-1]
    med = median(ordered)
    return {
        "min": round(ordered[0], 2),
        "q1": round(float(q1), 2),
        "median": round(float(med), 2),
        "q3": round(float(q3), 2),
        "max": round(ordered[-1], 2),
        "iqr": round(float(q3 - q1), 2),
    }


def _max_leave_one_out_delta(values: list[float]) -> float | None:
    if len(values) <= 1:
        return None
    overall_median = median(values)
    deltas = []
    for index in range(len(values)):
        subset = values[:index] + values[index + 1 :]
        if not subset:
            continue
        deltas.append(abs(median(subset) - overall_median))
    if not deltas:
        return None
    return round(max(deltas), 2)


def _median_absolute_deviation(values: list[float]) -> float | None:
    if not values:
        return None
    center = median(values)
    deviations = [abs(value - center) for value in values]
    return round(median(deviations), 2)


def _avg(values: list[float]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def _max_or_none(values: list[float | int]) -> float | int | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    maximum = max(filtered)
    return round(maximum, 6) if isinstance(maximum, float) else maximum


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
