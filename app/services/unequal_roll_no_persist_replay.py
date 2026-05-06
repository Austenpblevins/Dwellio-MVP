from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from app.services.unequal_roll_candidate_adjustment_math import (
    UnequalRollCandidateAdjustmentMathService,
)
from app.services.unequal_roll_candidate_adjustment_support import (
    UnequalRollCandidateAdjustmentSupportService,
)
from app.services.unequal_roll_candidate_chosen_comp import (
    UnequalRollCandidateChosenCompService,
)
from app.services.unequal_roll_candidate_discovery import (
    DISCOVERY_TIER_COUNTY_SFR_FALLBACK,
    DISCOVERY_TIER_SAME_NEIGHBORHOOD,
    MAX_AUTO_HARVEST,
    PREFERRED_RAW_CANDIDATE_POOL,
    SPARSE_UNIVERSE_WARNING_THRESHOLD,
    UnequalRollCandidateDiscoveryService,
)
from app.services.unequal_roll_candidate_final_selection_support import (
    UnequalRollCandidateFinalSelectionSupportService,
)
from app.services.unequal_roll_candidate_ranking import UnequalRollCandidateRankingService
from app.services.unequal_roll_candidate_scoring import compute_similarity_score
from app.services.unequal_roll_candidate_shortlist import UnequalRollCandidateShortlistService
from app.services.unequal_roll_final_value import UnequalRollFinalValueService
from app.services.unequal_roll_review_evidence import (
    MODEL_OUTCOME_STATUSES,
    classify_unsupported_value_semantics,
    evidence_completeness_grade,
    summarize_run_state_candidates,
)
from app.services.unequal_roll_subject_snapshot import UnequalRollSubjectSnapshotService
from app.services.unequal_roll_bathroom_support import attach_bathroom_support_context
from infra.scripts.emit_unequal_roll_producer_downstream_payloads import (
    _compact_from_final_value_detail_json,
)


SUPPORTED_REVIEWABLE_STATUSES = {"supported", "supported_with_review"}


@dataclass(frozen=True)
class UnequalRollReplayRequest:
    county_id: str
    account_number: str
    requested_tax_year: int


class UnequalRollNoPersistReplayService:
    def __init__(self) -> None:
        self._subject_snapshot_service = UnequalRollSubjectSnapshotService()
        self._discovery_service = UnequalRollCandidateDiscoveryService()
        self._ranking_service = UnequalRollCandidateRankingService()
        self._shortlist_service = UnequalRollCandidateShortlistService()
        self._final_selection_support_service = (
            UnequalRollCandidateFinalSelectionSupportService()
        )
        self._chosen_comp_service = UnequalRollCandidateChosenCompService()
        self._adjustment_support_service = UnequalRollCandidateAdjustmentSupportService()
        self._adjustment_math_service = UnequalRollCandidateAdjustmentMathService()
        self._final_value_service = UnequalRollFinalValueService()

    def connect_read_only(self, database_url: str) -> Any:
        connection = psycopg.connect(database_url, row_factory=dict_row)
        connection.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        return connection

    def replay_subject(
        self,
        cursor: Any,
        *,
        request: UnequalRollReplayRequest,
        statement_timeout: str = "120s",
        max_parallel_workers_per_gather: int = 0,
    ) -> dict[str, Any]:
        start = monotonic()
        cursor.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
        cursor.execute(
            f"SET LOCAL max_parallel_workers_per_gather = {int(max_parallel_workers_per_gather)}"
        )
        try:
            subject_row = self._subject_snapshot_service._fetch_subject_row(
                cursor,
                county_id=request.county_id,
                requested_tax_year=request.requested_tax_year,
                account_number=request.account_number,
            )
        except psycopg.errors.QueryCanceled:
            return self._blocked_subject_result(
                request=request,
                blocker_code="subject_snapshot_query_timeout",
                reason="subject snapshot source query timed out under the configured 120s read-only profile",
                elapsed_total_s=round(monotonic() - start, 4),
            )
        if subject_row is None:
            return self._blocked_subject_result(
                request=request,
                blocker_code="subject_not_found",
                reason="subject not found in Stage 21 source tables",
                elapsed_total_s=round(monotonic() - start, 4),
            )

        subject_snapshot = self._build_subject_snapshot(
            cursor,
            request=request,
            subject_row=subject_row,
        )
        if subject_snapshot["support_status"] == "unsupported":
            return self._blocked_subject_result(
                request=request,
                blocker_code=str(subject_snapshot.get("support_blocker_code") or "subject_not_ready"),
                reason="subject snapshot is unsupported under current source coverage/readiness rules",
                elapsed_total_s=round(monotonic() - start, 4),
                subject_snapshot=subject_snapshot,
            )

        candidates, discovery_summary = self._discover_candidates(
            cursor,
            subject_snapshot=subject_snapshot,
        )

        if not candidates:
            return self._blocked_subject_result(
                request=request,
                blocker_code="no_candidates_discovered",
                reason="candidate discovery returned no candidates",
                elapsed_total_s=round(monotonic() - start, 4),
                subject_snapshot=subject_snapshot,
                discovery_summary=discovery_summary,
            )

        run_context = self._build_initial_run_context(
            subject_snapshot=subject_snapshot,
            discovery_summary=discovery_summary,
        )

        self._apply_plan(
            candidates=candidates,
            plan=self._ranking_service._build_ranking_plan(candidates),
            version_fields={
                "ranking_version": "ranking_version",
                "ranking_config_version": "ranking_config_version",
            },
        )
        self._apply_plan(
            candidates=candidates,
            plan=self._shortlist_service._build_shortlist_plan(candidates),
            version_fields={
                "shortlist_version": "shortlist_version",
                "shortlist_config_version": "shortlist_config_version",
            },
        )
        self._apply_plan(
            candidates=candidates,
            plan=self._final_selection_support_service._build_support_plan(candidates),
            version_fields={
                "final_selection_support_version": "final_selection_support_version",
                "final_selection_support_config_version": "final_selection_support_config_version",
            },
        )

        chosen_comp_plan = self._chosen_comp_service._build_chosen_comp_plan(
            candidates,
            run_context=run_context,
        )
        self._apply_plan(candidates=candidates, plan=chosen_comp_plan, version_fields={})
        governance = self._chosen_comp_service._build_selection_governance(
            candidates=candidates,
            chosen_comp_plan=chosen_comp_plan,
            run_context=run_context,
        )
        run_context["final_comp_count_status"] = governance["final_comp_count_status"]
        run_context["selection_governance_status"] = governance["selection_governance_status"]
        run_context["selection_log_json"] = governance["selection_log_json"]

        adjustment_support_plan = self._adjustment_support_service._build_adjustment_support_plan(
            candidates=candidates,
            run_context=run_context,
        )
        self._apply_plan(candidates=candidates, plan=adjustment_support_plan, version_fields={})
        run_context["selection_log_json"] = (
            self._adjustment_support_service._build_adjustment_log_json(
                candidates=candidates,
                run_context=run_context,
                adjustment_support_plan=adjustment_support_plan,
            )
        )

        adjustment_math_plan, dispersion_support = (
            self._adjustment_math_service._build_adjustment_math_plan(
                candidates=candidates,
                run_context=run_context,
            )
        )
        self._apply_plan(candidates=candidates, plan=adjustment_math_plan, version_fields={})
        run_context["selection_log_json"] = self._adjustment_math_service._build_adjustment_log_json(
            run_context=run_context,
            adjustment_plan=adjustment_math_plan,
            dispersion_support=dispersion_support,
        )

        adjustment_lines = self._build_adjustment_lines(adjustment_math_plan)
        final_value_plan, final_value_output = self._final_value_service._build_final_value_plan(
            candidates=candidates,
            run_context=run_context,
            adjustment_lines=adjustment_lines,
        )
        self._apply_plan(candidates=candidates, plan=final_value_plan, version_fields={})
        run_context["selection_log_json"] = self._final_value_service._build_final_value_selection_log(
            run_context=run_context,
            final_value_output=final_value_output,
        )
        run_context["summary_json"] = self._final_value_service._build_run_summary_json(
            run_context=run_context,
            final_value_output=final_value_output,
        )

        full_final_value_detail = self._build_full_final_value_detail(
            final_value_output=final_value_output,
            candidates=candidates,
            adjustment_math_plan=adjustment_math_plan,
            run_context=run_context,
        )
        compact_review_payload = _compact_from_final_value_detail_json(
            full_final_value_detail,
            detail_source="no_persist_replay.final_value_detail_json",
        )

        unsupported_semantics = classify_unsupported_value_semantics(
            current_appraised_value=_as_float(subject_snapshot.get("appraised_value")),
            final_value_status=final_value_output.get("final_value_status"),
            exposed_requested_roll_value=_as_float(final_value_output.get("requested_roll_value")),
            exposed_requested_reduction_amount=_as_float(
                final_value_output.get("requested_reduction_amount")
            ),
            exposed_requested_reduction_pct=_as_float(
                final_value_output.get("requested_reduction_pct")
            ),
        )
        run_state_payload = self._build_run_state_payload(candidates)
        run_state_summary = summarize_run_state_candidates(run_state_payload)
        stability_metrics = dict(full_final_value_detail.get("stability_metrics") or {})
        stability_available = any(
            stability_metrics.get(key) is not None
            for key in (
                "median_all",
                "median_minus_high_low",
                "max_leave_one_out_delta",
                "adjusted_value_iqr",
            )
        )
        final_status = final_value_output.get("final_value_status")
        evidence_grade = evidence_completeness_grade(
            final_reconciled_status=final_status,
            model_outcome_complete=final_status in MODEL_OUTCOME_STATUSES,
            subject_context_present=True,
            comp_evidence_present=bool(
                compact_review_payload.get("included_comp_rows")
                or compact_review_payload.get("excluded_comp_rows")
            ),
            stability_metrics_present=stability_available,
            none_origin="not_applicable",
        )

        result = {
            "account": request.account_number,
            "county": request.county_id,
            "requested_tax_year": request.requested_tax_year,
            "served_tax_year": subject_snapshot.get("tax_year"),
            "parcel_id": subject_snapshot.get("parcel_id"),
            "address": subject_snapshot.get("address"),
            "neighborhood": subject_snapshot.get("neighborhood_code"),
            "current_appraised_value": _as_float(subject_snapshot.get("appraised_value")),
            "support_status": subject_snapshot.get("support_status"),
            "readiness_status": subject_snapshot.get("readiness_status"),
            "source_coverage_status": subject_snapshot.get("source_coverage_status"),
            "replay_status": "completed",
            "final_value_status": final_status,
            "requested_roll_value": final_value_output.get("requested_roll_value"),
            "requested_reduction_amount": final_value_output.get("requested_reduction_amount"),
            "requested_reduction_pct": final_value_output.get("requested_reduction_pct"),
            "same_neighborhood_count": discovery_summary.get("same_neighborhood_count"),
            "fallback_count": discovery_summary.get("county_sfr_fallback_count"),
            "fallback_used": discovery_summary.get("fallback_used"),
            "included_comp_count": final_value_output["final_value_set_summary"]["included_count"],
            "excluded_review_heavy_count": final_value_output["final_value_set_summary"][
                "excluded_review_heavy_count"
            ],
            "excluded_likely_exclude_count": final_value_output["final_value_set_summary"][
                "excluded_likely_exclude_count"
            ],
            "summary_json": run_context.get("summary_json"),
            "selection_log_json": run_context.get("selection_log_json"),
            "subject_snapshot_json": subject_snapshot.get("snapshot_json"),
            "subject_source_provenance_json": subject_snapshot.get("source_provenance_json"),
            "final_value_detail_json": full_final_value_detail,
            "compact_final_value_review_payload": compact_review_payload,
            "run_state_payload": run_state_payload,
            "run_state_summary": run_state_summary,
            "safe_requested_roll_value": unsupported_semantics["safe_requested_roll_value"],
            "safe_requested_reduction_amount": unsupported_semantics[
                "safe_requested_reduction_amount"
            ],
            "safe_requested_reduction_pct": unsupported_semantics[
                "safe_requested_reduction_pct"
            ],
            "value_interpretation": unsupported_semantics["value_interpretation"],
            "value_interpretation_reason": unsupported_semantics[
                "value_interpretation_reason"
            ],
            "evidence_completeness_grade": evidence_grade,
            "stability_metrics_available": stability_available,
            "stability_metrics_unavailable_reason": None
            if stability_available
            else self._stability_unavailable_reason(
                final_value_detail_json=full_final_value_detail
            ),
            "elapsed_total_s": round(monotonic() - start, 4),
        }
        return result

    def _build_subject_snapshot(
        self,
        cursor: Any,
        *,
        request: UnequalRollReplayRequest,
        subject_row: dict[str, Any],
    ) -> dict[str, Any]:
        served_tax_year = int(subject_row["tax_year"])
        parcel_id = str(subject_row["parcel_id"])
        valuation_bathroom_features_json = (
            self._subject_snapshot_service._build_valuation_bathroom_features_json(
                cursor,
                county_id=request.county_id,
                parcel_id=parcel_id,
                tax_year=served_tax_year,
            )
        )
        valuation_bathroom_features_json = attach_bathroom_support_context(
            county_id=request.county_id,
            canonical_full_baths=subject_row.get("full_baths"),
            canonical_half_baths=subject_row.get("half_baths"),
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        support_status, readiness_status, support_blocker_code = (
            self._subject_snapshot_service._derive_support_status(
                subject_row,
                county_id=request.county_id,
                requested_tax_year=request.requested_tax_year,
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            )
        )
        source_coverage_status = self._subject_snapshot_service._derive_source_coverage_status(
            county_id=request.county_id,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        summary_json = {
            "requested_tax_year": request.requested_tax_year,
            "served_tax_year": served_tax_year,
            "tax_year_fallback_applied": served_tax_year != request.requested_tax_year,
            "account_number": request.account_number,
            "support_status": support_status,
            "warning_codes": [str(code) for code in subject_row.get("warning_codes") or []],
            "completeness_score": float(subject_row.get("completeness_score") or 0.0),
            "source_coverage_status": source_coverage_status,
            "valuation_bathroom_attachment_status": (
                valuation_bathroom_features_json.get("attachment_status")
                if valuation_bathroom_features_json is not None
                else "not_applicable"
            ),
        }
        snapshot_json = self._subject_snapshot_service._build_snapshot_json(
            row=subject_row,
            requested_tax_year=request.requested_tax_year,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        source_provenance_json = self._subject_snapshot_service._build_source_provenance_json(
            row=subject_row,
            requested_tax_year=request.requested_tax_year,
            valuation_bathroom_features_json=valuation_bathroom_features_json,
        )
        bathroom_support = dict(snapshot_json.get("bathroom_support") or {})
        return {
            **subject_row,
            "requested_tax_year": request.requested_tax_year,
            "served_tax_year": served_tax_year,
            "parcel_id": parcel_id,
            "subdivision_name": str(subject_row.get("subdivision_name") or ""),
            "support_status": support_status,
            "readiness_status": readiness_status,
            "support_blocker_code": support_blocker_code,
            "source_coverage_status": source_coverage_status,
            "subject_snapshot_status": "completed",
            "full_baths": bathroom_support.get("resolved_full_baths"),
            "half_baths": bathroom_support.get("resolved_half_baths"),
            "valuation_bathroom_features_json": valuation_bathroom_features_json,
            "summary_json": summary_json,
            "snapshot_json": snapshot_json,
            "source_provenance_json": source_provenance_json,
        }

    def _discover_candidates(
        self,
        cursor: Any,
        *,
        subject_snapshot: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        same_neighborhood_rows = self._discovery_service._fetch_same_neighborhood_candidates(
            cursor,
            subject_snapshot=subject_snapshot,
        )
        rows_to_persist: list[tuple[str, dict[str, Any]]] = [
            (DISCOVERY_TIER_SAME_NEIGHBORHOOD, row) for row in same_neighborhood_rows
        ]
        if len(rows_to_persist) < PREFERRED_RAW_CANDIDATE_POOL:
            fallback_rows = self._discovery_service._fetch_county_sfr_fallback_candidates(
                cursor,
                subject_snapshot=subject_snapshot,
                already_selected_parcel_ids={str(row["parcel_id"]) for row in same_neighborhood_rows},
                remaining_limit=MAX_AUTO_HARVEST - len(rows_to_persist),
            )
            rows_to_persist.extend(
                (DISCOVERY_TIER_COUNTY_SFR_FALLBACK, row) for row in fallback_rows
            )

        candidates: list[dict[str, Any]] = []
        same_neighborhood_count = 0
        county_sfr_fallback_count = 0
        eligible_count = 0
        review_count = 0
        excluded_count = 0

        for discovery_tier, row in rows_to_persist:
            valuation_bathroom_features_json = (
                self._discovery_service._fetch_candidate_valuation_bathroom_features_json(
                    cursor,
                    county_id=str(subject_snapshot["county_id"]),
                    candidate_parcel_id=str(row["parcel_id"]),
                    tax_year=int(row["tax_year"]),
                )
            )
            valuation_bathroom_features_json = attach_bathroom_support_context(
                county_id=str(subject_snapshot["county_id"]),
                canonical_full_baths=row.get("full_baths"),
                canonical_half_baths=row.get("half_baths"),
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            )
            eligibility_status, eligibility_reason_code, eligibility_detail_json = (
                self._discovery_service._evaluate_candidate_eligibility(
                    subject_snapshot=subject_snapshot,
                    row=row,
                    discovery_tier=discovery_tier,
                    valuation_bathroom_features_json=valuation_bathroom_features_json,
                )
            )
            similarity_score_result = compute_similarity_score(
                subject_snapshot=subject_snapshot,
                row=row,
                discovery_tier=discovery_tier,
                eligibility_status=eligibility_status,
                eligibility_detail_json=eligibility_detail_json,
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            )
            source_provenance_json = self._discovery_service._build_source_provenance_json(
                subject_snapshot=subject_snapshot,
                row=row,
                discovery_tier=discovery_tier,
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            )
            candidate_snapshot_json = self._discovery_service._build_candidate_snapshot_json(
                subject_snapshot=subject_snapshot,
                row=row,
                discovery_tier=discovery_tier,
                eligibility_status=eligibility_status,
                eligibility_reason_code=eligibility_reason_code,
                eligibility_detail_json=eligibility_detail_json,
                valuation_bathroom_features_json=valuation_bathroom_features_json,
            )
            candidate_bathroom_support = dict(candidate_snapshot_json.get("bathroom_support") or {})
            candidates.append(
                {
                    "unequal_roll_candidate_id": str(uuid4()),
                    "candidate_parcel_id": str(row["parcel_id"]),
                    "county_id": row.get("county_id"),
                    "tax_year": row.get("tax_year"),
                    "account_number": row.get("account_number"),
                    "address": row.get("address"),
                    "neighborhood_code": row.get("neighborhood_code"),
                    "subdivision_name": row.get("subdivision_name"),
                    "property_type_code": row.get("property_type_code"),
                    "property_class_code": row.get("property_class_code"),
                    "living_area_sf": _as_float(row.get("living_area_sf")),
                    "year_built": _as_int(row.get("year_built")),
                    "effective_age": _as_float(row.get("effective_age")),
                    "bedrooms": _as_int(row.get("bedrooms")),
                    "full_baths": _as_float(candidate_bathroom_support.get("resolved_full_baths")),
                    "half_baths": _as_float(candidate_bathroom_support.get("resolved_half_baths")),
                    "total_rooms": _as_int(row.get("total_rooms")),
                    "stories": _as_float(row.get("stories")),
                    "quality_code": row.get("quality_code"),
                    "condition_code": row.get("condition_code"),
                    "pool_flag": _as_bool(row.get("pool_flag")),
                    "land_sf": _as_float(row.get("land_sf")),
                    "land_acres": _as_float(row.get("land_acres")),
                    "market_value": _as_float(row.get("market_value")),
                    "assessed_value": _as_float(row.get("assessed_value")),
                    "appraised_value": _as_float(row.get("appraised_value")),
                    "certified_value": _as_float(row.get("certified_value")),
                    "notice_value": _as_float(row.get("notice_value")),
                    "discovery_tier": discovery_tier,
                    "candidate_status": "discovered",
                    "eligibility_status": eligibility_status,
                    "eligibility_reason_code": eligibility_reason_code,
                    "eligibility_detail_json": eligibility_detail_json,
                    "source_provenance_json": source_provenance_json,
                    "candidate_snapshot_json": candidate_snapshot_json,
                    "raw_similarity_score": similarity_score_result.raw_similarity_score,
                    "normalized_similarity_score": similarity_score_result.normalized_similarity_score,
                    "scoring_version": similarity_score_result.scoring_version,
                    "scoring_config_version": similarity_score_result.scoring_config_version,
                    "similarity_score_detail_json": similarity_score_result.score_detail_json,
                }
            )
            if discovery_tier == DISCOVERY_TIER_SAME_NEIGHBORHOOD:
                same_neighborhood_count += 1
            else:
                county_sfr_fallback_count += 1
            if eligibility_status == "eligible":
                eligible_count += 1
            elif eligibility_status == "review":
                review_count += 1
            else:
                excluded_count += 1

        discovered_count = same_neighborhood_count + county_sfr_fallback_count
        fallback_used = county_sfr_fallback_count > 0
        discovery_summary = {
            "discovered_count": discovered_count,
            "same_neighborhood_count": same_neighborhood_count,
            "county_sfr_fallback_count": county_sfr_fallback_count,
            "eligible_count": eligible_count,
            "review_count": review_count,
            "excluded_count": excluded_count,
            "fallback_used": fallback_used,
            "sparse_universe_warning": discovered_count < SPARSE_UNIVERSE_WARNING_THRESHOLD,
            "same_neighborhood_insufficient_reason": (
                "same_neighborhood_supply_below_preferred_pool" if fallback_used else None
            ),
            "warning_codes": [
                code
                for code, flag in (
                    ("fallback_geography_used", fallback_used),
                    (
                        "sparse_candidate_universe",
                        discovered_count < SPARSE_UNIVERSE_WARNING_THRESHOLD,
                    ),
                )
                if flag
            ],
        }
        return candidates, discovery_summary

    def _build_initial_run_context(
        self,
        *,
        subject_snapshot: dict[str, Any],
        discovery_summary: dict[str, Any],
    ) -> dict[str, Any]:
        summary_json = dict(subject_snapshot.get("summary_json") or {})
        summary_json["candidate_discovery_summary"] = discovery_summary
        return {
            "unequal_roll_run_id": f"no-persist-{subject_snapshot['parcel_id']}",
            "support_status": subject_snapshot.get("support_status"),
            "selection_governance_status": None,
            "final_comp_count_status": None,
            "summary_json": summary_json,
            "selection_log_json": {},
            "source_review_carry_forward_flag": subject_snapshot.get("support_status")
            == "manual_review_required",
            "county_id": subject_snapshot.get("county_id"),
            "tax_year": subject_snapshot.get("tax_year"),
            "subject_parcel_id": subject_snapshot.get("parcel_id"),
            "subject_neighborhood_code": subject_snapshot.get("neighborhood_code"),
            "subject_subdivision_name": subject_snapshot.get("subdivision_name"),
            "appraised_value": _as_float(subject_snapshot.get("appraised_value")),
            "living_area_sf": _as_float(subject_snapshot.get("living_area_sf")),
            "year_built": _as_int(subject_snapshot.get("year_built")),
            "effective_age": _as_float(subject_snapshot.get("effective_age")),
            "bedrooms": _as_int(subject_snapshot.get("bedrooms")),
            "full_baths": _as_float(subject_snapshot.get("full_baths")),
            "half_baths": _as_float(subject_snapshot.get("half_baths")),
            "stories": _as_float(subject_snapshot.get("stories")),
            "quality_code": subject_snapshot.get("quality_code"),
            "condition_code": subject_snapshot.get("condition_code"),
            "pool_flag": _as_bool(subject_snapshot.get("pool_flag")),
            "land_sf": _as_float(subject_snapshot.get("land_sf")),
            "land_acres": _as_float(subject_snapshot.get("land_acres")),
        }

    def _apply_plan(
        self,
        *,
        candidates: list[dict[str, Any]],
        plan: dict[str, dict[str, Any]],
        version_fields: dict[str, str],
    ) -> None:
        candidates_by_id = {
            str(candidate["unequal_roll_candidate_id"]): candidate for candidate in candidates
        }
        for candidate_id, assignment in plan.items():
            candidate = candidates_by_id[candidate_id]
            candidate.update(assignment)
            for candidate_key, assignment_key in version_fields.items():
                detail_json = next(
                    (
                        value
                        for key, value in assignment.items()
                        if key.endswith("_detail_json") and isinstance(value, dict)
                    ),
                    {},
                )
                candidate[candidate_key] = detail_json.get(assignment_key)

    def _build_adjustment_lines(
        self,
        adjustment_math_plan: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for candidate_id, assignment in adjustment_math_plan.items():
            for line_item in assignment.get("line_items") or []:
                rows.append(
                    {
                        "unequal_roll_candidate_id": candidate_id,
                        "adjustment_type": line_item.get("adjustment_type"),
                        "signed_adjustment_amount": line_item.get("signed_adjustment_amount"),
                    }
                )
        return rows

    def _build_full_final_value_detail(
        self,
        *,
        final_value_output: dict[str, Any],
        candidates: list[dict[str, Any]],
        adjustment_math_plan: dict[str, dict[str, Any]],
        run_context: dict[str, Any],
    ) -> dict[str, Any]:
        candidates_by_id = {
            str(candidate["unequal_roll_candidate_id"]): candidate for candidate in candidates
        }
        line_items_by_candidate_id = {
            candidate_id: list(assignment.get("line_items") or [])
            for candidate_id, assignment in adjustment_math_plan.items()
        }

        def enrich_row(row: dict[str, Any]) -> dict[str, Any]:
            candidate = candidates_by_id.get(str(row.get("unequal_roll_candidate_id"))) or {}
            return {
                **row,
                "similarity_score": candidate.get("normalized_similarity_score"),
                "line_items": line_items_by_candidate_id.get(
                    str(row.get("unequal_roll_candidate_id")),
                    [],
                ),
            }

        included_rows = [
            enrich_row(row) for row in list(final_value_output.get("included_comp_rows") or [])
        ]
        excluded_rows = [
            enrich_row(row) for row in list(final_value_output.get("excluded_comp_rows") or [])
        ]
        ordered_adjusted_values = []
        for row in list(final_value_output.get("ordered_adjusted_values") or []):
            candidate = candidates_by_id.get(str(row.get("unequal_roll_candidate_id"))) or {}
            ordered_adjusted_values.append(
                {
                    **row,
                    "similarity_score": candidate.get("normalized_similarity_score"),
                }
            )

        return {
            **final_value_output,
            "final_value_status": final_value_output.get("final_value_status"),
            "included_comp_rows": included_rows,
            "excluded_comp_rows": excluded_rows,
            "ordered_adjusted_values": ordered_adjusted_values,
            "carried_forward_governance": {
                **dict(final_value_output.get("carried_forward_governance") or {}),
                "selection_log_json_keys": sorted(
                    list(dict(run_context.get("selection_log_json") or {}).keys())
                ),
            },
        }

    def _build_run_state_payload(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for candidate in candidates:
            adjustment_support_detail_json = dict(
                candidate.get("adjustment_support_detail_json") or {}
            )
            adjustment_summary_json = dict(candidate.get("adjustment_summary_json") or {})
            burden_governance = dict(adjustment_summary_json.get("burden_governance") or {})
            source_governance = dict(adjustment_summary_json.get("source_governance") or {})
            adjusted_set_governance = dict(
                adjustment_summary_json.get("adjusted_set_governance") or {}
            )
            reason_codes = []
            for group in (
                burden_governance.get("reason_codes") or [],
                source_governance.get("reason_codes") or [],
                adjusted_set_governance.get("reason_codes") or [],
            ):
                for code in group:
                    if code not in reason_codes:
                        reason_codes.append(code)
            rows.append(
                {
                    "final_value_status": candidate.get("final_value_status"),
                    "chosen_comp_status": candidate.get("chosen_comp_status"),
                    "county_id": candidate.get("county_id"),
                    "source_status": source_governance.get("source_governance_status"),
                    "burden_status": burden_governance.get("status"),
                    "adjusted_set_status": adjusted_set_governance.get("status"),
                    "reason_codes": reason_codes,
                    "review_carry_forward_flag": bool(
                        adjustment_summary_json.get("review_carry_forward_flag")
                    ),
                    "adjustment_support_channels": dict(
                        adjustment_support_detail_json.get("adjustment_channels") or {}
                    ),
                }
            )
        return {"candidates": rows}

    def _blocked_subject_result(
        self,
        *,
        request: UnequalRollReplayRequest,
        blocker_code: str,
        reason: str,
        elapsed_total_s: float,
        subject_snapshot: dict[str, Any] | None = None,
        discovery_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_appraised_value = _as_float(
            (subject_snapshot or {}).get("appraised_value")
        )
        semantics = classify_unsupported_value_semantics(
            current_appraised_value=current_appraised_value,
            final_value_status=None,
            exposed_requested_roll_value=None,
            exposed_requested_reduction_amount=None,
            exposed_requested_reduction_pct=None,
        )
        return {
            "account": request.account_number,
            "county": request.county_id,
            "requested_tax_year": request.requested_tax_year,
            "served_tax_year": (subject_snapshot or {}).get("tax_year"),
            "parcel_id": (subject_snapshot or {}).get("parcel_id"),
            "address": (subject_snapshot or {}).get("address"),
            "neighborhood": (subject_snapshot or {}).get("neighborhood_code"),
            "current_appraised_value": current_appraised_value,
            "support_status": (subject_snapshot or {}).get("support_status"),
            "readiness_status": (subject_snapshot or {}).get("readiness_status"),
            "source_coverage_status": (subject_snapshot or {}).get("source_coverage_status"),
            "replay_status": "blocked",
            "failure_reason": reason,
            "failure_code": blocker_code,
            "final_value_status": None,
            "requested_roll_value": None,
            "requested_reduction_amount": None,
            "requested_reduction_pct": None,
            "same_neighborhood_count": (discovery_summary or {}).get("same_neighborhood_count"),
            "fallback_count": (discovery_summary or {}).get("county_sfr_fallback_count"),
            "fallback_used": (discovery_summary or {}).get("fallback_used"),
            "included_comp_count": None,
            "excluded_review_heavy_count": None,
            "excluded_likely_exclude_count": None,
            "summary_json": (subject_snapshot or {}).get("summary_json"),
            "selection_log_json": None,
            "subject_snapshot_json": (subject_snapshot or {}).get("snapshot_json"),
            "subject_source_provenance_json": (subject_snapshot or {}).get(
                "source_provenance_json"
            ),
            "final_value_detail_json": None,
            "compact_final_value_review_payload": None,
            "run_state_payload": {"candidates": []},
            "run_state_summary": summarize_run_state_candidates({"candidates": []}),
            "safe_requested_roll_value": semantics["safe_requested_roll_value"],
            "safe_requested_reduction_amount": semantics["safe_requested_reduction_amount"],
            "safe_requested_reduction_pct": semantics["safe_requested_reduction_pct"],
            "value_interpretation": semantics["value_interpretation"],
            "value_interpretation_reason": semantics["value_interpretation_reason"],
            "evidence_completeness_grade": "not_reviewable",
            "stability_metrics_available": False,
            "stability_metrics_unavailable_reason": "final_value_not_built",
            "elapsed_total_s": elapsed_total_s,
        }

    def _stability_unavailable_reason(self, *, final_value_detail_json: dict[str, Any]) -> str:
        if not final_value_detail_json.get("included_comp_rows"):
            return "no_included_comp_rows"
        return "stability_metrics_missing_values"


def subject_requests_from_runtime_artifact(
    artifact_path: str | Path,
    *,
    requested_tax_year: int,
) -> list[UnequalRollReplayRequest]:
    import json

    artifact = json.loads(Path(artifact_path).read_text())
    requests: list[UnequalRollReplayRequest] = []
    for row in artifact.get("subjects") or []:
        account = str(row.get("subject_identifier") or "").strip()
        county = str(row.get("county") or "").strip()
        if account and county:
            requests.append(
                UnequalRollReplayRequest(
                    county_id=county,
                    account_number=account,
                    requested_tax_year=requested_tax_year,
                )
            )
    return requests


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y"}:
            return True
        if normalized in {"false", "f", "0", "no", "n"}:
            return False
    return bool(value)
