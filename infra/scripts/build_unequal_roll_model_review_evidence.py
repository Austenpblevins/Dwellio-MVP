from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.unequal_roll_review_evidence import (
    classify_unsupported_value_semantics,
    evidence_completeness_grade,
    normalize_taxpayer_favorable_tiebreak_review,
    reconcile_outcome_row,
    summarize_run_state_candidates,
)


HIGH_PRIORITY_PACKETS = {
    "harris_1139": [
        "0411050000081",
        "0411050000080",
        "0411050000070",
        "0411050000071",
        "0411050000077",
        "0642370000003",
    ],
    "fort_bend_outcomes": [
        "0226-00-000-0010-906",
        "0226-00-000-0470-906",
        "0226-00-000-0050-906",
        "4850-00-014-2300-907",
        "0044-00-000-0280-901",
    ],
    "land_site": [
        "1199140020001",
        "1199140010006",
        "1141410090001",
        "0226-00-000-0010-906",
        "0044-00-000-0280-901",
    ],
    "bedroom": [
        "1397720020002",
        "0411050000077",
        "1199140010006",
        "4850-00-014-2300-907",
    ],
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a decision-review unequal-roll evidence package from existing "
            "runtime/completeness artifacts plus read-only Stage 21 subject context."
        )
    )
    parser.add_argument(
        "--runtime-artifact",
        default="/private/tmp/unequal_roll_stage21_full100_runtime_probe_timeout120s_foundation.json",
    )
    parser.add_argument(
        "--classified-artifact",
        default=(
            "/private/tmp/unequal_roll_stage21_full100_"
            "completeness_classified_foundation_v14_producer_boundary.json"
        ),
    )
    parser.add_argument(
        "--producer-artifact",
        default=(
            "/private/tmp/unequal_roll_stage21_full100_"
            "runtime_probe_timeout120s_foundation_with_producer_payload_v2.json"
        ),
    )
    parser.add_argument(
        "--run-state",
        default="/private/tmp/unequal_roll_chunk_state.json",
    )
    parser.add_argument(
        "--runtime-context-artifact",
        default="/private/tmp/unequal_roll_stage21_expanded_plus20_runtime_probe_chunked_20260502.json",
    )
    parser.add_argument(
        "--chunk-patterns-glob",
        default="/private/tmp/unequal_roll_stage21_chunk*_runtime_branch*.json",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help=(
            "Optional read-only Stage 21 database URL. Falls back to DWELLIO_DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="/private/tmp",
    )
    parser.add_argument(
        "--timestamp",
        default="",
        help="Optional timestamp override, e.g. 20260503T153000.",
    )
    args = parser.parse_args()

    timestamp = args.timestamp or datetime.now().strftime("%Y%m%dT%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    runtime_payload = _load_json(args.runtime_artifact)
    producer_payload = _load_json(args.producer_artifact)
    classified_payload = _load_json(args.classified_artifact)
    run_state_payload = _load_json(args.run_state)
    plus20_payload = _load_json(args.runtime_context_artifact)

    runtime_rows = list(runtime_payload.get("subjects") or [])
    producer_rows_by_account = _index_rows(producer_payload.get("subjects") or [])
    classified_rows_by_account = _index_rows(classified_payload.get("subjects") or [])
    run_state_by_account = _index_run_state(run_state_payload)
    normalized_patterns_by_account = _load_best_patterns(args.chunk_patterns_glob)

    accounts = [str(row.get("subject_identifier")) for row in runtime_rows if row.get("subject_identifier")]
    runtime_current_values = {
        str(row.get("subject_identifier")): _as_float(row.get("current_appraised_value"))
        for row in runtime_rows
        if row.get("subject_identifier")
    }
    subject_context_by_account = _fetch_subject_context_map(
        database_url=args.database_url,
        accounts=accounts,
        runtime_current_values=runtime_current_values,
    )

    evidence_rows: list[dict[str, Any]] = []
    unsupported_value_audit: list[dict[str, Any]] = []
    outcome_reconciliation: list[dict[str, Any]] = []

    for runtime_row in runtime_rows:
        account = str(runtime_row.get("subject_identifier"))
        producer_row = producer_rows_by_account.get(account) or {}
        classified_row = classified_rows_by_account.get(account) or {}
        run_state = run_state_by_account.get(account)
        merged_runtime_row = dict(runtime_row)
        if producer_row.get("producer_downstream_payload") is not None:
            merged_runtime_row["producer_downstream_payload"] = producer_row.get(
                "producer_downstream_payload"
            )

        if not merged_runtime_row.get("patterns"):
            merged_runtime_row["patterns"] = normalized_patterns_by_account.get(account, {})

        reconciliation = reconcile_outcome_row(
            runtime_row=merged_runtime_row,
            classified_row=classified_row,
            run_state_payload=run_state,
        )
        safe_value_semantics = classify_unsupported_value_semantics(
            current_appraised_value=merged_runtime_row.get("current_appraised_value"),
            final_value_status=reconciliation.get("final_reconciled_status"),
            exposed_requested_roll_value=classified_row.get(
                "requested_roll_value", merged_runtime_row.get("requested_roll_value")
            ),
            exposed_requested_reduction_amount=classified_row.get(
                "requested_reduction_amount",
                merged_runtime_row.get("requested_reduction_amount"),
            ),
            exposed_requested_reduction_pct=classified_row.get(
                "requested_reduction_pct", merged_runtime_row.get("requested_reduction_pct")
            ),
        )

        run_state_summary = summarize_run_state_candidates(run_state) if run_state else None
        subject_context = subject_context_by_account.get(account) or {}
        compact_review_payload = _resolve_compact_review_payload(
            merged_runtime_row=merged_runtime_row,
            classified_row=classified_row,
            producer_row=producer_row,
        )
        stability_status = _build_stability_status(
            run_state=run_state,
            compact_review_payload=compact_review_payload,
        )
        completeness_grade = evidence_completeness_grade(
            final_reconciled_status=reconciliation.get("final_reconciled_status"),
            model_outcome_complete=bool(reconciliation.get("model_outcome_complete")),
            subject_context_present=bool(subject_context),
            comp_evidence_present=bool(compact_review_payload) or run_state_summary is not None,
            stability_metrics_present=stability_status["stability_metrics_present"],
            none_origin=str(reconciliation.get("none_origin") or ""),
        )

        pattern_summary = _normalize_pattern_payload(merged_runtime_row.get("patterns") or {})
        evidence_row = {
            "account": account,
            "county": merged_runtime_row.get("county"),
            "neighborhood": merged_runtime_row.get("neighborhood"),
            "source_chunk": merged_runtime_row.get("source_chunk"),
            "subject_context": subject_context,
            "current_appraised_value": merged_runtime_row.get("current_appraised_value"),
            "runtime": {
                "discovery_completion_status": merged_runtime_row.get(
                    "discovery_completion_status"
                ),
                "probe_error": merged_runtime_row.get("probe_error"),
                "elapsed_total_s": merged_runtime_row.get("elapsed_total_s"),
                "same_neighborhood_count": merged_runtime_row.get(
                    "same_neighborhood_count"
                ),
                "same_neighborhood_elapsed_s": merged_runtime_row.get(
                    "same_neighborhood_elapsed_s"
                ),
                "fallback_used": merged_runtime_row.get("fallback_used"),
                "fallback_count": merged_runtime_row.get("fallback_count"),
                "fallback_elapsed_s": merged_runtime_row.get("fallback_elapsed_s"),
                "fallback_prefilter_limit": merged_runtime_row.get(
                    "fallback_prefilter_limit"
                ),
            },
            "reconciliation": reconciliation,
            "value_semantics": {
                "exposed_requested_roll_value": classified_row.get(
                    "requested_roll_value", merged_runtime_row.get("requested_roll_value")
                ),
                "exposed_requested_reduction_amount": classified_row.get(
                    "requested_reduction_amount",
                    merged_runtime_row.get("requested_reduction_amount"),
                ),
                "exposed_requested_reduction_pct": classified_row.get(
                    "requested_reduction_pct", merged_runtime_row.get("requested_reduction_pct")
                ),
                **safe_value_semantics,
            },
            "counts": {
                "included_comp_count": classified_row.get(
                    "included_comp_count", merged_runtime_row.get("included_comp_count")
                ),
                "excluded_review_heavy_count": classified_row.get(
                    "excluded_review_heavy_count",
                    merged_runtime_row.get("excluded_review_heavy_count"),
                ),
                "excluded_likely_exclude_count": classified_row.get(
                    "excluded_likely_exclude_count",
                    merged_runtime_row.get("excluded_likely_exclude_count"),
                ),
            },
            "classified": {
                "completeness_status_code": classified_row.get("completeness_status_code"),
                "completeness_status_family": classified_row.get(
                    "completeness_status_family"
                ),
                "completeness_gate_pass": classified_row.get("completeness_gate_pass"),
                "completeness_defect_category": classified_row.get(
                    "completeness_defect_category"
                ),
                "missing_required_fields": list(
                    classified_row.get("missing_required_fields") or []
                ),
                "downstream_payload_attachment_status": classified_row.get(
                    "downstream_payload_attachment_status"
                ),
                "canonical_downstream_summary": classified_row.get(
                    "canonical_downstream_summary"
                ),
            },
            "producer_downstream_payload": producer_row.get("producer_downstream_payload"),
            "compact_final_value_review_payload": compact_review_payload,
            "patterns": pattern_summary,
            "run_state_summary": run_state_summary,
            "stability": stability_status,
            "field_availability": _field_availability_summary(
                run_state=run_state,
                subject_context=subject_context,
                compact_review_payload=compact_review_payload,
            ),
            "evidence_completeness_grade": completeness_grade,
            "missing_evidence": _missing_evidence_list(
                run_state=run_state,
                subject_context=subject_context,
                stability_status=stability_status,
                compact_review_payload=compact_review_payload,
            ),
            "preliminary_interpretation": _preliminary_interpretation(
                reconciliation=reconciliation,
                value_semantics=safe_value_semantics,
                run_state_summary=run_state_summary,
                pattern_summary=pattern_summary,
            ),
        }
        evidence_rows.append(evidence_row)

        outcome_reconciliation.append(
            {
                "account": account,
                "county": merged_runtime_row.get("county"),
                "neighborhood": merged_runtime_row.get("neighborhood"),
                "runtime_completed_flag": reconciliation.get("runtime_completed_flag"),
                "runtime_final_value_status": reconciliation.get(
                    "runtime_final_value_status"
                ),
                "recovered_v14_status": reconciliation.get("recovered_v14_status"),
                "final_reconciled_status": reconciliation.get(
                    "final_reconciled_status"
                ),
                "model_outcome_complete": reconciliation.get("model_outcome_complete"),
                "none_origin": reconciliation.get("none_origin"),
                "defect_code": reconciliation.get("defect_code"),
                "recovery_source": reconciliation.get("recovery_source"),
                "recommended_interpretation": reconciliation.get(
                    "recommended_interpretation"
                ),
            }
        )

        if reconciliation.get("final_reconciled_status") == "unsupported":
            unsupported_value_audit.append(
                {
                    "account": account,
                    "status": "unsupported",
                    "current_appraised_value": merged_runtime_row.get(
                        "current_appraised_value"
                    ),
                    "exposed_requested_roll_value": safe_value_semantics.get(
                        "safe_requested_roll_value"
                    )
                    if safe_value_semantics.get("value_interpretation")
                    == "suppressed_identity_value"
                    else classified_row.get(
                        "requested_roll_value", merged_runtime_row.get("requested_roll_value")
                    ),
                    "exposed_requested_reduction_amount": classified_row.get(
                        "requested_reduction_amount",
                        merged_runtime_row.get("requested_reduction_amount"),
                    ),
                    "exposed_requested_reduction_pct": classified_row.get(
                        "requested_reduction_pct", merged_runtime_row.get("requested_reduction_pct")
                    ),
                    "safe_requested_roll_value": safe_value_semantics.get(
                        "safe_requested_roll_value"
                    ),
                    "safe_requested_reduction_amount": safe_value_semantics.get(
                        "safe_requested_reduction_amount"
                    ),
                    "safe_requested_reduction_pct": safe_value_semantics.get(
                        "safe_requested_reduction_pct"
                    ),
                    "interpretation": safe_value_semantics.get("value_interpretation"),
                    "reason": safe_value_semantics.get("value_interpretation_reason"),
                }
            )

    summary = _build_summary(
        evidence_rows=evidence_rows,
        outcome_reconciliation=outcome_reconciliation,
        unsupported_value_audit=unsupported_value_audit,
        plus20_payload=plus20_payload,
    )
    focused_packets = _build_focused_packets(evidence_rows)
    diagnosis = _observability_diagnosis()

    package = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "runtime_artifact": str(Path(args.runtime_artifact)),
            "classified_artifact": str(Path(args.classified_artifact)),
            "producer_artifact": str(Path(args.producer_artifact)),
            "run_state_artifact": str(Path(args.run_state)),
            "runtime_context_artifact": str(Path(args.runtime_context_artifact)),
            "chunk_patterns_glob": args.chunk_patterns_glob,
            "database_url_used": bool(args.database_url or _env("DWELLIO_DATABASE_URL")),
            "subject_context_source": "public.parcel_year_snapshots + public.parcels + public.parcel_assessments + public.parcel_improvements + public.parcel_lands",
            "fort_bend_bathroom_source": "public.fort_bend_valuation_bathroom_features",
        },
        "observability_diagnosis": diagnosis,
        "summary": summary,
        "outcome_reconciliation": outcome_reconciliation,
        "unsupported_value_audit": unsupported_value_audit,
        "subjects": evidence_rows,
        "focused_packets": focused_packets,
    }

    json_path = output_dir / f"unequal_roll_stage21_full100_model_review_evidence_{timestamp}.json"
    csv_path = output_dir / f"unequal_roll_stage21_full100_model_review_evidence_{timestamp}.csv"
    md_path = output_dir / f"unequal_roll_stage21_full100_model_review_summary_{timestamp}.md"
    json_path.write_text(json.dumps(package, indent=2, default=str), encoding="utf-8")
    _write_csv(csv_path, evidence_rows)
    md_path.write_text(_build_markdown_summary(package), encoding="utf-8")

    print(json.dumps({
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "md_path": str(md_path),
        "summary": summary,
    }, indent=2, default=str))


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _index_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("subject_identifier")): row
        for row in rows
        if row.get("subject_identifier") is not None
    }


def _index_run_state(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for run in payload.get("runs") or []:
        summary = run.get("summary") or {}
        account = summary.get("account")
        if account is not None:
            mapping[str(account)] = run
    return mapping


def _load_best_patterns(artifacts_glob: str) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for artifact_path in sorted(glob.glob(artifacts_glob)):
        payload = _load_json(artifact_path)
        for row in payload.get("subjects") or []:
            normalized = _normalize_subject_row(row)
            account = normalized.get("subject_identifier")
            if account is None:
                continue
            patterns = _normalize_pattern_payload(normalized.get("patterns") or {})
            if not patterns:
                continue
            existing = mapping.get(str(account), {})
            if len(patterns) > len(existing):
                mapping[str(account)] = patterns
    return mapping


def _normalize_subject_row(row: dict[str, Any]) -> dict[str, Any]:
    result = row.get("result") or {}
    subject = row.get("subject") or {}
    return {
        "subject_identifier": row.get("subject_identifier", subject.get("account_number")),
        "patterns": row.get("patterns") or result.get("patterns") or {},
    }


def _normalize_pattern_payload(patterns: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(patterns, dict):
        return {}
    normalized = {
        "bedroom_signal_present": patterns.get(
            "bedroom_non_monetized_signal_present",
            patterns.get("bedroom_potential_adjustment_present"),
        ),
        "land_site_signal_present": patterns.get(
            "land_site_review_present",
            patterns.get("land_site_review_required_present"),
        ),
        "low_burden_excluded_present": patterns.get(
            "low_burden_excluded_present",
            bool(patterns.get("low_burden_excluded_review_heavy_count", 0)),
        ),
        "review_carry_forward_present": patterns.get("review_carry_forward_present"),
        "unresolved_bathroom_source_present": patterns.get(
            "unresolved_bathroom_source_present"
        ),
    }
    return {key: value for key, value in normalized.items() if value is not None}


def _fetch_subject_context_map(
    *,
    database_url: str,
    accounts: list[str],
    runtime_current_values: dict[str, float | None],
) -> dict[str, dict[str, Any]]:
    if not accounts:
        return {}
    db_url = database_url or _env("DWELLIO_DATABASE_URL")
    if not db_url:
        return {}

    import psycopg
    from psycopg.rows import dict_row

    context_rows: dict[str, list[dict[str, Any]]] = {}
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH requested(account_number) AS (
                  SELECT unnest(%s::text[])
                )
                SELECT
                  pys.county_id,
                  pys.parcel_id,
                  pys.tax_year,
                  pys.account_number,
                  p.situs_address AS address,
                  p.neighborhood_code,
                  p.subdivision_name,
                  p.property_type_code,
                  p.property_class_code,
                  pa.appraised_value,
                  pi.living_area_sf,
                  pi.bedrooms,
                  pi.full_baths,
                  pi.half_baths,
                  pi.stories,
                  pi.quality_code,
                  pi.condition_code,
                  pi.garage_spaces,
                  pi.pool_flag,
                  pl.land_sf,
                  pl.land_acres
                FROM requested r
                JOIN parcel_year_snapshots pys
                  ON pys.account_number = r.account_number
                 AND pys.is_current
                JOIN parcels p
                  ON p.parcel_id = pys.parcel_id
                LEFT JOIN parcel_assessments pa
                  ON pa.parcel_id = pys.parcel_id
                 AND pa.tax_year = pys.tax_year
                LEFT JOIN parcel_improvements pi
                  ON pi.parcel_id = pys.parcel_id
                 AND pi.tax_year = pys.tax_year
                LEFT JOIN parcel_lands pl
                  ON pl.parcel_id = pys.parcel_id
                 AND pl.tax_year = pys.tax_year
                """,
                (accounts,),
            )
            for row in cursor.fetchall():
                context_rows.setdefault(str(row["account_number"]), []).append(dict(row))

        best_rows = {
            account: _pick_best_subject_row(rows, runtime_current_values.get(account))
            for account, rows in context_rows.items()
        }

        fort_bend_keys = [
            (row["parcel_id"], row["tax_year"])
            for row in best_rows.values()
            if row and row.get("county_id") == "fort_bend" and row.get("parcel_id")
        ]
        bathroom_map: dict[tuple[str, int], dict[str, Any]] = {}
        if fort_bend_keys:
            parcel_ids = list({parcel_id for parcel_id, _ in fort_bend_keys})
            tax_years = list({tax_year for _, tax_year in fort_bend_keys})
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      parcel_id,
                      tax_year,
                      account_number,
                      bathroom_count_status,
                      bathroom_count_confidence,
                      full_baths_derived,
                      half_baths_derived,
                      bathroom_flags
                    FROM fort_bend_valuation_bathroom_features
                    WHERE parcel_id = ANY(%s)
                      AND tax_year = ANY(%s)
                    """,
                    (parcel_ids, tax_years),
                )
                for row in cursor.fetchall():
                    bathroom_map[(str(row["parcel_id"]), int(row["tax_year"]))] = dict(row)

    output: dict[str, dict[str, Any]] = {}
    for account, row in best_rows.items():
        if not row:
            continue
        subject_context = {
            "address": row.get("address"),
            "parcel_id": row.get("parcel_id"),
            "tax_year": row.get("tax_year"),
            "subdivision_name": row.get("subdivision_name"),
            "property_type_code": row.get("property_type_code"),
            "property_class_code": row.get("property_class_code"),
            "living_area_sf": _as_float(row.get("living_area_sf")),
            "bedrooms": _as_float(row.get("bedrooms")),
            "full_baths": _as_float(row.get("full_baths")),
            "half_baths": _as_float(row.get("half_baths")),
            "stories": _as_float(row.get("stories")),
            "quality_code": row.get("quality_code"),
            "condition_code": row.get("condition_code"),
            "garage_spaces": _as_float(row.get("garage_spaces")),
            "pool_flag": row.get("pool_flag"),
            "land_sf": _as_float(row.get("land_sf")),
            "land_acres": _as_float(row.get("land_acres")),
            "appraised_value": _as_float(row.get("appraised_value")),
            "completeness_score": None,
            "warning_codes": [],
            "public_summary_ready_flag": None,
            "admin_review_required": None,
        }
        if row.get("county_id") == "fort_bend":
            bathroom = bathroom_map.get((str(row.get("parcel_id")), int(row.get("tax_year"))))
            subject_context["fort_bend_bathroom_features"] = (
                {
                    "attachment_status": "attached",
                    "bathroom_count_status": bathroom.get("bathroom_count_status"),
                    "bathroom_count_confidence": bathroom.get(
                        "bathroom_count_confidence"
                    ),
                    "full_baths_derived": _as_float(bathroom.get("full_baths_derived")),
                    "half_baths_derived": _as_float(bathroom.get("half_baths_derived")),
                    "bathroom_flags": list(bathroom.get("bathroom_flags") or []),
                }
                if bathroom
                else {"attachment_status": "missing"}
            )
        output[account] = subject_context
    return output


def _pick_best_subject_row(
    rows: list[dict[str, Any]], runtime_current_value: float | None
) -> dict[str, Any]:
    def score(row: dict[str, Any]) -> tuple[Any, ...]:
        appraised_value = _as_float(row.get("appraised_value"))
        diff = abs((appraised_value or 0.0) - (runtime_current_value or appraised_value or 0.0))
        return (-int(row.get("tax_year") or 0), diff)

    return sorted(rows, key=score)[0]


def _resolve_compact_review_payload(
    *,
    merged_runtime_row: dict[str, Any],
    classified_row: dict[str, Any],
    producer_row: dict[str, Any],
) -> dict[str, Any] | None:
    for source in (
        merged_runtime_row,
        classified_row,
        producer_row.get("producer_downstream_payload") or {},
    ):
        payload = source.get("compact_final_value_review_payload")
        if isinstance(payload, dict):
            return payload
    return None


def _field_availability_summary(
    *,
    run_state: dict[str, Any] | None,
    subject_context: dict[str, Any],
    compact_review_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    availability = dict((compact_review_payload or {}).get("availability") or {})
    return {
        "subject_context_from_stage21": bool(subject_context),
        "comp_identity_available": bool(availability.get("comp_identity_available")),
        "comp_address_available": bool(availability.get("comp_address_available")),
        "similarity_scores_available": bool(availability.get("similarity_score_available")),
        "adjusted_appraised_values_available": bool(
            availability.get("adjusted_appraised_value_available")
        ),
        "included_adjusted_value_list_available": bool(
            availability.get("ordered_adjusted_values_available")
        ),
        "stability_metrics_available": bool(
            availability.get("stability_metrics_available")
        ),
        "final_value_detail_json_available": compact_review_payload is not None,
        "line_item_summary_available": bool(
            availability.get("line_item_summary_available")
        ),
        "run_state_comp_posture_available": run_state is not None,
    }


def _missing_evidence_list(
    *,
    run_state: dict[str, Any] | None,
    subject_context: dict[str, Any],
    stability_status: dict[str, Any],
    compact_review_payload: dict[str, Any] | None,
) -> list[str]:
    missing: list[str] = []
    if not subject_context:
        missing.append("subject_context_missing_from_stage21")
    if compact_review_payload is None and run_state is None:
        missing.append("comp_level_review_posture_missing")
    elif compact_review_payload is None:
        missing.extend(
            [
                "comp_identity_not_emitted_in_available_artifacts",
                "similarity_scores_not_emitted_in_available_artifacts",
                "adjusted_appraised_values_not_emitted_in_available_artifacts",
                "included_adjusted_value_list_not_emitted_in_available_artifacts",
            ]
        )
    else:
        missing.extend(list(compact_review_payload.get("missing_fields") or []))
    if not stability_status.get("stability_metrics_present"):
        missing.append(stability_status.get("stability_recovery_reason"))
    return missing


def _build_stability_status(
    *,
    run_state: dict[str, Any] | None,
    compact_review_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    compact_stability = dict((compact_review_payload or {}).get("stability_metrics") or {})
    ordered_adjusted_values = list(
        (compact_review_payload or {}).get("ordered_adjusted_values") or []
    )
    if compact_stability:
        adjusted_values = [
            row.get("adjusted_appraised_value")
            for row in ordered_adjusted_values
            if row.get("adjusted_appraised_value") is not None
        ]
        return {
            "stability_metrics_present": True,
            "stability_recovery_status": "present_from_compact_review_payload",
            "stability_recovery_reason": None,
            "median_all": compact_stability.get("median_all"),
            "median_minus_high_low": compact_stability.get("median_minus_high_low"),
            "max_leave_one_out_delta": compact_stability.get("max_leave_one_out_delta"),
            "adjusted_value_iqr": compact_stability.get("adjusted_value_iqr"),
            "included_adjusted_values": adjusted_values or None,
            "included_adjusted_value_count": len(adjusted_values) or None,
        }

    if run_state is None:
        return {
            "stability_metrics_present": False,
            "stability_recovery_status": "unrecoverable",
            "stability_recovery_reason": "run_state_unavailable_for_subject",
            "median_all": None,
            "median_minus_high_low": None,
            "max_leave_one_out_delta": None,
            "adjusted_value_iqr": None,
            "included_adjusted_values": None,
            "included_adjusted_value_count": None,
        }
    summary = run_state.get("summary") or {}
    stability = summary.get("stability") or {}
    if stability:
        return {
            "stability_metrics_present": True,
            "stability_recovery_status": "present",
            "stability_recovery_reason": None,
            "median_all": stability.get("median_all"),
            "median_minus_high_low": stability.get("median_minus_high_low"),
            "max_leave_one_out_delta": stability.get("max_leave_one_out_delta"),
            "adjusted_value_iqr": stability.get("adjusted_value_iqr"),
            "included_adjusted_values": stability.get("included_adjusted_values"),
            "included_adjusted_value_count": len(stability.get("included_adjusted_values") or []),
        }
    return {
        "stability_metrics_present": False,
        "stability_recovery_status": "unrecoverable",
        "stability_recovery_reason": "computed_upstream_but_not_emitted_in_available_artifacts",
        "median_all": None,
        "median_minus_high_low": None,
        "max_leave_one_out_delta": None,
        "adjusted_value_iqr": None,
        "included_adjusted_values": None,
        "included_adjusted_value_count": None,
    }


def _preliminary_interpretation(
    *,
    reconciliation: dict[str, Any],
    value_semantics: dict[str, Any],
    run_state_summary: dict[str, Any] | None,
    pattern_summary: dict[str, Any],
) -> str:
    if reconciliation.get("none_origin") == "payload_gap_unrecovered":
        return "payload_gap_blocks_decision"
    if pattern_summary.get("unresolved_bathroom_source_present"):
        return "source_data_issue"
    if reconciliation.get("final_reconciled_status") == "unsupported":
        if value_semantics.get("value_interpretation") == "diagnostic_only":
            return "needs_human_review"
        if run_state_summary and run_state_summary.get("excluded_count", 0) >= run_state_summary.get(
            "included_count", 0
        ):
            return "possibly_too_conservative"
        return "behavior_appears_justified"
    if reconciliation.get("final_reconciled_status") == "manual_review_required":
        return "needs_human_review"
    return "behavior_appears_justified"


def _observability_diagnosis() -> dict[str, Any]:
    return {
        "computed_upstream_but_absent_in_current_full100_source_artifacts": [
            "final_value_detail_json",
            "included_comp_rows_with_identity_and_adjusted_values",
            "excluded_comp_rows_with_identity_and_adjusted_values",
            "ordered_adjusted_values",
            "median_calculation",
            "stability_metrics.median_all",
            "stability_metrics.median_minus_high_low",
            "stability_metrics.max_leave_one_out_delta",
            "stability_metrics.adjusted_value_iqr",
            "qa_flags",
        ],
        "emitted_then_lost_or_degraded_in_downstream_artifacts": [
            "subject-level pattern booleans were present in chunk runtime artifacts but blank in the full100 aggregate runtime artifact",
            "unsupported positive reduction semantics were exposed without safe interpretation labels before review-evidence reconciliation",
        ],
        "recoverable_from_read_only_stage21": [
            "subject address",
            "subject parcel_id",
            "subject bedrooms/full_baths/half_baths/stories/living_area_sf",
            "subject land_sf/land_acres",
            "Fort Bend bathroom feature attachment status and derived bathroom posture",
        ],
        "truly_unavailable_without_producer_or_runtime_table_path": [
            "comp account/parcel identity",
            "comp address",
            "similarity score when not preserved by final-value source payload",
            "raw comp appraised value when not preserved by final-value source payload",
            "adjusted comp appraised value when not preserved by final-value source payload",
            "included adjusted value list when not preserved by final-value source payload",
            "adjustment line items",
            "final per-comp exclusion reason beyond summarized reason-code counts",
            "final_value_detail_json payload itself",
        ],
    }


def _build_summary(
    *,
    evidence_rows: list[dict[str, Any]],
    outcome_reconciliation: list[dict[str, Any]],
    unsupported_value_audit: list[dict[str, Any]],
    plus20_payload: dict[str, Any],
) -> dict[str, Any]:
    grade_counts = Counter(row["evidence_completeness_grade"] for row in evidence_rows)
    final_status_counts = Counter(
        row["reconciliation"].get("final_reconciled_status") for row in evidence_rows
    )
    none_origin_counts = Counter(
        row["reconciliation"].get("none_origin") for row in evidence_rows
    )
    unsupported_semantics = Counter(
        row["interpretation"] for row in unsupported_value_audit
    )
    diagnostic_rows = [
        row["account"]
        for row in unsupported_value_audit
        if row["interpretation"] == "diagnostic_only"
    ]
    reviewable_count = sum(
        1
        for row in evidence_rows
        if row["evidence_completeness_grade"]
        in {"complete_review_evidence", "usable_with_minor_gaps", "limited_review_evidence"}
    )
    plus20_summary = {
        "attempted": plus20_payload.get("attempted_subjects")
        or plus20_payload.get("summary", {}).get("attempted_subjects")
        or plus20_payload.get("meta", {}).get("sample_size"),
        "completed": plus20_payload.get("completed_subjects")
        or plus20_payload.get("summary", {}).get("completed_subjects")
        or sum(
            int(chunk.get("subjects_completed") or 0)
            for chunk in plus20_payload.get("chunk_summaries") or []
        ),
        "failed": plus20_payload.get("failed_subjects")
        or plus20_payload.get("summary", {}).get("failed_subjects")
        or len(plus20_payload.get("failures") or []),
    }
    return {
        "subject_count": len(evidence_rows),
        "decision_reviewable_count": reviewable_count,
        "payload_gap_count": none_origin_counts.get("payload_gap_unrecovered", 0)
        + none_origin_counts.get("payload_gap_recovered_from_downstream_payload", 0)
        + none_origin_counts.get("payload_gap_run_state_available_but_final_outcome_missing", 0),
        "unresolved_none_row_count": sum(
            1
            for row in outcome_reconciliation
            if row["runtime_final_value_status"] is None
            and row["final_reconciled_status"] is None
        ),
        "final_status_distribution": dict(final_status_counts),
        "none_origin_distribution": dict(none_origin_counts),
        "evidence_grade_distribution": dict(grade_counts),
        "unsupported_value_semantics_distribution": dict(unsupported_semantics),
        "unsupported_positive_reduction_rows_safely_labeled": diagnostic_rows,
        "stability_metrics_recovered_count": sum(
            1 for row in evidence_rows if row["stability"].get("stability_metrics_present")
        ),
        "run_state_comp_review_rows_available": sum(
            1 for row in evidence_rows if row.get("run_state_summary") is not None
        ),
        "compact_review_payload_rows_available": sum(
            1 for row in evidence_rows if row.get("compact_final_value_review_payload")
        ),
        "comp_identities_available": any(
            row["field_availability"].get("comp_identity_available")
            for row in evidence_rows
        ),
        "similarity_scores_available": any(
            row["field_availability"].get("similarity_scores_available")
            for row in evidence_rows
        ),
        "adjusted_values_available": any(
            row["field_availability"].get("adjusted_appraised_values_available")
            for row in evidence_rows
        ),
        "plus20_runtime_context": plus20_summary,
    }


def _build_focused_packets(evidence_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_account = {row["account"]: row for row in evidence_rows}
    packets: dict[str, list[dict[str, Any]]] = {}
    for bucket, accounts in HIGH_PRIORITY_PACKETS.items():
        packets[bucket] = [
            _focused_packet(by_account[account])
            for account in accounts
            if account in by_account
        ]
    return packets


def _focused_packet(row: dict[str, Any]) -> dict[str, Any]:
    compact_review_payload = row.get("compact_final_value_review_payload") or {}
    return {
        "account": row["account"],
        "county": row["county"],
        "neighborhood": row["neighborhood"],
        "subject_summary": {
            "address": row["subject_context"].get("address"),
            "current_appraised_value": row.get("current_appraised_value"),
            "living_area_sf": row["subject_context"].get("living_area_sf"),
            "bedrooms": row["subject_context"].get("bedrooms"),
            "full_baths": row["subject_context"].get("full_baths"),
            "half_baths": row["subject_context"].get("half_baths"),
            "land_sf": row["subject_context"].get("land_sf"),
            "land_acres": row["subject_context"].get("land_acres"),
            "subdivision_name": row["subject_context"].get("subdivision_name"),
        },
        "final_status_interpretation": row["reconciliation"],
        "safe_vs_diagnostic_value_interpretation": row["value_semantics"],
        "included_comp_table": compact_review_payload.get("included_comp_rows")
        or (row.get("run_state_summary") or {}).get("included_comp_rows", []),
        "excluded_comp_table": compact_review_payload.get("excluded_comp_rows")
        or (row.get("run_state_summary") or {}).get("excluded_comp_rows", []),
        "adjustment_burden_summary": {
            "reason_code_counts": (row.get("run_state_summary") or {}).get("reason_code_counts", {}),
            "burden_status_counts": (row.get("run_state_summary") or {}).get("burden_status_counts", {}),
            "dominant_adjustment_channels": compact_review_payload.get("dominant_adjustment_channels")
            or (row.get("run_state_summary") or {}).get("dominant_adjustment_channels", []),
        },
        "source_review_posture": {
            "source_status_counts": (row.get("run_state_summary") or {}).get(
                "source_status_counts", {}
            ),
            "adjusted_set_status_counts": (row.get("run_state_summary") or {}).get(
                "adjusted_set_status_counts", {}
            ),
            "fort_bend_bathroom_source_posture": (row.get("run_state_summary") or {}).get(
                "fort_bend_bathroom_source_posture", []
            ),
            "subject_bathroom_features": row["subject_context"].get(
                "fort_bend_bathroom_features"
            ),
        },
        "signals": {
            "patterns": row.get("patterns"),
            "bedroom_signal": (row.get("run_state_summary") or {}).get("bedroom_signal"),
            "land_site_signal_present": (row.get("run_state_summary") or {}).get(
                "land_site_signal_present"
            ),
            "taxpayer_favorable_tiebreak_review": normalize_taxpayer_favorable_tiebreak_review(
                compact_review_payload.get("taxpayer_favorable_tiebreak_review")
            ),
        },
        "stability_metrics": row.get("stability"),
        "compact_final_value_review_payload": compact_review_payload or None,
        "missing_evidence": row.get("missing_evidence"),
        "preliminary_interpretation": row.get("preliminary_interpretation"),
    }


def _write_csv(path: Path, evidence_rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "account",
        "county",
        "neighborhood",
        "address",
        "current_appraised_value",
        "final_reconciled_status",
        "runtime_final_value_status",
        "recovered_v14_status",
        "safe_requested_roll_value",
        "safe_requested_reduction_amount",
        "safe_requested_reduction_pct",
        "exposed_requested_roll_value",
        "exposed_requested_reduction_amount",
        "exposed_requested_reduction_pct",
        "value_interpretation",
        "runtime_completed_flag",
        "fallback_used",
        "same_neighborhood_count",
        "fallback_count",
        "included_comp_count",
        "excluded_review_heavy_count",
        "excluded_likely_exclude_count",
        "dominant_adjustment_channels",
        "bedroom_signal",
        "land_site_signal_present",
        "fort_bend_bathroom_source_posture_present",
        "completeness_status_code",
        "completeness_defect_category",
        "evidence_completeness_grade",
        "preliminary_interpretation",
        "missing_evidence",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in evidence_rows:
            writer.writerow(
                {
                    "account": row["account"],
                    "county": row["county"],
                    "neighborhood": row["neighborhood"],
                    "address": row["subject_context"].get("address"),
                    "current_appraised_value": row.get("current_appraised_value"),
                    "final_reconciled_status": row["reconciliation"].get(
                        "final_reconciled_status"
                    ),
                    "runtime_final_value_status": row["reconciliation"].get(
                        "runtime_final_value_status"
                    ),
                    "recovered_v14_status": row["reconciliation"].get(
                        "recovered_v14_status"
                    ),
                    "safe_requested_roll_value": row["value_semantics"].get(
                        "safe_requested_roll_value"
                    ),
                    "safe_requested_reduction_amount": row["value_semantics"].get(
                        "safe_requested_reduction_amount"
                    ),
                    "safe_requested_reduction_pct": row["value_semantics"].get(
                        "safe_requested_reduction_pct"
                    ),
                    "exposed_requested_roll_value": row["value_semantics"].get(
                        "exposed_requested_roll_value"
                    ),
                    "exposed_requested_reduction_amount": row["value_semantics"].get(
                        "exposed_requested_reduction_amount"
                    ),
                    "exposed_requested_reduction_pct": row["value_semantics"].get(
                        "exposed_requested_reduction_pct"
                    ),
                    "value_interpretation": row["value_semantics"].get(
                        "value_interpretation"
                    ),
                    "runtime_completed_flag": row["reconciliation"].get(
                        "runtime_completed_flag"
                    ),
                    "fallback_used": row["runtime"].get("fallback_used"),
                    "same_neighborhood_count": row["runtime"].get(
                        "same_neighborhood_count"
                    ),
                    "fallback_count": row["runtime"].get("fallback_count"),
                    "included_comp_count": row["counts"].get("included_comp_count"),
                    "excluded_review_heavy_count": row["counts"].get(
                        "excluded_review_heavy_count"
                    ),
                    "excluded_likely_exclude_count": row["counts"].get(
                        "excluded_likely_exclude_count"
                    ),
                    "dominant_adjustment_channels": json.dumps(
                        (row.get("run_state_summary") or {}).get(
                            "dominant_adjustment_channels", []
                        )
                    ),
                    "bedroom_signal": json.dumps(
                        (row.get("run_state_summary") or {}).get("bedroom_signal")
                    ),
                    "land_site_signal_present": (row.get("run_state_summary") or {}).get(
                        "land_site_signal_present"
                    ),
                    "fort_bend_bathroom_source_posture_present": bool(
                        (row.get("run_state_summary") or {}).get(
                            "fort_bend_bathroom_source_posture"
                        )
                    ),
                    "completeness_status_code": row["classified"].get(
                        "completeness_status_code"
                    ),
                    "completeness_defect_category": row["classified"].get(
                        "completeness_defect_category"
                    ),
                    "evidence_completeness_grade": row.get(
                        "evidence_completeness_grade"
                    ),
                    "preliminary_interpretation": row.get("preliminary_interpretation"),
                    "missing_evidence": json.dumps(row.get("missing_evidence") or []),
                }
            )


def _build_markdown_summary(package: dict[str, Any]) -> str:
    summary = package["summary"]
    diagnosis = package["observability_diagnosis"]
    lines = [
        "# Unequal-Roll Full100 Model Review Evidence",
        "",
        f"- Subject rows: {summary['subject_count']}",
        f"- Decision-reviewable rows: {summary['decision_reviewable_count']}",
        f"- Payload-gap rows: {summary['payload_gap_count']}",
        f"- Unresolved runtime-None rows: {summary['unresolved_none_row_count']}",
        f"- Stability metrics recovered rows: {summary['stability_metrics_recovered_count']}",
        f"- Run-state comp-review rows available: {summary['run_state_comp_review_rows_available']}",
        "",
        "## Final Status Distribution",
        "",
    ]
    for key, value in sorted(summary["final_status_distribution"].items(), key=lambda item: str(item[0])):
        lines.append(f"- {key}: {value}")
    lines.extend([
        "",
        "## Evidence Grade Distribution",
        "",
    ])
    for key, value in sorted(summary["evidence_grade_distribution"].items(), key=lambda item: str(item[0])):
        lines.append(f"- {key}: {value}")
    lines.extend([
        "",
        "## Unsupported Value Semantics",
        "",
    ])
    for key, value in sorted(summary["unsupported_value_semantics_distribution"].items(), key=lambda item: str(item[0])):
        lines.append(f"- {key}: {value}")
    lines.extend([
        "",
        "## Unsupported Positive-Reduction Rows Safely Labeled",
        "",
    ])
    for account in summary["unsupported_positive_reduction_rows_safely_labeled"]:
        lines.append(f"- {account}")
    lines.extend([
        "",
        "## Observability Diagnosis",
        "",
        "### Computed Upstream But Absent In Current Full100 Source Artifacts",
        "",
    ])
    for item in diagnosis["computed_upstream_but_absent_in_current_full100_source_artifacts"]:
        lines.append(f"- {item}")
    lines.extend([
        "",
        "### Recoverable From Read-Only Stage 21",
        "",
    ])
    for item in diagnosis["recoverable_from_read_only_stage21"]:
        lines.append(f"- {item}")
    lines.extend([
        "",
        "### Truly Unavailable Without Producer Or Runtime Table Path",
        "",
    ])
    for item in diagnosis["truly_unavailable_without_producer_or_runtime_table_path"]:
        lines.append(f"- {item}")
    lines.extend([
        "",
        "## Focused Review Packets",
        "",
    ])
    for bucket, rows in package["focused_packets"].items():
        lines.append(f"### {bucket}")
        lines.append("")
        for row in rows:
            lines.append(
                f"- {row['account']}: {row['final_status_interpretation'].get('final_reconciled_status')} / {row['preliminary_interpretation']}"
            )
            lines.append(
                f"  address={row['subject_summary'].get('address')} safe_value={row['safe_vs_diagnostic_value_interpretation'].get('safe_requested_roll_value')} diagnostic={row['safe_vs_diagnostic_value_interpretation'].get('value_interpretation')}"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def _env(key: str) -> str:
    import os

    return str(os.environ.get(key) or "")


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
