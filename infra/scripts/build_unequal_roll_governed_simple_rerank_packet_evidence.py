#!/usr/bin/env python3
"""Build complete packet evidence for governed simple rerank packets.

This bridge keeps the model frozen. It combines the retained governed rerank
complete-comp evidence with fallback rows from governed fallback artifacts, and
replays only material similarity_top_100 baseline-support candidates so packet
visible baseline rows have complete comp evidence.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from app.services.unequal_roll_no_persist_replay import (
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
)
from app.services.unequal_roll_smart_harvest import SIMILARITY_TOP_100
from infra.scripts.report_unequal_roll_governed_simple_rerank_pilot_packet import (
    MATERIAL_THRESHOLD,
    VARIANT_KEY,
)
from infra.scripts.run_unequal_roll_full_reranking_experiment import included_rows


DEFAULT_DATABASE_URL = "postgresql://stage21_admin:stage21_admin@localhost:55442/stage21_dev"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build governed simple-rerank packet evidence including baseline fallback rows."
    )
    parser.add_argument("--retained-complete-comp-evidence-artifact", type=Path, required=True)
    parser.add_argument("--governed-fallback-artifact", type=Path, action="append", default=[])
    parser.add_argument("--raw-artifact", type=Path, action="append", default=[])
    parser.add_argument("--database-url", default=os.environ.get("DWELLIO_DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument("--output-dir", type=Path, default=Path("/private/tmp"))
    parser.add_argument("--timestamp", default=None)
    return parser


def load_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        return json.load(fh)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def case_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("county_id") or ""),
        str(row.get("subject_account") or ""),
        str(row.get("neighborhood_code") or ""),
    )


def id_join(values: list[str]) -> str:
    return ";".join(str(value) for value in values if str(value))


def build_raw_lookup(raw_artifact_paths: list[Path]) -> dict[tuple[str, str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for path in raw_artifact_paths:
        payload = load_json(path)
        for row in payload.get("subject_rows") or payload.get("variant_rows") or []:
            if row.get("variant_key") != VARIANT_KEY:
                continue
            out = dict(row)
            out["raw_source_artifact"] = str(path)
            lookup[case_key(out)] = out
    return lookup


def load_governed_rows(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        payload = load_json(path)
        for row in payload.get("case_rows", []):
            if row.get("variant_key") != VARIANT_KEY:
                continue
            out = dict(row)
            out["source_governed_fallback_artifact"] = str(path)
            rows.append(out)
    return rows


def baseline_candidate(raw_row: dict[str, Any], governed_row: dict[str, Any]) -> bool:
    status = str(raw_row.get("smart_final_value_status") or governed_row.get("smart_final_status") or "")
    interpretation = str(raw_row.get("smart_value_interpretation") or governed_row.get("smart_value_interpretation") or "")
    included_count = int(raw_row.get("smart_included_comp_count") or governed_row.get("smart_included_comp_count") or 0)
    return (
        interpretation == "final_model_value"
        and status not in {"", "unsupported", "no_reduction"}
        and as_float(raw_row.get("smart_requested_reduction_amount")) >= MATERIAL_THRESHOLD
        and included_count > 0
    )


def total_abs_adjustment(detail: dict[str, Any]) -> float | None:
    line_items = list(detail.get("line_items") or [])
    if not line_items:
        return None
    total = 0.0
    found = False
    for item in line_items:
        amount = item.get("signed_adjustment_amount")
        if amount not in (None, ""):
            total += abs(as_float(amount))
            found = True
    return round(total, 2) if found else None


def comp_row_from_detail(
    *,
    case_row: dict[str, Any],
    detail: dict[str, Any],
    membership: str,
) -> dict[str, Any]:
    return {
        "variant_key": VARIANT_KEY,
        "county_id": case_row.get("county_id"),
        "subject_account": case_row.get("subject_account"),
        "neighborhood_code": case_row.get("neighborhood_code"),
        "governance_classification": case_row.get("governance_classification"),
        "governance_view": case_row.get("governance_view"),
        "comp_parcel_id": str(detail.get("candidate_parcel_id") or ""),
        "comp_account_number": detail.get("account_number") or detail.get("candidate_account_number"),
        "comp_address": detail.get("address"),
        "comp_tax_year": case_row.get("requested_tax_year"),
        "membership": membership,
        "adjusted_value": detail.get("adjusted_appraised_value"),
        "adjusted_value_per_sf": detail.get("adjusted_appraised_value_per_sf"),
        "raw_appraised_value": detail.get("raw_appraised_value"),
        "raw_appraised_value_per_sf": detail.get("raw_appraised_value_per_sf"),
        "comp_appraised_value": detail.get("raw_appraised_value"),
        "comp_value_per_sf": detail.get("raw_appraised_value_per_sf"),
        "comp_living_area_sf": detail.get("living_area_sf"),
        "comp_land_sf": detail.get("land_sf"),
        "comp_land_acres": detail.get("land_acres"),
        "comp_year_built": detail.get("year_built"),
        "comp_effective_age": detail.get("effective_age"),
        "similarity_score": detail.get("similarity_score"),
        "total_abs_adjustment": total_abs_adjustment(detail),
        "line_item_count": len(list(detail.get("line_items") or [])),
    }


def replay_similarity_baseline(
    service: UnequalRollNoPersistReplayService,
    conn: Any,
    *,
    county_id: str,
    subject_account: str,
    requested_tax_year: int,
) -> dict[str, Any]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            result = service.replay_subject(
                cur,
                request=UnequalRollReplayRequest(
                    county_id=county_id,
                    account_number=subject_account,
                    requested_tax_year=requested_tax_year,
                ),
                same_neighborhood_harvest_strategy=SIMILARITY_TOP_100,
                include_taxpayer_favorable_tiebreak_reporting=False,
            )
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise


def merge_fallback_case(
    *,
    raw_row: dict[str, Any],
    governed_row: dict[str, Any],
    requested_tax_year: int,
    baseline_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = {
        **raw_row,
        "variant_key": VARIANT_KEY,
        "county_id": governed_row.get("county_id"),
        "subject_account": governed_row.get("subject_account"),
        "neighborhood_code": governed_row.get("neighborhood_code"),
        "requested_tax_year": requested_tax_year,
        "governance_classification": governed_row.get("governance_classification"),
        "governance_view": governed_row.get("governance_view"),
        "governance_reasons": ";".join(governed_row.get("governance_reasons") or []),
        "fallback_to_similarity_top_100": governed_row.get("fallback_to_similarity_top_100"),
        "fallback_reason": ";".join(governed_row.get("governance_reasons") or []),
        "governed_taxpayer_delta_vs_similarity_top_100": governed_row.get("governed_delta_vs_smart"),
        "raw_taxpayer_delta_vs_similarity_top_100": governed_row.get("raw_delta_vs_smart"),
        "model_backed": governed_row.get("governed_model_backed"),
        "true_final_status_downgrade_raw": governed_row.get("true_final_status_downgrade_raw"),
        "true_transition_to_unsupported_raw": governed_row.get("true_transition_to_unsupported_raw"),
        "included_comp_collapse_raw": governed_row.get("included_comp_count_collapse_raw"),
        "similarity_delta": governed_row.get("similarity_delta"),
        "review_heavy_delta": governed_row.get("review_heavy_delta"),
        "likely_exclude_delta": governed_row.get("likely_exclude_delta"),
    }
    if baseline_result is None:
        return row

    included = included_rows(baseline_result)
    comp_ids = [str(item.get("candidate_parcel_id") or "") for item in included if item.get("candidate_parcel_id")]
    row.update(
        {
            "subject_parcel_id": baseline_result.get("parcel_id"),
            "smart_requested_roll_value": baseline_result.get("requested_roll_value"),
            "smart_requested_reduction_amount": baseline_result.get("requested_reduction_amount"),
            "smart_final_value_status": baseline_result.get("final_value_status"),
            "smart_value_interpretation": baseline_result.get("value_interpretation"),
            "smart_included_comp_count": baseline_result.get("included_comp_count"),
            "smart_replay_final_status": baseline_result.get("final_value_status"),
            "smart_replay_status": baseline_result.get("replay_status"),
            "smart_replay_value_interpretation": baseline_result.get("value_interpretation"),
            "smart_full_included_comp_ids": id_join(comp_ids),
            "rerank_full_included_comp_ids": id_join(comp_ids),
            "overlap_comp_ids": id_join(comp_ids),
            "added_comp_ids": "",
            "removed_comp_ids": "",
            "complete_comp_set_recovered": bool(comp_ids),
        }
    )
    return row


def build_payload(
    *,
    retained_payload: dict[str, Any],
    governed_rows: list[dict[str, Any]],
    raw_lookup: dict[tuple[str, str, str], dict[str, Any]],
    database_url: str,
    requested_tax_year: int,
) -> dict[str, Any]:
    case_rows = [dict(row) for row in retained_payload.get("case_rows", [])]
    comp_rows = [dict(row) for row in retained_payload.get("comp_rows", [])]
    existing_keys = {case_key(row) for row in case_rows}

    service = UnequalRollNoPersistReplayService()
    replayed_count = 0
    replay_blocked_count = 0
    fallback_rows_added = 0
    baseline_support_candidates = 0
    with service.connect_read_only(database_url) as conn:
        for governed in governed_rows:
            if governed.get("governance_view") != "fallback_blocked":
                continue
            key = case_key(governed)
            if key in existing_keys:
                continue
            raw = raw_lookup.get(key, {})
            baseline_result = None
            if baseline_candidate(raw, governed):
                baseline_support_candidates += 1
                baseline_result = replay_similarity_baseline(
                    service,
                    conn,
                    county_id=str(governed.get("county_id")),
                    subject_account=str(governed.get("subject_account")),
                    requested_tax_year=requested_tax_year,
                )
                replayed_count += 1
                if baseline_result.get("replay_status") != "completed":
                    replay_blocked_count += 1
                    baseline_result = None
            case = merge_fallback_case(
                raw_row=raw,
                governed_row=governed,
                requested_tax_year=requested_tax_year,
                baseline_result=baseline_result,
            )
            case_rows.append(case)
            fallback_rows_added += 1
            if baseline_result is not None:
                for detail in included_rows(baseline_result):
                    comp_rows.append(
                        comp_row_from_detail(
                            case_row=case,
                            detail=detail,
                            membership="overlap",
                        )
                    )

    return {
        "artifact_contract": {
            "analysis_mode": "governed_simple_rerank_packet_complete_evidence",
            "primary_variant": VARIANT_KEY,
            "requested_tax_year": requested_tax_year,
            "candidate_universe_mode": "true_full_pool_requested",
            "candidate_universe_limit": None,
            "bounded_proxy_used_for_conclusions": False,
            "created_at": datetime.now().strftime("%Y%m%dT%H%M%S"),
        },
        "case_rows": case_rows,
        "comp_rows": comp_rows,
        "guardrails": {
            "db_writes_occurred": False,
            "migrations_added": False,
            "runtime_defaults_changed": False,
            "production_scoring_adjustment_median_governance_final_value_changed": False,
            "model_feature_or_penalty_changes": False,
            "reranking_remains_no_persist_experiment_only": True,
        },
        "summary": {
            "case_count": len(case_rows),
            "comp_row_count": len(comp_rows),
            "retained_case_count": len(retained_payload.get("case_rows", [])),
            "fallback_rows_added": fallback_rows_added,
            "baseline_support_candidates": baseline_support_candidates,
            "baseline_replayed_count": replayed_count,
            "baseline_replay_blocked_count": replay_blocked_count,
            "governance_view_counts": dict(Counter(row.get("governance_view") for row in case_rows)),
            "membership_counts": dict(Counter(row.get("membership") for row in comp_rows)),
        },
    }


def output_paths(output_dir: Path, timestamp: str) -> dict[str, Path]:
    prefix = output_dir / f"unequal_roll_governed_simple_rerank_packet_complete_evidence_{timestamp}"
    return {
        "json": prefix.with_suffix(".json"),
        "case_csv": Path(f"{prefix}.csv"),
        "comp_csv": Path(f"{prefix}_comp_details.csv"),
    }


def main() -> None:
    args = build_parser().parse_args()
    retained_payload = load_json(args.retained_complete_comp_evidence_artifact)
    raw_lookup = build_raw_lookup(args.raw_artifact)
    governed_rows = load_governed_rows(args.governed_fallback_artifact)
    payload = build_payload(
        retained_payload=retained_payload,
        governed_rows=governed_rows,
        raw_lookup=raw_lookup,
        database_url=args.database_url,
        requested_tax_year=args.requested_tax_year,
    )
    ts = args.timestamp or payload["artifact_contract"]["created_at"]
    payload["artifact_contract"]["created_at"] = ts
    args.output_dir.mkdir(parents=True, exist_ok=True)
    paths = output_paths(args.output_dir, ts)
    write_json(paths["json"], payload)
    write_csv(paths["case_csv"], payload["case_rows"])
    write_csv(paths["comp_csv"], payload["comp_rows"])
    for path in paths.values():
        print(path)
    print(json.dumps(payload["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
