from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.unequal_roll_no_persist_replay import (
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
)
from app.services.unequal_roll_smart_harvest import CURRENT_ORDER_CAP_100, SIMILARITY_TOP_100
from app.services.unequal_roll_taxpayer_favorable_tiebreak import (
    TaxpayerFavorableTieBreakConfig,
    UnequalRollTaxpayerFavorableTieBreakService,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run broader no-persist taxpayer-favorable tie-break validation."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-dir", default="/private/tmp")
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument(
        "--large-validation-json",
        default="/private/tmp/unequal_roll_smart_harvest_large_validation_20260507T170326.json",
    )
    parser.add_argument(
        "--dynamic-validation-json",
        default="/private/tmp/unequal_roll_smart_harvest_dynamic_cap_validation_20260507T211738.json",
    )
    parser.add_argument(
        "--controls-json",
        default="/private/tmp/unequal_roll_taxpayer_favorable_tiebreak_prototype_controls_20260507T221731.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    prefix = Path(args.output_dir) / f"unequal_roll_taxpayer_favorable_tiebreak_broader_validation_{timestamp}"

    cohort, cohort_metadata = build_cohort(
        large_validation_json=Path(args.large_validation_json),
        dynamic_validation_json=Path(args.dynamic_validation_json),
        controls_json=Path(args.controls_json),
    )

    service = UnequalRollNoPersistReplayService()
    tie_service = UnequalRollTaxpayerFavorableTieBreakService()
    rows: list[dict[str, Any]] = []

    with service.connect_read_only(args.database_url) as conn:
        for item in cohort:
            county = item["county"]
            account = item["account"]
            current = replay(service, conn, county, account, args.requested_tax_year, CURRENT_ORDER_CAP_100)
            smart = replay(service, conn, county, account, args.requested_tax_year, SIMILARITY_TOP_100)
            parcel_map = load_parcel_account_map(
                conn,
                current_result=current,
                smart_result=smart,
            )
            tie1 = tie_service.simulate(
                current_result=current,
                smart_result=smart,
                config=TaxpayerFavorableTieBreakConfig(max_swaps=1),
            )
            tie2 = tie_service.simulate(
                current_result=current,
                smart_result=smart,
                config=TaxpayerFavorableTieBreakConfig(max_swaps=2),
            )

            rows.append(
                {
                    "county": county,
                    "account": account,
                    "neighborhood_code": item.get("neighborhood_code"),
                    "cohort_tags": item.get("cohort_tags") or [],
                    "current": summarize_strategy(current),
                    "similarity_top_100": summarize_strategy(smart),
                    "tie_break_1_swap": summarize_tiebreak(
                        result=tie1,
                        smart=smart,
                        current=current,
                        parcel_account_map=parcel_map,
                    ),
                    "tie_break_2_swap": summarize_tiebreak(
                        result=tie2,
                        smart=smart,
                        current=current,
                        parcel_account_map=parcel_map,
                    ),
                }
            )

    payload = {
        "generated_at": datetime.now().isoformat(),
        "requested_tax_year": args.requested_tax_year,
        "cohort_size": len(rows),
        "cohort_metadata": cohort_metadata,
        "cases": rows,
        "summary": build_summary(rows),
    }

    Path(f"{prefix}.json").write_text(json.dumps(payload, indent=2))
    write_csv(Path(f"{prefix}.csv"), rows)
    write_md(Path(f"{prefix}.md"), payload)
    print(
        json.dumps(
            {
                "json": f"{prefix}.json",
                "csv": f"{prefix}.csv",
                "md": f"{prefix}.md",
                "summary": payload["summary"],
            },
            indent=2,
        )
    )


def build_cohort(
    *,
    large_validation_json: Path,
    dynamic_validation_json: Path,
    controls_json: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    large = json.loads(large_validation_json.read_text())
    subjects = list(large.get("subjects") or [])
    loss_cases: dict[tuple[str, str], dict[str, Any]] = {}
    for subject in subjects:
        reduction_change = float(subject.get("reduction_change_amount") or 0.0)
        current_reduction = float((subject.get("current") or {}).get("requested_reduction_amount") or 0.0)
        smart_reduction = float((subject.get("similarity_top_100") or {}).get("requested_reduction_amount") or 0.0)
        high_risk_flags = set(subject.get("high_risk_flags") or [])
        if (
            reduction_change < 0
            or "positive_reduction_becomes_no_reduction" in high_risk_flags
            or ("overfit_similarity_taxpayer_loss" in high_risk_flags and current_reduction > smart_reduction)
        ):
            key = (subject["county"], subject["account"])
            loss_cases[key] = {
                "county": subject["county"],
                "account": subject["account"],
                "neighborhood_code": subject.get("neighborhood_code"),
                "cohort_tags": sorted(
                    set(
                        [subject.get("neighborhood_bucket"), subject.get("bucket")]
                        + list(subject.get("high_risk_flags") or [])
                        + (["harris_large_neighborhood_loss"] if subject["county"] == "harris" and subject.get("neighborhood_bucket") == "large" else [])
                    )
                ),
            }

    # Keep these artifact dependencies explicit even if the union does not grow.
    dynamic = json.loads(dynamic_validation_json.read_text())
    dynamic_subjects = list(dynamic.get("subjects") or [])
    dynamic_loss_keys = {
        (subject["county"], subject["account"])
        for subject in dynamic_subjects
        if float(((subject.get("similarity_dynamic_cap") or {}).get("requested_reduction_amount") or 0.0))
        < float(((subject.get("similarity_top_100") or {}).get("requested_reduction_amount") or 0.0))
    }

    controls = json.loads(controls_json.read_text())
    control_keys = {(case["county"], case["account"]) for case in controls.get("cases") or []}
    for key in control_keys:
        if key in loss_cases:
            loss_cases[key]["cohort_tags"] = sorted(set(loss_cases[key]["cohort_tags"] + ["prototype_control_regression"]))

    cohort = sorted(loss_cases.values(), key=lambda row: (row["county"], row["account"]))
    metadata = {
        "from_large_validation_loss_cases": len(loss_cases),
        "dynamic_cases_with_lower_reduction_than_top100": len(dynamic_loss_keys),
        "control_regression_cases_present": len(control_keys & set(loss_cases.keys())),
    }
    return cohort, metadata


def replay(service, conn, county: str, account: str, requested_tax_year: int, strategy: str) -> dict[str, Any]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            result = service.replay_subject(
                cur,
                request=UnequalRollReplayRequest(
                    county_id=county,
                    account_number=account,
                    requested_tax_year=requested_tax_year,
                ),
                same_neighborhood_harvest_strategy=strategy,
            )
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise


def load_parcel_account_map(conn, *, current_result: dict[str, Any], smart_result: dict[str, Any]) -> dict[str, str]:
    parcel_ids = set()
    for result in (current_result, smart_result):
        detail = dict(result.get("final_value_detail_json") or {})
        for row in list(detail.get("included_comp_rows") or []) + list(detail.get("excluded_comp_rows") or []):
            parcel_id = row.get("candidate_parcel_id")
            if parcel_id:
                parcel_ids.add(str(parcel_id))
    if not parcel_ids:
        return {}
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select parcel_id::text, account_number
                from parcels
                where parcel_id = any(%s::uuid[])
                """,
                (list(parcel_ids),),
            )
            rows = cur.fetchall()
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    return {parcel_id: account_number for parcel_id, account_number in rows}


def summarize_strategy(result: dict[str, Any]) -> dict[str, Any]:
    detail = result.get("final_value_detail_json") or {}
    stability_metrics = detail.get("stability_metrics") or {}
    return {
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "stability_metrics": stability_metrics,
        "adjusted_median": stability_metrics.get("median_all"),
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "requested_reduction_pct": result.get("requested_reduction_pct"),
    }


def summarize_tiebreak(
    *,
    result: dict[str, Any],
    smart: dict[str, Any],
    current: dict[str, Any],
    parcel_account_map: dict[str, str],
) -> dict[str, Any]:
    accepted_swaps = list(result.get("accepted_swaps") or [])
    rejected_alternatives = list(result.get("rejected_alternatives") or [])
    automation_assessment = dict(result.get("automation_assessment") or {})
    reduction_gain = round(
        float((result.get("requested_reduction_amount") or 0.0))
        - float((smart.get("requested_reduction_amount") or 0.0)),
        2,
    )
    smart_reduction = float((smart.get("requested_reduction_amount") or 0.0))
    rejected_reason_counts = Counter()
    for row in rejected_alternatives:
        rejected_reason_counts.update(row.get("rejection_reasons") or [])
    review_visible_involvement = any(
        row.get("review_visible_flag")
        for row in rejected_alternatives
    )
    return {
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "swapped_comp_count": result.get("swapped_comp_count"),
        "accepted_swap_count": len(accepted_swaps),
        "swapped_in_candidate_parcel_ids": [
            row["swapped_in_candidate_parcel_id"] for row in accepted_swaps
        ],
        "swapped_out_candidate_parcel_ids": [
            row["swapped_out_candidate_parcel_id"] for row in accepted_swaps
        ],
        "swapped_in_accounts": [
            parcel_account_map.get(str(row["swapped_in_candidate_parcel_id"]), str(row["swapped_in_candidate_parcel_id"]))
            for row in accepted_swaps
        ],
        "swapped_out_accounts": [
            parcel_account_map.get(str(row["swapped_out_candidate_parcel_id"]), str(row["swapped_out_candidate_parcel_id"]))
            for row in accepted_swaps
        ],
        "rejected_alternatives_count": len(rejected_alternatives),
        "rejected_reason_counts": dict(rejected_reason_counts),
        "rejected_alternatives": rejected_alternatives,
        "alternatives_considered_count": result.get("alternatives_considered_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "stability_metrics": result.get("stability_metrics"),
        "iqr_dispersion": (result.get("stability_metrics") or {}).get("adjusted_value_iqr"),
        "adjusted_median": result.get("requested_roll_value"),
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "requested_reduction_pct": result.get("requested_reduction_pct"),
        "taxpayer_gain_vs_smart": reduction_gain,
        "taxpayer_gain_vs_current": round(
            float((result.get("requested_reduction_amount") or 0.0))
            - float((current.get("requested_reduction_amount") or 0.0)),
            2,
        ),
        "benefit_below_500": reduction_gain < 500,
        "benefit_below_1000": reduction_gain < 1000,
        "benefit_below_2500": reduction_gain < 2500,
        "benefit_pct_of_smart_reduction": round(reduction_gain / smart_reduction, 6) if smart_reduction > 0 else None,
        "complexity_justified": reduction_gain >= 1000 and len(accepted_swaps) <= 2,
        "review_visible_comp_involvement": bool(
            review_visible_involvement
            or any("review_visible" in reason for reason in automation_assessment.get("automation_reasons") or [])
        ),
        "remains_defensible": result.get("remains_defensible"),
        "qa_flags": result.get("qa_flags"),
        "governance_refinement_detail": result.get("governance_refinement_detail"),
        "simulation_metadata": result.get("simulation_metadata"),
        "automation_assessment": {
            "automation_status": automation_assessment.get("automation_status"),
            "primary_reason": (automation_assessment.get("automation_reasons") or [None])[0],
            "secondary_reasons": (automation_assessment.get("automation_reasons") or [])[1:],
            "automation_reasons": automation_assessment.get("automation_reasons") or [],
            "reduction_gain_vs_smart": automation_assessment.get("reduction_gain_vs_smart"),
        },
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cases_reviewed": len(rows),
        "tie_break_1": summarize_collection(rows, "tie_break_1_swap"),
        "tie_break_2": summarize_collection(rows, "tie_break_2_swap"),
    }


def summarize_collection(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    safe_rows = [
        row for row in rows
        if (row[key].get("automation_assessment") or {}).get("automation_status") == "safe_automated_candidate"
    ]
    manual_rows = [
        row for row in rows
        if (row[key].get("automation_assessment") or {}).get("automation_status") == "manual_review_only"
    ]
    no_safe_rows = [
        row for row in rows
        if (row[key].get("automation_assessment") or {}).get("automation_status") == "no_safe_opportunity"
    ]
    added_value_from_two_swaps = None
    if key == "tie_break_2_swap":
        added_value_from_two_swaps = {
            "cases_with_meaningful_added_value_over_1_swap": sum(
                1 for row in rows if (row["tie_break_2_swap"]["taxpayer_gain_vs_smart"] - row["tie_break_1_swap"]["taxpayer_gain_vs_smart"]) >= 500
            ),
            "cases_where_2_swap_changes_automation_status": sum(
                1
                for row in rows
                if (row["tie_break_2_swap"].get("automation_assessment") or {}).get("automation_status")
                != (row["tie_break_1_swap"].get("automation_assessment") or {}).get("automation_status")
            ),
        }
    return {
        "safe_automated_candidate_count": len(safe_rows),
        "manual_review_only_count": len(manual_rows),
        "no_safe_opportunity_count": len(no_safe_rows),
        "total_recovery_safe_automated_only": round(sum(row[key]["taxpayer_gain_vs_smart"] for row in safe_rows), 2),
        "total_recovery_manual_review_only": round(sum(row[key]["taxpayer_gain_vs_smart"] for row in manual_rows), 2),
        "average_recovery_per_safe_case": round(
            sum(row[key]["taxpayer_gain_vs_smart"] for row in safe_rows) / len(safe_rows),
            2,
        ) if safe_rows else 0.0,
        "status_worsened_count": sum(
            1
            for row in rows
            if _status_rank(row[key]["final_status"]) < _status_rank(row["similarity_top_100"]["final_status"])
        ),
        "support_status_worsened_count": sum(
            1
            for row in rows
            if row[key]["support_status"] != row["similarity_top_100"]["support_status"]
        ),
        "review_heavy_increase_count": sum(
            1
            for row in rows
            if (row[key]["review_heavy_count"] or 0) > (row["similarity_top_100"]["review_heavy_count"] or 0)
        ),
        "likely_exclude_increase_count": sum(
            1
            for row in rows
            if (row[key]["likely_exclude_count"] or 0) > (row["similarity_top_100"]["likely_exclude_count"] or 0)
        ),
        "stability_failure_count": sum(
            1
            for row in rows
            if any(
                row[key].get("qa_flags", {}).get(flag)
                for flag in (
                    "leave_one_out_review_flag",
                    "high_low_removal_review_flag",
                    "adjusted_value_iqr_review_flag",
                )
            )
        ),
        "benefit_below_500_count": sum(1 for row in rows if row[key]["benefit_below_500"]),
        "benefit_below_1000_count": sum(1 for row in rows if row[key]["benefit_below_1000"]),
        "benefit_below_2500_count": sum(1 for row in rows if row[key]["benefit_below_2500"]),
        "added_value_from_two_swaps": added_value_from_two_swaps,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "county",
        "account",
        "neighborhood_code",
        "strategy",
        "requested_reduction_amount",
        "taxpayer_gain_vs_smart",
        "taxpayer_gain_vs_current",
        "automation_status",
        "primary_reason",
        "secondary_reasons",
        "accepted_swap_count",
        "rejected_alternatives_count",
        "swapped_in_accounts",
        "swapped_out_accounts",
        "review_visible_comp_involvement",
        "benefit_below_500",
        "benefit_below_1000",
        "benefit_below_2500",
        "complexity_justified",
        "final_status",
        "support_status",
        "review_heavy_count",
        "likely_exclude_count",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            for strategy in ("similarity_top_100", "tie_break_1_swap", "tie_break_2_swap"):
                item = row[strategy]
                assessment = item.get("automation_assessment") or {}
                writer.writerow(
                    {
                        "county": row["county"],
                        "account": row["account"],
                        "neighborhood_code": row.get("neighborhood_code"),
                        "strategy": strategy,
                        "requested_reduction_amount": item.get("requested_reduction_amount"),
                        "taxpayer_gain_vs_smart": item.get("taxpayer_gain_vs_smart"),
                        "taxpayer_gain_vs_current": item.get("taxpayer_gain_vs_current"),
                        "automation_status": assessment.get("automation_status"),
                        "primary_reason": assessment.get("primary_reason"),
                        "secondary_reasons": "|".join(assessment.get("secondary_reasons") or []),
                        "accepted_swap_count": item.get("accepted_swap_count"),
                        "rejected_alternatives_count": item.get("rejected_alternatives_count"),
                        "swapped_in_accounts": "|".join(item.get("swapped_in_accounts") or []),
                        "swapped_out_accounts": "|".join(item.get("swapped_out_accounts") or []),
                        "review_visible_comp_involvement": item.get("review_visible_comp_involvement"),
                        "benefit_below_500": item.get("benefit_below_500"),
                        "benefit_below_1000": item.get("benefit_below_1000"),
                        "benefit_below_2500": item.get("benefit_below_2500"),
                        "complexity_justified": item.get("complexity_justified"),
                        "final_status": item.get("final_status"),
                        "support_status": item.get("support_status"),
                        "review_heavy_count": item.get("review_heavy_count"),
                        "likely_exclude_count": item.get("likely_exclude_count"),
                    }
                )


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Unequal Roll Taxpayer-Favorable Tie-Break Broader Validation",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Cohort size: {payload['cohort_size']}",
        f"- Cohort metadata: {json.dumps(payload['cohort_metadata'])}",
        "",
        "## Summary",
        f"- 1-swap safe automated: {payload['summary']['tie_break_1']['safe_automated_candidate_count']}",
        f"- 1-swap manual-review-only: {payload['summary']['tie_break_1']['manual_review_only_count']}",
        f"- 1-swap no-safe-opportunity: {payload['summary']['tie_break_1']['no_safe_opportunity_count']}",
        f"- 1-swap safe-only recovery: {payload['summary']['tie_break_1']['total_recovery_safe_automated_only']}",
        f"- 2-swap safe automated: {payload['summary']['tie_break_2']['safe_automated_candidate_count']}",
        f"- 2-swap manual-review-only: {payload['summary']['tie_break_2']['manual_review_only_count']}",
        f"- 2-swap no-safe-opportunity: {payload['summary']['tie_break_2']['no_safe_opportunity_count']}",
        f"- 2-swap safe-only recovery: {payload['summary']['tie_break_2']['total_recovery_safe_automated_only']}",
        "",
        "## Cases",
    ]
    for row in payload["cases"]:
        lines.append(
            f"- {row['county']} {row['account']} ({row.get('neighborhood_code')}): "
            f"smart={row['similarity_top_100']['requested_reduction_amount']}, "
            f"tie1={row['tie_break_1_swap']['requested_reduction_amount']} "
            f"[{(row['tie_break_1_swap'].get('automation_assessment') or {}).get('automation_status')}], "
            f"tie2={row['tie_break_2_swap']['requested_reduction_amount']} "
            f"[{(row['tie_break_2_swap'].get('automation_assessment') or {}).get('automation_status')}]"
        )
    path.write_text("\n".join(lines))


def _status_rank(status: str | None) -> int:
    order = {"unsupported": 0, "manual_review_required": 1, "supported_with_review": 2, "supported": 3}
    return order.get(status or "", -1)


if __name__ == "__main__":
    main()
