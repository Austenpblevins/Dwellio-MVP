from __future__ import annotations

import argparse
import csv
import json
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

SAFE_CASES = [
    ("harris", "1397720020001"),
    ("harris", "0250700000016"),
    ("harris", "1193500010001"),
    ("harris", "1056000000001"),
]
OPTIONAL_CASES = [
    ("harris", "1193500010003"),
    ("harris", "0610630010002"),
    ("harris", "1397720010001"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run no-persist taxpayer-favorable tie-break prototype.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-dir", default="/private/tmp")
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument("--include-optional-cases", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = UnequalRollNoPersistReplayService()
    tie_service = UnequalRollTaxpayerFavorableTieBreakService()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    cohort = list(SAFE_CASES)
    if args.include_optional_cases:
        cohort.extend(OPTIONAL_CASES)
        prefix = Path(args.output_dir) / f"unequal_roll_taxpayer_favorable_tiebreak_prototype_controls_{timestamp}"
    else:
        prefix = Path(args.output_dir) / f"unequal_roll_taxpayer_favorable_tiebreak_prototype_{timestamp}"

    rows: list[dict[str, Any]] = []
    with service.connect_read_only(args.database_url) as conn:
        for county, account in cohort:
            current = replay(service, conn, county, account, args.requested_tax_year, CURRENT_ORDER_CAP_100)
            smart = replay(service, conn, county, account, args.requested_tax_year, SIMILARITY_TOP_100)
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
                    "current": summarize_strategy(current),
                    "similarity_top_100": summarize_strategy(smart),
                    "tie_break_1_swap": summarize_tiebreak(tie1, smart, current),
                    "tie_break_2_swap": summarize_tiebreak(tie2, smart, current),
                }
            )

    payload = {
        "generated_at": datetime.now().isoformat(),
        "requested_tax_year": args.requested_tax_year,
        "cases": rows,
        "summary": build_summary(rows),
    }
    Path(f"{prefix}.json").write_text(json.dumps(payload, indent=2))
    write_csv(Path(f"{prefix}.csv"), rows)
    write_md(Path(f"{prefix}.md"), payload)
    print(json.dumps({"json": f"{prefix}.json", "csv": f"{prefix}.csv", "md": f"{prefix}.md", "summary": payload["summary"]}, indent=2))


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


def summarize_strategy(result: dict[str, Any]) -> dict[str, Any]:
    detail = result.get("final_value_detail_json") or {}
    return {
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "stability_metrics": detail.get("stability_metrics"),
        "adjusted_median": (detail.get("stability_metrics") or {}).get("median_all"),
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "requested_reduction_pct": result.get("requested_reduction_pct"),
    }


def summarize_tiebreak(result: dict[str, Any], smart: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_status": result.get("final_value_status"),
        "support_status": result.get("support_status"),
        "included_comp_count": result.get("included_comp_count"),
        "swapped_comp_count": result.get("swapped_comp_count"),
        "swapped_in_candidate_parcel_ids": [
            row["swapped_in_candidate_parcel_id"] for row in result.get("accepted_swaps") or []
        ],
        "swapped_out_candidate_parcel_ids": [
            row["swapped_out_candidate_parcel_id"] for row in result.get("accepted_swaps") or []
        ],
        "rejected_alternatives": result.get("rejected_alternatives"),
        "alternatives_considered_count": result.get("alternatives_considered_count"),
        "review_heavy_count": result.get("excluded_review_heavy_count"),
        "likely_exclude_count": result.get("excluded_likely_exclude_count"),
        "stability_metrics": result.get("stability_metrics"),
        "iqr_dispersion": (result.get("stability_metrics") or {}).get("adjusted_value_iqr"),
        "adjusted_median": result.get("requested_roll_value"),
        "requested_reduction_amount": result.get("requested_reduction_amount"),
        "requested_reduction_pct": result.get("requested_reduction_pct"),
        "taxpayer_gain_vs_smart": round((result.get("requested_reduction_amount") or 0.0) - (smart.get("requested_reduction_amount") or 0.0), 2),
        "taxpayer_gain_vs_current": round((result.get("requested_reduction_amount") or 0.0) - (current.get("requested_reduction_amount") or 0.0), 2),
        "remains_defensible": result.get("remains_defensible"),
        "qa_flags": result.get("qa_flags"),
        "governance_refinement_detail": result.get("governance_refinement_detail"),
        "simulation_metadata": result.get("simulation_metadata"),
        "automation_assessment": result.get("automation_assessment"),
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "cases_reviewed": len(rows),
        "tie_break_1": summarize_collection(rows, "tie_break_1_swap"),
        "tie_break_2": summarize_collection(rows, "tie_break_2_swap"),
    }
    return summary


def summarize_collection(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    return {
        "safe_automated_candidate_count": sum(
            1
            for row in rows
            if (row[key].get("automation_assessment") or {}).get("automation_status")
            == "safe_automated_candidate"
        ),
        "manual_review_only_count": sum(
            1
            for row in rows
            if (row[key].get("automation_assessment") or {}).get("automation_status")
            == "manual_review_only"
        ),
        "no_safe_opportunity_count": sum(
            1
            for row in rows
            if (row[key].get("automation_assessment") or {}).get("automation_status")
            == "no_safe_opportunity"
        ),
        "total_reduction_recovered_vs_smart": round(
            sum(row[key]["taxpayer_gain_vs_smart"] for row in rows), 2
        ),
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
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "county",
        "account",
        "strategy",
        "final_status",
        "support_status",
        "included_comp_count",
        "swapped_comp_count",
        "swapped_in_candidate_parcel_ids",
        "swapped_out_candidate_parcel_ids",
        "review_heavy_count",
        "likely_exclude_count",
        "adjusted_median",
        "requested_reduction_amount",
        "requested_reduction_pct",
        "taxpayer_gain_vs_smart",
        "taxpayer_gain_vs_current",
        "remains_defensible",
        "automation_status",
        "automation_reasons",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            for strategy in ("current", "similarity_top_100", "tie_break_1_swap", "tie_break_2_swap"):
                item = row[strategy]
                writer.writerow(
                    {
                        "county": row["county"],
                        "account": row["account"],
                        "strategy": strategy,
                        "final_status": item.get("final_status"),
                        "support_status": item.get("support_status"),
                        "included_comp_count": item.get("included_comp_count"),
                        "swapped_comp_count": item.get("swapped_comp_count"),
                        "swapped_in_candidate_parcel_ids": "|".join(item.get("swapped_in_candidate_parcel_ids") or []),
                        "swapped_out_candidate_parcel_ids": "|".join(item.get("swapped_out_candidate_parcel_ids") or []),
                        "review_heavy_count": item.get("review_heavy_count"),
                        "likely_exclude_count": item.get("likely_exclude_count"),
                        "adjusted_median": item.get("adjusted_median"),
                        "requested_reduction_amount": item.get("requested_reduction_amount"),
                        "requested_reduction_pct": item.get("requested_reduction_pct"),
                        "taxpayer_gain_vs_smart": item.get("taxpayer_gain_vs_smart"),
                        "taxpayer_gain_vs_current": item.get("taxpayer_gain_vs_current"),
                        "remains_defensible": item.get("remains_defensible"),
                        "automation_status": (item.get("automation_assessment") or {}).get("automation_status"),
                        "automation_reasons": "|".join((item.get("automation_assessment") or {}).get("automation_reasons") or []),
                    }
                )


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Unequal Roll Taxpayer-Favorable Tie-Break Prototype",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Cases reviewed: {payload['summary']['cases_reviewed']}",
        "",
        "## Summary",
        f"- 1-swap safe automated candidates: {payload['summary']['tie_break_1']['safe_automated_candidate_count']}",
        f"- 1-swap manual-review-only: {payload['summary']['tie_break_1']['manual_review_only_count']}",
        f"- 1-swap no-safe-opportunity: {payload['summary']['tie_break_1']['no_safe_opportunity_count']}",
        f"- 1-swap reduction recovered vs smart: {payload['summary']['tie_break_1']['total_reduction_recovered_vs_smart']}",
        f"- 2-swap safe automated candidates: {payload['summary']['tie_break_2']['safe_automated_candidate_count']}",
        f"- 2-swap manual-review-only: {payload['summary']['tie_break_2']['manual_review_only_count']}",
        f"- 2-swap no-safe-opportunity: {payload['summary']['tie_break_2']['no_safe_opportunity_count']}",
        f"- 2-swap reduction recovered vs smart: {payload['summary']['tie_break_2']['total_reduction_recovered_vs_smart']}",
        "",
        "## Cases",
    ]
    for row in payload["cases"]:
        lines.append(
            f"- {row['county']} {row['account']}: smart={row['similarity_top_100']['requested_reduction_amount']}, "
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
