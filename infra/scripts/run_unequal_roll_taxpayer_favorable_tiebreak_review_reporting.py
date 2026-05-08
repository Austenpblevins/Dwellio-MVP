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


SMART_CASES = [
    ("harris", "1397720020001"),
    ("harris", "0250700000016"),
    ("harris", "1193500010001"),
    ("harris", "1193500010003"),
    ("harris", "1056000000001"),
    ("harris", "0610630010002"),
    ("harris", "1397720010001"),
]
NOT_EVALUATED_CONTROL = ("harris", "1397720020001")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate no-persist review reporting artifacts for taxpayer-favorable tie-break opportunities."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-dir", default="/private/tmp")
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    prefix = Path(args.output_dir) / f"unequal_roll_taxpayer_favorable_tiebreak_review_reporting_{timestamp}"
    service = UnequalRollNoPersistReplayService()
    rows: list[dict[str, Any]] = []

    with service.connect_read_only(args.database_url) as conn:
        for county, account in SMART_CASES:
            baseline = replay(
                service,
                conn,
                request=UnequalRollReplayRequest(county_id=county, account_number=account, requested_tax_year=args.requested_tax_year),
                strategy=SIMILARITY_TOP_100,
                include_reporting=False,
            )
            reported = replay(
                service,
                conn,
                request=UnequalRollReplayRequest(county_id=county, account_number=account, requested_tax_year=args.requested_tax_year),
                strategy=SIMILARITY_TOP_100,
                include_reporting=True,
            )
            rows.append(summarize_case(county, account, "similarity_top_100", baseline, reported))

        county, account = NOT_EVALUATED_CONTROL
        baseline = replay(
            service,
            conn,
            request=UnequalRollReplayRequest(county_id=county, account_number=account, requested_tax_year=args.requested_tax_year),
            strategy=CURRENT_ORDER_CAP_100,
            include_reporting=False,
        )
        reported = replay(
            service,
            conn,
            request=UnequalRollReplayRequest(county_id=county, account_number=account, requested_tax_year=args.requested_tax_year),
            strategy=CURRENT_ORDER_CAP_100,
            include_reporting=True,
        )
        rows.append(summarize_case(county, account, "current_order_cap_100", baseline, reported))

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


def replay(service, conn, *, request: UnequalRollReplayRequest, strategy: str, include_reporting: bool) -> dict[str, Any]:
    conn.execute("BEGIN READ ONLY")
    try:
        with conn.cursor() as cur:
            result = service.replay_subject(
                cur,
                request=request,
                same_neighborhood_harvest_strategy=strategy,
                include_taxpayer_favorable_tiebreak_reporting=include_reporting,
            )
        conn.rollback()
        return result
    except Exception:
        conn.rollback()
        raise


def summarize_case(county: str, account: str, strategy: str, baseline: dict[str, Any], reported: dict[str, Any]) -> dict[str, Any]:
    review = dict(reported.get("taxpayer_favorable_tiebreak_review") or {})
    compact_review = dict((reported.get("compact_final_value_review_payload") or {}).get("taxpayer_favorable_tiebreak_review") or {})
    drift = {
        "final_status_unchanged": baseline.get("final_value_status") == reported.get("final_value_status"),
        "support_status_unchanged": baseline.get("support_status") == reported.get("support_status"),
        "requested_reduction_unchanged": baseline.get("requested_reduction_amount") == reported.get("requested_reduction_amount"),
        "included_comp_count_unchanged": baseline.get("included_comp_count") == reported.get("included_comp_count"),
        "review_heavy_count_unchanged": baseline.get("excluded_review_heavy_count") == reported.get("excluded_review_heavy_count"),
        "likely_exclude_count_unchanged": baseline.get("excluded_likely_exclude_count") == reported.get("excluded_likely_exclude_count"),
    }
    return {
        "county": county,
        "account": account,
        "strategy": strategy,
        "baseline_final_status": baseline.get("final_value_status"),
        "reported_final_status": reported.get("final_value_status"),
        "baseline_support_status": baseline.get("support_status"),
        "reported_support_status": reported.get("support_status"),
        "baseline_requested_reduction_amount": baseline.get("requested_reduction_amount"),
        "reported_requested_reduction_amount": reported.get("requested_reduction_amount"),
        "baseline_included_comp_count": baseline.get("included_comp_count"),
        "reported_included_comp_count": reported.get("included_comp_count"),
        "baseline_review_heavy_count": baseline.get("excluded_review_heavy_count"),
        "reported_review_heavy_count": reported.get("excluded_review_heavy_count"),
        "baseline_likely_exclude_count": baseline.get("excluded_likely_exclude_count"),
        "reported_likely_exclude_count": reported.get("excluded_likely_exclude_count"),
        "behavior_drift": drift,
        "taxpayer_favorable_tiebreak_review": review,
        "compact_payload_review": compact_review,
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cases_reviewed": len(rows),
        "class_counts": _class_counts(rows),
        "all_behavior_unchanged": all(all(item["behavior_drift"].values()) for item in rows),
    }


def _class_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        cls = str((row.get("taxpayer_favorable_tiebreak_review") or {}).get("taxpayer_favorable_tiebreak_class") or "not_evaluated")
        counts[cls] = counts.get(cls, 0) + 1
    return counts


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "county",
        "account",
        "strategy",
        "taxpayer_favorable_tiebreak_class",
        "taxpayer_favorable_tiebreak_primary_reason",
        "taxpayer_favorable_tiebreak_swap_count",
        "taxpayer_favorable_tiebreak_estimated_reduction_impact",
        "baseline_requested_reduction_amount",
        "reported_requested_reduction_amount",
        "final_status_unchanged",
        "support_status_unchanged",
        "requested_reduction_unchanged",
        "included_comp_count_unchanged",
        "review_heavy_count_unchanged",
        "likely_exclude_count_unchanged",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            review = row["taxpayer_favorable_tiebreak_review"]
            drift = row["behavior_drift"]
            writer.writerow(
                {
                    "county": row["county"],
                    "account": row["account"],
                    "strategy": row["strategy"],
                    "taxpayer_favorable_tiebreak_class": review.get("taxpayer_favorable_tiebreak_class"),
                    "taxpayer_favorable_tiebreak_primary_reason": review.get("taxpayer_favorable_tiebreak_primary_reason"),
                    "taxpayer_favorable_tiebreak_swap_count": review.get("taxpayer_favorable_tiebreak_swap_count"),
                    "taxpayer_favorable_tiebreak_estimated_reduction_impact": review.get("taxpayer_favorable_tiebreak_estimated_reduction_impact"),
                    "baseline_requested_reduction_amount": row["baseline_requested_reduction_amount"],
                    "reported_requested_reduction_amount": row["reported_requested_reduction_amount"],
                    "final_status_unchanged": drift["final_status_unchanged"],
                    "support_status_unchanged": drift["support_status_unchanged"],
                    "requested_reduction_unchanged": drift["requested_reduction_unchanged"],
                    "included_comp_count_unchanged": drift["included_comp_count_unchanged"],
                    "review_heavy_count_unchanged": drift["review_heavy_count_unchanged"],
                    "likely_exclude_count_unchanged": drift["likely_exclude_count_unchanged"],
                }
            )


def write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Unequal Roll Taxpayer-Favorable Tie-Break Review Reporting",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Cases reviewed: {payload['summary']['cases_reviewed']}",
        f"- All behavior unchanged: {payload['summary']['all_behavior_unchanged']}",
        f"- Class counts: {json.dumps(payload['summary']['class_counts'])}",
        "",
        "## Cases",
    ]
    for row in payload["cases"]:
        review = row["taxpayer_favorable_tiebreak_review"]
        lines.append(
            f"- {row['county']} {row['account']} [{row['strategy']}]: "
            f"class={review.get('taxpayer_favorable_tiebreak_class')}, "
            f"reason={review.get('taxpayer_favorable_tiebreak_primary_reason')}, "
            f"impact={review.get('taxpayer_favorable_tiebreak_estimated_reduction_impact')}"
        )
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
