from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import mean
from time import monotonic
from typing import Any

from app.services.unequal_roll_candidate_discovery import DISCOVERY_TIER_SAME_NEIGHBORHOOD
from app.services.unequal_roll_candidate_scoring import compute_similarity_score
from app.services.unequal_roll_no_persist_replay import (
    UnequalRollNoPersistReplayService,
    UnequalRollReplayRequest,
)
from app.services.unequal_roll_smart_harvest import (
    CURRENT_ORDER_CAP_100,
    DYNAMIC_CAP_150,
    SIMILARITY_TOP_100,
    SIMILARITY_TOP_150,
    cheap_same_neighborhood_similarity_score,
    select_same_neighborhood_harvest,
)

COHORT = [
    ("harris", "1397720020001"),
    ("harris", "0411050000070"),
    ("fort_bend", "5922-00-013-0050-907"),
    ("fort_bend", "4850-00-014-2300-907"),
    ("fort_bend", "8695-01-001-0090-901"),
    ("fort_bend", "0226-00-000-0470-906"),
]

STRATEGIES = [
    CURRENT_ORDER_CAP_100,
    DYNAMIC_CAP_150,
    SIMILARITY_TOP_100,
    SIMILARITY_TOP_150,
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run bounded smart-harvest prototype replay against Stage 21 read-only data."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--requested-tax-year", type=int, default=2026)
    parser.add_argument("--output-dir", default="/private/tmp")
    parser.add_argument("--statement-timeout", default="120s")
    parser.add_argument("--max-parallel-workers-per-gather", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_prefix = Path(args.output_dir) / f"unequal_roll_smart_harvest_prototype_{timestamp}"
    service = UnequalRollNoPersistReplayService()

    analysis = {
        "generated_at": datetime.now().isoformat(),
        "requested_tax_year": args.requested_tax_year,
        "cohort": [{"county": c, "account": a} for c, a in COHORT],
        "strategies": STRATEGIES,
        "subjects": [],
    }

    connection = service.connect_read_only(args.database_url)
    try:
        for county, account in COHORT:
            request = UnequalRollReplayRequest(
                county_id=county,
                account_number=account,
                requested_tax_year=args.requested_tax_year,
            )
            connection.execute("BEGIN READ ONLY")
            with connection.cursor() as cursor:
                subject_row = service._subject_snapshot_service._fetch_subject_row(
                    cursor,
                    county_id=request.county_id,
                    requested_tax_year=request.requested_tax_year,
                    account_number=request.account_number,
                )
                if subject_row is None:
                    connection.rollback()
                    analysis["subjects"].append(
                        {
                            "county": county,
                            "account": account,
                            "error": "subject_not_found",
                        }
                    )
                    continue
                subject_snapshot = service._build_subject_snapshot(
                    cursor,
                    request=request,
                    subject_row=subject_row,
                )
                universe_rows = service._discovery_service._fetch_same_neighborhood_candidates(
                    cursor,
                    subject_snapshot=subject_snapshot,
                    limit=None,
                )
            connection.rollback()

            universe_metrics = _score_universe(
                service=service,
                subject_snapshot=subject_snapshot,
                universe_rows=universe_rows,
            )
            subject_result = {
                "county": county,
                "account": account,
                "neighborhood_code": subject_snapshot.get("neighborhood_code"),
                "subdivision_name": subject_snapshot.get("subdivision_name"),
                "total_same_neighborhood_candidates": len(universe_rows),
                "strategies": {},
            }

            current_accounts: set[str] = set()
            for strategy in STRATEGIES:
                selection = select_same_neighborhood_harvest(
                    subject_snapshot=subject_snapshot,
                    same_neighborhood_rows=universe_rows,
                    strategy=strategy,
                )
                harvested_accounts = [
                    str(row.get("account_number") or "") for row in selection.selected_rows
                ]
                if strategy == CURRENT_ORDER_CAP_100:
                    current_accounts = set(harvested_accounts)

                strategy_metrics = _strategy_candidate_metrics(
                    universe_metrics=universe_metrics,
                    harvested_accounts=harvested_accounts,
                )
                replay_result = _run_replay(
                    service=service,
                    connection=connection,
                    request=request,
                    strategy=strategy,
                    statement_timeout=args.statement_timeout,
                    max_parallel_workers_per_gather=args.max_parallel_workers_per_gather,
                )
                overlap = len(current_accounts & set(harvested_accounts)) if current_accounts else len(
                    set(harvested_accounts)
                )
                subject_result["strategies"][strategy] = {
                    "harvested_before_scoring": len(harvested_accounts),
                    "excluded_by_cap": max(0, len(universe_rows) - len(harvested_accounts)),
                    "overlap_with_current": overlap,
                    "replacement_count_vs_current": (
                        0 if strategy == CURRENT_ORDER_CAP_100 else len(harvested_accounts) - overlap
                    ),
                    "avg_harvested_similarity_score": strategy_metrics["avg_harvested_similarity_score"],
                    "eligible_count_before_downstream": strategy_metrics["eligible_count_before_downstream"],
                    "review_count_before_downstream": strategy_metrics["review_count_before_downstream"],
                    "excluded_count_before_downstream": strategy_metrics["excluded_count_before_downstream"],
                    "included_comp_count": replay_result.get("included_comp_count"),
                    "review_heavy_count": replay_result.get("excluded_review_heavy_count"),
                    "likely_exclude_count": replay_result.get("excluded_likely_exclude_count"),
                    "final_value_status": replay_result.get("final_value_status"),
                    "requested_reduction_amount": replay_result.get("requested_reduction_amount"),
                    "requested_reduction_pct": replay_result.get("requested_reduction_pct"),
                    "median_adjusted_value": (
                        (replay_result.get("final_value_detail_json") or {})
                        .get("stability_metrics", {})
                        .get("median_all")
                    ),
                    "elapsed_total_s": replay_result.get("elapsed_total_s"),
                }
            analysis["subjects"].append(subject_result)
    finally:
        connection.close()

    analysis["summary"] = _build_summary(analysis["subjects"])
    _write_outputs(output_prefix=output_prefix, analysis=analysis)
    print(json.dumps(
        {
            "json": f"{output_prefix}.json",
            "csv": f"{output_prefix}.csv",
            "md": f"{output_prefix}.md",
            "summary": analysis["summary"],
        },
        indent=2,
    ))


def _score_universe(
    *,
    service: UnequalRollNoPersistReplayService,
    subject_snapshot: dict[str, Any],
    universe_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    scored: dict[str, dict[str, Any]] = {}
    for row in universe_rows:
        eligibility_status, _, eligibility_detail_json = (
            service._discovery_service._evaluate_candidate_eligibility(
                subject_snapshot=subject_snapshot,
                row=row,
                discovery_tier=DISCOVERY_TIER_SAME_NEIGHBORHOOD,
                valuation_bathroom_features_json=None,
            )
        )
        similarity_score = compute_similarity_score(
            subject_snapshot=subject_snapshot,
            row=row,
            discovery_tier=DISCOVERY_TIER_SAME_NEIGHBORHOOD,
            eligibility_status=eligibility_status,
            eligibility_detail_json=eligibility_detail_json,
            valuation_bathroom_features_json=None,
        )
        account = str(row.get("account_number") or "")
        scored[account] = {
            "eligibility_status": eligibility_status,
            "raw_similarity_score": similarity_score.raw_similarity_score,
            "cheap_similarity_score": cheap_same_neighborhood_similarity_score(
                subject_snapshot=subject_snapshot,
                row=row,
            ),
        }
    return scored


def _strategy_candidate_metrics(
    *,
    universe_metrics: dict[str, dict[str, Any]],
    harvested_accounts: list[str],
) -> dict[str, Any]:
    harvested = [universe_metrics[account] for account in harvested_accounts if account in universe_metrics]
    if not harvested:
        return {
            "avg_harvested_similarity_score": None,
            "eligible_count_before_downstream": 0,
            "review_count_before_downstream": 0,
            "excluded_count_before_downstream": 0,
        }
    return {
        "avg_harvested_similarity_score": round(
            mean(row["raw_similarity_score"] for row in harvested), 4
        ),
        "eligible_count_before_downstream": sum(
            1 for row in harvested if row["eligibility_status"] == "eligible"
        ),
        "review_count_before_downstream": sum(
            1 for row in harvested if row["eligibility_status"] == "review"
        ),
        "excluded_count_before_downstream": sum(
            1 for row in harvested if row["eligibility_status"] == "excluded"
        ),
    }


def _run_replay(
    *,
    service: UnequalRollNoPersistReplayService,
    connection: Any,
    request: UnequalRollReplayRequest,
    strategy: str,
    statement_timeout: str,
    max_parallel_workers_per_gather: int,
) -> dict[str, Any]:
    connection.execute("BEGIN READ ONLY")
    try:
        with connection.cursor() as cursor:
            result = service.replay_subject(
                cursor,
                request=request,
                statement_timeout=statement_timeout,
                max_parallel_workers_per_gather=max_parallel_workers_per_gather,
                same_neighborhood_harvest_strategy=strategy,
                include_discovery_debug=True,
            )
        connection.rollback()
        return result
    except Exception:
        connection.rollback()
        raise


def _build_summary(subjects: list[dict[str, Any]]) -> dict[str, Any]:
    strategy_rows: dict[str, list[dict[str, Any]]] = {strategy: [] for strategy in STRATEGIES}
    for subject in subjects:
        for strategy, payload in subject.get("strategies", {}).items():
            strategy_rows[strategy].append(payload)
    summary: dict[str, Any] = {}
    for strategy, rows in strategy_rows.items():
        included_counts = [
            row["included_comp_count"] for row in rows if row.get("included_comp_count") is not None
        ]
        runtimes = [row["elapsed_total_s"] for row in rows if row.get("elapsed_total_s") is not None]
        summary[strategy] = {
            "avg_included_comp_count": round(mean(included_counts), 2)
            if included_counts
            else None,
            "avg_runtime_s": round(mean(runtimes), 4) if runtimes else None,
        }
    return summary


def _write_outputs(*, output_prefix: Path, analysis: dict[str, Any]) -> None:
    json_path = Path(f"{output_prefix}.json")
    csv_path = Path(f"{output_prefix}.csv")
    md_path = Path(f"{output_prefix}.md")
    json_path.write_text(json.dumps(analysis, indent=2))

    flat_rows = []
    for subject in analysis["subjects"]:
        base = {
            "county": subject.get("county"),
            "account": subject.get("account"),
            "neighborhood_code": subject.get("neighborhood_code"),
            "total_same_neighborhood_candidates": subject.get("total_same_neighborhood_candidates"),
        }
        for strategy, payload in subject.get("strategies", {}).items():
            row = dict(base)
            row["strategy"] = strategy
            row.update(payload)
            flat_rows.append(row)
    if flat_rows:
        with csv_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(flat_rows[0].keys()))
            writer.writeheader()
            writer.writerows(flat_rows)

    lines = [
        "# Smart Harvest Prototype",
        "",
        "## Summary",
        "",
    ]
    for strategy, payload in analysis.get("summary", {}).items():
        lines.append(f"- {strategy}: {payload}")
    lines.extend(["", "## Subjects", ""])
    for subject in analysis["subjects"]:
        lines.append(
            f"- {subject.get('county')} {subject.get('account')} "
            f"neighborhood={subject.get('neighborhood_code')} "
            f"same_neighborhood_total={subject.get('total_same_neighborhood_candidates')}"
        )
        for strategy, payload in subject.get("strategies", {}).items():
            lines.append(
                f"  - {strategy}: harvested={payload['harvested_before_scoring']} "
                f"overlap={payload['overlap_with_current']} "
                f"replacements={payload['replacement_count_vs_current']} "
                f"included={payload['included_comp_count']} "
                f"reduction={payload['requested_reduction_amount']} "
                f"runtime_s={payload['elapsed_total_s']}"
            )
    md_path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
