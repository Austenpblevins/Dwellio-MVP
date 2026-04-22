from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from app.core.config import get_settings
from infra.scripts.report_instant_quote_v5_stage7_flagged_savings_rollout import build_payload

DEFAULT_COUNTIES = ("harris", "fort_bend")
DEFAULT_STAGE5_ARTIFACT = Path(
    "docs/architecture/instant-quote-v5-stage5-shadow-comparison-20260422.json"
)
DEFAULT_STAGE6_ARTIFACT = Path(
    "docs/architecture/instant-quote-v5-stage6-product-state-rollout-20260422.json"
)
DEFAULT_STAGE7_ARTIFACT = Path(
    "docs/architecture/instant-quote-v5-stage7-flagged-savings-rollout-20260422.json"
)
DEFAULT_CURRENT_ROLLOUT_COUNTIES = "fort_bend"
DEFAULT_CURRENT_ROLLOUT_STATES = "total_exemption_low_cash"
DEFAULT_CANDIDATE_ROLLOUT_COUNTIES = "harris,fort_bend"
DEFAULT_CANDIDATE_ROLLOUT_STATES = (
    "total_exemption_low_cash,"
    "near_total_exemption_low_cash,"
    "high_opportunity_low_cash,"
    "opportunity_only_tax_profile_incomplete,"
    "school_limited_non_school_possible,"
    "shadow_quoteable_public_refined_review"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Review Stage 8 rollout calibration by comparing the current evidence-backed "
            "Stage 7 rollout scope to broader candidate cohort expansions."
        )
    )
    parser.add_argument("--county-ids", nargs="+", default=list(DEFAULT_COUNTIES))
    parser.add_argument("--tax-year", required=True, type=int)
    parser.add_argument("--stage5-artifact", default=str(DEFAULT_STAGE5_ARTIFACT))
    parser.add_argument("--stage6-artifact", default=str(DEFAULT_STAGE6_ARTIFACT))
    parser.add_argument("--stage7-artifact", default=str(DEFAULT_STAGE7_ARTIFACT))
    parser.add_argument(
        "--current-rollout-county-ids",
        default=DEFAULT_CURRENT_ROLLOUT_COUNTIES,
    )
    parser.add_argument(
        "--current-rollout-states",
        default=DEFAULT_CURRENT_ROLLOUT_STATES,
    )
    parser.add_argument(
        "--candidate-rollout-county-ids",
        default=DEFAULT_CANDIDATE_ROLLOUT_COUNTIES,
    )
    parser.add_argument(
        "--candidate-rollout-states",
        default=DEFAULT_CANDIDATE_ROLLOUT_STATES,
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DWELLIO_DATABASE_URL before running the Stage 8 calibration review.",
    )
    return parser


def build_stage8_payload(
    *,
    county_ids: list[str],
    tax_year: int,
    stage5_artifact: Path,
    stage6_artifact: Path,
    stage7_artifact: Path,
    current_rollout_county_ids: str,
    current_rollout_states: str,
    candidate_rollout_county_ids: str,
    candidate_rollout_states: str,
) -> dict[str, object]:
    stage5 = json.loads(stage5_artifact.read_text())
    stage6 = json.loads(stage6_artifact.read_text())
    stage7 = json.loads(stage7_artifact.read_text())

    current_payload = build_payload(
        county_ids=county_ids,
        tax_year=tax_year,
        stage6_artifact=stage6_artifact,
        rollout_county_ids=current_rollout_county_ids,
        rollout_states=current_rollout_states,
    )
    candidate_payload = build_payload(
        county_ids=county_ids,
        tax_year=tax_year,
        stage6_artifact=stage6_artifact,
        rollout_county_ids=candidate_rollout_county_ids,
        rollout_states=candidate_rollout_states,
    )

    return {
        "reviewed_artifacts": {
            "stage5": str(stage5_artifact),
            "stage6": str(stage6_artifact),
            "stage7": str(stage7_artifact),
        },
        "current_public_savings_model": "reduction_estimate_times_effective_tax_rate",
        "current_calibrated_rollout": {
            "configuration": current_payload["rollout_configuration"],
            "summary": _summarize_rollout_payload(current_payload),
        },
        "candidate_expansion_review": {
            "configuration": candidate_payload["rollout_configuration"],
            "summary": _summarize_rollout_payload(candidate_payload),
        },
        "stage6_target_shadow_state_counts": _stage6_counts(stage6),
        "stage5_shadow_signals": _stage5_shadow_signals(stage5),
        "stage7_material_translation_rows": _stage7_material_translation_rows(stage7),
        "calibration_decisions": _build_calibration_decisions(
            stage5=stage5,
            stage6=stage6,
            current_payload=current_payload,
            candidate_payload=candidate_payload,
        ),
    }


def _summarize_rollout_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    county_summaries: list[dict[str, object]] = []
    for county in payload.get("counties", []):
        cohort_summaries: dict[str, dict[str, object]] = {}
        for cohort_name, rows in county.get("cohorts", {}).items():
            changed_rows = [row for row in rows if row.get("translation_changed_public_estimate")]
            applied_rows = [
                row
                for row in rows
                if row.get("flagged_path", {}).get("savings_translation_applied_flag")
            ]
            cohort_summaries[cohort_name] = {
                "row_count": len(rows),
                "applied_count": len(applied_rows),
                "changed_count": len(changed_rows),
                "changed_accounts": [
                    {
                        "account_number": row["account_number"],
                        "current_midpoint": row["current_path"]["savings_midpoint_display"],
                        "flagged_midpoint": row["flagged_path"]["savings_midpoint_display"],
                        "savings_translation_reason_code": row["flagged_path"][
                            "savings_translation_reason_code"
                        ],
                    }
                    for row in changed_rows
                ],
                "applied_accounts": [
                    {
                        "account_number": row["account_number"],
                        "public_rollout_state": row["flagged_path"]["public_rollout_state"],
                        "savings_translation_mode": row["flagged_path"][
                            "savings_translation_mode"
                        ],
                        "savings_translation_reason_code": row["flagged_path"][
                            "savings_translation_reason_code"
                        ],
                    }
                    for row in applied_rows
                ],
            }
        county_summaries.append(
            {
                "county_id": county["county_id"],
                "tax_year": county["tax_year"],
                "cohorts": cohort_summaries,
            }
        )
    return county_summaries


def _stage6_counts(stage6: dict[str, object]) -> list[dict[str, object]]:
    return [
        {
            "county_id": county["county_id"],
            "tax_year": county["tax_year"],
            "target_shadow_state_counts": county["target_shadow_state_counts"],
        }
        for county in stage6.get("counties", [])
    ]


def _stage5_shadow_signals(stage5: dict[str, object]) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []
    for county in stage5.get("counties", []):
        summary = county.get("comparison_summary", {})
        signals.append(
            {
                "county_id": county["county_id"],
                "tax_year": county["tax_year"],
                "public_unsupported_shadow_quoteable_count": summary.get(
                    "public_unsupported_shadow_quoteable_count"
                ),
                "high_opportunity_low_cash_count": summary.get("high_opportunity_low_cash_count"),
                "max_abs_shadow_delta_raw": summary.get("max_abs_shadow_delta_raw"),
                "mean_shadow_delta_raw": summary.get("mean_shadow_delta_raw"),
            }
        )
    return signals


def _stage7_material_translation_rows(stage7: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for county in stage7.get("counties", []):
        for cohort_name, cohort_rows in county.get("cohorts", {}).items():
            for row in cohort_rows:
                if not row.get("translation_changed_public_estimate"):
                    continue
                rows.append(
                    {
                        "county_id": county["county_id"],
                        "cohort": cohort_name,
                        "account_number": row["account_number"],
                        "current_midpoint": row["current_path"]["savings_midpoint_display"],
                        "flagged_midpoint": row["flagged_path"]["savings_midpoint_display"],
                    }
                )
    return rows


def _build_calibration_decisions(
    *,
    stage5: dict[str, object],
    stage6: dict[str, object],
    current_payload: dict[str, object],
    candidate_payload: dict[str, object],
) -> dict[str, object]:
    stage6_counts = {
        county["county_id"]: county.get("target_shadow_state_counts", {})
        for county in stage6.get("counties", [])
    }
    candidate_summary = {
        county["county_id"]: county.get("cohorts", {})
        for county in _summarize_rollout_payload(candidate_payload)
    }
    current_summary = {
        county["county_id"]: county.get("cohorts", {})
        for county in _summarize_rollout_payload(current_payload)
    }
    fort_bend_shadow_signal = next(
        (
            county.get("comparison_summary", {})
            for county in stage5.get("counties", [])
            if county.get("county_id") == "fort_bend"
        ),
        {},
    )

    return {
        "keep_enabled_default_rollout_states": ["total_exemption_low_cash"],
        "near_total_exemption_low_cash": {
            "decision": "keep_implemented_but_not_default_enabled",
            "reason": "no isolated 2026 live rows were observed in Harris or Fort Bend",
            "observed_rows": {
                "harris": stage6_counts.get("harris", {}).get("near_total_exemption_low_cash", 0),
                "fort_bend": stage6_counts.get("fort_bend", {}).get(
                    "near_total_exemption_low_cash", 0
                ),
            },
        },
        "additional_fort_bend_expansion": {
            "decision": "do_not_expand_beyond_total_exemption_low_cash",
            "reason": (
                "school-limited and refined-review cohorts remain unsupported, and "
                "high_opportunity_low_cash showed no public estimate improvement in candidate runs."
            ),
            "high_opportunity_candidate_applied_count": candidate_summary.get(
                "fort_bend", {}
            ).get("high_opportunity_low_cash", {}).get("applied_count", 0),
            "high_opportunity_candidate_changed_count": candidate_summary.get(
                "fort_bend", {}
            ).get("high_opportunity_low_cash", {}).get("changed_count", 0),
            "refined_review_shadow_quoteable_count": fort_bend_shadow_signal.get(
                "public_unsupported_shadow_quoteable_count"
            ),
        },
        "harris_expansion": {
            "decision": "remain_constrained",
            "reason": (
                "candidate high_opportunity rollout would apply a fallback-driven translated path "
                "without producing better public estimates, and opportunity-only rows remain too incomplete."
            ),
            "high_opportunity_candidate_applied_count": candidate_summary.get(
                "harris", {}
            ).get("high_opportunity_low_cash", {}).get("applied_count", 0),
            "high_opportunity_candidate_changed_count": candidate_summary.get(
                "harris", {}
            ).get("high_opportunity_low_cash", {}).get("changed_count", 0),
        },
        "school_limited_non_school_possible": {
            "decision": "still_too_risky_for_public_flagged_rollout",
            "reason": "rows remain unsupported with tax_limitation_uncertain and did not become safely quoteable.",
        },
        "shadow_quoteable_public_refined_review": {
            "decision": "still_too_risky_for_public_flagged_rollout",
            "reason": "manual-review safeguards remain appropriate even when the shadow path shows analytical signal.",
        },
        "stage8_code_calibration": [
            "default rollout states reduced to total_exemption_low_cash",
            "translation now requires stage8-calibrated state-to-limiting-code alignment",
            "translation now refuses to raise public savings above the current public estimate",
        ],
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.database_url:
        os.environ["DWELLIO_DATABASE_URL"] = args.database_url
    get_settings.cache_clear()
    payload = build_stage8_payload(
        county_ids=list(args.county_ids),
        tax_year=args.tax_year,
        stage5_artifact=Path(args.stage5_artifact),
        stage6_artifact=Path(args.stage6_artifact),
        stage7_artifact=Path(args.stage7_artifact),
        current_rollout_county_ids=args.current_rollout_county_ids,
        current_rollout_states=args.current_rollout_states,
        candidate_rollout_county_ids=args.candidate_rollout_county_ids,
        candidate_rollout_states=args.candidate_rollout_states,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
