from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path("artifacts/quote_quality_monitor/latest")
DEFAULT_TMP_OUTPUT_DIR = Path("/tmp/stage19_weekly_quote_quality_monitor_artifacts")
DEFAULT_ALERT_WEBHOOK_ENV_VAR = "DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL"
ARTIFACT_FILENAMES = {
    "json": "stage19_weekly_quote_quality_monitor.json",
    "markdown": "stage19_weekly_quote_quality_monitor.md",
    "zero_watchlist": "stage19_refresh_watchlist_zero_savings.csv",
    "outlier_watchlist": "stage19_refresh_watchlist_top_outliers.csv",
    "watchlist_summary": "stage19_refresh_watchlist_summary.md",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the weekly quote-quality monitor and persist artifacts."
    )
    parser.add_argument("--county-ids", default="harris,fort_bend")
    parser.add_argument("--tax-year", type=int, default=2026)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--tmp-output-dir",
        type=Path,
        default=None,
        help="Optional transient mirror path for operator convenience.",
    )
    parser.add_argument(
        "--alert-webhook-env-var",
        default=DEFAULT_ALERT_WEBHOOK_ENV_VAR,
        help="Environment variable name reserved for future alert webhook integration.",
    )
    parser.add_argument("--python-executable", default="python3")
    parser.add_argument(
        "--hard-fail",
        action="store_true",
        help="Return the monitor process exit code instead of writing a soft-fail run state.",
    )
    return parser


def build_monitor_command(
    *,
    output_dir: Path,
    county_ids: str,
    tax_year: int,
    python_executable: str = "python3",
) -> list[str]:
    return [
        python_executable,
        "infra/scripts/report_quote_quality_monitor.py",
        "--county-ids",
        county_ids,
        "--tax-year",
        str(tax_year),
        "--json-output",
        str(output_dir / ARTIFACT_FILENAMES["json"]),
        "--markdown-output",
        str(output_dir / ARTIFACT_FILENAMES["markdown"]),
        "--zero-watchlist-output",
        str(output_dir / ARTIFACT_FILENAMES["zero_watchlist"]),
        "--outlier-watchlist-output",
        str(output_dir / ARTIFACT_FILENAMES["outlier_watchlist"]),
        "--watchlist-summary-output",
        str(output_dir / ARTIFACT_FILENAMES["watchlist_summary"]),
    ]


def resolve_output_dir(*, output_dir: Path, repo_root: Path) -> Path:
    return output_dir if output_dir.is_absolute() else repo_root / output_dir


def build_alert_payload(
    *,
    monitor_payload: dict[str, Any] | None,
    run_state: dict[str, Any],
) -> dict[str, Any]:
    alerts: list[dict[str, Any]] = []
    if int(run_state.get("exit_code") or 0) != 0:
        alerts.append(
            {
                "code": "quote_quality_monitor_job_failed",
                "severity": "critical",
                "message": "Weekly quote-quality monitor command failed.",
                "details": {
                    "exit_code": run_state.get("exit_code"),
                    "stderr_path": run_state.get("stderr_path"),
                },
            }
        )

    if monitor_payload is not None:
        for county in monitor_payload.get("counties", []):
            county_id = county.get("county_id")
            denominator_shift = county.get("denominator_shift") or {}
            for metric_name in ("all_sfr_flagged", "strict_sfr_eligible"):
                metric = denominator_shift.get(metric_name) or {}
                if metric.get("triggered"):
                    alerts.append(
                        {
                            "code": "quote_quality_denominator_shift_alert",
                            "severity": "warning",
                            "county_id": county_id,
                            "message": f"{metric_name} denominator changed beyond threshold.",
                            "details": metric,
                        }
                    )
            validation_alert = denominator_shift.get("validation_alert") or {}
            if validation_alert.get("triggered"):
                alerts.append(
                    {
                        "code": "quote_quality_validation_denominator_shift_alert",
                        "severity": "warning",
                        "county_id": county_id,
                        "message": "Validation report denominator-shift alert triggered.",
                        "details": validation_alert,
                    }
                )

            leakage = county.get("excluded_class_leakage") or {}
            if int(leakage.get("strong_signal_excluded") or 0) > 0:
                alerts.append(
                    {
                        "code": "quote_quality_excluded_class_leakage",
                        "severity": "warning",
                        "county_id": county_id,
                        "message": "Excluded SFR-ineligible classes have strong residential signals.",
                        "details": leakage,
                    }
                )

            freshness = county.get("validation_freshness") or {}
            if freshness.get("warning_code"):
                alerts.append(
                    {
                        "code": freshness["warning_code"],
                        "severity": "warning",
                        "county_id": county_id,
                        "message": "Latest validation freshness requires operator review.",
                        "details": freshness,
                    }
                )

    return {
        "generated_at": run_state.get("finished_at"),
        "county_ids": run_state.get("county_ids"),
        "tax_year": run_state.get("tax_year"),
        "should_notify": bool(alerts),
        "alerts": alerts,
    }


def build_alert_delivery(
    *,
    alert_payload: dict[str, Any],
    webhook_env_var: str,
) -> dict[str, Any]:
    if not alert_payload.get("should_notify"):
        return {
            "status": "noop_no_alerts",
            "webhook_env_var": webhook_env_var,
            "message": "No alertable quote-quality monitor conditions were found.",
        }
    if os.getenv(webhook_env_var):
        return {
            "status": "noop_webhook_configured_dry_run",
            "webhook_env_var": webhook_env_var,
            "message": (
                "Alert payload assembled; webhook delivery is intentionally a stub until "
                "the team wires this env var to its chosen alert provider."
            ),
        }
    return {
        "status": "noop_notifier_not_configured",
        "webhook_env_var": webhook_env_var,
        "message": "Alert payload assembled; configure this env var to connect Slack/email/webhook delivery.",
    }


def build_manifest(
    *,
    monitor_payload: dict[str, Any] | None,
    run_state: dict[str, Any],
    durable_output_dir: Path,
    tmp_output_dir: Path | None,
) -> dict[str, Any]:
    durable_paths = {
        name: str(durable_output_dir / filename)
        for name, filename in ARTIFACT_FILENAMES.items()
    }
    durable_paths.update(
        {
            "run_state": str(durable_output_dir / "run_state.json"),
            "alert_payload": str(durable_output_dir / "alert_payload.json"),
            "stdout": str(durable_output_dir / "monitor_stdout.json"),
            "stderr": str(durable_output_dir / "monitor_stderr.log"),
            "manifest": str(durable_output_dir / "manifest.json"),
        }
    )
    tmp_paths: dict[str, str] = {}
    if tmp_output_dir is not None:
        tmp_paths = {
            artifact_name: str(tmp_output_dir / Path(path).name)
            for artifact_name, path in durable_paths.items()
        }
    return {
        "run_timestamp": run_state.get("finished_at"),
        "county_ids": run_state.get("county_ids"),
        "tax_year": run_state.get("tax_year"),
        "status": run_state.get("status"),
        "exit_code": run_state.get("exit_code"),
        "latest_refresh_run_ids": {
            county["county_id"]: county.get("latest_refresh_run_id")
            for county in (monitor_payload or {}).get("counties", [])
        },
        "latest_validation_timestamps": {
            county["county_id"]: county.get("latest_validated_at")
            for county in (monitor_payload or {}).get("counties", [])
        },
        "durable_artifact_paths": durable_paths,
        "tmp_artifact_paths": tmp_paths,
    }


def _load_monitor_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _mirror_to_tmp(*, durable_output_dir: Path, tmp_output_dir: Path) -> None:
    tmp_output_dir.mkdir(parents=True, exist_ok=True)
    for path in durable_output_dir.iterdir():
        if path.is_file():
            shutil.copy2(path, tmp_output_dir / path.name)


def run_monitor(
    *,
    output_dir: Path,
    county_ids: str,
    tax_year: int,
    tmp_output_dir: Path | None = None,
    alert_webhook_env_var: str = DEFAULT_ALERT_WEBHOOK_ENV_VAR,
    python_executable: str = "python3",
    repo_root: Path | None = None,
    subprocess_run: Any = subprocess.run,
    now_fn: Any = lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, Any], int]:
    resolved_repo_root = repo_root or Path(__file__).resolve().parents[2]
    resolved_output_dir = resolve_output_dir(output_dir=output_dir, repo_root=resolved_repo_root)
    resolved_tmp_output_dir = (
        resolve_output_dir(output_dir=tmp_output_dir, repo_root=resolved_repo_root)
        if tmp_output_dir is not None
        else None
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    command = build_monitor_command(
        output_dir=resolved_output_dir,
        county_ids=county_ids,
        tax_year=tax_year,
        python_executable=python_executable,
    )
    started_at = now_fn().isoformat()
    result = subprocess_run(
        command,
        cwd=resolved_repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    finished_at = now_fn().isoformat()
    stdout_path = resolved_output_dir / "monitor_stdout.json"
    stderr_path = resolved_output_dir / "monitor_stderr.log"
    stdout_path.write_text(result.stdout or "")
    stderr_path.write_text(result.stderr or "")
    artifact_paths = {
        name: str(resolved_output_dir / filename)
        for name, filename in ARTIFACT_FILENAMES.items()
    }
    state = {
        "status": "completed" if result.returncode == 0 else "failed_soft",
        "exit_code": result.returncode,
        "started_at": started_at,
        "finished_at": finished_at,
        "county_ids": county_ids,
        "tax_year": tax_year,
        "command": command,
        "artifact_paths": artifact_paths,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    monitor_payload = _load_monitor_payload(resolved_output_dir / ARTIFACT_FILENAMES["json"])
    alert_payload = build_alert_payload(monitor_payload=monitor_payload, run_state=state)
    alert_delivery = build_alert_delivery(
        alert_payload=alert_payload,
        webhook_env_var=alert_webhook_env_var,
    )
    state["alert_payload_path"] = str(resolved_output_dir / "alert_payload.json")
    state["alert_delivery"] = alert_delivery
    manifest = build_manifest(
        monitor_payload=monitor_payload,
        run_state=state,
        durable_output_dir=resolved_output_dir,
        tmp_output_dir=resolved_tmp_output_dir,
    )
    state["manifest_path"] = str(resolved_output_dir / "manifest.json")
    state["tmp_output_dir"] = str(resolved_tmp_output_dir) if resolved_tmp_output_dir else None
    (resolved_output_dir / "alert_payload.json").write_text(
        json.dumps(alert_payload, indent=2, sort_keys=True) + "\n"
    )
    (resolved_output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    (resolved_output_dir / "run_state.json").write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n"
    )
    if resolved_tmp_output_dir is not None:
        _mirror_to_tmp(
            durable_output_dir=resolved_output_dir,
            tmp_output_dir=resolved_tmp_output_dir,
        )
    return state, result.returncode


def main() -> None:
    args = build_parser().parse_args()
    state, return_code = run_monitor(
        output_dir=args.output_dir,
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        tmp_output_dir=args.tmp_output_dir,
        alert_webhook_env_var=args.alert_webhook_env_var,
        python_executable=args.python_executable,
    )
    print(json.dumps(state, indent=2, sort_keys=True))
    if args.hard_fail and return_code != 0:
        raise SystemExit(return_code)


if __name__ == "__main__":
    main()
