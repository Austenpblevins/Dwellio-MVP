from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path("/tmp/stage19_weekly_quote_quality_monitor_artifacts")
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


def run_monitor(
    *,
    output_dir: Path,
    county_ids: str,
    tax_year: int,
    python_executable: str = "python3",
    repo_root: Path | None = None,
    subprocess_run: Any = subprocess.run,
    now_fn: Any = lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, Any], int]:
    resolved_repo_root = repo_root or Path(__file__).resolve().parents[2]
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_monitor_command(
        output_dir=output_dir,
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
    stdout_path = output_dir / "monitor_stdout.json"
    stderr_path = output_dir / "monitor_stderr.log"
    stdout_path.write_text(result.stdout or "")
    stderr_path.write_text(result.stderr or "")
    artifact_paths = {
        name: str(output_dir / filename)
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
    (output_dir / "run_state.json").write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    return state, result.returncode


def main() -> None:
    args = build_parser().parse_args()
    state, return_code = run_monitor(
        output_dir=args.output_dir,
        county_ids=args.county_ids,
        tax_year=args.tax_year,
        python_executable=args.python_executable,
    )
    print(json.dumps(state, indent=2, sort_keys=True))
    if args.hard_fail and return_code != 0:
        raise SystemExit(return_code)


if __name__ == "__main__":
    main()
