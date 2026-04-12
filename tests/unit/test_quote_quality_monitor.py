from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from infra.scripts.report_quote_quality_monitor import (
    build_payload,
    denominator_shift_status,
    divergence_drill_result,
    normalize_county_ids,
    select_watchlist_rows,
    validation_freshness_status,
)
from infra.scripts.run_weekly_quote_quality_monitor import (
    build_alert_delivery,
    build_alert_payload,
    build_monitor_command,
    run_monitor,
)


class StubCursor:
    def __init__(self) -> None:
        self._rows: list[dict[str, object]] = []

    def __enter__(self) -> StubCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "FROM instant_quote_refresh_runs" in sql and "validation_report IS NOT NULL" in sql:
            county_id = params[0] if params is not None else "harris"
            self._rows = [
                {
                    "instant_quote_refresh_run_id": "run-current",
                    "refresh_status": "completed",
                    "refresh_started_at": datetime(2026, 4, 11, tzinfo=timezone.utc),
                    "refresh_finished_at": datetime(2026, 4, 11, 1, tzinfo=timezone.utc),
                    "validated_at": datetime(2026, 4, 11, 2, tzinfo=timezone.utc),
                    "validation_report": {
                        "total_count_all_sfr_flagged": 105,
                        "support_count_all_sfr_flagged": 100,
                        "support_rate_all_sfr_flagged": 100 / 105,
                        "total_count_strict_sfr_eligible": 80,
                        "support_count_strict_sfr_eligible": 78,
                        "support_rate_strict_sfr_eligible": 78 / 80,
                        "denominator_shift_alert": {
                            "status": "within_threshold",
                            "triggered": False,
                        },
                        "monitored_zero_savings_supported_quote_count": 10,
                        "monitored_zero_savings_quote_count": 4,
                        "high_value_support_rate": 0.9,
                        "special_district_heavy_support_rate": 0.95,
                    },
                },
                {
                    "instant_quote_refresh_run_id": f"{county_id}-run-prior",
                    "refresh_status": "completed",
                    "refresh_started_at": datetime(2026, 4, 4, tzinfo=timezone.utc),
                    "refresh_finished_at": datetime(2026, 4, 4, 1, tzinfo=timezone.utc),
                    "validated_at": datetime(2026, 4, 4, 2, tzinfo=timezone.utc),
                    "validation_report": {
                        "total_count_all_sfr_flagged": 100,
                        "total_count_strict_sfr_eligible": 79,
                    },
                },
            ]
        elif "WITH excluded AS" in sql:
            self._rows = [
                {
                    "total_excluded": 12,
                    "strong_signal_excluded": 0,
                    "leakage_ratio": 0,
                    "top_class_codes": [
                        {
                            "property_class_code": "F1",
                            "excluded_count": 10,
                            "strong_signal_count": 0,
                        }
                    ],
                }
            ]
        elif "WITH latest_runs AS" in sql:
            self._rows = [
                {
                    "county_id": "harris",
                    "account_number": "B",
                    "account_hash": "002",
                    "supported": True,
                    "projected_savings_display": 0,
                },
                {
                    "county_id": "harris",
                    "account_number": "A",
                    "account_hash": "001",
                    "supported": True,
                    "projected_savings_display": 5000,
                },
            ]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class StubConnection:
    def __enter__(self) -> StubConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> StubCursor:
        return StubCursor()


def test_denominator_shift_status_threshold_behavior() -> None:
    assert denominator_shift_status(current=105, prior=100, threshold_pct=0.05)[
        "triggered"
    ] is False
    assert denominator_shift_status(current=106, prior=100, threshold_pct=0.05)[
        "triggered"
    ] is True
    assert denominator_shift_status(current=100, prior=None, threshold_pct=0.05)[
        "status"
    ] == "no_prior_run"


def test_normalize_county_ids_accepts_comma_or_space_separated_values() -> None:
    assert normalize_county_ids(["harris,fort_bend"]) == ["harris", "fort_bend"]
    assert normalize_county_ids(["harris", "fort_bend"]) == ["harris", "fort_bend"]


def test_validation_freshness_status_reports_fresh_stale_and_missing() -> None:
    now = datetime(2026, 4, 11, 12, tzinfo=timezone.utc)

    fresh = validation_freshness_status(
        latest_validated_at=datetime(2026, 4, 11, 1, tzinfo=timezone.utc),
        now=now,
        threshold_hours=24,
    )
    stale = validation_freshness_status(
        latest_validated_at=datetime(2026, 4, 10, 1, tzinfo=timezone.utc),
        now=now,
        threshold_hours=24,
    )
    missing = validation_freshness_status(
        latest_validated_at=None,
        now=now,
        threshold_hours=24,
    )

    assert fresh["status"] == "fresh"
    assert fresh["warning_code"] is None
    assert stale["status"] == "stale"
    assert stale["warning_code"] == "validation_stale"
    assert missing["status"] == "missing"
    assert missing["warning_code"] == "validation_missing"


def test_weekly_monitor_payload_contains_denominator_and_leakage_trends() -> None:
    payload = build_payload(
        county_ids=["harris"],
        tax_year=2026,
        recent_run_limit=2,
        threshold_pct=0.05,
        now_fn=lambda: datetime(2026, 4, 11, 12, tzinfo=timezone.utc),
        connection_factory=lambda: StubConnection(),
    )

    county = payload["counties"][0]
    assert county["denominator_shift"]["total_count_all_sfr_flagged"] == 105
    assert county["denominator_shift"]["prior_total_count_all_sfr_flagged"] == 100
    assert county["excluded_class_leakage"]["strong_signal_excluded"] == 0
    assert county["zero_savings"]["monitored_zero_savings_quote_share"] == 0.4
    assert county["validation_freshness"]["status"] == "fresh"
    assert payload["combined"]["strong_signal_leakage_count"] == 0
    assert payload["combined"]["validation_freshness_warning_count"] == 0


def test_watchlist_selection_is_deterministic_by_kind() -> None:
    rows = [
        {
            "county_id": "harris",
            "account_number": "B",
            "account_hash": "002",
            "supported": True,
            "projected_savings_display": 0,
        },
        {
            "county_id": "harris",
            "account_number": "A",
            "account_hash": "001",
            "supported": True,
            "projected_savings_display": 0,
        },
        {
            "county_id": "harris",
            "account_number": "C",
            "account_hash": "003",
            "supported": True,
            "projected_savings_display": 5000,
            "assessment_basis_value": 700000,
        },
    ]

    zero_rows = select_watchlist_rows(rows, kind="zero_savings", limit_per_county=2)
    outlier_rows = select_watchlist_rows(rows, kind="top_outliers", limit_per_county=1)

    assert [row["account_number"] for row in zero_rows] == ["A", "B"]
    assert [row["account_number"] for row in outlier_rows] == ["C"]


def test_non_prod_divergence_drill_proves_kpi_paths_can_diverge() -> None:
    result = divergence_drill_result(
        all_sfr_total=100,
        all_sfr_supported=75,
        strict_sfr_total=80,
        strict_sfr_supported=75,
    )

    assert result["environment"] == "non_prod_only"
    assert result["diverged"] is True
    assert result["pass"] is True


def test_weekly_runner_builds_artifact_command_and_soft_fails(tmp_path) -> None:
    command = build_monitor_command(
        output_dir=tmp_path,
        county_ids="harris,fort_bend",
        tax_year=2026,
    )

    assert "--county-ids" in command
    assert "harris,fort_bend" in command
    assert str(tmp_path / "stage19_weekly_quote_quality_monitor.json") in command

    def fake_run(*args, **kwargs) -> SimpleNamespace:
        return SimpleNamespace(returncode=2, stdout="", stderr="db unavailable")

    state, return_code = run_monitor(
        output_dir=tmp_path,
        county_ids="harris,fort_bend",
        tax_year=2026,
        repo_root=tmp_path,
        subprocess_run=fake_run,
        now_fn=lambda: datetime(2026, 4, 11, 12, tzinfo=timezone.utc),
    )

    assert return_code == 2
    assert state["status"] == "failed_soft"
    assert state["exit_code"] == 2
    assert (tmp_path / "run_state.json").exists()
    assert (tmp_path / "monitor_stderr.log").read_text() == "db unavailable"
    assert (tmp_path / "alert_payload.json").exists()
    assert (tmp_path / "manifest.json").exists()


def test_weekly_runner_resolves_durable_and_tmp_artifact_paths(tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    durable_output = "artifacts/quote_quality_monitor/latest"
    tmp_output = tmp_path / "tmp-monitor"

    def fake_run(command, **kwargs) -> SimpleNamespace:
        json_path = command[command.index("--json-output") + 1]
        Path(json_path).write_text(
            """
            {
              "counties": [
                {
                  "county_id": "harris",
                  "latest_refresh_run_id": "refresh-1",
                  "latest_validated_at": "2026-04-11T02:00:00+00:00",
                  "denominator_shift": {
                    "all_sfr_flagged": {"triggered": false},
                    "strict_sfr_eligible": {"triggered": false},
                    "validation_alert": {"triggered": false}
                  },
                  "excluded_class_leakage": {"strong_signal_excluded": 0},
                  "validation_freshness": {"warning_code": null}
                }
              ]
            }
            """
        )
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    state, return_code = run_monitor(
        output_dir=Path(durable_output),
        tmp_output_dir=tmp_output,
        county_ids="harris,fort_bend",
        tax_year=2026,
        repo_root=repo_root,
        subprocess_run=fake_run,
        now_fn=lambda: datetime(2026, 4, 11, 12, tzinfo=timezone.utc),
    )

    durable_dir = repo_root / durable_output
    assert return_code == 0
    assert state["status"] == "completed"
    assert Path(state["manifest_path"]) == durable_dir / "manifest.json"
    assert (durable_dir / "run_state.json").exists()
    assert (tmp_output / "manifest.json").exists()
    assert (tmp_output / "alert_payload.json").exists()


def test_weekly_runner_alert_payload_covers_monitor_alert_conditions() -> None:
    payload = {
        "counties": [
            {
                "county_id": "harris",
                "denominator_shift": {
                    "all_sfr_flagged": {"triggered": True},
                    "strict_sfr_eligible": {"triggered": False},
                    "validation_alert": {"triggered": True},
                },
                "excluded_class_leakage": {"strong_signal_excluded": 3},
                "validation_freshness": {"warning_code": "validation_stale"},
            }
        ]
    }

    alert_payload = build_alert_payload(
        monitor_payload=payload,
        run_state={
            "exit_code": 0,
            "finished_at": "2026-04-11T12:00:00+00:00",
            "county_ids": "harris,fort_bend",
            "tax_year": 2026,
        },
    )
    job_failed = build_alert_payload(
        monitor_payload=None,
        run_state={
            "exit_code": 2,
            "finished_at": "2026-04-11T12:00:00+00:00",
            "county_ids": "harris,fort_bend",
            "tax_year": 2026,
            "stderr_path": "/tmp/monitor_stderr.log",
        },
    )

    codes = {alert["code"] for alert in alert_payload["alerts"]}
    assert alert_payload["should_notify"] is True
    assert "quote_quality_denominator_shift_alert" in codes
    assert "quote_quality_validation_denominator_shift_alert" in codes
    assert "quote_quality_excluded_class_leakage" in codes
    assert "validation_stale" in codes
    assert job_failed["alerts"][0]["code"] == "quote_quality_monitor_job_failed"


def test_alert_delivery_posts_payload_when_webhook_configured() -> None:
    captured: dict[str, object] = {}

    class StubResponse:
        status = 202

        def __enter__(self) -> StubResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def getcode(self) -> int:
            return self.status

        def read(self) -> bytes:
            return b'{"ok":true}'

    def fake_urlopen(request, timeout: float):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return StubResponse()

    delivery = build_alert_delivery(
        alert_payload={"should_notify": True, "alerts": [{"code": "x"}]},
        webhook_env_var="DWELLIO_QUOTE_QUALITY_MONITOR_WEBHOOK_URL",
        webhook_url="https://example.invalid/webhook",
        timeout_seconds=9.0,
        urlopen_fn=fake_urlopen,
    )

    assert captured["url"] == "https://example.invalid/webhook"
    assert captured["timeout"] == 9.0
    assert captured["payload"] == {"should_notify": True, "alerts": [{"code": "x"}]}
    assert delivery["status"] == "delivered_webhook"
    assert delivery["http_status"] == 202


def test_weekly_runner_force_alert_injects_synthetic_alert(tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    class StubResponse:
        status = 200

        def __enter__(self) -> StubResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def getcode(self) -> int:
            return self.status

        def read(self) -> bytes:
            return b"ok"

    def fake_urlopen(request, timeout: float):
        return StubResponse()

    def fake_run(command, **kwargs) -> SimpleNamespace:
        json_path = command[command.index("--json-output") + 1]
        Path(json_path).write_text(
            """
            {
              "counties": [
                {
                  "county_id": "harris",
                  "denominator_shift": {
                    "all_sfr_flagged": {"triggered": false},
                    "strict_sfr_eligible": {"triggered": false},
                    "validation_alert": {"triggered": false}
                  },
                  "excluded_class_leakage": {"strong_signal_excluded": 0},
                  "validation_freshness": {"warning_code": null}
                }
              ]
            }
            """
        )
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    state, return_code = run_monitor(
        output_dir=Path("artifacts/quote_quality_monitor/latest"),
        county_ids="harris,fort_bend",
        tax_year=2026,
        repo_root=repo_root,
        subprocess_run=fake_run,
        urlopen_fn=fake_urlopen,
        force_alert=True,
        now_fn=lambda: datetime(2026, 4, 11, 12, tzinfo=timezone.utc),
    )

    alert_payload = json.loads(
        (repo_root / "artifacts/quote_quality_monitor/latest/alert_payload.json").read_text()
    )
    codes = {alert["code"] for alert in alert_payload["alerts"]}
    assert return_code == 0
    assert state["force_alert"] is True
    assert state["alert_delivery"]["status"] == "noop_notifier_not_configured"
    assert alert_payload["should_notify"] is True
    assert "quote_quality_forced_test_alert" in codes
