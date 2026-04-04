from __future__ import annotations

import sys

from app.jobs import cli
from app.jobs.job_set_tax_rate_adoption_status import run as run_set_tax_rate_adoption_status
from app.services.instant_quote_tax_rate_adoption_status import (
    InstantQuoteTaxRateAdoptionStatusService,
)


class _StatusCursor:
    def __init__(self) -> None:
        self._row: dict[str, object] | None = None
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> _StatusCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))
        if "INSERT INTO instant_quote_tax_rate_adoption_statuses" not in sql:
            raise AssertionError(f"Unexpected SQL: {sql}")
        assert params is not None
        self._row = {
            "county_id": params[0],
            "tax_year": params[1],
            "adoption_status": params[2],
            "adoption_status_reason": params[3],
            "status_source": params[4],
            "source_note": params[5],
        }

    def fetchone(self) -> dict[str, object] | None:
        return self._row


class _StatusConnection:
    def __init__(self, cursor: _StatusCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def __enter__(self) -> _StatusConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _StatusCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True


def test_tax_rate_adoption_status_service_upserts_requested_status(monkeypatch) -> None:
    cursor = _StatusCursor()
    connection = _StatusConnection(cursor)
    monkeypatch.setattr(
        "app.services.instant_quote_tax_rate_adoption_status.get_connection",
        lambda: connection,
    )

    record = InstantQuoteTaxRateAdoptionStatusService().upsert_status(
        county_id="harris",
        tax_year=2026,
        adoption_status="prior_year_adopted_rates",
        adoption_status_reason="board has not adopted same-year rates",
        source_note="appeal-season carry-forward",
    )

    assert record.county_id == "harris"
    assert record.tax_year == 2026
    assert record.adoption_status == "prior_year_adopted_rates"
    assert record.adoption_status_reason == "board has not adopted same-year rates"
    assert record.status_source == "operator_asserted"
    assert record.source_note == "appeal-season carry-forward"
    assert connection.committed is True


def test_job_set_tax_rate_adoption_status_runs_schema_check_and_upsert(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "app.jobs.job_set_tax_rate_adoption_status.assert_job_schema_ready",
        lambda job_name, *, tax_year=None: captured.update(
            {"job_name": job_name, "tax_year": tax_year}
        ),
    )

    class _StubService:
        def upsert_status(self, **kwargs):
            captured["kwargs"] = kwargs
            return type(
                "_Record",
                (),
                {
                    "adoption_status": kwargs["adoption_status"],
                    "adoption_status_reason": kwargs["adoption_status_reason"],
                    "status_source": kwargs["status_source"],
                    "source_note": kwargs["source_note"],
                },
            )()

    monkeypatch.setattr(
        "app.jobs.job_set_tax_rate_adoption_status.InstantQuoteTaxRateAdoptionStatusService",
        _StubService,
    )

    run_set_tax_rate_adoption_status(
        county_id="fort_bend",
        tax_year=2026,
        tax_rate_basis_status="current_year_final_adopted_rates",
        tax_rate_basis_status_reason="adopted at board meeting",
        tax_rate_basis_status_note="minutes posted internally",
        tax_rate_basis_status_source="operator_asserted",
    )

    assert captured["job_name"] == "job_set_tax_rate_adoption_status"
    assert captured["tax_year"] == 2026
    assert captured["kwargs"] == {
        "county_id": "fort_bend",
        "tax_year": 2026,
        "adoption_status": "current_year_final_adopted_rates",
        "adoption_status_reason": "adopted at board meeting",
        "status_source": "operator_asserted",
        "source_note": "minutes posted internally",
    }


def test_cli_passes_tax_rate_adoption_status_arguments(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        cli,
        "execute_job",
        lambda job_name, job_callable, **job_kwargs: captured.update(
            {"job_name": job_name, "job_callable": job_callable, "job_kwargs": job_kwargs}
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cli.py",
            "job_set_tax_rate_adoption_status",
            "--county-id",
            "harris",
            "--tax-year",
            "2026",
            "--tax-rate-basis-status",
            "current_year_unofficial_or_proposed_rates",
            "--tax-rate-basis-status-reason",
            "pre-adoption estimate season",
            "--tax-rate-basis-status-note",
            "operator review note",
        ],
    )

    cli.main()

    assert captured["job_name"] == "job_set_tax_rate_adoption_status"
    assert captured["job_kwargs"] == {
        "county_id": "harris",
        "tax_year": 2026,
        "tax_rate_basis_status": "current_year_unofficial_or_proposed_rates",
        "tax_rate_basis_status_reason": "pre-adoption estimate season",
        "tax_rate_basis_status_note": "operator review note",
    }
