from __future__ import annotations

from app.jobs import (
    job_comp_candidates,
    job_features,
    job_refresh_quote_cache,
    job_score_models,
    job_score_savings,
)


def test_job_features_calls_schema_preflight(monkeypatch) -> None:
    observed: list[tuple[str, int | None]] = []
    monkeypatch.setattr(
        job_features,
        "assert_job_schema_ready",
        lambda job_name, *, tax_year=None: observed.append((job_name, tax_year)),
    )

    job_features.run(county_id="harris", tax_year=2026)

    assert observed == [("job_features", 2026)]


def test_job_comp_candidates_calls_schema_preflight(monkeypatch) -> None:
    observed: list[tuple[str, int | None]] = []
    monkeypatch.setattr(
        job_comp_candidates,
        "assert_job_schema_ready",
        lambda job_name, *, tax_year=None: observed.append((job_name, tax_year)),
    )

    job_comp_candidates.run(county_id="harris", tax_year=2026)

    assert observed == [("job_comp_candidates", 2026)]


def test_job_score_models_calls_schema_preflight(monkeypatch) -> None:
    observed: list[tuple[str, int | None]] = []
    monkeypatch.setattr(
        job_score_models,
        "assert_job_schema_ready",
        lambda job_name, *, tax_year=None: observed.append((job_name, tax_year)),
    )

    job_score_models.run(county_id="harris", tax_year=2026)

    assert observed == [("job_score_models", 2026)]


def test_job_score_savings_calls_schema_preflight(monkeypatch) -> None:
    observed: list[tuple[str, int | None]] = []
    monkeypatch.setattr(
        job_score_savings,
        "assert_job_schema_ready",
        lambda job_name, *, tax_year=None: observed.append((job_name, tax_year)),
    )

    job_score_savings.run(county_id="harris", tax_year=2026)

    assert observed == [("job_score_savings", 2026)]


def test_job_refresh_quote_cache_calls_schema_preflight(monkeypatch) -> None:
    observed: list[tuple[str, int | None]] = []
    monkeypatch.setattr(
        job_refresh_quote_cache,
        "assert_job_schema_ready",
        lambda job_name, *, tax_year=None: observed.append((job_name, tax_year)),
    )

    job_refresh_quote_cache.run(county_id="harris", tax_year=2026)

    assert observed == [("job_refresh_quote_cache", 2026)]
