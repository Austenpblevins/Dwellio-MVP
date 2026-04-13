from __future__ import annotations

from app.jobs import (
    job_comp_candidates,
    job_features,
    job_refresh_quote_cache,
    job_score_models,
    job_score_savings,
)
from app.services.quote_generation import QuoteGenerationSummary


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
    monkeypatch.setattr(
        job_score_models,
        "QuoteGenerationService",
        lambda: _StubQuoteGenerationService(),
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
    monkeypatch.setattr(
        job_score_savings,
        "QuoteGenerationService",
        lambda: _StubQuoteGenerationService(),
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
    monkeypatch.setattr(
        job_refresh_quote_cache,
        "QuoteGenerationService",
        lambda: _StubQuoteGenerationService(),
    )

    job_refresh_quote_cache.run(county_id="harris", tax_year=2026)

    assert observed == [("job_refresh_quote_cache", 2026)]


def test_job_score_models_forwards_account_numbers(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class _ForwardingService:
        def score_models(
            self,
            *,
            county_id: str | None = None,
            tax_year: int | None = None,
            account_numbers: tuple[str, ...] | None = None,
        ) -> QuoteGenerationSummary:
            observed["county_id"] = county_id
            observed["tax_year"] = tax_year
            observed["account_numbers"] = account_numbers
            return QuoteGenerationSummary(processed_count=2, created_count=2, skipped_count=0)

    monkeypatch.setattr(job_score_models, "assert_job_schema_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_score_models, "QuoteGenerationService", lambda: _ForwardingService())

    job_score_models.run(
        county_id="harris",
        tax_year=2026,
        account_numbers=("1001001001001", "1002002002002"),
    )

    assert observed == {
        "county_id": "harris",
        "tax_year": 2026,
        "account_numbers": ("1001001001001", "1002002002002"),
    }


class _StubQuoteGenerationService:
    def score_models(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: tuple[str, ...] | None = None,
    ) -> QuoteGenerationSummary:
        return QuoteGenerationSummary(processed_count=1, created_count=1, skipped_count=0)

    def score_savings(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: tuple[str, ...] | None = None,
    ) -> QuoteGenerationSummary:
        return QuoteGenerationSummary(processed_count=1, created_count=1, skipped_count=0)

    def refresh_quote_cache(
        self,
        *,
        county_id: str | None = None,
        tax_year: int | None = None,
        account_numbers: tuple[str, ...] | None = None,
    ) -> QuoteGenerationSummary:
        return QuoteGenerationSummary(processed_count=1, created_count=1, skipped_count=0)
