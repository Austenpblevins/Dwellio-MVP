from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from app.models.parcel import ParcelSearchResult, ParcelSummaryResponse
from app.services.address_resolver import AddressResolverService
from app.services.parcel_summary import ParcelSummaryService
from app.services.search_index import SearchIndexService
from app.utils.text_normalization import normalize_address_query


class FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((query, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self.rows

    def fetchone(self) -> dict[str, object] | None:
        return self.rows[0] if self.rows else None


class FakeConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.commit_calls = 0
        self.cursor_instance = FakeCursor(rows)

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1


def connection_factory(rows: list[dict[str, object]]):
    @contextmanager
    def _connection():
        yield FakeConnection(rows)

    return _connection


def test_normalize_address_query_applies_common_street_token_normalization() -> None:
    assert normalize_address_query("101 Main Street Apt 2, Houston, Texas 77002") == (
        "101 MAIN ST 2 HOUSTON TX 77002"
    )


def test_search_index_service_rebuild_calls_refresh_function(monkeypatch) -> None:
    rows = [{"refreshed_count": 4}]
    monkeypatch.setattr("app.services.search_index.get_connection", connection_factory(rows))

    service = SearchIndexService()
    refreshed = service.rebuild_search_documents(county_id="harris", tax_year=2026)

    assert refreshed == 4


def test_address_resolver_search_maps_rows_to_ranked_results(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Alex Example",
            "match_basis": "address_exact",
            "match_score": 0.98,
        }
    ]
    monkeypatch.setattr("app.services.address_resolver.get_connection", connection_factory(rows))

    results = AddressResolverService().search_by_query("101 Main Street Houston TX 77002")

    assert len(results) == 1
    assert isinstance(results[0], ParcelSearchResult)
    assert results[0].match_basis == "address_exact"
    assert results[0].confidence_label == "very_high"
    assert results[0].owner_name == "A. Example"


def test_address_resolver_search_keeps_short_address_trigram_candidates(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Alex Example",
            "match_basis": "address_trigram",
            "match_score": 0.2948,
        }
    ]
    connection = FakeConnection(rows)

    @contextmanager
    def _connection():
        yield connection

    monkeypatch.setattr("app.services.address_resolver.get_connection", _connection)

    results = AddressResolverService().search_by_query("101 Main")

    assert len(results) == 1
    assert results[0].address == "101 Main St, Houston, TX 77002"
    assert results[0].match_basis == "address_trigram"
    assert results[0].confidence_label == "medium"

    sql, params = connection.cursor_instance.execute_calls[0]
    assert "WHEN 'address_trigram' THEN %s" in sql
    assert params[-3] == 0.29
    assert params[-2] == 0.35


def test_address_resolver_search_keeps_account_and_exact_ranking_ahead_of_trigram(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Alex Example",
            "match_basis": "account_exact",
            "match_score": 1.0,
        },
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001002",
            "parcel_id": uuid4(),
            "address": "101 Main Street, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Jordan Example",
            "match_basis": "address_exact",
            "match_score": 0.98,
        },
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001003",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Taylor Example",
            "match_basis": "address_trigram",
            "match_score": 0.2948,
        },
    ]
    monkeypatch.setattr("app.services.address_resolver.get_connection", connection_factory(rows))

    results = AddressResolverService().search_by_query("101 Main")

    assert [result.match_basis for result in results] == [
        "account_exact",
        "address_exact",
        "address_trigram",
    ]


def test_address_resolver_inspect_search_returns_debug_components(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "situs_zip": "77002",
            "owner_name": "Alex Example",
            "match_basis": "owner_fallback",
            "match_score": 0.58,
            "address_similarity": 0.31,
            "search_text_similarity": 0.44,
            "owner_similarity": 0.77,
            "matched_fields": ["normalized_owner_name"],
        }
    ]
    monkeypatch.setattr("app.services.address_resolver.get_connection", connection_factory(rows))

    response = AddressResolverService().inspect_search("Alex Example")

    assert response.query == "Alex Example"
    assert len(response.candidates) == 1
    candidate = response.candidates[0]
    assert candidate.confidence_label == "medium"
    assert candidate.confidence_reasons == ["owner_name_fallback", "score_weak"]
    assert candidate.matched_fields == ["normalized_owner_name"]
    assert candidate.score_components.owner_similarity == 0.77


def test_parcel_summary_service_returns_summary_model(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "cad_property_id": "CAD-1",
            "situs_address": "101 Main St",
            "situs_city": "Houston",
            "situs_state": "TX",
            "situs_zip": "77002",
            "normalized_address": "101 MAIN ST HOUSTON TX 77002",
            "address": "101 Main St, Houston, TX 77002",
            "owner_name": "Alex Example",
            "owner_name_normalized": "ALEX EXAMPLE",
            "owner_source_basis": "cad_owner_snapshot",
            "owner_confidence_score": 0.5,
            "owner_override_flag": False,
            "cad_owner_name": "Alex Example",
            "cad_owner_name_normalized": "ALEX EXAMPLE",
            "property_type_code": "sfr",
            "property_class_code": "A1",
            "neighborhood_code": "NBHD-1",
            "subdivision_name": "Example Oaks",
            "school_district_name": "Houston ISD",
            "living_area_sf": 2100.0,
            "year_built": 2001,
            "effective_year_built": 2005,
            "effective_age": 20.0,
            "bedrooms": 4,
            "full_baths": 2.0,
            "half_baths": 1.0,
            "stories": 2.0,
            "quality_code": "AVG",
            "condition_code": "GOOD",
            "garage_spaces": 2.0,
            "pool_flag": False,
            "land_sf": 6500.0,
            "land_acres": 0.15,
            "market_value": 450000.0,
            "assessed_value": 350000.0,
            "appraised_value": 350000.0,
            "certified_value": 345000.0,
            "notice_value": 350000.0,
            "exemption_value_total": 100000.0,
            "homestead_flag": True,
            "over65_flag": False,
            "disabled_flag": False,
            "disabled_veteran_flag": False,
            "freeze_flag": False,
            "effective_tax_rate": 0.021,
            "estimated_taxable_value": 245000.0,
            "estimated_annual_tax": 5145.0,
            "exemption_type_codes": ["homestead"],
            "raw_exemption_codes": ["HS"],
            "component_breakdown_json": [
                {
                    "unit_type_code": "county",
                    "unit_code": "HC",
                    "unit_name": "Harris County",
                    "rate_component": "maintenance",
                    "rate_value": 0.01,
                    "rate_per_100": 1.0,
                    "assignment_method": "gis",
                    "assignment_confidence": 0.99,
                    "assignment_reason_code": "polygon_match",
                    "is_primary": True,
                    "match_basis_json": {"city": "Houston"},
                }
            ],
            "completeness_score": 80.0,
            "warning_codes": ["missing_geometry"],
            "public_summary_ready_flag": True,
        }
    ]
    monkeypatch.setattr("app.services.parcel_summary.get_connection", connection_factory(rows))

    summary = ParcelSummaryService().get_summary(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert isinstance(summary, ParcelSummaryResponse)
    assert summary.account_number == "1001001001001"
    assert summary.requested_tax_year == 2026
    assert summary.served_tax_year == 2026
    assert summary.tax_year_fallback_applied is False
    assert summary.tax_year_fallback_reason is None
    assert summary.data_freshness_label == "current_year"
    assert summary.public_summary_ready_flag is True
    assert summary.owner_name == "A. Example"
    assert summary.owner_summary is not None
    assert summary.owner_summary.privacy_mode == "masked_individual_name"
    assert summary.tax_summary is not None
    assert len(summary.tax_summary.component_breakdown) == 1
    assert "assignment_method" not in summary.tax_summary.component_breakdown[0].model_dump()
    assert "assignment_confidence" not in summary.tax_summary.component_breakdown[0].model_dump()
    assert "assignment_reason_code" not in summary.tax_summary.component_breakdown[0].model_dump()
    assert "match_basis_json" not in summary.tax_summary.component_breakdown[0].model_dump()
    assert summary.caveats[0].code == "missing_geometry"


def test_parcel_summary_service_applies_prior_year_fallback(monkeypatch) -> None:
    rows = [
        {
            "county_id": "harris",
            "tax_year": 2025,
            "account_number": "1001001001001",
            "parcel_id": uuid4(),
            "address": "101 Main St, Houston, TX 77002",
            "owner_name": "Alex Example",
            "owner_confidence_score": 0.5,
            "component_breakdown_json": [],
            "completeness_score": 86.0,
            "warning_codes": [],
            "public_summary_ready_flag": True,
        }
    ]
    monkeypatch.setattr("app.services.parcel_summary.get_connection", connection_factory(rows))

    summary = ParcelSummaryService().get_summary(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    assert summary.tax_year == 2025
    assert summary.requested_tax_year == 2026
    assert summary.served_tax_year == 2025
    assert summary.tax_year_fallback_applied is True
    assert summary.tax_year_fallback_reason == "requested_year_unavailable"
    assert summary.data_freshness_label == "prior_year_fallback"


def test_parcel_summary_service_raises_when_missing(monkeypatch) -> None:
    monkeypatch.setattr("app.services.parcel_summary.get_connection", connection_factory([]))

    with pytest.raises(LookupError):
        ParcelSummaryService().get_summary(
            county_id="harris",
            tax_year=2026,
            account_number="missing",
        )


def test_parcel_summary_service_queries_nearest_prior_year(monkeypatch) -> None:
    connection = FakeConnection(
        [
            {
                "county_id": "harris",
                "tax_year": 2025,
                "account_number": "1001001001001",
                "parcel_id": uuid4(),
                "address": "101 Main St, Houston, TX 77002",
                "owner_name": "Alex Example",
                "owner_confidence_score": 0.5,
                "component_breakdown_json": [],
                "completeness_score": 86.0,
                "warning_codes": [],
                "public_summary_ready_flag": True,
            }
        ]
    )

    @contextmanager
    def _connection():
        yield connection

    monkeypatch.setattr("app.services.parcel_summary.get_connection", _connection)

    ParcelSummaryService().get_summary(
        county_id="harris",
        tax_year=2026,
        account_number="1001001001001",
    )

    sql, params = connection.cursor_instance.execute_calls[0]
    assert "tax_year <= %s" in sql
    assert "ORDER BY tax_year DESC" in sql
    assert params == ("harris", "1001001001001", 2026)
