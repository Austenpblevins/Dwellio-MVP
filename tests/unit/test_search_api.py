from __future__ import annotations

from uuid import uuid4

from app.api.parcel import get_parcel_summary
from app.api.search import get_autocomplete, get_search
from app.models.parcel import ParcelSearchResult, ParcelSummaryResponse


class StubAddressResolverService:
    def search_by_query(self, query: str, *, limit: int = 10) -> list[ParcelSearchResult]:
        assert query == "101 Main"
        assert limit == 10
        return [
            ParcelSearchResult(
                county_id="harris",
                tax_year=2026,
                account_number="1001001001001",
                parcel_id=uuid4(),
                address="101 Main St, Houston, TX 77002",
                situs_zip="77002",
                owner_name="Alex Example",
                match_basis="address_exact",
                match_score=0.98,
                confidence_label="high",
            )
        ]

    def autocomplete(self, query: str, *, limit: int = 8) -> list[ParcelSearchResult]:
        assert query == "101"
        assert limit == 8
        return [
            ParcelSearchResult(
                county_id="harris",
                tax_year=2026,
                account_number="1001001001001",
                parcel_id=uuid4(),
                address="101 Main St, Houston, TX 77002",
                situs_zip="77002",
                owner_name="Alex Example",
                match_basis="address_prefix",
                match_score=0.99,
                confidence_label="high",
            )
        ]


class StubParcelSummaryService:
    def get_summary(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> ParcelSummaryResponse:
        assert county_id == "harris"
        assert tax_year == 2026
        assert account_number == "1001001001001"
        return ParcelSummaryResponse(
            county_id=county_id,
            tax_year=tax_year,
            requested_tax_year=tax_year,
            served_tax_year=tax_year,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
            account_number=account_number,
            parcel_id=uuid4(),
            address="101 Main St, Houston, TX 77002",
            owner_name="Alex Example",
            completeness_score=90.0,
            warning_codes=[],
            public_summary_ready_flag=True,
        )


def test_get_search_wraps_results(monkeypatch) -> None:
    monkeypatch.setattr("app.api.search.AddressResolverService", StubAddressResolverService)

    response = get_search("101 Main")

    assert len(response.results) == 1
    assert response.results[0].match_basis == "address_exact"


def test_get_autocomplete_wraps_suggestions(monkeypatch) -> None:
    monkeypatch.setattr("app.api.search.AddressResolverService", StubAddressResolverService)

    response = get_autocomplete("101")

    assert len(response.suggestions) == 1
    assert response.suggestions[0].match_basis == "address_prefix"


def test_get_parcel_summary_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr("app.api.parcel.ParcelSummaryService", StubParcelSummaryService)

    response = get_parcel_summary("harris", 2026, "1001001001001")

    assert response.account_number == "1001001001001"
    assert response.public_summary_ready_flag is True
