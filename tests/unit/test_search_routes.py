from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.models.admin import (
    AdminSearchInspectCandidate,
    AdminSearchInspectResponse,
    AdminSearchScoreComponents,
)
from app.models.parcel import ParcelSearchResult


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
                confidence_label="very_high",
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
                match_score=0.97,
                confidence_label="high",
            )
        ]

    def inspect_search(self, query: str, *, limit: int = 10) -> AdminSearchInspectResponse:
        assert query == "101 Main"
        assert limit == 5
        return AdminSearchInspectResponse(
            query=query,
            normalized_address_query="101 MAIN",
            normalized_owner_query=None,
            candidates=[
                AdminSearchInspectCandidate(
                    county_id="harris",
                    tax_year=2026,
                    account_number="1001001001001",
                    parcel_id=str(uuid4()),
                    address="101 Main St, Houston, TX 77002",
                    situs_zip="77002",
                    owner_name="Alex Example",
                    match_basis="address_exact",
                    match_score=0.98,
                    confidence_label="very_high",
                    confidence_reasons=["exact_match", "score_strong"],
                    matched_fields=["normalized_address"],
                    score_components=AdminSearchScoreComponents(
                        basis_rank=2,
                        address_similarity=1.0,
                        search_text_similarity=0.91,
                        owner_similarity=0.0,
                    ),
                )
            ],
        )


def test_public_search_route_keeps_public_safe_payload(monkeypatch) -> None:
    monkeypatch.setattr("app.api.search.AddressResolverService", StubAddressResolverService)

    client = TestClient(app)
    response = client.get("/search", params={"address": "101 Main"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["results"][0]["match_basis"] == "address_exact"
    assert payload["results"][0]["confidence_label"] == "very_high"
    assert "confidence_reasons" not in payload["results"][0]
    assert "score_components" not in payload["results"][0]
    assert "matched_fields" not in payload["results"][0]


def test_public_autocomplete_route_keeps_public_safe_payload(monkeypatch) -> None:
    monkeypatch.setattr("app.api.search.AddressResolverService", StubAddressResolverService)

    client = TestClient(app)
    response = client.get("/search/autocomplete", params={"query": "101"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["suggestions"][0]["match_basis"] == "address_prefix"
    assert payload["suggestions"][0]["confidence_label"] == "high"
    assert "confidence_reasons" not in payload["suggestions"][0]
    assert "score_components" not in payload["suggestions"][0]
    assert "matched_fields" not in payload["suggestions"][0]


def test_admin_search_inspect_route_returns_debug_fields(monkeypatch) -> None:
    monkeypatch.setattr("app.api.admin.AddressResolverService", StubAddressResolverService)

    client = TestClient(app)
    response = client.get(
        "/admin/search/inspect",
        params={"query": "101 Main", "limit": 5},
        headers={"x-dwellio-admin-token": "dev-admin-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["access_scope"] == "internal"
    assert payload["candidates"][0]["confidence_reasons"] == ["exact_match", "score_strong"]
    assert payload["candidates"][0]["score_components"]["basis_rank"] == 2
    assert payload["candidates"][0]["matched_fields"] == ["normalized_address"]
