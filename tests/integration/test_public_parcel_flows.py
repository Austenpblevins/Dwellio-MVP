from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.models.lead import LeadCreateResponse
from app.models.parcel import (
    ParcelDataCaveat,
    ParcelExemptionSummary,
    ParcelOwnerSummary,
    ParcelSearchResult,
    ParcelSummaryResponse,
    ParcelTaxRateComponent,
    ParcelTaxSummary,
    ParcelValueSummary,
)
from app.models.quote import (
    InstantQuoteEstimate,
    InstantQuoteExplanation,
    InstantQuoteResponse,
    InstantQuoteSubject,
    QuoteExplanationResponse,
    QuoteResponse,
)


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
                owner_name="A. Example",
                match_basis="address_exact",
                match_score=0.98,
                confidence_label="very_high",
            )
        ]

    def autocomplete(self, query: str, *, limit: int = 8) -> list[ParcelSearchResult]:
        return self.search_by_query(query, limit=limit)


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
            owner_name="A. Example",
            market_value=450000,
            assessed_value=350000,
            effective_tax_rate=0.021,
            estimated_annual_tax=5145,
            completeness_score=86,
            warning_codes=["missing_geometry"],
            public_summary_ready_flag=True,
            owner_summary=ParcelOwnerSummary(
                display_name="A. Example",
                owner_type="individual",
                privacy_mode="masked_individual_name",
                confidence_label="medium",
            ),
            value_summary=ParcelValueSummary(
                market_value=450000,
                assessed_value=350000,
                appraised_value=350000,
                certified_value=345000,
                notice_value=350000,
            ),
            exemption_summary=ParcelExemptionSummary(
                exemption_value_total=100000,
                homestead_flag=True,
                over65_flag=False,
                disabled_flag=False,
                disabled_veteran_flag=False,
                freeze_flag=False,
                exemption_type_codes=["homestead"],
                raw_exemption_codes=["HS"],
            ),
            tax_summary=ParcelTaxSummary(
                effective_tax_rate=0.021,
                estimated_taxable_value=245000,
                estimated_annual_tax=5145,
                component_breakdown=[
                    ParcelTaxRateComponent(
                        unit_type_code="county",
                        unit_name="Harris County",
                        rate_component="maintenance",
                        rate_value=0.01,
                    )
                ],
            ),
            caveats=[
                ParcelDataCaveat(
                    code="missing_geometry",
                    severity="info",
                    title="Map geometry pending",
                    message="Map geometry is not linked yet.",
                )
            ],
        )


class StubQuoteReadService:
    def get_quote(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteResponse:
        return QuoteResponse(
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
            defensible_value_point=320000,
            expected_tax_savings_point=975,
            protest_recommendation="file_protest",
            explanation_bullets=["Comparable evidence supports a lower value."],
        )

    def get_explanation(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> QuoteExplanationResponse:
        return QuoteExplanationResponse(
            county_id=county_id,
            tax_year=tax_year,
            requested_tax_year=tax_year,
            served_tax_year=tax_year,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
            account_number=account_number,
            explanation_json={"basis": "market_and_equity"},
            explanation_bullets=["Comparable evidence supports a lower value."],
        )


class StubInstantQuoteService:
    def get_quote(
        self,
        *,
        county_id: str,
        tax_year: int,
        account_number: str,
    ) -> InstantQuoteResponse:
        return InstantQuoteResponse(
            supported=True,
            county_id=county_id,
            tax_year=tax_year,
            requested_tax_year=tax_year,
            served_tax_year=tax_year,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
            data_freshness_label="current_year",
            account_number=account_number,
            basis_code="assessment_basis_segment_blend",
            subject=InstantQuoteSubject(
                parcel_id=uuid4(),
                address="101 Main St, Houston, TX 77002",
                neighborhood_code="NBHD-1",
                school_district_name="Houston ISD",
                property_type_code="sfr",
                property_class_code="A1",
                living_area_sf=2200,
                year_built=2001,
                notice_value=350000,
                homestead_flag=True,
                freeze_flag=False,
            ),
            estimate=InstantQuoteEstimate(
                savings_range_low=700,
                savings_range_high=1500,
                savings_midpoint_display=1100,
                estimate_bucket="500_to_1499",
                estimate_strength_label="medium",
                tax_protection_limited=False,
            ),
            explanation=InstantQuoteExplanation(
                methodology="segment_within_neighborhood",
                estimate_strength_label="medium",
                summary="This fast estimate blends neighborhood and segment assessment patterns for a public-safe savings range.",
                bullets=["The estimate uses 25 nearby assessment peers from the same neighborhood."],
            ),
            disclaimers=["Instant quote is a fast estimate for protest opportunity, not a final valuation."],
        )


def test_public_search_parcel_and_quote_flow(monkeypatch) -> None:
    lead_id = uuid4()

    def stub_create_lead(request) -> LeadCreateResponse:
        assert request.county_id == "harris"
        assert request.tax_year == 2026
        assert request.account_number == "1001001001001"
        assert request.email == "alex@example.com"
        assert request.anonymous_session_id == "anon-public-flow-1"
        assert request.funnel_stage == "quote_gate"
        return LeadCreateResponse(
            lead_id=lead_id,
            context_status="quote_ready",
            county_supported=True,
            property_supported=True,
            quote_ready=True,
            parcel_id=None,
            requested_tax_year=2026,
            served_tax_year=2026,
            tax_year_fallback_applied=False,
            tax_year_fallback_reason=None,
        )

    monkeypatch.setattr("app.api.search.AddressResolverService", StubAddressResolverService)
    monkeypatch.setattr("app.api.parcel.ParcelSummaryService", StubParcelSummaryService)
    monkeypatch.setattr("app.api.quote.QuoteReadService", StubQuoteReadService)
    monkeypatch.setattr("app.api.quote.InstantQuoteService", StubInstantQuoteService)
    monkeypatch.setattr("app.api.routes.leads.create_lead", stub_create_lead)

    client = TestClient(app)

    search_response = client.get("/search", params={"address": "101 Main"})
    assert search_response.status_code == 200
    search_payload = search_response.json()
    assert search_payload["results"][0]["owner_name"] == "A. Example"
    assert "confidence_reasons" not in search_payload["results"][0]
    assert "agent_remarks" not in search_payload["results"][0]
    assert "listing_history" not in search_payload["results"][0]

    parcel_response = client.get("/parcel/harris/2026/1001001001001")
    assert parcel_response.status_code == 200
    parcel_payload = parcel_response.json()
    assert parcel_payload["owner_summary"]["privacy_mode"] == "masked_individual_name"
    assert parcel_payload["tax_summary"]["component_breakdown"][0]["unit_name"] == "Harris County"
    assert parcel_payload["tax_year_fallback_applied"] is False
    assert "admin_review_required" not in parcel_payload
    assert "cad_owner_name" not in parcel_payload
    assert "owner_source_basis" not in parcel_payload
    assert "owner_confidence_score" not in parcel_payload
    assert "assignment_method" not in parcel_payload["tax_summary"]["component_breakdown"][0]
    assert "assignment_confidence" not in parcel_payload["tax_summary"]["component_breakdown"][0]
    assert "assignment_reason_code" not in parcel_payload["tax_summary"]["component_breakdown"][0]
    assert "component_breakdown_json" not in parcel_payload
    assert "agent_remarks" not in parcel_payload
    assert "listing_history" not in parcel_payload

    quote_response = client.get("/quote/harris/2026/1001001001001")
    assert quote_response.status_code == 200
    quote_payload = quote_response.json()
    assert quote_payload["defensible_value_point"] == 320000
    assert quote_payload["tax_year_fallback_applied"] is False
    assert "comp_candidates" not in quote_payload
    assert "agent_remarks" not in quote_payload
    assert "listing_history" not in quote_payload

    instant_quote_response = client.get("/quote/instant/harris/2026/1001001001001")
    assert instant_quote_response.status_code == 200
    instant_quote_payload = instant_quote_response.json()
    assert instant_quote_payload["supported"] is True
    assert instant_quote_payload["estimate"]["savings_range_low"] == 700
    assert "confidence_score" not in instant_quote_payload
    assert "target_psf" not in instant_quote_payload

    explanation_response = client.get("/quote/harris/2026/1001001001001/explanation")
    assert explanation_response.status_code == 200
    explanation_payload = explanation_response.json()
    assert explanation_payload["served_tax_year"] == 2026
    assert explanation_payload["explanation_json"]["basis"] == "market_and_equity"
    assert "agent_remarks" not in explanation_payload
    assert "listing_history" not in explanation_payload

    lead_response = client.post(
        "/lead",
        json={
            "county_id": "harris",
            "tax_year": 2026,
            "account_number": "1001001001001",
            "email": "alex@example.com",
            "anonymous_session_id": "anon-public-flow-1",
            "funnel_stage": "quote_gate",
        },
    )
    assert lead_response.status_code == 200
    lead_payload = lead_response.json()
    assert lead_payload["status"] == "accepted"
    assert lead_payload["context_status"] == "quote_ready"
    assert lead_payload["lead_id"] == str(lead_id)
