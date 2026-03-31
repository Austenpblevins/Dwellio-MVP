from __future__ import annotations

from app.models.quote import InstantQuoteResponse, QuoteExplanationResponse, QuoteResponse
from app.services.instant_quote import InstantQuoteService
from app.services.quote_read import QuoteReadService


def get_quote(county_id: str, tax_year: int, account_number: str) -> QuoteResponse:
    service = QuoteReadService()
    return service.get_quote(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
    )


def get_quote_explanation(
    county_id: str, tax_year: int, account_number: str
) -> QuoteExplanationResponse:
    service = QuoteReadService()
    return service.get_explanation(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
    )


def get_instant_quote(
    county_id: str,
    tax_year: int,
    account_number: str,
) -> InstantQuoteResponse:
    service = InstantQuoteService()
    return service.get_quote(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
    )
