from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.api.quote import get_instant_quote, get_quote, get_quote_explanation
from app.models.quote import InstantQuoteResponse, QuoteExplanationResponse, QuoteResponse

router = APIRouter()


@router.get(
    "/quote/instant/{county_id}/{tax_year}/{account_number}",
    response_model=InstantQuoteResponse,
)
def instant_quote_endpoint(
    county_id: str,
    tax_year: int,
    account_number: str,
) -> InstantQuoteResponse:
    try:
        return get_instant_quote(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc


@router.get("/quote/{county_id}/{tax_year}/{account_number}", response_model=QuoteResponse)
def quote_endpoint(county_id: str, tax_year: int, account_number: str) -> QuoteResponse:
    try:
        return get_quote(county_id=county_id, tax_year=tax_year, account_number=account_number)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc


@router.get(
    "/quote/{county_id}/{tax_year}/{account_number}/explanation",
    response_model=QuoteExplanationResponse,
)
def quote_explanation_endpoint(
    county_id: str, tax_year: int, account_number: str
) -> QuoteExplanationResponse:
    try:
        return get_quote_explanation(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc
