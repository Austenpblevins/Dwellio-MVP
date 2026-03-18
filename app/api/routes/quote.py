from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.api.quote import get_quote, get_quote_explanation
from app.models.common import DwellioBaseModel
from app.models.quote import QuoteResponse

router = APIRouter()


class QuoteExplanationResponse(DwellioBaseModel):
    county_id: str
    tax_year: int
    account_number: str
    explanation_json: dict[str, object]
    explanation_bullets: list[str]


@router.get("/quote/{county_id}/{tax_year}/{account_number}", response_model=QuoteResponse)
def quote_endpoint(county_id: str, tax_year: int, account_number: str) -> QuoteResponse:
    try:
        return get_quote(county_id=county_id, tax_year=tax_year, account_number=account_number)
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
        payload = get_quote_explanation(
            county_id=county_id,
            tax_year=tax_year,
            account_number=account_number,
        )
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc)) from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="Quote explanation payload must be a dictionary.")

    explanation_json = payload.get("explanation_json", {})
    if not isinstance(explanation_json, dict):
        raise HTTPException(status_code=500, detail="Quote explanation json payload must be a dict.")

    explanation_bullets = payload.get("explanation_bullets", [])
    if not isinstance(explanation_bullets, list) or not all(
        isinstance(item, str) for item in explanation_bullets
    ):
        raise HTTPException(
            status_code=500,
            detail="Quote explanation bullets payload must be a list of strings.",
        )

    return QuoteExplanationResponse(
        county_id=county_id,
        tax_year=tax_year,
        account_number=account_number,
        explanation_json=explanation_json,
        explanation_bullets=explanation_bullets,
    )
