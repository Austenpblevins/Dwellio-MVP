from __future__ import annotations

from app.models.quote import QuoteResponse


def get_quote(county_id: str, tax_year: int, account_number: str) -> QuoteResponse:
    _ = (county_id, tax_year, account_number)
    raise NotImplementedError("Wire to v_quote_read_model")


def get_quote_explanation(
    county_id: str, tax_year: int, account_number: str
) -> dict[str, object]:
    _ = (county_id, tax_year, account_number)
    raise NotImplementedError("Wire to v_quote_read_model explanation fields")
