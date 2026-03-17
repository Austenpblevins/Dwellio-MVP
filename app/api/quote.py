from __future__ import annotations

def get_quote(county_id: str, tax_year: int, account_number: str):
    raise NotImplementedError('Wire to v_quote_read_model')

def get_quote_explanation(county_id: str, tax_year: int, account_number: str):
    raise NotImplementedError('Wire to v_quote_read_model explanation fields')
