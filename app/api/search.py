from __future__ import annotations

from app.models.parcel import ParcelAutocompleteResponse
from app.models.quote import AutocompleteResponse, SearchResponse
from app.services.address_resolver import AddressResolverService


def get_search(address: str) -> SearchResponse:
    service = AddressResolverService()
    return SearchResponse(results=service.search_by_query(address, limit=10))


def get_autocomplete(query: str) -> ParcelAutocompleteResponse:
    service = AddressResolverService()
    return AutocompleteResponse(suggestions=service.autocomplete(query, limit=8))
