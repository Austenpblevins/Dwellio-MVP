from __future__ import annotations
from app.models.quote import SearchRequest, SearchResponse
from app.services.address_resolver import AddressResolverService

def get_search(address: str) -> SearchResponse:
    service = AddressResolverService()
    return SearchResponse(results=service.search(SearchRequest(address=address)))
