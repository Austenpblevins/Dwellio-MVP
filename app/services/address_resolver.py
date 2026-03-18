from __future__ import annotations

from app.models.parcel import ParcelSearchResult
from app.models.quote import SearchRequest
from app.utils.text_normalization import normalize_address_query


class AddressResolverService:
    def search(self, request: SearchRequest) -> list[ParcelSearchResult]:
        normalized = normalize_address_query(request.address)
        _ = normalized
        return []
