from __future__ import annotations

from fastapi import APIRouter, Query

from app.api.search import get_search
from app.models.quote import SearchResponse

router = APIRouter()


@router.get("/search", response_model=SearchResponse)
def search_by_address(address: str = Query(..., min_length=3)) -> SearchResponse:
    return get_search(address)

