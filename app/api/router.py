from __future__ import annotations

from fastapi import APIRouter

from app.api.routes import health, leads, parcel, quote, search


def build_api_router() -> APIRouter:
    router = APIRouter()
    router.include_router(health.router, tags=["health"])
    router.include_router(search.router, tags=["search"])
    router.include_router(parcel.router, tags=["parcel"])
    router.include_router(quote.router, tags=["quote"])
    router.include_router(leads.router, tags=["lead"])
    return router
