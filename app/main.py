from __future__ import annotations

from fastapi import FastAPI

from app.api.router import build_api_router
from app.core.config import get_settings
from app.core.lifecycle import app_lifespan

settings = get_settings()

app = FastAPI(
    title="Dwellio API",
    version="0.1.0",
    docs_url="/docs" if settings.env != "prod" else None,
    redoc_url="/redoc" if settings.env != "prod" else None,
    lifespan=app_lifespan,
)
app.include_router(build_api_router())
