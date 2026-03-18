from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import get_settings
from app.utils.logging import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def app_lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logger.info(
        "app startup",
        extra={"env": settings.env, "api_host": settings.api_host, "api_port": settings.api_port},
    )
    yield
    logger.info("app shutdown", extra={"env": settings.env})

