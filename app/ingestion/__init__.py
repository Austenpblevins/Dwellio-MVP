from __future__ import annotations

__all__ = ["IngestionLifecycleService", "get_adapter"]


def __getattr__(name: str):
    if name == "IngestionLifecycleService":
        from app.ingestion.service import IngestionLifecycleService

        return IngestionLifecycleService
    if name == "get_adapter":
        from app.ingestion.registry import get_adapter

        return get_adapter
    raise AttributeError(name)
