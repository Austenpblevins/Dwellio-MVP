from __future__ import annotations

from fastapi import APIRouter

from app.db.connection import is_postgis_enabled

router = APIRouter()


@router.get("/healthz")
def healthcheck() -> dict[str, object]:
    try:
        postgis_enabled = is_postgis_enabled()
        return {"status": "ok", "database": "reachable", "postgis_enabled": postgis_enabled}
    except Exception as exc:
        return {"status": "degraded", "database": "unreachable", "error": str(exc)}

