from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_healthz_returns_ok_when_dependency_check_succeeds(monkeypatch) -> None:
    monkeypatch.setattr("app.api.routes.health.is_postgis_enabled", lambda: True)

    client = TestClient(app)
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "database": "reachable",
        "postgis_enabled": True,
    }


def test_healthz_returns_degraded_when_dependency_check_fails(monkeypatch) -> None:
    def failing_postgis_check() -> bool:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr("app.api.routes.health.is_postgis_enabled", failing_postgis_check)

    client = TestClient(app)
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert response.json()["database"] == "unreachable"
    assert response.json()["error"] == "database unavailable"
