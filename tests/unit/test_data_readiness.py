from __future__ import annotations

from app.services.data_readiness import DataReadinessService


class StubCursor:
    def __init__(self) -> None:
        self._row: dict[str, object] | None = None

    def __enter__(self) -> StubCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        if "FROM tax_years WHERE tax_year = %s" in sql:
            self._row = {"present": 1}
        elif "FROM import_batches ib" in sql:
            if params == ("harris", 2025, "property_roll"):
                self._row = {
                    "import_batch_id": "batch-1",
                    "status": "normalized",
                    "publish_state": "published",
                }
            else:
                self._row = None
        elif "FROM raw_files WHERE county_id = %s AND tax_year = %s AND file_kind = %s" in sql:
            count = 1 if params == ("harris", 2025, "property_roll") else 0
            self._row = {"count": count}
        elif "information_schema.tables" in sql:
            table_name = params[0]
            self._row = {"present": table_name in {"search_documents", "parcel_features", "comp_candidate_pools"}}
        elif "information_schema.views" in sql:
            view_name = params[0]
            self._row = {"present": view_name in {"parcel_summary_view", "v_quote_read_model"}}
        elif "FROM parcel_summary_view" in sql:
            self._row = {"count": 2}
        elif "FROM search_documents" in sql:
            self._row = {"count": 2}
        elif "FROM parcel_features pf" in sql:
            self._row = {"count": 0}
        elif "FROM comp_candidate_pools" in sql:
            self._row = {"count": 0}
        elif "FROM v_quote_read_model" in sql:
            self._row = {"count": 0}
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, object] | None:
        return self._row


class StubConnection:
    def __enter__(self) -> StubConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> StubCursor:
        return StubCursor()


def test_data_readiness_summary(monkeypatch) -> None:
    monkeypatch.setattr("app.services.data_readiness.get_connection", lambda: StubConnection())

    readiness = DataReadinessService().build_tax_year_readiness(county_id="harris", tax_year=2025)

    assert readiness.tax_year_known is True
    property_roll = next(item for item in readiness.datasets if item.dataset_type == "property_roll")
    assert property_roll.access_method == "manual_upload"
    assert property_roll.availability_status == "manual_upload_required"
    assert property_roll.raw_file_count == 1
    assert property_roll.latest_import_status == "normalized"
    assert property_roll.canonical_published is True
    assert readiness.derived.parcel_summary_ready is True
    assert readiness.derived.search_support_ready is True
    assert readiness.derived.feature_ready is False
    assert readiness.derived.quote_ready is False
