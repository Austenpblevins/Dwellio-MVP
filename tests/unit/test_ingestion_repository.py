from __future__ import annotations

from app.ingestion.repository import IngestionRepository


def test_capture_property_roll_rollback_manifest_includes_new_accounts_with_none_prior_state(
    monkeypatch,
) -> None:
    repository = IngestionRepository(connection=None)  # type: ignore[arg-type]

    monkeypatch.setattr(repository, "_fetch_rows", lambda query, params: [])

    manifest = repository.capture_property_roll_rollback_manifest(
        county_id="harris",
        tax_year=2024,
        account_numbers=["1001001001001", "1001001001002"],
    )

    assert manifest == {
        "dataset_type": "property_roll",
        "entries": [
            {"account_number": "1001001001001", "prior_state": None},
            {"account_number": "1001001001002", "prior_state": None},
        ],
    }
