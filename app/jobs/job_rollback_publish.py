from __future__ import annotations

from app.ingestion.service import IngestionLifecycleService


def run(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    dataset_type: str | None = None,
    import_batch_id: str | None = None,
) -> None:
    if county_id is None or tax_year is None or dataset_type is None:
        raise ValueError("job_rollback_publish requires county_id, tax_year, and dataset_type.")

    IngestionLifecycleService().rollback_publish(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        import_batch_id=import_batch_id,
    )
