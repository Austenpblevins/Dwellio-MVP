from __future__ import annotations

from app.ingestion.service import IngestionLifecycleService
from app.utils.logging import get_logger

logger = get_logger(__name__)

def run(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    dataset_type: str | None = None,
    dry_run: bool = False,
    import_batch_id: str | None = None,
) -> object:
    if county_id is None or tax_year is None or dataset_type is None:
        raise ValueError("job_load_staging requires county_id, tax_year, and dataset_type.")

    logger.info(
        "job_load_staging started",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
            "import_batch_id": import_batch_id,
        },
    )
    result = IngestionLifecycleService().load_staging(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        import_batch_id=import_batch_id,
        dry_run=dry_run,
    )
    logger.info(
        "job_load_staging finished",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
            "import_batch_id": import_batch_id,
        },
    )
    return result
