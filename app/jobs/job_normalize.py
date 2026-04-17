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
        raise ValueError("job_normalize requires county_id, tax_year, and dataset_type.")
    if import_batch_id is None:
        raise ValueError("job_normalize requires import_batch_id.")

    logger.info(
        "job_normalize started",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
            "import_batch_id": import_batch_id,
        },
    )
    result = IngestionLifecycleService().normalize(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        import_batch_id=import_batch_id,
        dry_run=dry_run,
    )
    logger.info(
        "job_normalize finished",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
            "import_batch_id": import_batch_id,
        },
    )
    return result
