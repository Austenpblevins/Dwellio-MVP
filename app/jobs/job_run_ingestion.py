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
        raise ValueError("job_run_ingestion requires county_id, tax_year, and dataset_type.")
    if import_batch_id is not None:
        raise ValueError("job_run_ingestion does not accept import_batch_id; it creates a fresh import batch.")

    logger.info(
        "job_run_ingestion started",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
        },
    )
    result = IngestionLifecycleService().run_dataset_lifecycle(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        dry_run=dry_run,
    )
    logger.info(
        "job_run_ingestion finished",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "dry_run": dry_run,
            "import_batch_id": result.import_batch_id,
            "rerun_of_import_batch_id": result.rerun_of_import_batch_id,
        },
    )
    return result
