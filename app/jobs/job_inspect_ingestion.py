from __future__ import annotations

from app.ingestion.service import IngestionLifecycleService
from app.utils.logging import get_logger

logger = get_logger(__name__)


def run(
    *,
    county_id: str | None = None,
    tax_year: int | None = None,
    dataset_type: str | None = None,
    import_batch_id: str | None = None,
    dry_run: bool = False,
) -> object:
    if dry_run:
        raise ValueError("job_inspect_ingestion does not accept dry_run.")
    if county_id is None or tax_year is None or dataset_type is None:
        raise ValueError("job_inspect_ingestion requires county_id, tax_year, and dataset_type.")

    logger.info(
        "job_inspect_ingestion started",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "import_batch_id": import_batch_id,
        },
    )
    result = IngestionLifecycleService().inspect_import_batch(
        county_id=county_id,
        tax_year=tax_year,
        dataset_type=dataset_type,
        import_batch_id=import_batch_id,
    )
    logger.info(
        "job_inspect_ingestion finished",
        extra={
            "county_id": county_id,
            "tax_year": tax_year,
            "dataset_type": dataset_type,
            "import_batch_id": result.import_batch_id,
            "validation_error_count": result.validation_error_count,
        },
    )
    return result
