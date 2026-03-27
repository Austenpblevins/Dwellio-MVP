from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.ingestion.manual_backfill import (
    ManualImportRegistrationResult,
    register_manual_import,
)
from app.ingestion.service import IngestionLifecycleService, PipelineStepResult


@dataclass(frozen=True)
class HistoricalBackfillDatasetResult:
    county_id: str
    tax_year: int
    dataset_type: str
    source_file_path: str
    import_batch_id: str
    skipped_duplicate: bool
    existing_status: str | None
    existing_publish_state: str | None
    staging_result: PipelineStepResult | None
    normalize_result: PipelineStepResult | None


class HistoricalBackfillOrchestrator:
    def __init__(self, *, service: IngestionLifecycleService | None = None) -> None:
        self.service = service or IngestionLifecycleService()

    def run(
        self,
        *,
        counties: list[str],
        tax_years: list[int],
        dataset_types: list[str],
        ready_root: str,
        dry_run: bool = False,
    ) -> list[HistoricalBackfillDatasetResult]:
        results: list[HistoricalBackfillDatasetResult] = []
        for county_id in counties:
            for tax_year in tax_years:
                for dataset_type in dataset_types:
                    source_file_path = str(
                        self.resolve_ready_file(
                            ready_root=ready_root,
                            county_id=county_id,
                            tax_year=tax_year,
                            dataset_type=dataset_type,
                        )
                    )
                    registration = register_manual_import(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=dataset_type,
                        source_file_path=source_file_path,
                        dry_run=dry_run,
                    )
                    if (
                        registration.skipped_duplicate
                        and registration.existing_status == "normalized"
                        and registration.existing_publish_state == "published"
                    ):
                        results.append(
                            HistoricalBackfillDatasetResult(
                                county_id=county_id,
                                tax_year=tax_year,
                                dataset_type=dataset_type,
                                source_file_path=source_file_path,
                                import_batch_id=registration.import_batch_id,
                                skipped_duplicate=True,
                                existing_status=registration.existing_status,
                                existing_publish_state=registration.existing_publish_state,
                                staging_result=None,
                                normalize_result=None,
                            )
                        )
                        continue

                    staging_result = self.service.load_staging(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=dataset_type,
                        import_batch_id=registration.import_batch_id,
                        dry_run=dry_run,
                    )
                    normalize_result = self.service.normalize(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=dataset_type,
                        import_batch_id=registration.import_batch_id,
                        dry_run=dry_run,
                    )
                    results.append(
                        HistoricalBackfillDatasetResult(
                            county_id=county_id,
                            tax_year=tax_year,
                            dataset_type=dataset_type,
                            source_file_path=source_file_path,
                            import_batch_id=registration.import_batch_id,
                            skipped_duplicate=registration.skipped_duplicate,
                            existing_status=registration.existing_status,
                            existing_publish_state=registration.existing_publish_state,
                            staging_result=staging_result,
                            normalize_result=normalize_result,
                        )
                    )
        return results

    def resolve_ready_file(
        self,
        *,
        ready_root: str,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> Path:
        base_name = f"{county_id}_{dataset_type}_{tax_year}"
        root = Path(ready_root).expanduser().resolve()
        candidates = [
            root / f"{base_name}.json",
            root / f"{base_name}.csv",
            root / f"{base_name}.txt",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"Missing adapter-ready backfill file for {county_id}/{dataset_type}/{tax_year} "
            f"under {root}. Expected one of: {', '.join(str(candidate) for candidate in candidates)}"
        )
