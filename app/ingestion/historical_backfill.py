from __future__ import annotations

import hashlib
import json
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
    manifest_path: str
    source_checksum: str
    import_batch_id: str
    skipped_duplicate: bool
    existing_status: str | None
    existing_publish_state: str | None
    staging_result: PipelineStepResult | None
    normalize_result: PipelineStepResult | None


@dataclass(frozen=True)
class ReadyDatasetManifest:
    county_id: str
    tax_year: int
    dataset_type: str
    source_file_path: str
    source_checksum: str
    manifest_path: str


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
                    ready_dataset = self.resolve_ready_dataset(
                        ready_root=ready_root,
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=dataset_type,
                    )
                    registration = register_manual_import(
                        county_id=county_id,
                        tax_year=tax_year,
                        dataset_type=dataset_type,
                        source_file_path=ready_dataset.source_file_path,
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
                                source_file_path=ready_dataset.source_file_path,
                                manifest_path=ready_dataset.manifest_path,
                                source_checksum=ready_dataset.source_checksum,
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
                            source_file_path=ready_dataset.source_file_path,
                            manifest_path=ready_dataset.manifest_path,
                            source_checksum=ready_dataset.source_checksum,
                            import_batch_id=registration.import_batch_id,
                            skipped_duplicate=registration.skipped_duplicate,
                            existing_status=registration.existing_status,
                            existing_publish_state=registration.existing_publish_state,
                            staging_result=staging_result,
                            normalize_result=normalize_result,
                        )
                    )
        return results

    def resolve_ready_dataset(
        self,
        *,
        ready_root: str,
        county_id: str,
        tax_year: int,
        dataset_type: str,
    ) -> ReadyDatasetManifest:
        root = Path(ready_root).expanduser().resolve()
        base_name = f"{county_id}_{dataset_type}_{tax_year}"
        manifest_path = root / f"{base_name}.manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Missing prepared manifest for {county_id}/{dataset_type}/{tax_year} under {root}. "
                "Run prepare_manual_county_files first and use its generated ready outputs."
            )
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            payload.get("county_id") != county_id
            or int(payload.get("tax_year") or 0) != tax_year
            or payload.get("dataset_type") != dataset_type
        ):
            raise ValueError(
                f"Manifest mismatch for {manifest_path}: expected "
                f"{county_id}/{dataset_type}/{tax_year}."
            )
        validation_status = str(payload.get("validation", {}).get("status") or "")
        if validation_status != "passed":
            raise ValueError(
                f"Prepared manifest {manifest_path} is not validation-passed "
                f"(status={validation_status or 'missing'})."
            )
        output_files = list(payload.get("output_files") or [])
        if not output_files:
            raise ValueError(f"Prepared manifest {manifest_path} does not include output_files.")
        output_file = output_files[0]
        source_path = Path(str(output_file.get("path") or "")).expanduser()
        if not source_path.is_absolute():
            source_path = (root / source_path).resolve()
        else:
            source_path = source_path.resolve()
        if not source_path.exists():
            raise FileNotFoundError(
                f"Prepared output file missing for manifest {manifest_path}: {source_path}"
            )
        expected_checksum = str(output_file.get("checksum_sha256") or "")
        actual_checksum = self._sha256_file(source_path)
        if expected_checksum and expected_checksum != actual_checksum:
            raise ValueError(
                f"Prepared output checksum mismatch for {source_path}: "
                f"manifest={expected_checksum} actual={actual_checksum}."
            )
        return ReadyDatasetManifest(
            county_id=county_id,
            tax_year=tax_year,
            dataset_type=dataset_type,
            source_file_path=str(source_path),
            source_checksum=actual_checksum,
            manifest_path=str(manifest_path),
        )

    def _sha256_file(self, path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()
