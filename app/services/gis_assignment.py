from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class SpatialAssignmentCandidate:
    parcel_id: str
    tax_year: int
    taxing_unit_id: str
    taxing_unit_boundary_id: str
    unit_type_code: str
    boundary_scope: str
    match_basis: str
    assignment_confidence: float
    overlap_ratio: float | None
    centroid_within: bool
    polygon_contained: bool
    parcel_geometry_source: str


class GISAssignmentService:
    def fetch_spatial_candidates(
        self,
        connection: Any,
        *,
        parcel_id: str,
        tax_year: int,
        unit_type_codes: list[str] | None = None,
        boundary_scopes: list[str] | None = None,
    ) -> list[SpatialAssignmentCandidate]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  parcel_id,
                  tax_year,
                  taxing_unit_id,
                  taxing_unit_boundary_id,
                  unit_type_code,
                  boundary_scope,
                  match_basis,
                  assignment_confidence,
                  overlap_ratio,
                  centroid_within,
                  polygon_contained,
                  parcel_geometry_source
                FROM dwellio_spatial_assignment_candidates(%s, %s, %s, %s)
                """,
                (parcel_id, tax_year, unit_type_codes, boundary_scopes),
            )
            rows = cursor.fetchall()

        return [
            SpatialAssignmentCandidate(
                parcel_id=row["parcel_id"],
                tax_year=row["tax_year"],
                taxing_unit_id=row["taxing_unit_id"],
                taxing_unit_boundary_id=row["taxing_unit_boundary_id"],
                unit_type_code=row["unit_type_code"],
                boundary_scope=row["boundary_scope"],
                match_basis=row["match_basis"],
                assignment_confidence=float(row["assignment_confidence"]),
                overlap_ratio=(
                    float(row["overlap_ratio"]) if row["overlap_ratio"] is not None else None
                ),
                centroid_within=bool(row["centroid_within"]),
                polygon_contained=bool(row["polygon_contained"]),
                parcel_geometry_source=row["parcel_geometry_source"],
            )
            for row in rows
        ]

    def select_preferred_assignments(
        self,
        candidates: list[SpatialAssignmentCandidate],
    ) -> list[SpatialAssignmentCandidate]:
        preferred: dict[str, SpatialAssignmentCandidate] = {}
        for candidate in sorted(candidates, key=self._sort_key, reverse=True):
            if candidate.unit_type_code not in preferred:
                preferred[candidate.unit_type_code] = candidate
        return list(preferred.values())

    def build_assignment_rows(
        self,
        *,
        parcel_id: str,
        tax_year: int,
        candidates: list[SpatialAssignmentCandidate],
        source_system_id: str | None = None,
        import_batch_id: str | None = None,
        job_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for candidate in self.select_preferred_assignments(candidates):
            rows.append(
                {
                    "parcel_id": parcel_id,
                    "tax_year": tax_year,
                    "taxing_unit_id": candidate.taxing_unit_id,
                    "assignment_method": "gis",
                    "assignment_confidence": round(candidate.assignment_confidence, 4),
                    "is_primary": False,
                    "source_system_id": source_system_id,
                    "import_batch_id": import_batch_id,
                    "job_run_id": job_run_id,
                    "source_record_hash": None,
                    "notes": self._serialize_notes(
                        {
                            "unit_type_code": candidate.unit_type_code,
                            "boundary_scope": candidate.boundary_scope,
                            "match_basis": candidate.match_basis,
                            "overlap_ratio": candidate.overlap_ratio,
                            "centroid_within": candidate.centroid_within,
                            "polygon_contained": candidate.polygon_contained,
                            "parcel_geometry_source": candidate.parcel_geometry_source,
                            "taxing_unit_boundary_id": candidate.taxing_unit_boundary_id,
                        }
                    ),
                }
            )
        return rows

    def summarize_candidates(
        self,
        candidates: list[SpatialAssignmentCandidate],
    ) -> dict[str, Any]:
        selected = self.select_preferred_assignments(candidates)
        return {
            "candidate_count": len(candidates),
            "selected_count": len(selected),
            "unit_type_codes": sorted({candidate.unit_type_code for candidate in selected}),
            "match_bases": sorted({candidate.match_basis for candidate in candidates}),
        }

    def _sort_key(self, candidate: SpatialAssignmentCandidate) -> tuple[int, float, float, str]:
        return (
            self._match_rank(candidate.match_basis),
            candidate.assignment_confidence,
            candidate.overlap_ratio or 0.0,
            candidate.taxing_unit_id,
        )

    def _match_rank(self, match_basis: str) -> int:
        ranks = {
            "parcel_polygon_contained": 3,
            "parcel_polygon_overlap": 2,
            "parcel_centroid_within": 1,
        }
        return ranks.get(match_basis, 0)

    def _serialize_notes(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, default=self._json_default)

    def _json_default(self, value: Any) -> str:
        if isinstance(value, UUID):
            return str(value)
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
