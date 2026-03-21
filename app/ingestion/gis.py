from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.county_adapters.common.base import StagingRow
from app.utils.hashing import sha256_text


@dataclass(frozen=True)
class GeometrySummary:
    geometry_type: str
    centroid_wkt: str | None
    bbox: tuple[float, float, float, float] | None
    validation_issues: tuple[str, ...]


def build_gis_staging_rows(
    *,
    content: bytes | str | dict[str, Any],
    table_name: str = "stg_gis_raw",
) -> list[StagingRow]:
    rows: list[StagingRow] = []
    for feature in load_geojson_features(content):
        canonical_json = json.dumps(feature, sort_keys=True)
        rows.append(
            StagingRow(
                table_name=table_name,
                raw_payload=feature,
                row_hash=sha256_text(canonical_json),
            )
        )
    return rows


def load_geojson_features(content: bytes | str | dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(content, bytes):
        raw = json.loads(content.decode("utf-8"))
    elif isinstance(content, str):
        raw = json.loads(content)
    else:
        raw = content

    if isinstance(raw, list):
        return [_coerce_feature(item) for item in raw]
    if not isinstance(raw, dict):
        raise ValueError("Expected a GeoJSON object, feature list, or feature collection.")

    geojson_type = raw.get("type")
    if geojson_type == "FeatureCollection":
        return [_coerce_feature(item) for item in raw.get("features", [])]
    if geojson_type == "Feature":
        return [_coerce_feature(raw)]
    if "coordinates" in raw:
        return [_coerce_feature({"type": "Feature", "properties": {}, "geometry": raw})]

    raise ValueError("Unsupported GeoJSON payload.")


def summarize_geometry(geometry: dict[str, Any]) -> GeometrySummary:
    geometry_type = str(geometry.get("type", "Unknown"))
    validation_issues = tuple(_geometry_validation_issues(geometry))
    bbox = _geometry_bbox(geometry)
    centroid_wkt = _geometry_centroid_wkt(geometry)
    return GeometrySummary(
        geometry_type=geometry_type,
        centroid_wkt=centroid_wkt,
        bbox=bbox,
        validation_issues=validation_issues,
    )


def build_parcel_geometry_record(
    *,
    feature: dict[str, Any],
    parcel_id: str,
    tax_year: int,
    geometry_role: str,
    source_system_id: str | None = None,
    import_batch_id: str | None = None,
) -> dict[str, Any]:
    geometry = _feature_geometry(feature)
    summary = summarize_geometry(geometry)
    return {
        "parcel_id": parcel_id,
        "tax_year": tax_year,
        "geometry_role": geometry_role,
        "geom_wkt": geometry_to_wkt(
            geometry, promote_polygon_to_multi=geometry_role != "parcel_centroid"
        ),
        "centroid_wkt": summary.centroid_wkt,
        "source_system_id": source_system_id,
        "import_batch_id": import_batch_id,
        "source_record_hash": _feature_hash(feature),
        "feature_id": feature.get("id"),
        "properties": dict(feature.get("properties", {})),
        "validation_issues": list(summary.validation_issues),
        "bbox": list(summary.bbox) if summary.bbox is not None else None,
    }


def build_taxing_unit_boundary_record(
    *,
    feature: dict[str, Any],
    taxing_unit_id: str,
    tax_year: int,
    boundary_scope: str = "service_area",
    source_system_id: str | None = None,
    import_batch_id: str | None = None,
) -> dict[str, Any]:
    geometry = _feature_geometry(feature)
    summary = summarize_geometry(geometry)
    properties = dict(feature.get("properties", {}))
    return {
        "taxing_unit_id": taxing_unit_id,
        "tax_year": tax_year,
        "boundary_scope": boundary_scope,
        "boundary_name": properties.get("boundary_name") or properties.get("unit_name"),
        "geom_wkt": geometry_to_wkt(geometry, promote_polygon_to_multi=True),
        "centroid_wkt": summary.centroid_wkt,
        "source_system_id": source_system_id,
        "import_batch_id": import_batch_id,
        "source_record_hash": _feature_hash(feature),
        "feature_id": feature.get("id"),
        "properties": properties,
        "validation_issues": list(summary.validation_issues),
        "bbox": list(summary.bbox) if summary.bbox is not None else None,
    }


def geometry_to_wkt(geometry: dict[str, Any], *, promote_polygon_to_multi: bool = False) -> str:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if geometry_type == "Point":
        lon, lat = _coerce_position(coordinates)
        return f"POINT ({_fmt(lon)} {_fmt(lat)})"

    if geometry_type == "Polygon":
        polygon_wkt = _polygon_to_wkt(coordinates)
        if not promote_polygon_to_multi:
            return f"POLYGON {polygon_wkt}"
        return f"MULTIPOLYGON ({polygon_wkt})"

    if geometry_type == "MultiPolygon":
        polygons = ", ".join(_polygon_to_wkt(polygon) for polygon in coordinates)
        return f"MULTIPOLYGON ({polygons})"

    raise ValueError(f"Unsupported geometry type for WKT conversion: {geometry_type}")


def _coerce_feature(raw_feature: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw_feature, dict):
        raise ValueError("Expected each feature to be a mapping.")
    if raw_feature.get("type") != "Feature":
        raise ValueError("Expected GeoJSON Feature records.")
    if "geometry" not in raw_feature:
        raise ValueError("GeoJSON feature is missing a geometry.")
    return {
        "type": "Feature",
        "id": raw_feature.get("id"),
        "properties": dict(raw_feature.get("properties", {})),
        "geometry": raw_feature.get("geometry"),
    }


def _feature_geometry(feature: dict[str, Any]) -> dict[str, Any]:
    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("Feature geometry must be a mapping.")
    return geometry


def _feature_hash(feature: dict[str, Any]) -> str:
    return sha256_text(json.dumps(feature, sort_keys=True))


def _geometry_validation_issues(geometry: dict[str, Any]) -> list[str]:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    issues: list[str] = []

    if geometry_type not in {"Point", "Polygon", "MultiPolygon"}:
        issues.append("unsupported_geometry_type")
        return issues

    if coordinates in (None, []):
        issues.append("missing_coordinates")
        return issues

    if geometry_type == "Point":
        try:
            _coerce_position(coordinates)
        except (TypeError, ValueError):
            issues.append("invalid_point_coordinates")
        return issues

    polygons = [coordinates] if geometry_type == "Polygon" else list(coordinates)
    if not polygons:
        issues.append("missing_polygon_coordinates")
        return issues

    for polygon in polygons:
        if not polygon:
            issues.append("missing_linear_ring")
            continue
        for ring in polygon:
            if len(ring) < 4:
                issues.append("ring_too_short")
                continue
            normalized_ring = [_coerce_position(point) for point in ring]
            if normalized_ring[0] != normalized_ring[-1]:
                issues.append("ring_not_closed")

    return sorted(set(issues))


def _geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    positions = list(_iter_positions(geometry))
    if not positions:
        return None

    longitudes = [position[0] for position in positions]
    latitudes = [position[1] for position in positions]
    return (
        min(longitudes),
        min(latitudes),
        max(longitudes),
        max(latitudes),
    )


def _geometry_centroid_wkt(geometry: dict[str, Any]) -> str | None:
    geometry_type = geometry.get("type")

    if geometry_type == "Point":
        lon, lat = _coerce_position(geometry.get("coordinates"))
        return f"POINT ({_fmt(lon)} {_fmt(lat)})"

    if geometry_type not in {"Polygon", "MultiPolygon"}:
        return None

    polygons = (
        [geometry.get("coordinates")] if geometry_type == "Polygon" else geometry.get("coordinates")
    )
    best_centroid: tuple[float, float] | None = None
    best_area = -1.0
    for polygon in polygons:
        if not polygon:
            continue
        exterior_ring = [_coerce_position(point) for point in polygon[0]]
        area = abs(_ring_signed_area(exterior_ring))
        if area == 0:
            continue
        centroid = _ring_centroid(exterior_ring)
        if area > best_area:
            best_area = area
            best_centroid = centroid

    if best_centroid is None:
        bbox = _geometry_bbox(geometry)
        if bbox is None:
            return None
        centroid = ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)
    else:
        centroid = best_centroid

    return f"POINT ({_fmt(centroid[0])} {_fmt(centroid[1])})"


def _iter_positions(geometry: dict[str, Any]):
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if geometry_type == "Point":
        yield _coerce_position(coordinates)
        return

    if geometry_type == "Polygon":
        for ring in coordinates:
            for point in ring:
                yield _coerce_position(point)
        return

    if geometry_type == "MultiPolygon":
        for polygon in coordinates:
            for ring in polygon:
                for point in ring:
                    yield _coerce_position(point)


def _coerce_position(value: Any) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        raise ValueError("Expected a longitude/latitude position.")
    return float(value[0]), float(value[1])


def _polygon_to_wkt(polygon: list[Any]) -> str:
    rings = ", ".join(_ring_to_wkt(ring) for ring in polygon)
    return f"({rings})"


def _ring_to_wkt(ring: list[Any]) -> str:
    coordinates = ", ".join(
        f"{_fmt(lon)} {_fmt(lat)}" for lon, lat in (_coerce_position(point) for point in ring)
    )
    return f"({coordinates})"


def _ring_signed_area(ring: list[tuple[float, float]]) -> float:
    area = 0.0
    for index in range(len(ring) - 1):
        x1, y1 = ring[index]
        x2, y2 = ring[index + 1]
        area += (x1 * y2) - (x2 * y1)
    return area / 2.0


def _ring_centroid(ring: list[tuple[float, float]]) -> tuple[float, float]:
    area = _ring_signed_area(ring)
    if area == 0:
        raise ValueError("Cannot calculate centroid for a zero-area ring.")

    factor = 0.0
    centroid_x = 0.0
    centroid_y = 0.0
    for index in range(len(ring) - 1):
        x1, y1 = ring[index]
        x2, y2 = ring[index + 1]
        factor = (x1 * y2) - (x2 * y1)
        centroid_x += (x1 + x2) * factor
        centroid_y += (y1 + y2) * factor
    denominator = 6.0 * area
    return centroid_x / denominator, centroid_y / denominator


def _fmt(value: float) -> str:
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text if text else "0"
