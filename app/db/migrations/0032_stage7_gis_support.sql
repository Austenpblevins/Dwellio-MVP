ALTER TABLE taxing_unit_boundaries
  ADD COLUMN IF NOT EXISTS centroid geometry(Point, 4326),
  ADD COLUMN IF NOT EXISTS area_sqft numeric;

CREATE INDEX IF NOT EXISTS idx_parcel_geometries_current_role
  ON parcel_geometries(parcel_id, tax_year, geometry_role, is_current);

CREATE INDEX IF NOT EXISTS idx_taxing_unit_boundaries_scope_lookup
  ON taxing_unit_boundaries(tax_year, boundary_scope, taxing_unit_id, is_current);

CREATE OR REPLACE FUNCTION dwellio_geometry_to_4326(input_geom geometry)
RETURNS geometry
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    CASE
      WHEN input_geom IS NULL THEN NULL
      WHEN ST_SRID(input_geom) = 0 THEN ST_SetSRID(ST_Force2D(input_geom), 4326)
      WHEN ST_SRID(input_geom) = 4326 THEN ST_Force2D(input_geom)
      ELSE ST_Transform(ST_Force2D(input_geom), 4326)
    END
$$;

CREATE OR REPLACE FUNCTION dwellio_normalize_geometry(
  input_geom geometry,
  expected_role text DEFAULT NULL
)
RETURNS geometry
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  working geometry;
BEGIN
  IF input_geom IS NULL THEN
    RETURN NULL;
  END IF;

  working := ST_MakeValid(dwellio_geometry_to_4326(input_geom));

  IF expected_role = 'polygon' THEN
    working := ST_CollectionExtract(working, 3);
    IF GeometryType(working) = 'POLYGON' THEN
      working := ST_Multi(working);
    END IF;
  ELSIF expected_role = 'point' THEN
    IF GeometryType(working) <> 'POINT' THEN
      working := ST_PointOnSurface(working);
    END IF;
  END IF;

  RETURN working;
END
$$;

CREATE OR REPLACE FUNCTION dwellio_geometry_anchor_point(input_geom geometry)
RETURNS geometry(Point, 4326)
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    CASE
      WHEN input_geom IS NULL OR ST_IsEmpty(input_geom) THEN NULL
      ELSE ST_PointOnSurface(dwellio_normalize_geometry(input_geom))::geometry(Point, 4326)
    END
$$;

CREATE OR REPLACE FUNCTION dwellio_geometry_area_sqft(input_geom geometry)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    CASE
      WHEN input_geom IS NULL OR ST_IsEmpty(input_geom) THEN NULL
      WHEN ST_Dimension(dwellio_normalize_geometry(input_geom, 'polygon')) < 2 THEN 0
      ELSE ROUND(
        (
          ST_Area(dwellio_normalize_geometry(input_geom, 'polygon')::geography) * 10.7639104167097
        )::numeric,
        2
      )
    END
$$;

CREATE OR REPLACE FUNCTION dwellio_geometry_validation_issues(
  input_geom geometry,
  expected_role text DEFAULT NULL
)
RETURNS text[]
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  issues text[] := ARRAY[]::text[];
  working geometry;
BEGIN
  IF input_geom IS NULL THEN
    RETURN ARRAY['missing_geometry'];
  END IF;

  IF ST_IsEmpty(input_geom) THEN
    issues := array_append(issues, 'empty_geometry');
  END IF;

  IF ST_SRID(input_geom) = 0 THEN
    issues := array_append(issues, 'missing_srid');
  END IF;

  IF NOT ST_IsValid(input_geom) THEN
    issues := array_append(issues, 'invalid_topology:' || ST_IsValidReason(input_geom));
  END IF;

  working := dwellio_normalize_geometry(input_geom, expected_role);
  IF working IS NULL OR ST_IsEmpty(working) THEN
    issues := array_append(issues, 'normalization_failed');
    RETURN issues;
  END IF;

  IF expected_role = 'polygon' AND ST_Dimension(working) < 2 THEN
    issues := array_append(issues, 'not_polygonal');
  END IF;

  IF expected_role = 'point' AND GeometryType(working) <> 'POINT' THEN
    issues := array_append(issues, 'not_point');
  END IF;

  RETURN issues;
END
$$;

CREATE OR REPLACE FUNCTION dwellio_set_geometry_derived_fields()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_geom geometry;
  normalized_centroid geometry(Point, 4326);
BEGIN
  IF NEW.geom IS NULL THEN
    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'parcel_geometries' THEN
    IF NEW.geometry_role = 'parcel_centroid' THEN
      normalized_geom := dwellio_normalize_geometry(NEW.geom, 'point');
      IF normalized_geom IS NULL OR ST_IsEmpty(normalized_geom) THEN
        RAISE EXCEPTION 'Parcel centroid geometry must normalize to a point.';
      END IF;

      NEW.geom := normalized_geom;
      NEW.centroid := dwellio_geometry_anchor_point(normalized_geom);
      NEW.area_sqft := 0;
      RETURN NEW;
    END IF;

    normalized_geom := dwellio_normalize_geometry(NEW.geom, 'polygon');
    IF normalized_geom IS NULL OR ST_IsEmpty(normalized_geom) THEN
      RAISE EXCEPTION 'Parcel polygon geometry must normalize to a polygon.';
    END IF;

    normalized_centroid := COALESCE(
      dwellio_normalize_geometry(NEW.centroid, 'point')::geometry(Point, 4326),
      dwellio_geometry_anchor_point(normalized_geom)
    );

    NEW.geom := normalized_geom;
    NEW.centroid := normalized_centroid;
    NEW.area_sqft := dwellio_geometry_area_sqft(normalized_geom);
    RETURN NEW;
  END IF;

  IF TG_TABLE_NAME = 'taxing_unit_boundaries' THEN
    normalized_geom := dwellio_normalize_geometry(NEW.geom, 'polygon');
    IF normalized_geom IS NULL OR ST_IsEmpty(normalized_geom) THEN
      RAISE EXCEPTION 'Taxing unit boundary geometry must normalize to a polygon.';
    END IF;

    NEW.geom := normalized_geom::geometry(MultiPolygon, 4326);
    NEW.centroid := dwellio_geometry_anchor_point(normalized_geom);
    NEW.area_sqft := dwellio_geometry_area_sqft(normalized_geom);
    RETURN NEW;
  END IF;

  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS set_parcel_geometries_derived_fields ON parcel_geometries;
CREATE TRIGGER set_parcel_geometries_derived_fields
BEFORE INSERT OR UPDATE ON parcel_geometries
FOR EACH ROW
EXECUTE FUNCTION dwellio_set_geometry_derived_fields();

DROP TRIGGER IF EXISTS set_taxing_unit_boundaries_derived_fields ON taxing_unit_boundaries;
CREATE TRIGGER set_taxing_unit_boundaries_derived_fields
BEFORE INSERT OR UPDATE ON taxing_unit_boundaries
FOR EACH ROW
EXECUTE FUNCTION dwellio_set_geometry_derived_fields();

UPDATE parcel_geometries
SET
  geom = CASE
    WHEN geometry_role = 'parcel_centroid' THEN dwellio_normalize_geometry(geom, 'point')
    ELSE dwellio_normalize_geometry(geom, 'polygon')
  END,
  centroid = CASE
    WHEN geometry_role = 'parcel_centroid' THEN dwellio_geometry_anchor_point(geom)
    ELSE COALESCE(
      dwellio_normalize_geometry(centroid, 'point')::geometry(Point, 4326),
      dwellio_geometry_anchor_point(geom)
    )
  END,
  area_sqft = CASE
    WHEN geometry_role = 'parcel_centroid' THEN 0
    ELSE dwellio_geometry_area_sqft(geom)
  END
WHERE geom IS NOT NULL;

UPDATE taxing_unit_boundaries
SET
  geom = dwellio_normalize_geometry(geom, 'polygon')::geometry(MultiPolygon, 4326),
  centroid = dwellio_geometry_anchor_point(geom),
  area_sqft = dwellio_geometry_area_sqft(geom)
WHERE geom IS NOT NULL;

CREATE OR REPLACE FUNCTION dwellio_spatial_assignment_candidates(
  p_parcel_id uuid,
  p_tax_year integer,
  p_unit_type_codes text[] DEFAULT NULL,
  p_boundary_scopes text[] DEFAULT NULL
)
RETURNS TABLE (
  parcel_id uuid,
  tax_year integer,
  taxing_unit_id uuid,
  taxing_unit_boundary_id uuid,
  unit_type_code text,
  boundary_scope text,
  match_basis text,
  assignment_confidence numeric(5, 4),
  overlap_ratio numeric(10, 6),
  centroid_within boolean,
  polygon_contained boolean,
  parcel_geometry_source text
)
LANGUAGE sql
STABLE
AS $$
WITH parcel_shape AS (
  SELECT
    p.parcel_id,
    p.tax_year,
    COALESCE(
      (
        SELECT dwellio_normalize_geometry(pg.geom, 'polygon')
        FROM parcel_geometries pg
        WHERE pg.parcel_id = p.parcel_id
          AND pg.tax_year = p_tax_year
          AND pg.is_current
          AND pg.geometry_role = 'parcel_polygon'
        ORDER BY pg.created_at DESC
        LIMIT 1
      ),
      CASE
        WHEN p.geom IS NOT NULL THEN dwellio_normalize_geometry(p.geom, 'polygon')
        ELSE NULL
      END
    ) AS parcel_geom,
    COALESCE(
      (
        SELECT COALESCE(
          dwellio_normalize_geometry(pg.centroid, 'point')::geometry(Point, 4326),
          dwellio_geometry_anchor_point(pg.geom)
        )
        FROM parcel_geometries pg
        WHERE pg.parcel_id = p.parcel_id
          AND pg.tax_year = p_tax_year
          AND pg.is_current
          AND pg.geometry_role IN ('parcel_centroid', 'parcel_polygon')
        ORDER BY
          CASE
            WHEN pg.geometry_role = 'parcel_centroid' THEN 0
            ELSE 1
          END,
          pg.created_at DESC
        LIMIT 1
      ),
      CASE
        WHEN p.longitude IS NOT NULL AND p.latitude IS NOT NULL THEN
          ST_SetSRID(
            ST_MakePoint(p.longitude::double precision, p.latitude::double precision),
            4326
          )::geometry(Point, 4326)
        WHEN p.geom IS NOT NULL THEN dwellio_geometry_anchor_point(p.geom)
        ELSE NULL
      END
    ) AS parcel_centroid,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM parcel_geometries pg
        WHERE pg.parcel_id = p.parcel_id
          AND pg.tax_year = p_tax_year
          AND pg.is_current
          AND pg.geometry_role = 'parcel_polygon'
      ) THEN 'parcel_geometries'
      WHEN p.geom IS NOT NULL THEN 'parcels.geom'
      WHEN p.longitude IS NOT NULL AND p.latitude IS NOT NULL THEN 'parcel_lat_lon'
      ELSE 'missing'
    END AS parcel_geometry_source
  FROM parcels p
  WHERE p.parcel_id = p_parcel_id
    AND p.tax_year = p_tax_year
)
SELECT
  ps.parcel_id,
  p_tax_year AS tax_year,
  tu.taxing_unit_id,
  tub.taxing_unit_boundary_id,
  tu.unit_type_code,
  tub.boundary_scope,
  CASE
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_CoveredBy(ps.parcel_geom, tub.geom) THEN 'parcel_polygon_contained'
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_Intersects(ps.parcel_geom, tub.geom) THEN 'parcel_polygon_overlap'
    WHEN ps.parcel_centroid IS NOT NULL
      AND ST_Intersects(ps.parcel_centroid, tub.geom) THEN 'parcel_centroid_within'
    ELSE NULL
  END AS match_basis,
  CASE
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_CoveredBy(ps.parcel_geom, tub.geom) THEN 0.9900
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_Intersects(ps.parcel_geom, tub.geom) THEN LEAST(
        0.9800,
        GREATEST(
          0.5000,
          COALESCE(
            ST_Area(ST_Intersection(ps.parcel_geom, tub.geom)::geography)
            / NULLIF(ST_Area(ps.parcel_geom::geography), 0),
            0
          )
        )
      )
    WHEN ps.parcel_centroid IS NOT NULL
      AND ST_Intersects(ps.parcel_centroid, tub.geom) THEN 0.6000
    ELSE NULL
  END::numeric(5, 4) AS assignment_confidence,
  CASE
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_Intersects(ps.parcel_geom, tub.geom) THEN ROUND(
        (
          ST_Area(ST_Intersection(ps.parcel_geom, tub.geom)::geography)
          / NULLIF(ST_Area(ps.parcel_geom::geography), 0)
        )::numeric,
        6
      )
    ELSE NULL
  END::numeric(10, 6) AS overlap_ratio,
  CASE
    WHEN ps.parcel_centroid IS NOT NULL
      AND ST_Intersects(ps.parcel_centroid, tub.geom) THEN true
    ELSE false
  END AS centroid_within,
  CASE
    WHEN ps.parcel_geom IS NOT NULL
      AND ST_CoveredBy(ps.parcel_geom, tub.geom) THEN true
    ELSE false
  END AS polygon_contained,
  ps.parcel_geometry_source
FROM parcel_shape ps
JOIN taxing_unit_boundaries tub
  ON tub.tax_year = p_tax_year
  AND tub.is_current
JOIN taxing_units tu
  ON tu.taxing_unit_id = tub.taxing_unit_id
  AND tu.tax_year = p_tax_year
  AND tu.active_flag
WHERE (p_unit_type_codes IS NULL OR tu.unit_type_code = ANY (p_unit_type_codes))
  AND (p_boundary_scopes IS NULL OR tub.boundary_scope = ANY (p_boundary_scopes))
  AND (
    (ps.parcel_geom IS NOT NULL AND ST_Intersects(ps.parcel_geom, tub.geom))
    OR (ps.parcel_centroid IS NOT NULL AND ST_Intersects(ps.parcel_centroid, tub.geom))
  );
$$;

COMMENT ON COLUMN taxing_unit_boundaries.centroid IS 'Derived anchor point for GIS QA, labeling, and centroid-based parcel assignment fallback.';
COMMENT ON COLUMN taxing_unit_boundaries.area_sqft IS 'Derived service-area size in square feet using geography-based PostGIS calculation.';

COMMENT ON FUNCTION dwellio_normalize_geometry(geometry, text) IS 'Normalizes PostGIS geometries to SRID 4326 and expected point or polygon roles.';
COMMENT ON FUNCTION dwellio_geometry_anchor_point(geometry) IS 'Returns a stable point-on-surface anchor for parcel and taxing-unit geometries.';
COMMENT ON FUNCTION dwellio_geometry_area_sqft(geometry) IS 'Computes polygonal area in square feet for auditable GIS QA and overlap scoring.';
COMMENT ON FUNCTION dwellio_geometry_validation_issues(geometry, text) IS 'Returns lightweight validation issue codes for raw or canonical GIS geometries.';
COMMENT ON FUNCTION dwellio_spatial_assignment_candidates(uuid, integer, text[], text[]) IS 'Produces auditable spatial parcel-to-tax-unit assignment candidates for school district, MUD, and similar boundary workflows.';
