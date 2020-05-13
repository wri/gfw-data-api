#!/bin/bash

set -e

## Add GFW specific layers
#psql -c "ALTER TABLE $DATASET.$VERSION ADD COLUMN ${GEOMETRY_NAME}_wm geometry(MultiPolygon,3857);
#         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_area__ha NUMERIC;
#         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geostore_id UUID;
#         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geojson TEXT;
#         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_bbox BOX2D;"
#
#
## Set storage to external for faster querying
## http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
#psql -c "ALTER TABLE $DATASET.$VERSION ALTER COLUMN $GEOMETRY_NAME SET STORAGE EXTERNAL;
#         ALTER TABLE $DATASET.$VERSION ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;"


# Repair geometries
psql -c "
WITH a AS (
	SELECT
		$FID_NAME,
		st_makevalid($GEOMETRY_NAME) AS $GEOMETRY_NAME
	FROM
		$DATASET.$VERSION
),
b AS (
	SELECT
		$FID_NAME,CASE
			WHEN st_geometrytype($GEOMETRY_NAME) = 'ST_GeometryCollection' :: TEXT THEN st_collectionextract($GEOMETRY_NAME, 3)
			WHEN st_geometrytype($GEOMETRY_NAME) = 'ST_Polygon'
			OR st_geometrytype($GEOMETRY_NAME) = 'ST_MultiPolygon' THEN $GEOMETRY_NAME
		END AS $GEOMETRY_NAME
	FROM
		a
)

UPDATE
	$DATASET.$VERSION
SET
	$GEOMETRY_NAME = b.$GEOMETRY_NAME
FROM
	b
WHERE
	$DATASET.$VERSION.$FID_NAME = b.$FID_NAME;"

# Update GFW columns
psql -c "UPDATE $DATASET.$VERSION SET ${GEOMETRY_NAME}_wm = ST_Transform($GEOMETRY_NAME, 3857),
                                      gfw_area__ha = ST_area($GEOMETRY_NAME::geography)/10000,
                                      gfw_geostore_id = md5(ST_asgeojson($GEOMETRY_NAME))::uuid,
                                      gfw_geojson = ST_asgeojson($GEOMETRY_NAME),
                                      gfw_bbox = box2d($GEOMETRY_NAME);"

# Create indices
psql -c "CREATE INDEX IF NOT EXISTS ${VERSION}_${GEOMETRY_NAME}_id_idx
     ON $DATASET.$VERSION USING gist
     (${GEOMETRY_NAME});"
psql -c "CREATE INDEX IF NOT EXISTS ${VERSION}_${GEOMETRY_NAME}_wm_id_idx
     ON $DATASET.$VERSION USING gist
     (${GEOMETRY_NAME}_wm);"
psql -c "CREATE INDEX IF NOT EXISTS ${VERSION}_gfw_geostore_id_idx
     ON $DATASET.$VERSION USING hash
     (gfw_geostore_id);"

# Inherit from geostore
psql -c "ALTER TABLE $DATASET.$VERSION INHERIT public.geostore;"