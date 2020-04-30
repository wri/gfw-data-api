#!/bin/bash

set -e

psql -c "
CREATE MATERIALIZED VIEW $DATASET.{$VERSION}__1x1
	WITH a AS (
			SELECT $FID_NAME
				,gfw_grid_1x1_id
				,gfw_grid_10x10_id
				,st_makevalid(st_intersection(w.$GEOMETRY_NAME, g.geom)) AS $GEOMETRY_NAME
			FROM $DATASET.$VERSION w
				,gfw_grid_1x1 g
			WHERE st_intersects(w.$GEOMETRY_NAME, g.geom)
			)
		,b AS (
			SELECT $FID_NAME
				,gfw_grid_1x1_id
				,gfw_grid_10x10_id
				,CASE 
					WHEN st_geometrytype($GEOMETRY_NAME) = 'ST_GeometryCollection'::TEXT
						THEN st_collectionextract($GEOMETRY_NAME, 3)
					ELSE $GEOMETRY_NAME
					END AS $GEOMETRY_NAME
			FROM a
			)

SELECT $FID_NAME
	,gfw_grid_1x1_id
	,gfw_grid_10x10_id
	,$GEOMETRY_NAME
FROM b
WHERE st_geometrytype($GEOMETRY_NAME) = 'ST_Polygon'
	OR st_geometrytype($GEOMETRY_NAME) = 'ST_MultiPolygon'
GROUP BY $FID_NAME;"


# Create indices
psql -c "CREATE INDEX IF NOT EXISTS ${VERSION}__1x1_${GEOMETRY_NAME}_id_idx
     ON $DATASET.${VERSION}__1x1 USING gist
     (${GEOMETRY_NAME});"