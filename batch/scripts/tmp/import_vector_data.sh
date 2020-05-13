#!/bin/bash

set -e

echo "OGR2OGR: Import ${DATASET}.${VERSION} from ${SRC} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$SRC" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$VERSION" \
     -lco SCHEMA="$DATASET" -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 -limit 0
#     --config PG_USE_COPY YES -makevalid


echo "PSQL: Add GFW specific layers"
psql -c "ALTER TABLE $DATASET.$VERSION ADD COLUMN ${GEOMETRY_NAME}_wm geometry(MultiPolygon,3857);
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_area__ha NUMERIC;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geostore_id UUID;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_geojson TEXT;
         ALTER TABLE $DATASET.$VERSION ADD COLUMN gfw_bbox BOX2D;"


# http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
echo "PSQL: Set storage to external for faster querying"
psql -c "ALTER TABLE $DATASET.$VERSION ALTER COLUMN $GEOMETRY_NAME SET STORAGE EXTERNAL;
         ALTER TABLE $DATASET.$VERSION ALTER COLUMN ${GEOMETRY_NAME}_wm SET STORAGE EXTERNAL;"
