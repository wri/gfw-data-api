#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
. ./get_arguments.sh "@"

echo "OGR2OGR: Import ${DATASET}.${VERSION} from ${SRC} ${SRC_LAYER}"
# Create schema only, using ogr2ogr
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$SRC" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$VERSION" \
     -lco SCHEMA="$DATASET" -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 -limit 0

# Set storage to external for faster querying
# http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
echo "PSQL: ALTER TABLE. Set storage external"
psql -c "ALTER TABLE $DATASET.$VERSION ALTER COLUMN $GEOMETRY_NAME SET STORAGE EXTERNAL;"

echo "DONE"