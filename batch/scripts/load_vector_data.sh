#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
. get_arguments.sh "$@"

echo "OGR2OGR: Load data for ${DATASET}.${VERSION} from ${SRC} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$SRC" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$VERSION" \
     -lco SCHEMA="$DATASET" -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 --config PG_USE_COPY YES \
     -update -append -makevalid