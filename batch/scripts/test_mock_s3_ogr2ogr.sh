#!/bin/bash

#set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM S3 to STDOUT"
# shellcheck disable=SC2086
aws s3 cp "$SRC" "$LOCAL_FILE"

echo "OGR2OGR: Import ${DATASET}.${VERSION} from ${LOCAL_FILE} ${SRC_LAYER}"
# Create schema only, using ogr2ogr
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$LOCAL_FILE" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$VERSION" \
     -lco SCHEMA="$DATASET" -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 -limit 0
