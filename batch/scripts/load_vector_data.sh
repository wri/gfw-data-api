#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
ME=$(basename "$0")
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM S3 to STDOUT"
aws s3 cp "$SRC" "$LOCAL_FILE"

echo "OGR2OGR: Import \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$LOCAL_FILE" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -t_srs EPSG:4326 --config PG_USE_COPY YES \
     -update -append -makevalid