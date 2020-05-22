#!/bin/bash

set -e


# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM $SRC TO $LOCAL_FILE"
aws s3 cp "$SRC" "$LOCAL_FILE"

echo "OGR2OGR: Create table schema for \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$LOCAL_FILE" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -lco GEOMETRY_NAME="$GEOMETRY_NAME" -lco SPATIAL_INDEX=NONE -lco FID="$FID_NAME" \
     -t_srs EPSG:4326 -limit 0


# Set storage to external for faster querying
# http://blog.cleverelephant.ca/2018/09/postgis-external-storage.html
echo "PSQL: ALTER TABLE. Set storage external"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN $GEOMETRY_NAME SET STORAGE EXTERNAL;"

echo "DONE"
