#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -l | --source_layer
# -f | --local_file
# -X | --zipped

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM $SRC TO $LOCAL_FILE"
aws s3 cp "$SRC" "$LOCAL_FILE"

# use virtual GDAL vsizip wrapper for ZIP files
# TODO: [GTC-661] Allow for a more flexible file structure inside the ZIP file
#  the current implementation assumes that the file sits at the root level of the zip file
#  and can't be in sub directory.
if [ "${ZIPPED}" == "True" ]; then
  LOCAL_FILE="/vsizip/${LOCAL_FILE}"
fi

echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". ALTER COLUMN gfw_geostore_id DROP NOT NULL IF gfw_geostore_id NOT NULL"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN gfw_geostore_id DROP NOT NULL IF gfw_geostore_id NOT NULL;"

echo "OGR2OGR: Import \"${DATASET}\".\"${VERSION}\" from ${LOCAL_FILE} ${SRC_LAYER}"
ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$LOCAL_FILE" "$SRC_LAYER" \
     -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -t_srs EPSG:4326 --config PG_USE_COPY YES \
     -update -append -makevalid