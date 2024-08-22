#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -f | --local_file
# -F | --format
# -T | --target
# -C | --column_names
# -X | --zipped

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)
# -i | --fid_name (get_arguments.sh specifies default)
#      --simplify
# -w | --where

ME=$(basename "$0")
. get_arguments.sh "$@"

GEOMETRY_EXPRESSION="${GEOMETRY_NAME}"
if [ "${SIMPLIFY_GEOMETRY}" == "TRUE" ]; then
  GEOMETRY_EXPRESSION="ST_RemoveRepeatedPoints(${GEOMETRY_NAME}, 1)"
fi


echo "OGR2OGR: Export table \"${DATASET}\".\"${VERSION}\" using format ${FORMAT}"
echo "Export columns $COLUMN_NAMES"
ogr2ogr -f "$FORMAT" "$LOCAL_FILE" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
      -sql "SELECT $COLUMN_NAMES, ${GEOMETRY_EXPRESSION} AS $GEOMETRY_NAME FROM \"${DATASET}\".\"${VERSION}\" $WHERE" -geomfield "${GEOMETRY_NAME}" \
      -lco FID="$FID_NAME"

if [ "${ZIPPED}" == "True" ]; then
  BASE_NAME="${LOCAL_FILE%.*}"
  LOCAL_FILE="${BASE_NAME}.zip"
  find . -name "${BASE_NAME}.*" | zip -@ -j "${LOCAL_FILE}"
fi

echo "AWSCLI: COPY DATA FROM $LOCAL_FILE TO $TARGET"
aws s3 cp "$LOCAL_FILE" "$TARGET"

echo "Done"