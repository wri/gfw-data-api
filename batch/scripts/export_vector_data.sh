#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -f | --local_file
# -F | --format
# -T | --target
# -w | --where
# -C | --column_names



ME=$(basename "$0")
. get_arguments.sh "$@"

echo "OGR2OGR: Export table \"${DATASET}\".\"${VERSION}\" using format ${FORMAT}"
ogr2ogr -f "$FORMAT" "$LOCAL_FILE" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
      -sql "SELECT $COLUMN_NAMES FROM \"${DATASET}\".\"${VERSION}\" $WHERE"

echo "AWSCLI: COPY DATA FROM $LOCAL_FILE TO $TARGET"
aws s3 cp "$LOCAL_FILE" "$TARGET"

echo "Done"