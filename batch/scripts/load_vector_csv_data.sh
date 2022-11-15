#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

# Set gfw_geostore_id nullable if it's not, such as if we're appending
echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". ALTER COLUMN gfw_geostore_id DROP NOT NULL IF gfw_geostore_id NOT NULL"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN gfw_geostore_id DROP NOT NULL IF gfw_geostore_id NOT NULL;"

for uri in "${SRC[@]}"; do
  # convert to vsis3 protocol for ogr
  VSIS3_URI=$(sed 's/s3:\//\/vsis3/g' <<< "$uri")

  # since CSV has no inherent concept of CRS, just manually set source CRS (s_srs) to EPSG:4326
  ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$VSIS3_URI" -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO \
     -t_srs EPSG:4326 -s_srs EPSG:4326 --config PG_USE_COPY YES \
     -update -append -makevalid
done