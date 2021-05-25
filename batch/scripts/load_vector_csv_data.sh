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


for uri in "${SRC[@]}"; do
  # convert to vsis3 protocol for ogr
  VSIS3_URI=$(sed 's/s3:\//\/vsis3/g' <<< "$uri")

  ogr2ogr -f "PostgreSQL" PG:"password=$PGPASSWORD host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" \
     "$VSIS3_URI" -nlt PROMOTE_TO_MULTI -nln "$DATASET.$VERSION" \
     -oo GEOM_POSSIBLE_NAMES="$GEOMETRY_NAME" -oo KEEP_GEOM_COLUMNS=NO \
     -t_srs EPSG:4326 -s_srs EPSG:4326 --config PG_USE_COPY YES \
     -update -append -makevalid
done