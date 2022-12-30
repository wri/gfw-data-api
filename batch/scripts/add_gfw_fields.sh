#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version

# optional arguments
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"

set -u

# Add GFW-specific columns to the new table
TABLE_MISSING_COLUMNS="\"$DATASET\".\"$VERSION\""

# Get geometry type of the new table
# GEOMETRY_TYPE_SQL is defined by sourcing _get_geometry_type_sql.sh
# It contains the SQL snippet we'll pass to the psql client command
. _get_geometry_type_sql.sh

# Get the geometry type of the new table
GEOMETRY_TYPE=$(psql -X -A -t -c "${GEOMETRY_TYPE_SQL}")

# ADD_GFW_FIELDS_SQL is defined by sourcing _add_gfw_fields_sql.sh
# It contains the SQL snippet we'll pass to the psql client command
. _add_gfw_fields_sql.sh

echo "PSQL: ALTER TABLE $TABLE_MISSING_COLUMNS. Add GFW columns"
psql -c "$ADD_GFW_FIELDS_SQL"

# Set gfw_geostore_id not NULL to be compliant with GEOSTORE
echo "PSQL: ALTER TABLE \"$DATASET\".\"$VERSION\". ALTER COLUMN gfw_geostore_id SET NOT NULL"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" ALTER COLUMN gfw_geostore_id SET NOT NULL;"

set +u