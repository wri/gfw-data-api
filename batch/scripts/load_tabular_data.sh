#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter

# optional arguments
# --lat
# --lng
# -g | --geometry_name (get_arguments.sh specifies default)

ME=$(basename "$0")
. get_arguments.sh "$@"


# Unescape TAB character
if [ "$DELIMITER" == "\t" ]; then
  DELIMITER=$(echo -e "\t")
fi

# I think Postgres temporary tables are such that concurrent jobs won't
# interfere with each other, but make the temp table name unique just
# in case.
UUID=$(python -c 'import uuid; print(uuid.uuid4(), end="")' | sed s/-//g)
TEMP_TABLE="temp_${UUID}"

# IF GEOMETRY_NAME, LAT and LNG are defined, set ADD_POINT_GEOMETRY_FIELDS_SQL
# by sourcing _add_point_geometry_fields_sql.sh
# It defines a SQL snippet we'll run later
if [[ -n "${GEOMETRY_NAME:-}" ]] && [[ -n "${LAT:-}" ]] && [[ -n "${LNG:-}" ]]
then
  . _add_point_geometry_fields_sql.sh
else
  ADD_POINT_GEOMETRY_FIELDS_SQL=""
fi

for uri in "${SRC[@]}"; do
# https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update
  aws s3 cp "${uri}" - | psql -c "BEGIN;
    CREATE TEMP TABLE \"$TEMP_TABLE\"
    (LIKE \"$DATASET\".\"$VERSION\" INCLUDING DEFAULTS)
    ON COMMIT DROP;

    ALTER TABLE \"$TEMP_TABLE\" DROP COLUMN IF EXISTS ${GEOMETRY_NAME};
    ALTER TABLE \"$TEMP_TABLE\" DROP COLUMN IF EXISTS ${GEOMETRY_NAME}_wm;

    COPY \"$TEMP_TABLE\" FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER);

    $ADD_POINT_GEOMETRY_FIELDS_SQL

    INSERT INTO \"$DATASET\".\"$VERSION\"
    SELECT * FROM \"$TEMP_TABLE\"
    ON CONFLICT DO NOTHING;

    COMMIT;"
done