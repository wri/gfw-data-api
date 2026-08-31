#!/bin/bash

set -e
# Without pipefail, a pipeline's exit status is just its last command's,
# so e.g. `aws s3 cp ... | sed ...` would report success even if the
# download itself failed. We rely on failures from any stage of any
# pipe in this script (aws s3 cp, sed, psql) being visible.
set -o pipefail

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
  . _fill_point_geometry_fields_sql.sh
else
  ADD_POINT_GEOMETRY_FIELDS_SQL=""
  FILL_POINT_GEOMETRY_FIELDS_SQL=""
fi

for uri in "${SRC[@]}"; do
# https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update
#
# NOTE: `psql -c "...COPY ... FROM STDIN...more SQL..."` is NOT reliable
# when the COPY isn't the LAST statement in the string -- psql's -c
# handling of multiple bundled commands is documented as producing
# "unexpected results" (https://postgresql.org/message-id/5466.1392310398%40sss.pgh.pa.us),
# and in practice it can abort the connection outright as soon as it
# hits the COPY ("unexpected COPY_IN result, aborting connection")
# without ever reading any data.
#
# Instead we feed psql ONE continuous script over its real stdin: the
# header SQL (ending in the COPY statement), then the streamed S3 data,
# then an explicit end-of-copy marker (\.), then the trailer SQL that
# finishes the transaction. This is the same pattern as
# `psql -f <(cat header.sql data.csv footer.sql)`, just streamed
# instead of buffered so we don't have to hold the whole file in memory.
  {
    cat <<EOSQL
BEGIN;

CREATE TEMP TABLE "$TEMP_TABLE"
(LIKE "$DATASET"."$VERSION" INCLUDING DEFAULTS)
ON COMMIT DROP;

ALTER TABLE "$TEMP_TABLE" DROP COLUMN IF EXISTS ${GEOMETRY_NAME};
ALTER TABLE "$TEMP_TABLE" DROP COLUMN IF EXISTS ${GEOMETRY_NAME}_wm;

COPY "$TEMP_TABLE" FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER);
EOSQL

    # Stream the source data straight through, normalizing it to end in
    # exactly one newline. GNU sed appends a trailing newline only when
    # one is missing, and passes an existing one through unchanged --
    # this keeps the \. marker below cleanly on its own line without
    # ever introducing a spurious blank data row (which COPY would
    # otherwise choke on for any table with more than one column).
    aws s3 cp "${uri}" - | sed -e '$a\'

    cat <<EOSQL
\.

$ADD_POINT_GEOMETRY_FIELDS_SQL
$FILL_POINT_GEOMETRY_FIELDS_SQL

INSERT INTO "$DATASET"."$VERSION"
SELECT * FROM "$TEMP_TABLE"
ON CONFLICT DO NOTHING;

COMMIT;
EOSQL
  } | psql -v ON_ERROR_STOP=1
done
