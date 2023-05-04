#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter

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

# https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update
for uri in "${SRC[@]}"; do
  aws s3 cp "${uri}" - | psql -c "BEGIN;
    CREATE TEMP TABLE \"$TEMP_TABLE\"
    (LIKE \"$DATASET\".\"$VERSION\" INCLUDING DEFAULTS)
    ON COMMIT DROP;

    COPY \"$TEMP_TABLE\" STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER);

    INSERT INTO \"$DATASET\".\"$VERSION\"
    SELECT * FROM \"$TEMP_TABLE\"
    ON CONFLICT DO NOTHING;

    COMMIT;"
done