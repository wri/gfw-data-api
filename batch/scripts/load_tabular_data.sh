#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -s | --source
# -D | --delimiter
. get_arguments.sh "$@"

# TODO: remove force_null option. Make sure that types are correctly inferred
aws s3 cp "${SRC}" - | psql -c "COPY \"$DATASET\".\"$VERSION\" FROM STDIN WITH (FORMAT CSV, DELIMITER '$DELIMITER', HEADER)" #, FORCE_NULL (rspo_oil_palm__certification_status, per_forest_concession__type, idn_forest_area__type))"

