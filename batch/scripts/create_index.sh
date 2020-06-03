#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -c | --column_name
# -x | --index_type
. get_arguments.sh "$@"

psql -c "\d+ \"$DATASET\".\"$VERSION\" "

psql -c "CREATE INDEX IF NOT EXISTS \"${VERSION}_${COLUMN_NAME}_${INDEX_TYPE}_idx\"
     ON \"$DATASET\".\"$VERSION\" USING $INDEX_TYPE
     (${COLUMN_NAME});"
