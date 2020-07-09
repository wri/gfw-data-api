#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -c | --column_name
# -x | --index_type
ME=$(basename "$0")
. get_arguments.sh "$@"

echo "PSQL: CLUSTER \"$DATASET\".\"$VERSION\" USING \"${VERSION}_${COLUMN_NAME}_${INDEX_TYPE}_idx\""
psql -c "CLUSTER \"$DATASET\".\"$VERSION\" USING \"${VERSION}_${COLUMN_NAME}_${INDEX_TYPE}_idx\";"