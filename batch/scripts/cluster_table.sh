#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -c | --column_name
# -x | --index_type
. get_arguments.sh "$@"

psql -c "CLUSTER \"$DATASET\".\"$VERSION\" USING \"${VERSION}_${COLUMN_NAME}_${INDEX_TYPE}_idx\";"