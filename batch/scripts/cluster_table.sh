#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -c | --column_name
# -x | --index_type
ME=$(basename "$0")
. get_arguments.sh "$@"

COLUMN_NAMES_UNDERSCORED="$(echo "$COLUMN_NAMES" | sed 's/,/_/g' | cut -c 1-63)"
echo "PSQL: CLUSTER \"$DATASET\".\"$VERSION\" USING \"${VERSION}_${COLUMN_NAMES_UNDERSCORED}_${INDEX_TYPE}_idx\""
psql -c "CLUSTER \"$DATASET\".\"$VERSION\" USING \"${VERSION}_${COLUMN_NAMES_UNDERSCORED}_${INDEX_TYPE}_idx\";"