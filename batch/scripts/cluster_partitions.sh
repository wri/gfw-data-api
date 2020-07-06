#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -p | --partition_type
# -P | --partition_schema
# -x | --index_type
# -c | --column_name
# -PS | --partition_suffix

ME=$(basename "$0")
. get_arguments.sh "$@"

# While it seems unnecessary here to pass the arguments through the get_arguments.sh script
# I prefer to still do it. This way, we have a consistent way to log the env variables and can make sure
# that argument names are used consistently across all tools.
echo "PYTHON: Cluster partitions"
cluster_partitions.py -d "$DATASET" -v "$VERSION" -p "$PARTITION_TYPE" -P "$PARTITION_SCHEMA" -x "$INDEX_TYPE" -c "$COLUMN_NAME" -PS "$PARTITION_SUFFIX"
