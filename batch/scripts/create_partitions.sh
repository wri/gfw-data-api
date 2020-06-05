#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -p | --partition_type
# -P | --partition_schema
ME=$(basename "$0")

echo "$@"
echo "PYTHON: Create partitions"
create_partitions.py "$@"
#. get_arguments.sh "$@"

echo "PYTHON: Create partitions"

#create_partitions.py -d "$DATASET" -v "$VERSION" -p "$PARTITION_TYPE" -P "$PARTITION_SCHEMA"
