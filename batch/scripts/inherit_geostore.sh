#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
ME=$(basename "$0")
. get_arguments.sh "$@"

# Inherit from geostore
echo "PSQL: ALTER TABLE. Inherit from geostore"
psql -c "ALTER TABLE \"$DATASET\".\"$VERSION\" INHERIT public.geostore;"