#!/bin/bash

set -e

# requires arguments
# -d | --dataset
# -v | --version
# -C | --column_names

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "PYTHON: Create 1x1 grid files"
export_1x1_grid.py -d "$DATASET" -v "$VERSION" -C "$COLUMN_NAMES"

echo *.tmp | xargs cat >> "${DATASET}_${VERSION}_1x1.tsv"
