#!/bin/bash

set -e

# required arguments
# -d | --dataset
# -v | --version
# -C | --column_names
# -T | --target
#
# optional arguments
# --include_tile_id

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "PYTHON: Create 1x1 grid files"
export_1x1_grid.py -d "$DATASET" -v "$VERSION" -C "$COLUMN_NAMES" --include_tile_id "$INCLUDE_TILE_ID"

echo "Combine output files"
echo ./*.tmp | xargs cat >> "${DATASET}_${VERSION}_1x1.tsv"

echo "Post-process geometries"
extract_geometries.py "${DATASET}_${VERSION}_1x1.tsv"

echo "AWSCLI: upload to data lake"
aws s3 cp "${DATASET}_${VERSION}_1x1.tsv" "$TARGET"
