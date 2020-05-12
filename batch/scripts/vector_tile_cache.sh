#!/usr/bin/env bash

set -e

# Set TILESTRATEGY
case ${TILESTRATEGY} in
    discontinuous) # Discontinuous polygon features
        STRATEGY=drop-densest-as-needed
        ;;
    continuous) # Continuous polygon features
        STRATEGY=coalesce-densest-as-needed
        ;;
     *)
        echo "Invalid Tile Cache option -${TILESTRATEGY}"
        exit 1
        ;;
esac

echo "Fetch NDJSON data from Data Lake"
aws s3 cp s3://${DATA_LAKE}/${DATASET}/${VERSION}/vector/${DATASET}.ndjson .

echo "Build Tile Cache"
tippecanoe -Z${MINZOOM} -z${MAXZOOM} -e tilecache --${STRATEGY} --extend-zooms-if-still-dropping -P -n ${DATASET} ${DATASET}.ndjson

echo "Upload tiles to S3"
tileputty tilecache --bucket ${TILE_CACHE} --layer ${DATASET} --version ${VERSION} --ext pbf --option ${IMPLEMENTATION}
