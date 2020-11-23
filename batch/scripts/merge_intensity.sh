#!/bin/bash

set -e

# requires positional arguments
# date_conf_uri
# intensity_uri
# destination_prefix

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Merge date_conf and intensity assets and upload to S3"

merge_intensity.py "${POSITIONAL[@]}"