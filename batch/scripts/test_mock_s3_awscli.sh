#!/bin/bash

#set -e

# requires arguments
# -s | --source
ME=$(basename "$0")
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM S3 to STDOUT"
aws s3 cp "${SRC}" -
