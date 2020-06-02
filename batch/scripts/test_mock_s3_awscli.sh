#!/bin/bash

#set -e

# requires arguments
# -s | --source
. get_arguments.sh "$@"

echo "AWSCLI: COPY DATA FROM S3 to STDOUT"
aws s3 cp ${SRC} -
