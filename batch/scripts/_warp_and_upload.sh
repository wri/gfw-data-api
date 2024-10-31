#!/bin/bash

set -e

# arguments:
# $0 - The name of this script
# $1 - local_src_file
# $2 - local_warped_file
# $3 - target_crs
# $4 - remote target file

if aws s3 ls "$4"; then
  echo "Remote target file $4 already exists, skipping..."
  exit 0
fi

echo "Now warping $1 to $2"
gdalwarp "$1" "$2" -t_srs "$3" -co COMPRESS=DEFLATE -co TILED=yes
echo "Done warping $1 to $2"

echo "Now uploading $2 to $4"
aws s3 cp --no-progress "$2" "$4"
echo "Done uploading $2 to $4"

echo "Finally, deleting local files $1 and $2"
rm "$1" "$2"
echo "Done deleting local files $1 and $2"
