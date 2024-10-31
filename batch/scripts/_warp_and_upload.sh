#!/bin/bash

# arguments:
# $0 - The name of this script
# $1 - remote_src_file
# $2 - local_src_file
# $3 - local_warped_file
# $4 - target_crs
# $5 - remote target file

if aws s3 ls "$5"; then
  echo "Remote target file $5 already exists, skipping..."
  exit 0
fi

echo "Now downloading $1 to $2"
time gsutil cp "$1" "$2"
echo "Done downloading $1 to $2"

echo "Now warping $2 to $3"
time gdalwarp "$2" "$3" -t_srs "$4" -co COMPRESS=DEFLATE -co TILED=yes
echo "Done warping $2 to $3"

echo "Now uploading $3 to $5"
time aws s3 cp "$3" "$5"
echo "Done uploading $3 to $5"

echo "Finally, deleting local files $2 and $3"
rm "$2" "$3"
echo "Done deleting local files $2 and $3"
