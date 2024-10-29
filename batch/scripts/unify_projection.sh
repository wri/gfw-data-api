#!/bin/bash

set -e

# requires arguments
# -s | --source
# -T | --target
#      --target_crs

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Reproject to a common CRS"

# Build an array of arguments to pass to unify_projection.py
#ARG_ARRAY=("--source" "${SRC}")
#
#ARG_ARRAY+=("--target" "${TARGET}")
#
#ARG_ARRAY+=("--target-crs" "${TARGET_CRS}")

# Run unify_projection.py with the array of arguments
#unify_projection.py "${ARG_ARRAY[@]}"

#gdist="gs://earthenginepartners-hansen/DIST-ALERT"

# files=""
# for i in {2..60}; do files="$files $i.tif"; done
# Skip 01.tif, 59.tif, and 60.tif for now (problems around the date line)
files="02.tif 03.tif 04.tif 05.tif 06.tif 07.tif 08.tif 09.tif 10.tif 11.tif 12.tif 13.tif 14.tif 15.tif 16.tif 17.tif 18.tif 19.tif 20.tif 21.tif 22.tif 23.tif 24.tif 25.tif 26.tif 27.tif 28.tif 29.tif 30.tif 31.tif 32.tif 33.tif 34.tif 35.tif 36.tif 37.tif 38.tif 39.tif 40.tif 41.tif 42.tif 43.tif 44.tif 45.tif 46.tif 47.tif 48.tif 49.tif 50.tif 51.tif 52.tif 53.tif 54.tif 55.tif 56.tif 57.tif 58.tif"

src_count = 0

cd /tmp
rm -f /tmp/*tif*
for s in ${SRC}; do
  for f in ${files}; do
    remote_target_file=${TARGET}/SRC_${src_count}/${f}
    if aws s3 ls ${remote_target_file}; then
      echo "Remote target file ${remote_target_file} already exists, skipping..."
      continue
    fi

    remote_src_file=${s}/${f}
    local_src_file=SRC_${src_count}/${f}
    echo "Now downloading ${remote_src_file} to ${local_src_file}"
    time gsutil cp ${s}/${f} ${local_src_file}
    echo "Done"

    local_warped_file=REPROJECTED_${src_count}/${f}
    echo "Now warping ${local_src_file} to ${local_warped_file}"
    time gdalwarp ${local_src_file} ${local_warped_file} -t_srs "${TARGET_CRS}" -co COMPRESS=DEFLATE -co TILED=yes
    echo "Done warping ${local_src_file} to ${local_warped_file}"

    echo "Now uploading ${local_warped_file} to ${remote_target_file}"
    time aws s3 cp ${local_warped_file} ${remote_target_file}
    echo "Done uploading ${local_warped_file} to ${remote_target_file}"

    echo "Finally, deleting local files ${local_src_file} and ${local_warped_file}"
    rm ${local_src_file} ${local_warped_file}
  done
  ((count++))
done