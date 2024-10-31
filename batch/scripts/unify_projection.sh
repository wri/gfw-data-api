#!/bin/bash

set -e

# requires arguments
# -s | --source
# -T | --target
#      --target_crs

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Reproject to a common CRS"

# files=""
# for i in {2..60}; do files="$files $i.tif"; done
# Skip 01.tif, 59.tif, and 60.tif for now (problems around the date line)
files="02.tif 03.tif 04.tif 05.tif 06.tif 07.tif 08.tif 09.tif 10.tif 11.tif 12.tif 13.tif 14.tif 15.tif 16.tif 17.tif 18.tif 19.tif 20.tif 21.tif 22.tif 23.tif 24.tif 25.tif 26.tif 27.tif 28.tif 29.tif 30.tif 31.tif 32.tif 33.tif 34.tif 35.tif 36.tif 37.tif 38.tif 39.tif 40.tif 41.tif 42.tif 43.tif 44.tif 45.tif 46.tif 47.tif 48.tif 49.tif 50.tif 51.tif 52.tif 53.tif 54.tif 55.tif 56.tif 57.tif 58.tif"

src_count=0

CMD_ARGS=()

for s in ${SRC[@]}; do
  mkdir -p "SRC_${src_count}"
  mkdir -p "REPROJECTED_${src_count}"

  for f in ${files}; do
    remote_src_file=${s}/${f}
    local_src_file=SRC_${src_count}/${f}
    local_warped_file=REPROJECTED_${src_count}/${f}
    remote_target_file=${TARGET}/SRC_${src_count}/${f}

    CMD_ARGS+=("${remote_src_file}" "${local_src_file}" "${local_warped_file}" "${TARGET_CRS}" "${remote_target_file}")
  done
  src_count=$(($src_count+1))
done

echo "${CMD_ARGS[@]}" | xargs -n 5 -P 32 _warp_and_upload.sh
