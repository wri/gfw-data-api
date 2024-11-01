#!/bin/bash

set -e

# requires arguments
# -s | --source
# -T | --target
#      --target_crs

ME=$(basename "$0")
. get_arguments.sh "$@"

echo "Reproject to a common CRS"

src_count=0
CMD_ARGS=()

for s in ${SRC[@]}; do
  source_dir="SRC_${src_count}"
  mkdir -p "$source_dir"

  echo "Now recursively downloading $s to $source_dir"
  if [[ $s == gs://* ]]; then
    gsutil -m cp -r "$s" "$source_dir"
  elif [[ $s == s3://* ]]; then
    aws s3 cp --recursive --no-progress "$s" "$source_dir"
  fi
  echo "Done downloading $s to $source_dir"

  reprojected_dir="REPROJECTED_${src_count}"
  mkdir -p "$reprojected_dir"

  cd $source_dir
  for d in $((tree -dfi)); do
    mkdir -p "../${reprojected_dir}/${d}"
  done

  for f in $((find . -iname "*.tif")); do
    local_src_file="${source_dir}/${f}"
    local_warped_file="${reprojected_dir}/${f}"
    remote_target_file="${TARGET}/SRC_${src_count}/${f}"

    CMD_ARGS+=("${local_src_file}" "${local_warped_file}" "${TARGET_CRS}" "${remote_target_file}")
  done
  cd ..

  src_count=$(($src_count+1))
done

echo "${CMD_ARGS[@]}" | xargs -n 5 -P 32 _warp_and_upload.sh
