#!/bin/bash

set -e

# make sure we work in a distinct folder for the batch job with in /tmp directory
WORK_DIR="/tmp/$AWS_BATCH_JOB_ID"
mkdir -p "$WORK_DIR"
pushd "${WORK_DIR}"

if [[ -n "${DEBUG}" ]]; then

  echo "--------------"
  echo "CMD ARGUMENTS"
  echo "--------------"
  echo "$ME $*"
  echo

  if [[ -f /root/.aws/config ]]; then
    echo "--------------"
    echo "AWS CONFIG:"
    echo "--------------"
    cat /root/.aws/config
    echo
  else
    echo "No AWS config found"
  fi

  echo "--------------"
  echo "ENVIRONMENT VARIABLES:"
  echo "--------------"
  printenv
  echo

  echo "--------------"
  echo "LOGS"
  echo "--------------"

#  set -x
fi


# Default values
POSITIONAL=()
GEOMETRY_NAME="geom"
FID_NAME="gfw_fid"

# extracting cmd line arguments
while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
      -a|--alpha)
      ALPHA="$2"
      shift # past argument
      shift # past value
      ;;
      -b|--bit_depth)
      BIT_DEPTH="$2"
      shift # past argument
      shift # past value
      ;;
      --block_size)
      BLOCK_SIZE="$2"
      shift # past argument
      shift # past value
      ;;
      -c|--column_name)
      COLUMN_NAME="$2"
      shift # past argument
      shift # past value
      ;;
      -C|--column_names)
      COLUMN_NAMES="$2"
      shift # past argument
      shift # past value
      ;;
      -d|--dataset)
      DATASET="$2"
      shift # past argument
      shift # past value
      ;;
      -D|--delimiter)
      DELIMITER="$2"
      shift # past argument
      shift # past value
      ;;
      -f|--file)
      LOCAL_FILE="$2"
      shift # past argument
      shift # past value
      ;;
      --filter)
      FILTER="$2"
      shift
      shift
      ;;
      -F|--format)
      FORMAT="$2"
      shift # past argument
      shift # past value
      ;;
      -g|--geometry_name)
      GEOMETRY_NAME="$2"
      shift # past argument
      shift # past value
      ;;
      -G|--export_to_gee)
      EXPORT_TO_GEE="TRUE"
      shift # past argument
      ;;
      -i|--fid_name)
      FID_NAME="$2"
      shift # past argument
      shift # past value
      ;;
      -I|--implementation)
      IMPLEMENTATION="$2"
      shift # past argument
      shift # past value
      ;;
      --include_tile_id)
      INCLUDE_TILE_ID="TRUE"
      shift # past argument
      ;;
      -j|--json)
      JSON="$2"
      shift # past argument
      shift # past value
      ;;
      -l|--source_layer)
      SRC_LAYER="$2"
      shift # past argument
      shift # past value
      ;;
      --lat)
      LAT="$2"
      shift # past argument
      shift # past value
      ;;
      --lng)
      LNG="$2"
      shift # past argument
      shift # past value
      ;;
      -m|--field_map)
      FIELD_MAP="$2"
      shift # past argument
      shift # past value
      ;;
      -n|--no_data)
      NO_DATA="$2"
      shift # past argument
      shift # past value
      ;;
      --overwrite)
      OVERWRITE="TRUE"
      shift # past argument
      ;;
      -p|--partition_type)
      PARTITION_TYPE="$2"
      shift # past argument
      shift # past value
      ;;
      -P|--partition_schema)
      PARTITION_SCHEMA="$2"
      shift # past argument
      shift # past value
      ;;
      --prefix)
      PREFIX="$2"
      shift # past argument
      shift # past value
      ;;
      -r|--resampling_method)
      RESAMPLE="$2"
      shift # past argument
      shift # past value
      ;;
      -s|--source)
      SRC+=("$2")
      shift # past argument
      shift # past value
      ;;
      --skip)
      SKIP="TRUE"
      shift # past argument
      ;;
      --subset)
      SUBSET="$2"
      shift # past argument
      shift # past value
      ;;
      -t|--tile_strategy)
      TILE_STRATEGY="$2"
      shift # past argument
      shift # past value
      ;;
      -T|--target)
      TARGET="$2"
      shift # past argument
      shift # past value
      ;;
      --target_crs)
      TARGET_CRS="$2"
      shift # past argument
      shift # past value
      ;;
      --target_bucket)
      TARGET_BUCKET="$2"
      shift # past argument
      shift # past value
      ;;
      -u|--unique_constraint)
      UNIQUE_CONSTRAINT_COLUMN_NAMES="$2"
      shift # past argument
      shift # past value
      ;;
      -v|--version)
      VERSION="$2"
      shift # past argument
      shift # past value
      ;;
      -w|--where)
      WHERE="$2"
      shift # past argument
      shift # past value
      ;;
      -x|--index_type)
      INDEX_TYPE="$2"
      shift # past argument
      shift # past value
      ;;
      -X|--zipped)
      ZIPPED="$2"
      shift # past argument
      shift # past value
      ;;
      -z|--max_zoom)
      MAX_ZOOM="$2"
      shift # past argument
      shift # past value
      ;;
      -Z|--min_zoom)
      MIN_ZOOM="$2"
      shift # past argument
      shift # past value
      ;;
      --zoom_level)
      ZOOM_LEVEL="$2"
      shift # past argument
      shift # past value
      ;;

      *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift # past argument
      ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters
