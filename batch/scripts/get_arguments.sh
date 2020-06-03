#!/bin/bash

set -e

if [[ -n "${DEBUG}" ]]; then

  echo "--------------"
  echo "AWS CONFIG:"
  echo "--------------"
  cat /root/.aws/config
  echo

  echo "--------------"
  echo "ENVIRONMENT VARIABLES:"
  echo "--------------"
  printenv
  echo

  echo "--------------"
  echo "CMD ARGUMENTS"
  echo "--------------"
  echo "$@"
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
      -c|--column_name)
      COLUMN_NAME="$2"
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
      -g|--geometry_name)
      GEOMETRY_NAME="$2"
      shift # past argument
      shift # past value
      ;;
      -i|--fid_name)
      FID_NAME="$2"
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
      -p|--partition_type)
      PARTITION_TYPE="$2"
      shift # past argument
      shift # past value
      ;;
      -s|--source)
      SRC="$2"
      shift # past argument
      shift # past value
      ;;
      -t|--tile_strategy)
      TILE_STRATEGY="$2"
      shift # past argument
      shift # past value
      ;;
      -v|--version)
      VERSION="$2"
      shift # past argument
      shift # past value
      ;;
      -x|--index_type)
      INDEX_TYPE="$2"
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
      *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift # past argument
      ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters
