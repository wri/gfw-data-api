#!/bin/bash
#
# Small Script to check if input raster will
# cross dateline when converting to EPSG:4326
#
# USAGE: ./crosses_dateline.sh infile [outfile]
#
# if no outfile is given, the script returns "true" or "false"
#
# Needs gdal 2.0+ and Python
#
# Credit: Slightly modified from https://gis.stackexchange.com/a/222341


if [ -z "${1}" ]; then
    echo -e "Error: No input rasterfile given.\n> USAGE: ./crosses_dateline.sh infile"
    exit 1
fi

# Get information, save it to variable as we need it several times
gdalinfo=$(gdalinfo "${1}" -json)

# If -json switch is not available exit!
if [ ! -z $(echo $gdalinfo | grep "^Usage:") ]; then
    echo -e "Error: GDAL command failed, Version 2.0+ is needed"
    exit 1
fi

function jsonq {
    echo "${1}" | python -c "import json,sys; jdata = sys.stdin.read(); data = json.loads(jdata); print(data${2});"
}

ulx=$(jsonq "$gdalinfo" "['wgs84Extent']['coordinates'][0][0][0]")
llx=$(jsonq "$gdalinfo" "['wgs84Extent']['coordinates'][0][1][0]")
lrx=$(jsonq "$gdalinfo" "['wgs84Extent']['coordinates'][0][3][0]")
urx=$(jsonq "$gdalinfo" "['wgs84Extent']['coordinates'][0][2][0]")

crossing_dateline=false
test $(echo "${ulx}>${lrx}" | bc) -eq 1 && crossing_dateline=true
test $(echo "${llx}>${urx}" | bc) -eq 1 && crossing_dateline=true

echo -n "${crossing_dateline}"