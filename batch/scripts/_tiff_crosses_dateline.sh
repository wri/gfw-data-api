#!/bin/bash
#
# USAGE: _tiff_crosses_dateline.sh raster_file
#
# Prints the string "true" if the input raster will cross the dateline
# when converting to EPSG:4326, "false" otherwise
#
# Needs GDAL 2.0+ and Python
#
# Credit: Slightly modified from https://gis.stackexchange.com/a/222341


if [ -z "${1}" ]; then
    echo -e "Error: No input raster file given.\n> USAGE: _tiff_crosses_dateline.sh raster_file"
    exit 1
fi

# Get raster info, save it to a variable as we need it several times
gdalinfo=$(gdalinfo "${1}" -json)

# Exit if -json switch is not available
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
test $(python -c "print(${ulx}>${lrx})") = True && crossing_dateline=true
test $(python -c "print(${ulx}>${lrx})") = True && crossing_dateline=true

echo -n "${crossing_dateline}"