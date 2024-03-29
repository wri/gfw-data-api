#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# NOTICE: This is a copy of the gdal2tiles.py script hacked to suit our
# particular needs. In particular it avoids some of the fancy logic
# around masks and alpha channels, and writes PNGs with 16 bits per channel
# rather than the usual 8. Also, I wanted something that would just
# take a 4 channel TIFF and generate 4 channel PNGs as-is, because we
# are using the alpha channel as just another place to put data, not
# as a mask. At some point I would like to write a much simpler program
# to do just that rather than include this hacked copy of gdal2tiles.


# ******************************************************************************
#  $Id: gdal2tiles.py c793e83ae7f219bdbdc06432ef707c25ae260aad 2020-10-26 12:15:22 +0100 Even Rouault $
#
# Project:  Google Summer of Code 2007, 2008 (http://code.google.com/soc/)
# Support:  BRGM (http://www.brgm.fr)
# Purpose:  Convert a raster into TMS (Tile Map Service) tiles in a directory.
#           - generate Google Earth metadata (KML SuperOverlay)
#           - generate simple HTML viewer based on Google Maps and OpenLayers
#           - support of global tiles (Spherical Mercator) for compatibility
#               with interactive web maps a la Google Maps
# Author:   Klokan Petr Pridal, klokan at klokan dot cz
# Web:      http://www.klokan.cz/projects/gdal2tiles/
# GUI:      http://www.maptiler.org/
#
###############################################################################
# Copyright (c) 2008, Klokan Petr Pridal
# Copyright (c) 2010-2013, Even Rouault <even dot rouault at spatialys.com>
#
#  Permission is hereby granted, free of charge, to any person obtaining a
#  copy of this software and associated documentation files (the "Software"),
#  to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense,
#  and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
# ******************************************************************************

from __future__ import division, print_function

import glob
import json
import math
import os
import shutil
import sys
import tempfile
import threading
from functools import partial
from multiprocessing import Pool
from typing import List, Tuple
from uuid import uuid4
from xml.etree import ElementTree

from osgeo import gdal, osr

try:
    import numpy
    import osgeo.gdal_array as gdalarray
    from PIL import Image

    numpy_available = True
except ImportError:
    # 'antialias' resampling is not available
    numpy_available = False

__version__ = "$Id: gdal2tiles.py c793e83ae7f219bdbdc06432ef707c25ae260aad 2020-10-26 12:15:22 +0100 Even Rouault $"

resampling_list = (
    "average",
    "near",
    "bilinear",
    "cubic",
    "cubicspline",
    "lanczos",
    "antialias",
    "mode",
    "max",
    "min",
    "med",
    "q1",
    "q3",
)
webviewer_list = ("all", "google", "openlayers", "leaflet", "mapml", "none")


class UnsupportedTileMatrixSet(Exception):
    pass


class TileMatrixSet(object):
    def __init__(self):
        self.identifier = None
        self.srs = None
        self.topleft_x = None
        self.topleft_y = None
        self.matrix_width = None  # at zoom 0
        self.matrix_height = None  # at zoom 0
        self.tile_size = None
        self.resolution = None  # at zoom 0
        self.level_count = None

    def GeorefCoordToTileCoord(self, x, y, z, overriden_tile_size):
        res = self.resolution * self.tile_size / overriden_tile_size / (2 ** z)
        tx = int((x - self.topleft_x) / (res * overriden_tile_size))
        # In default mode, we use a bottom-y origin
        ty = int(
            (
                y
                - (
                    self.topleft_y
                    - self.matrix_height * self.tile_size * self.resolution
                )
            )
            / (res * overriden_tile_size)
        )
        return tx, ty

    def ZoomForPixelSize(self, pixelSize, overriden_tile_size):
        """Maximal scaledown zoom of the pyramid closest to the pixelSize."""

        for i in range(self.level_count):
            res = self.resolution * self.tile_size / overriden_tile_size / (2 ** i)
            if pixelSize > res:
                return max(0, i - 1)  # We don't want to scale up
        return self.level_count - 1

    def PixelsToMeters(self, px, py, zoom, overriden_tile_size):
        """Converts pixel coordinates in given zoom level of pyramid to
        EPSG:3857."""

        res = self.resolution * self.tile_size / overriden_tile_size / (2 ** zoom)
        mx = px * res + self.topleft_x
        my = py * res + (
            self.topleft_y - self.matrix_height * self.tile_size * self.resolution
        )
        return mx, my

    def TileBounds(self, tx, ty, zoom, overriden_tile_size):
        """Returns bounds of the given tile in georef coordinates."""

        minx, miny = self.PixelsToMeters(
            tx * overriden_tile_size,
            ty * overriden_tile_size,
            zoom,
            overriden_tile_size,
        )
        maxx, maxy = self.PixelsToMeters(
            (tx + 1) * overriden_tile_size,
            (ty + 1) * overriden_tile_size,
            zoom,
            overriden_tile_size,
        )
        return (minx, miny, maxx, maxy)

    @staticmethod
    def parse(j):
        assert "identifier" in j
        assert "supportedCRS" in j
        assert "tileMatrix" in j
        assert isinstance(j["tileMatrix"], list)
        srs = osr.SpatialReference()
        assert srs.SetFromUserInput(str(j["supportedCRS"])) == 0
        swapaxis = srs.EPSGTreatsAsLatLong() or srs.EPSGTreatsAsNorthingEasting()
        metersPerUnit = 1.0
        if srs.IsProjected():
            metersPerUnit = srs.GetLinearUnits()
        elif srs.IsGeographic():
            metersPerUnit = srs.GetSemiMajor() * math.pi / 180
        tms = TileMatrixSet()
        tms.srs = srs
        tms.identifier = str(j["identifier"])
        for i, tileMatrix in enumerate(j["tileMatrix"]):
            assert "topLeftCorner" in tileMatrix
            assert isinstance(tileMatrix["topLeftCorner"], list)
            topLeftCorner = tileMatrix["topLeftCorner"]
            assert len(topLeftCorner) == 2
            assert "scaleDenominator" in tileMatrix
            assert "tileWidth" in tileMatrix
            assert "tileHeight" in tileMatrix

            topleft_x = topLeftCorner[0]
            topleft_y = topLeftCorner[1]
            tileWidth = tileMatrix["tileWidth"]
            tileHeight = tileMatrix["tileHeight"]
            if tileWidth != tileHeight:
                raise UnsupportedTileMatrixSet("Only square tiles supported")
            # Convention in OGC TileMatrixSet definition. See gcore/tilematrixset.cpp
            resolution = tileMatrix["scaleDenominator"] * 0.28e-3 / metersPerUnit
            if swapaxis:
                topleft_x, topleft_y = topleft_y, topleft_x
            if i == 0:
                tms.topleft_x = topleft_x
                tms.topleft_y = topleft_y
                tms.resolution = resolution
                tms.tile_size = tileWidth

                assert "matrixWidth" in tileMatrix
                assert "matrixHeight" in tileMatrix
                tms.matrix_width = tileMatrix["matrixWidth"]
                tms.matrix_height = tileMatrix["matrixHeight"]
            else:
                if topleft_x != tms.topleft_x or topleft_y != tms.topleft_y:
                    raise UnsupportedTileMatrixSet("All levels should have same origin")
                if abs(tms.resolution / (1 << i) - resolution) > 1e-8 * resolution:
                    raise UnsupportedTileMatrixSet(
                        "Only resolutions varying as power-of-two supported"
                    )
                if tileWidth != tms.tile_size:
                    raise UnsupportedTileMatrixSet(
                        "All levels should have same tile size"
                    )
        tms.level_count = len(j["tileMatrix"])
        return tms


tmsMap = {}

profile_list = ["mercator", "geodetic", "raster"]

# Read additional tile matrix sets from GDAL data directory
filename = gdal.FindFile("gdal", "tms_MapML_APSTILE.json")
if filename:
    dirname = os.path.dirname(filename)
    for tmsfilename in glob.glob(os.path.join(dirname, "tms_*.json")):
        data = open(tmsfilename, "rb").read()
        try:
            j = json.loads(data.decode("utf-8"))
        except:
            j = None
        if j is None:
            print("Cannot parse " + tmsfilename)
            continue
        try:
            tms = TileMatrixSet.parse(j)
        except UnsupportedTileMatrixSet:
            continue
        except:
            print("Cannot parse " + tmsfilename)
            continue
        tmsMap[tms.identifier] = tms
        profile_list.append(tms.identifier)

threadLocal = threading.local()

# =============================================================================
# =============================================================================
# =============================================================================

__doc__globalmaptiles = """
globalmaptiles.py

Global Map Tiles as defined in Tile Map Service (TMS) Profiles
==============================================================

Functions necessary for generation of global tiles used on the web.
It contains classes implementing coordinate conversions for:

  - GlobalMercator (based on EPSG:3857)
       for Google Maps, Yahoo Maps, Bing Maps compatible tiles
  - GlobalGeodetic (based on EPSG:4326)
       for OpenLayers Base Map and Google Earth compatible tiles

More info at:

http://wiki.osgeo.org/wiki/Tile_Map_Service_Specification
http://wiki.osgeo.org/wiki/WMS_Tiling_Client_Recommendation
http://msdn.microsoft.com/en-us/library/bb259689.aspx
http://code.google.com/apis/maps/documentation/overlays.html#Google_Maps_Coordinates

Created by Klokan Petr Pridal on 2008-07-03.
Google Summer of Code 2008, project GDAL2Tiles for OSGEO.

In case you use this class in your product, translate it to another language
or find it useful for your project please let me know.
My email: klokan at klokan dot cz.
I would like to know where it was used.

Class is available under the open-source GDAL license (www.gdal.org).
"""

MAXZOOMLEVEL = 32


class GlobalMercator(object):
    r"""
    TMS Global Mercator Profile
    ---------------------------

    Functions necessary for generation of tiles in Spherical Mercator projection,
    EPSG:3857.

    Such tiles are compatible with Google Maps, Bing Maps, Yahoo Maps,
    UK Ordnance Survey OpenSpace API, ...
    and you can overlay them on top of base maps of those web mapping applications.

    Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

    What coordinate conversions do we need for TMS Global Mercator tiles::

         LatLon      <->       Meters      <->     Pixels    <->       Tile

     WGS84 coordinates   Spherical Mercator  Pixels in pyramid  Tiles in pyramid
         lat/lon            XY in meters     XY pixels Z zoom      XYZ from TMS
        EPSG:4326           EPSG:387
         .----.              ---------               --                TMS
        /      \     <->     |       |     <->     /----/    <->      Google
        \      /             |       |           /--------/          QuadTree
         -----               ---------         /------------/
       KML, public         WebMapService         Web Clients      TileMapService

    What is the coordinate extent of Earth in EPSG:3857?

      [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
      Constant 20037508.342789244 comes from the circumference of the Earth in meters,
      which is 40 thousand kilometers, the coordinate origin is in the middle of extent.
      In fact you can calculate the constant as: 2 * math.pi * 6378137 / 2.0
      $ echo 180 85 | gdaltransform -s_srs EPSG:4326 -t_srs EPSG:3857
      Polar areas with abs(latitude) bigger then 85.05112878 are clipped off.

    What are zoom level constants (pixels/meter) for pyramid with EPSG:3857?

      whole region is on top of pyramid (zoom=0) covered by 256x256 pixels tile,
      every lower zoom level resolution is always divided by two
      initialResolution = 20037508.342789244 * 2 / 256 = 156543.03392804062

    What is the difference between TMS and Google Maps/QuadTree tile name convention?

      The tile raster itself is the same (equal extent, projection, pixel size),
      there is just different identification of the same raster tile.
      Tiles in TMS are counted from [0,0] in the bottom-left corner, id is XYZ.
      Google placed the origin [0,0] to the top-left corner, reference is XYZ.
      Microsoft is referencing tiles by a QuadTree name, defined on the website:
      http://msdn2.microsoft.com/en-us/library/bb259689.aspx

    The lat/lon coordinates are using WGS84 datum, yes?

      Yes, all lat/lon we are mentioning should use WGS84 Geodetic Datum.
      Well, the web clients like Google Maps are projecting those coordinates by
      Spherical Mercator, so in fact lat/lon coordinates on sphere are treated as if
      the were on the WGS84 ellipsoid.

      From MSDN documentation:
      To simplify the calculations, we use the spherical form of projection, not
      the ellipsoidal form. Since the projection is used only for map display,
      and not for displaying numeric coordinates, we don't need the extra precision
      of an ellipsoidal projection. The spherical projection causes approximately
      0.33 percent scale distortion in the Y direction, which is not visually
      noticeable.

    How do I create a raster in EPSG:3857 and convert coordinates with PROJ.4?

      You can use standard GIS tools like gdalwarp, cs2cs or gdaltransform.
      All of the tools supports -t_srs 'epsg:3857'.

      For other GIS programs check the exact definition of the projection:
      More info at http://spatialreference.org/ref/user/google-projection/
      The same projection is designated as EPSG:3857. WKT definition is in the
      official EPSG database.

      Proj4 Text:
        +proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0
        +k=1.0 +units=m +nadgrids=@null +no_defs

      Human readable WKT format of EPSG:3857:
         PROJCS["Google Maps Global Mercator",
             GEOGCS["WGS 84",
                 DATUM["WGS_1984",
                     SPHEROID["WGS 84",6378137,298.257223563,
                         AUTHORITY["EPSG","7030"]],
                     AUTHORITY["EPSG","6326"]],
                 PRIMEM["Greenwich",0],
                 UNIT["degree",0.0174532925199433],
                 AUTHORITY["EPSG","4326"]],
             PROJECTION["Mercator_1SP"],
             PARAMETER["central_meridian",0],
             PARAMETER["scale_factor",1],
             PARAMETER["false_easting",0],
             PARAMETER["false_northing",0],
             UNIT["metre",1,
                 AUTHORITY["EPSG","9001"]]]
    """

    def __init__(self, tile_size=256):
        """Initialize the TMS Global Mercator pyramid."""
        self.tile_size = tile_size
        self.initialResolution = 2 * math.pi * 6378137 / self.tile_size
        # 156543.03392804062 for tile_size 256 pixels
        self.originShift = 2 * math.pi * 6378137 / 2.0
        # 20037508.342789244

    def LatLonToMeters(self, lat, lon):
        """Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator
        EPSG:3857."""

        mx = lon * self.originShift / 180.0
        my = math.log(math.tan((90 + lat) * math.pi / 360.0)) / (math.pi / 180.0)

        my = my * self.originShift / 180.0
        return mx, my

    def MetersToLatLon(self, mx, my):
        """Converts XY point from Spherical Mercator EPSG:3857 to lat/lon in
        WGS84 Datum."""

        lon = (mx / self.originShift) * 180.0
        lat = (my / self.originShift) * 180.0

        lat = (
            180
            / math.pi
            * (2 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
        )
        return lat, lon

    def PixelsToMeters(self, px, py, zoom):
        """Converts pixel coordinates in given zoom level of pyramid to
        EPSG:3857."""

        res = self.Resolution(zoom)
        mx = px * res - self.originShift
        my = py * res - self.originShift
        return mx, my

    def MetersToPixels(self, mx, my, zoom):
        """Converts EPSG:3857 to pyramid pixel coordinates in given zoom
        level."""

        res = self.Resolution(zoom)
        px = (mx + self.originShift) / res
        py = (my + self.originShift) / res
        return px, py

    def PixelsToTile(self, px, py):
        """Returns a tile covering region in given pixel coordinates."""

        tx = int(math.ceil(px / float(self.tile_size)) - 1)
        ty = int(math.ceil(py / float(self.tile_size)) - 1)
        return tx, ty

    def PixelsToRaster(self, px, py, zoom):
        """Move the origin of pixel coordinates to top-left corner."""

        mapSize = self.tile_size << zoom
        return px, mapSize - py

    def MetersToTile(self, mx, my, zoom):
        """Returns tile for given mercator coordinates."""

        px, py = self.MetersToPixels(mx, my, zoom)
        return self.PixelsToTile(px, py)

    def TileBounds(self, tx, ty, zoom):
        """Returns bounds of the given tile in EPSG:3857 coordinates."""

        minx, miny = self.PixelsToMeters(tx * self.tile_size, ty * self.tile_size, zoom)
        maxx, maxy = self.PixelsToMeters(
            (tx + 1) * self.tile_size, (ty + 1) * self.tile_size, zoom
        )
        return (minx, miny, maxx, maxy)

    def TileLatLonBounds(self, tx, ty, zoom):
        """Returns bounds of the given tile in latitude/longitude using WGS84
        datum."""

        bounds = self.TileBounds(tx, ty, zoom)
        minLat, minLon = self.MetersToLatLon(bounds[0], bounds[1])
        maxLat, maxLon = self.MetersToLatLon(bounds[2], bounds[3])

        return (minLat, minLon, maxLat, maxLon)

    def Resolution(self, zoom):
        """Resolution (meters/pixel) for given zoom level (measured at
        Equator)"""

        # return (2 * math.pi * 6378137) / (self.tile_size * 2**zoom)
        return self.initialResolution / (2 ** zoom)

    def ZoomForPixelSize(self, pixelSize):
        """Maximal scaledown zoom of the pyramid closest to the pixelSize."""

        for i in range(MAXZOOMLEVEL):
            if pixelSize > self.Resolution(i):
                return max(0, i - 1)  # We don't want to scale up
        return MAXZOOMLEVEL - 1

    def GoogleTile(self, tx, ty, zoom):
        """Converts TMS tile coordinates to Google Tile coordinates."""

        # coordinate origin is moved from bottom-left to top-left corner of the extent
        return tx, (2 ** zoom - 1) - ty

    def QuadTree(self, tx, ty, zoom):
        """Converts TMS tile coordinates to Microsoft QuadTree."""

        quadKey = ""
        ty = (2 ** zoom - 1) - ty
        for i in range(zoom, 0, -1):
            digit = 0
            mask = 1 << (i - 1)
            if (tx & mask) != 0:
                digit += 1
            if (ty & mask) != 0:
                digit += 2
            quadKey += str(digit)

        return quadKey


class GlobalGeodetic(object):
    r"""
    TMS Global Geodetic Profile
    ---------------------------

    Functions necessary for generation of global tiles in Plate Carre projection,
    EPSG:4326, "unprojected profile".

    Such tiles are compatible with Google Earth (as any other EPSG:4326 rasters)
    and you can overlay the tiles on top of OpenLayers base map.

    Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

    What coordinate conversions do we need for TMS Global Geodetic tiles?

      Global Geodetic tiles are using geodetic coordinates (latitude,longitude)
      directly as planar coordinates XY (it is also called Unprojected or Plate
      Carre). We need only scaling to pixel pyramid and cutting to tiles.
      Pyramid has on top level two tiles, so it is not square but rectangle.
      Area [-180,-90,180,90] is scaled to 512x256 pixels.
      TMS has coordinate origin (for pixels and tiles) in bottom-left corner.
      Rasters are in EPSG:4326 and therefore are compatible with Google Earth.

         LatLon      <->      Pixels      <->     Tiles

     WGS84 coordinates   Pixels in pyramid  Tiles in pyramid
         lat/lon         XY pixels Z zoom      XYZ from TMS
        EPSG:4326
         .----.                ----
        /      \     <->    /--------/    <->      TMS
        \      /         /--------------/
         -----        /--------------------/
       WMS, KML    Web Clients, Google Earth  TileMapService
    """

    def __init__(self, tmscompatible, tile_size=256):
        self.tile_size = tile_size
        if tmscompatible is not None:
            # Defaults the resolution factor to 0.703125 (2 tiles @ level 0)
            # Adhers to OSGeo TMS spec
            # http://wiki.osgeo.org/wiki/Tile_Map_Service_Specification#global-geodetic
            self.resFact = 180.0 / self.tile_size
        else:
            # Defaults the resolution factor to 1.40625 (1 tile @ level 0)
            # Adheres OpenLayers, MapProxy, etc default resolution for WMTS
            self.resFact = 360.0 / self.tile_size

    def LonLatToPixels(self, lon, lat, zoom):
        """Converts lon/lat to pixel coordinates in given zoom of the EPSG:4326
        pyramid."""

        res = self.resFact / 2 ** zoom
        px = (180 + lon) / res
        py = (90 + lat) / res
        return px, py

    def PixelsToTile(self, px, py):
        """Returns coordinates of the tile covering region in pixel
        coordinates."""

        tx = int(math.ceil(px / float(self.tile_size)) - 1)
        ty = int(math.ceil(py / float(self.tile_size)) - 1)
        return tx, ty

    def LonLatToTile(self, lon, lat, zoom):
        """Returns the tile for zoom which covers given lon/lat coordinates."""

        px, py = self.LonLatToPixels(lon, lat, zoom)
        return self.PixelsToTile(px, py)

    def Resolution(self, zoom):
        """Resolution (arc/pixel) for given zoom level (measured at Equator)"""

        return self.resFact / 2 ** zoom

    def ZoomForPixelSize(self, pixelSize):
        """Maximal scaledown zoom of the pyramid closest to the pixelSize."""

        for i in range(MAXZOOMLEVEL):
            if pixelSize > self.Resolution(i):
                return max(0, i - 1)  # We don't want to scale up
        return MAXZOOMLEVEL - 1

    def TileBounds(self, tx, ty, zoom):
        """Returns bounds of the given tile."""
        res = self.resFact / 2 ** zoom
        return (
            tx * self.tile_size * res - 180,
            ty * self.tile_size * res - 90,
            (tx + 1) * self.tile_size * res - 180,
            (ty + 1) * self.tile_size * res - 90,
        )

    def TileLatLonBounds(self, tx, ty, zoom):
        """Returns bounds of the given tile in the SWNE form."""
        b = self.TileBounds(tx, ty, zoom)
        return (b[1], b[0], b[3], b[2])


class Zoomify(object):
    """
    Tiles compatible with the Zoomify viewer
    ----------------------------------------
    """

    def __init__(self, width, height, tile_size=256, tileformat="jpg"):
        """Initialization of the Zoomify tile tree."""

        self.tile_size = tile_size
        self.tileformat = tileformat
        imagesize = (width, height)
        tiles = (math.ceil(width / tile_size), math.ceil(height / tile_size))

        # Size (in tiles) for each tier of pyramid.
        self.tierSizeInTiles = []
        self.tierSizeInTiles.append(tiles)

        # Image size in pixels for each pyramid tierself
        self.tierImageSize = []
        self.tierImageSize.append(imagesize)

        while imagesize[0] > tile_size or imagesize[1] > tile_size:
            imagesize = (math.floor(imagesize[0] / 2), math.floor(imagesize[1] / 2))
            tiles = (
                math.ceil(imagesize[0] / tile_size),
                math.ceil(imagesize[1] / tile_size),
            )
            self.tierSizeInTiles.append(tiles)
            self.tierImageSize.append(imagesize)

        self.tierSizeInTiles.reverse()
        self.tierImageSize.reverse()

        # Depth of the Zoomify pyramid, number of tiers (zoom levels)
        self.numberOfTiers = len(self.tierSizeInTiles)

        # Number of tiles up to the given tier of pyramid.
        self.tileCountUpToTier = []
        self.tileCountUpToTier[0] = 0
        for i in range(1, self.numberOfTiers + 1):
            self.tileCountUpToTier.append(
                self.tierSizeInTiles[i - 1][0] * self.tierSizeInTiles[i - 1][1]
                + self.tileCountUpToTier[i - 1]
            )

    def tilefilename(self, x, y, z):
        """Returns filename for tile with given coordinates."""

        tileIndex = x + y * self.tierSizeInTiles[z][0] + self.tileCountUpToTier[z]
        return os.path.join(
            "TileGroup%.0f" % math.floor(tileIndex / 256),
            "%s-%s-%s.%s" % (z, x, y, self.tileformat),
        )


class GDALError(Exception):
    pass


def exit_with_error(message, details=""):
    # Message printing and exit code kept from the way it worked using the OptionParser (in case
    # someone parses the error output)
    sys.stderr.write("Usage: gdal2tiles.py [options] input_file [output]\n\n")
    sys.stderr.write("gdal2tiles.py: error: %s\n" % message)
    if details:
        sys.stderr.write("\n\n%s\n" % details)

    sys.exit(2)


def set_cache_max(cache_in_bytes):
    # We set the maximum using `SetCacheMax` and `GDAL_CACHEMAX` to support both fork and spawn as multiprocessing start methods.
    # https://github.com/OSGeo/gdal/pull/2112
    os.environ["GDAL_CACHEMAX"] = "%d" % int(cache_in_bytes / 1024 / 1024)
    gdal.SetCacheMax(cache_in_bytes)


def generate_kml(
    tx, ty, tz, tileext, tile_size, tileswne, options, children=None, **args
):
    """Template for the KML.

    Returns filled string.
    """
    if not children:
        children = []

    args["tx"], args["ty"], args["tz"] = tx, ty, tz
    args["tileformat"] = tileext
    if "tile_size" not in args:
        args["tile_size"] = tile_size

    if "minlodpixels" not in args:
        args["minlodpixels"] = int(args["tile_size"] / 2)
    if "maxlodpixels" not in args:
        args["maxlodpixels"] = int(args["tile_size"] * 8)
    if children == []:
        args["maxlodpixels"] = -1

    if tx is None:
        tilekml = False
        args["title"] = options.title
    else:
        tilekml = True
        args["realtiley"] = GDAL2Tiles.getYTile(ty, tz, options)
        args["title"] = "%d/%d/%d.kml" % (tz, tx, args["realtiley"])
        args["south"], args["west"], args["north"], args["east"] = tileswne(tx, ty, tz)

    if tx == 0:
        args["drawOrder"] = 2 * tz + 1
    elif tx is not None:
        args["drawOrder"] = 2 * tz
    else:
        args["drawOrder"] = 0

    url = options.url
    if not url:
        if tilekml:
            url = "../../"
        else:
            url = ""

    s = (
        """<?xml version="1.0" encoding="utf-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>%(title)s</name>
    <description></description>
    <Style>
      <ListStyle id="hideChildren">
        <listItemType>checkHideChildren</listItemType>
      </ListStyle>
    </Style>"""
        % args
    )
    if tilekml:
        s += (
            """
    <Region>
      <LatLonAltBox>
        <north>%(north).14f</north>
        <south>%(south).14f</south>
        <east>%(east).14f</east>
        <west>%(west).14f</west>
      </LatLonAltBox>
      <Lod>
        <minLodPixels>%(minlodpixels)d</minLodPixels>
        <maxLodPixels>%(maxlodpixels)d</maxLodPixels>
      </Lod>
    </Region>
    <GroundOverlay>
      <drawOrder>%(drawOrder)d</drawOrder>
      <Icon>
        <href>%(realtiley)d.%(tileformat)s</href>
      </Icon>
      <LatLonBox>
        <north>%(north).14f</north>
        <south>%(south).14f</south>
        <east>%(east).14f</east>
        <west>%(west).14f</west>
      </LatLonBox>
    </GroundOverlay>
"""
            % args
        )

    for cx, cy, cz in children:
        csouth, cwest, cnorth, ceast = tileswne(cx, cy, cz)
        ytile = GDAL2Tiles.getYTile(cy, cz, options)
        s += """
    <NetworkLink>
      <name>%d/%d/%d.%s</name>
      <Region>
        <LatLonAltBox>
          <north>%.14f</north>
          <south>%.14f</south>
          <east>%.14f</east>
          <west>%.14f</west>
        </LatLonAltBox>
        <Lod>
          <minLodPixels>%d</minLodPixels>
          <maxLodPixels>-1</maxLodPixels>
        </Lod>
      </Region>
      <Link>
        <href>%s%d/%d/%d.kml</href>
        <viewRefreshMode>onRegion</viewRefreshMode>
        <viewFormat/>
      </Link>
    </NetworkLink>
        """ % (
            cz,
            cx,
            ytile,
            args["tileformat"],
            cnorth,
            csouth,
            ceast,
            cwest,
            args["minlodpixels"],
            url,
            cz,
            cx,
            ytile,
        )

    s += """      </Document>
</kml>
    """
    return s


def scale_query_to_tile(dsquery, dstile, tiledriver, options, tilefilename=""):
    """Scales down query dataset to the tile dataset."""

    querysize = dsquery.RasterXSize
    tile_size = dstile.RasterXSize
    tilebands = dstile.RasterCount

    if options.resampling == "average":

        # Function: gdal.RegenerateOverview()
        for i in range(1, tilebands + 1):
            # Black border around NODATA
            res = gdal.RegenerateOverview(
                dsquery.GetRasterBand(i), dstile.GetRasterBand(i), "average"
            )
            if res != 0:
                exit_with_error(
                    "RegenerateOverview() failed on %s, error %d" % (tilefilename, res)
                )

    elif options.resampling == "antialias" and numpy_available:

        # Scaling by PIL (Python Imaging Library) - improved Lanczos
        array = numpy.zeros((querysize, querysize, tilebands), numpy.uint16)
        for i in range(tilebands):
            array[:, :, i] = gdalarray.BandReadAsArray(
                dsquery.GetRasterBand(i + 1), 0, 0, querysize, querysize
            )
        im = Image.fromarray(array, "RGBA")  # Always four bands
        im1 = im.resize((tile_size, tile_size), Image.ANTIALIAS)
        if os.path.exists(tilefilename):
            im0 = Image.open(tilefilename)
            im1 = Image.composite(im1, im0, im1)
        im1.save(tilefilename, tiledriver)

    else:

        if options.resampling == "near":
            gdal_resampling = gdal.GRA_NearestNeighbour

        elif options.resampling == "bilinear":
            gdal_resampling = gdal.GRA_Bilinear

        elif options.resampling == "cubic":
            gdal_resampling = gdal.GRA_Cubic

        elif options.resampling == "cubicspline":
            gdal_resampling = gdal.GRA_CubicSpline

        elif options.resampling == "lanczos":
            gdal_resampling = gdal.GRA_Lanczos

        elif options.resampling == "mode":
            gdal_resampling = gdal.GRA_Mode

        elif options.resampling == "max":
            gdal_resampling = gdal.GRA_Max

        elif options.resampling == "min":
            gdal_resampling = gdal.GRA_Min

        elif options.resampling == "med":
            gdal_resampling = gdal.GRA_Med

        elif options.resampling == "q1":
            gdal_resampling = gdal.GRA_Q1

        elif options.resampling == "q3":
            gdal_resampling = gdal.GRA_Q3

        # Other algorithms are implemented by gdal.ReprojectImage().
        dsquery.SetGeoTransform(
            (
                0.0,
                tile_size / float(querysize),
                0.0,
                0.0,
                0.0,
                tile_size / float(querysize),
            )
        )
        dstile.SetGeoTransform((0.0, 1.0, 0.0, 0.0, 0.0, 1.0))

        res = gdal.ReprojectImage(dsquery, dstile, None, None, gdal_resampling)
        if res != 0:
            exit_with_error(
                "ReprojectImage() failed on %s, error %d" % (tilefilename, res)
            )


def setup_no_data_values(input_dataset, options):
    """Extract the NODATA values from the dataset or use the passed arguments
    as override if any."""
    in_nodata = []
    if options.srcnodata:
        nds = list(map(float, options.srcnodata.split(",")))
        if len(nds) < input_dataset.RasterCount:
            in_nodata = (nds * input_dataset.RasterCount)[: input_dataset.RasterCount]
        else:
            in_nodata = nds
    else:
        for i in range(1, input_dataset.RasterCount + 1):
            band = input_dataset.GetRasterBand(i)
            raster_no_data = band.GetNoDataValue()
            if raster_no_data is not None:
                # Ignore nodata values that are not in the range of the band data type (see https://github.com/OSGeo/gdal/pull/2299)
                if band.DataType == gdal.GDT_Byte and (
                    raster_no_data != int(raster_no_data)
                    or raster_no_data < 0
                    or raster_no_data > 255
                ):
                    # We should possibly do similar check for other data types
                    in_nodata = []
                    break
                in_nodata.append(raster_no_data)

    if options.verbose:
        print("NODATA: %s" % in_nodata)

    return in_nodata


def setup_input_srs(input_dataset, options):
    """Determines and returns the Input Spatial Reference System (SRS) as an
    osr object and as a WKT representation.

    Uses in priority the one passed in the command line arguments. If
    None, tries to extract them from the input dataset
    """

    input_srs = None
    input_srs_wkt = None

    if options.s_srs:
        input_srs = osr.SpatialReference()
        input_srs.SetFromUserInput(options.s_srs)
        input_srs_wkt = input_srs.ExportToWkt()
    else:
        input_srs_wkt = input_dataset.GetProjection()
        if not input_srs_wkt and input_dataset.GetGCPCount() != 0:
            input_srs_wkt = input_dataset.GetGCPProjection()
        if input_srs_wkt:
            input_srs = osr.SpatialReference()
            input_srs.ImportFromWkt(input_srs_wkt)

    if input_srs is not None:
        input_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    return input_srs, input_srs_wkt


def setup_output_srs(input_srs, options):
    """Setup the desired SRS (based on options)"""
    output_srs = osr.SpatialReference()

    if options.profile == "mercator":
        output_srs.ImportFromEPSG(3857)
    elif options.profile == "geodetic":
        output_srs.ImportFromEPSG(4326)
    elif options.profile == "raster":
        output_srs = input_srs
    else:
        output_srs = tmsMap[options.profile].srs.Clone()

    if output_srs:
        output_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    return output_srs


def has_georeference(dataset):
    return (
        dataset.GetGeoTransform() != (0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
        or dataset.GetGCPCount() != 0
    )


def reproject_dataset(from_dataset, from_srs, to_srs, options=None):
    """Returns the input dataset in the expected "destination" SRS.

    If the dataset is already in the correct SRS, returns it unmodified
    """
    if not from_srs or not to_srs:
        raise GDALError("from and to SRS must be defined to reproject the dataset")

    if (from_srs.ExportToProj4() != to_srs.ExportToProj4()) or (
        from_dataset.GetGCPCount() != 0
    ):
        to_dataset = gdal.AutoCreateWarpedVRT(
            from_dataset, from_srs.ExportToWkt(), to_srs.ExportToWkt()
        )

        if options and options.verbose:
            print(
                "Warping of the raster by AutoCreateWarpedVRT (result saved into 'tiles.vrt')"
            )
            to_dataset.GetDriver().CreateCopy("tiles.vrt", to_dataset)

        return to_dataset
    else:
        return from_dataset


def add_gdal_warp_options_to_string(vrt_string, warp_options):
    if not warp_options:
        return vrt_string

    vrt_root = ElementTree.fromstring(vrt_string)
    options = vrt_root.find("GDALWarpOptions")

    if options is None:
        return vrt_string

    for key, value in warp_options.items():
        tb = ElementTree.TreeBuilder()
        tb.start("Option", {"name": key})
        tb.data(value)
        tb.end("Option")
        elem = tb.close()
        options.insert(0, elem)

    return ElementTree.tostring(vrt_root).decode()


def update_no_data_values(warped_vrt_dataset, nodata_values, options=None):
    """Takes an array of NODATA values and forces them on the WarpedVRT file
    dataset passed."""
    # TODO: gbataille - Seems that I forgot tests there
    assert nodata_values != []

    vrt_string = warped_vrt_dataset.GetMetadata("xml:VRT")[0]

    vrt_string = add_gdal_warp_options_to_string(
        vrt_string, {"INIT_DEST": "NO_DATA", "UNIFIED_SRC_NODATA": "YES"}
    )

    # TODO: gbataille - check the need for this replacement. Seems to work without
    #         # replace BandMapping tag for NODATA bands....
    #         for i in range(len(nodata_values)):
    #             s = s.replace(
    #                 '<BandMapping src="%i" dst="%i"/>' % ((i+1), (i+1)),
    #                 """
    # <BandMapping src="%i" dst="%i">
    # <SrcNoDataReal>%i</SrcNoDataReal>
    # <SrcNoDataImag>0</SrcNoDataImag>
    # <DstNoDataReal>%i</DstNoDataReal>
    # <DstNoDataImag>0</DstNoDataImag>
    # </BandMapping>
    #                 """ % ((i+1), (i+1), nodata_values[i], nodata_values[i]))

    corrected_dataset = gdal.Open(vrt_string)

    # set NODATA_VALUE metadata
    corrected_dataset.SetMetadataItem(
        "NODATA_VALUES", " ".join([str(i) for i in nodata_values])
    )

    if options and options.verbose:
        print("Modified warping result saved into 'tiles1.vrt'")

        with open("tiles1.vrt", "w") as f:
            f.write(corrected_dataset.GetMetadata("xml:VRT")[0])

    return corrected_dataset


def add_alpha_band_to_string_vrt(vrt_string):
    # TODO: gbataille - Old code speak of this being equivalent to gdalwarp -dstalpha
    # To be checked

    vrt_root = ElementTree.fromstring(vrt_string)

    index = 0
    nb_bands = 0
    for subelem in list(vrt_root):
        if subelem.tag == "VRTRasterBand":
            nb_bands += 1
            color_node = subelem.find("./ColorInterp")
            if color_node is not None and color_node.text == "Alpha":
                break
                raise Exception("Alpha band already present")
        else:
            if nb_bands:
                # This means that we are one element after the Band definitions
                break

        index += 1

    tb = ElementTree.TreeBuilder()
    # FIXME: This looks sus
    tb.start(
        "VRTRasterBand",
        {
            "dataType": "UInt16",
            "band": str(nb_bands + 1),
            "subClass": "VRTWarpedRasterBand",
        },
    )
    tb.start("ColorInterp", {})
    tb.data("Alpha")
    tb.end("ColorInterp")
    tb.end("VRTRasterBand")
    elem = tb.close()

    vrt_root.insert(index, elem)

    warp_options = vrt_root.find(".//GDALWarpOptions")
    tb = ElementTree.TreeBuilder()
    tb.start("DstAlphaBand", {})
    tb.data(str(nb_bands + 1))
    tb.end("DstAlphaBand")
    elem = tb.close()
    warp_options.append(elem)

    # TODO: gbataille - this is a GDALWarpOptions. Why put it in a specific place?
    tb = ElementTree.TreeBuilder()
    tb.start("Option", {"name": "INIT_DEST"})
    tb.data("0")
    tb.end("Option")
    elem = tb.close()
    warp_options.append(elem)

    return ElementTree.tostring(vrt_root).decode()


def update_alpha_value_for_non_alpha_inputs(warped_vrt_dataset, options=None):
    """Handles dataset with 1 or 3 bands, i.e. without alpha channel, in the
    case the nodata value has not been forced by options."""
    if warped_vrt_dataset.RasterCount in [1, 3]:

        vrt_string = warped_vrt_dataset.GetMetadata("xml:VRT")[0]

        vrt_string = add_alpha_band_to_string_vrt(vrt_string)

        warped_vrt_dataset = gdal.Open(vrt_string)

        if options and options.verbose:
            print("Modified -dstalpha warping result saved into 'tiles1.vrt'")

            with open("tiles1.vrt", "w") as f:
                f.write(warped_vrt_dataset.GetMetadata("xml:VRT")[0])

    return warped_vrt_dataset


def nb_data_bands(dataset):
    """Return the number of data (non-alpha) bands of a gdal dataset."""
    alphaband = dataset.GetRasterBand(1).GetMaskBand()
    if (
        (alphaband.GetMaskFlags() & gdal.GMF_ALPHA)
        or dataset.RasterCount == 4
        or dataset.RasterCount == 2
    ):
        return dataset.RasterCount - 1
    return dataset.RasterCount


def nb_alpha_bands(dataset):
    """Return the number of alpha bands of a gdal dataset."""
    alphaband = dataset.GetRasterBand(1).GetMaskBand()
    if (
        (alphaband.GetMaskFlags() & gdal.GMF_ALPHA)
        or dataset.RasterCount == 4
        or dataset.RasterCount == 2
    ):
        return 1
    return 0


def create_base_tile(tile_job_info, tile_detail):
    alphaBandsCount = tile_job_info.nb_alpha_bands
    dataBandsCount = tile_job_info.nb_data_bands
    output = tile_job_info.output_file_path
    tileext = tile_job_info.tile_extension
    tile_size = tile_job_info.tile_size
    options = tile_job_info.options

    tilebands = dataBandsCount + alphaBandsCount

    cached_ds = getattr(threadLocal, "cached_ds", None)
    if cached_ds and cached_ds.GetDescription() == tile_job_info.src_file:
        ds = cached_ds
    else:
        ds = gdal.Open(tile_job_info.src_file, gdal.GA_ReadOnly)
        threadLocal.cached_ds = ds

    mem_drv = gdal.GetDriverByName("MEM")
    out_drv = gdal.GetDriverByName(tile_job_info.tile_driver)
    # alphaband = ds.GetRasterBand(1).GetMaskBand()

    tx = tile_detail.tx
    ty = tile_detail.ty
    tz = tile_detail.tz
    rx = tile_detail.rx
    ry = tile_detail.ry
    rxsize = tile_detail.rxsize
    rysize = tile_detail.rysize
    wx = tile_detail.wx
    wy = tile_detail.wy
    wxsize = tile_detail.wxsize
    wysize = tile_detail.wysize
    querysize = tile_detail.querysize

    # Tile dataset in memory
    tilefilename = os.path.join(output, str(tz), str(tx), "%s.%s" % (ty, tileext))
    dstile = mem_drv.Create("", tile_size, tile_size, tilebands, gdal.GDT_UInt16)

    data = alpha = None

    if options.verbose:
        print(
            "\tReadRaster Extent: ", (rx, ry, rxsize, rysize), (wx, wy, wxsize, wysize)
        )

    # Query is in 'nearest neighbour' but can be bigger in then the tile_size
    # We scale down the query to the tile_size by supplied algorithm.

    if rxsize != 0 and rysize != 0 and wxsize != 0 and wysize != 0:
        # alpha = alphaband.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)

        # Detect totally transparent tile and skip its creation
        # if tile_job_info.exclude_transparent and len(alpha) == alpha.count('\x00'.encode('ascii')):
        #     return

        data = ds.ReadRaster(
            rx,
            ry,
            rxsize,
            rysize,
            wxsize,
            wysize,
            band_list=list(range(1, tilebands + 1)),
        )

    # The tile in memory is a transparent file by default. Write pixel values into it if
    # any
    if data:
        if tile_size == querysize:
            # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
            dstile.WriteRaster(
                wx, wy, wxsize, wysize, data, band_list=list(range(1, tilebands + 1))
            )
            # dstile.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[tilebands])

            # Note: For source drivers based on WaveLet compression (JPEG2000, ECW,
            # MrSID) the ReadRaster function returns high-quality raster (not ugly
            # nearest neighbour)
            # TODO: Use directly 'near' for WaveLet files
        else:
            print("Warning, seems to be resizing!")
            # Big ReadRaster query in memory scaled to the tile_size - all but 'near'
            # algo
            dsquery = mem_drv.Create(
                "", querysize, querysize, tilebands, gdal.GDT_UInt16
            )
            # TODO: fill the null value in case a tile without alpha is produced (now
            # only png tiles are supported)
            dsquery.WriteRaster(
                wx,
                wy,
                wxsize,
                wysize,
                data,
                band_list=list(range(1, tilebands + 1)),
            )
            dsquery.WriteRaster(
                wx, wy, wxsize, wysize, alpha, band_list=[tilebands + 1]
            )

            scale_query_to_tile(
                dsquery,
                dstile,
                tile_job_info.tile_driver,
                options,
                tilefilename=tilefilename,
            )
            del dsquery

    del data

    if options.resampling != "antialias":
        # Write a copy of tile to png/jpg
        out_drv.CreateCopy(tilefilename, dstile, strict=0)

    del dstile

    # Create a KML file for this tile.
    # if tile_job_info.kml:
    #     swne = get_tile_swne(tile_job_info, options)
    #     if swne is not None:
    #         kmlfilename = os.path.join(output, str(tz), str(tx), '%d.kml' % GDAL2Tiles.getYTile(ty, tz, options))
    #         if not options.resume or not os.path.exists(kmlfilename):
    #             with open(kmlfilename, 'wb') as f:
    #                 f.write(generate_kml(
    #                     tx, ty, tz, tile_job_info.tile_extension, tile_job_info.tile_size,
    #                     swne, tile_job_info.options
    #                 ).encode('utf-8'))


def create_overview_tiles(tile_job_info, output_folder, options):
    """Generation of the overview tiles (higher in the pyramid) based on
    existing tiles."""
    mem_driver = gdal.GetDriverByName("MEM")
    tile_driver = tile_job_info.tile_driver
    out_driver = gdal.GetDriverByName(tile_driver)

    tilebands = tile_job_info.nb_data_bands + tile_job_info.nb_alpha_bands

    # Usage of existing tiles: from 4 underlying tiles generate one as overview.

    tcount = 0
    for tz in range(tile_job_info.tmaxz - 1, tile_job_info.tminz - 1, -1):
        tminx, tminy, tmaxx, tmaxy = tile_job_info.tminmax[tz]
        tcount += (1 + abs(tmaxx - tminx)) * (1 + abs(tmaxy - tminy))

    ti = 0

    if tcount == 0:
        return

    if not options.quiet:
        print("Generating Overview Tiles:")

    progress_bar = ProgressBar(tcount)
    progress_bar.start()

    for tz in range(tile_job_info.tmaxz - 1, tile_job_info.tminz - 1, -1):
        tminx, tminy, tmaxx, tmaxy = tile_job_info.tminmax[tz]
        for ty in range(tmaxy, tminy - 1, -1):
            for tx in range(tminx, tmaxx + 1):

                ti += 1
                ytile = GDAL2Tiles.getYTile(ty, tz, options)
                tilefilename = os.path.join(
                    output_folder,
                    str(tz),
                    str(tx),
                    "%s.%s" % (ytile, tile_job_info.tile_extension),
                )

                if options.verbose:
                    print(ti, "/", tcount, tilefilename)

                if options.resume and os.path.exists(tilefilename):
                    if options.verbose:
                        print("Tile generation skipped because of --resume")
                    else:
                        progress_bar.log_progress()
                    continue

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(tilefilename)):
                    os.makedirs(os.path.dirname(tilefilename))

                dsquery = mem_driver.Create(
                    "",
                    2 * tile_job_info.tile_size,
                    2 * tile_job_info.tile_size,
                    tilebands,
                )
                # TODO: fill the null value
                dstile = mem_driver.Create(
                    "", tile_job_info.tile_size, tile_job_info.tile_size, tilebands
                )

                # TODO: Implement more clever walking on the tiles with cache functionality
                # probably walk should start with reading of four tiles from top left corner
                # Hilbert curve

                children = []
                # Read the tiles and write them to query window
                for y in range(2 * ty, 2 * ty + 2):
                    for x in range(2 * tx, 2 * tx + 2):
                        minx, miny, maxx, maxy = tile_job_info.tminmax[tz + 1]
                        if x >= minx and x <= maxx and y >= miny and y <= maxy:
                            ytile2 = GDAL2Tiles.getYTile(y, tz + 1, options)
                            base_tile_path = os.path.join(
                                output_folder,
                                str(tz + 1),
                                str(x),
                                "%s.%s" % (ytile2, tile_job_info.tile_extension),
                            )
                            if not os.path.isfile(base_tile_path):
                                continue

                            dsquerytile = gdal.Open(base_tile_path, gdal.GA_ReadOnly)

                            if x == 2 * tx:
                                tileposx = 0
                            else:
                                tileposx = tile_job_info.tile_size

                            if options.xyz and options.profile == "raster":
                                if y == 2 * ty:
                                    tileposy = 0
                                else:
                                    tileposy = tile_job_info.tile_size
                            else:
                                if y == 2 * ty:
                                    tileposy = tile_job_info.tile_size
                                else:
                                    tileposy = 0

                            dsquery.WriteRaster(
                                tileposx,
                                tileposy,
                                tile_job_info.tile_size,
                                tile_job_info.tile_size,
                                dsquerytile.ReadRaster(
                                    0,
                                    0,
                                    tile_job_info.tile_size,
                                    tile_job_info.tile_size,
                                ),
                                band_list=list(range(1, tilebands + 1)),
                            )
                            children.append([x, y, tz + 1])

                if children:
                    scale_query_to_tile(
                        dsquery, dstile, tile_driver, options, tilefilename=tilefilename
                    )
                    # Write a copy of tile to png/jpg
                    if options.resampling != "antialias":
                        # Write a copy of tile to png/jpg
                        out_driver.CreateCopy(tilefilename, dstile, strict=0)

                    if options.verbose:
                        print(
                            "\tbuild from zoom",
                            tz + 1,
                            " tiles:",
                            (2 * tx, 2 * ty),
                            (2 * tx + 1, 2 * ty),
                            (2 * tx, 2 * ty + 1),
                            (2 * tx + 1, 2 * ty + 1),
                        )

                    # Create a KML file for this tile.
                    if tile_job_info.kml:
                        swne = get_tile_swne(tile_job_info, options)
                        if swne is not None:
                            with open(
                                os.path.join(
                                    output_folder, "%d/%d/%d.kml" % (tz, tx, ytile)
                                ),
                                "wb",
                            ) as f:
                                f.write(
                                    generate_kml(
                                        tx,
                                        ty,
                                        tz,
                                        tile_job_info.tile_extension,
                                        tile_job_info.tile_size,
                                        swne,
                                        options,
                                        children,
                                    ).encode("utf-8")
                                )

                if not options.verbose and not options.quiet:
                    progress_bar.log_progress()


def optparse_init():
    """Prepare the option parser for input (argv)"""

    from optparse import OptionGroup, OptionParser

    usage = "Usage: %prog [options] input_file [output]"
    p = OptionParser(usage, version="%prog " + __version__)
    p.add_option(
        "-p",
        "--profile",
        dest="profile",
        type="choice",
        choices=profile_list,
        help=(
            "Tile cutting profile (%s) - default 'mercator' "
            "(Google Maps compatible)" % ",".join(profile_list)
        ),
    )
    p.add_option(
        "-r",
        "--resampling",
        dest="resampling",
        type="choice",
        choices=resampling_list,
        help="Resampling method (%s) - default 'average'" % ",".join(resampling_list),
    )
    p.add_option(
        "-s",
        "--s_srs",
        dest="s_srs",
        metavar="SRS",
        help="The spatial reference system used for the source input data",
    )
    p.add_option(
        "-z",
        "--zoom",
        dest="zoom",
        help="Zoom levels to render (format:'2-5' or '10').",
    )
    p.add_option(
        "-e",
        "--resume",
        dest="resume",
        action="store_true",
        help="Resume mode. Generate only missing files.",
    )
    p.add_option(
        "-a",
        "--srcnodata",
        dest="srcnodata",
        metavar="NODATA",
        help="Value in the input dataset considered as transparent",
    )
    p.add_option(
        "-d",
        "--tmscompatible",
        dest="tmscompatible",
        action="store_true",
        help=(
            "When using the geodetic profile, specifies the base resolution "
            "as 0.703125 or 2 tiles at zoom level 0."
        ),
    )
    p.add_option(
        "--xyz",
        action="store_true",
        dest="xyz",
        help="Use XYZ tile numbering (OSM Slippy Map tiles) instead of TMS",
    )
    p.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        help="Print status messages to stdout",
    )
    p.add_option(
        "-x",
        "--exclude",
        action="store_true",
        dest="exclude_transparent",
        help="Exclude transparent tiles from result tileset",
    )
    p.add_option(
        "-q",
        "--quiet",
        action="store_true",
        dest="quiet",
        help="Disable messages and status to stdout",
    )
    p.add_option(
        "--processes",
        dest="nb_processes",
        type="int",
        help="Number of processes to use for tiling",
    )
    p.add_option(
        "--tilesize",
        dest="tilesize",
        metavar="PIXELS",
        default=256,
        type="int",
        help="Width and height in pixel of a tile",
    )

    # KML options
    g = OptionGroup(
        p,
        "KML (Google Earth) options",
        "Options for generated Google Earth SuperOverlay metadata",
    )
    g.add_option(
        "-k",
        "--force-kml",
        dest="kml",
        action="store_true",
        help=(
            "Generate KML for Google Earth - default for 'geodetic' profile and "
            "'raster' in EPSG:4326. For a dataset with different projection use "
            "with caution!"
        ),
    )
    g.add_option(
        "-n",
        "--no-kml",
        dest="kml",
        action="store_false",
        help="Avoid automatic generation of KML files for EPSG:4326",
    )
    g.add_option(
        "-u",
        "--url",
        dest="url",
        help="URL address where the generated tiles are going to be published",
    )
    p.add_option_group(g)

    # HTML options
    g = OptionGroup(
        p, "Web viewer options", "Options for generated HTML viewers a la Google Maps"
    )
    g.add_option(
        "-w",
        "--webviewer",
        dest="webviewer",
        type="choice",
        choices=webviewer_list,
        help="Web viewer to generate (%s) - default 'all'" % ",".join(webviewer_list),
    )
    g.add_option("-t", "--title", dest="title", help="Title of the map")
    g.add_option("-c", "--copyright", dest="copyright", help="Copyright for the map")
    g.add_option(
        "-g",
        "--googlekey",
        dest="googlekey",
        help="Google Maps API key from http://code.google.com/apis/maps/signup.html",
    )
    g.add_option(
        "-b",
        "--bingkey",
        dest="bingkey",
        help="Bing Maps API key from https://www.bingmapsportal.com/",
    )
    p.add_option_group(g)

    # MapML options
    g = OptionGroup(p, "MapML options", "Options for generated MapML file")
    g.add_option(
        "--mapml-template",
        dest="mapml_template",
        action="store_true",
        help=(
            "Filename of a template mapml file where variables will "
            "be substituted. If not specified, the generic "
            "template_tiles.mapml file from GDAL data resources "
            "will be used"
        ),
    )
    p.add_option_group(g)

    p.set_defaults(
        verbose=False,
        profile="mercator",
        kml=False,
        url="",
        webviewer="all",
        copyright="",
        resampling="average",
        resume=False,
        googlekey="INSERT_YOUR_KEY_HERE",
        bingkey="INSERT_YOUR_KEY_HERE",
        processes=1,
    )

    return p


def process_args(argv):
    parser = optparse_init()
    options, args = parser.parse_args(args=argv)

    # Args should be either an input file OR an input file and an output folder
    if not args:
        exit_with_error(
            "You need to specify at least an input file as argument to the script"
        )
    if len(args) > 2:
        exit_with_error(
            "Processing of several input files is not supported.",
            "Please first use a tool like gdal_vrtmerge.py or gdal_merge.py on the "
            "files: gdal_vrtmerge.py -o merged.vrt %s" % " ".join(args),
        )

    input_file = args[0]
    if not os.path.isfile(input_file):
        exit_with_error(
            "The provided input file %s does not exist or is not a file" % input_file
        )

    if len(args) == 2:
        output_folder = args[1]
    else:
        # Directory with input filename without extension in actual directory
        output_folder = os.path.splitext(os.path.basename(input_file))[0]

    if options.webviewer == "mapml":
        options.xyz = True
        if options.profile == "geodetic":
            options.tmscompatible = True

    options = options_post_processing(options, input_file, output_folder)

    return input_file, output_folder, options


def options_post_processing(options, input_file, output_folder):
    if not options.title:
        options.title = os.path.basename(input_file)

    if options.url and not options.url.endswith("/"):
        options.url += "/"
    if options.url:
        out_path = output_folder
        if out_path.endswith("/"):
            out_path = out_path[:-1]
        options.url += os.path.basename(out_path) + "/"

    # Supported options
    if options.resampling == "antialias" and not numpy_available:
        exit_with_error(
            "'antialias' resampling algorithm is not available.",
            "Install PIL (Python Imaging Library) and numpy.",
        )

    try:
        os.path.basename(input_file).encode("ascii")
    except UnicodeEncodeError:
        full_ascii = False
    else:
        full_ascii = True

    # LC_CTYPE check
    if not full_ascii and "UTF-8" not in os.environ.get("LC_CTYPE", ""):
        if not options.quiet:
            print(
                "\nWARNING: "
                "You are running gdal2tiles.py with a LC_CTYPE environment variable that is "
                "not UTF-8 compatible, and your input file contains non-ascii characters. "
                "The generated sample googlemaps, openlayers or "
                "leaflet files might contain some invalid characters as a result\n"
            )

    # Output the results
    if options.verbose:
        print("Options:", options)
        print("Input:", input_file)
        print("Output:", output_folder)
        print("Cache: %s MB" % (gdal.GetCacheMax() / 1024 / 1024))
        print("")

    return options


class TileDetail(object):
    tx = 0
    ty = 0
    tz = 0
    rx = 0
    ry = 0
    rxsize = 0
    rysize = 0
    wx = 0
    wy = 0
    wxsize = 0
    wysize = 0
    querysize = 0

    def __init__(self, **kwargs):
        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __unicode__(self):
        return "TileDetail %s\n%s\n%s\n" % (self.tx, self.ty, self.tz)

    def __str__(self):
        return "TileDetail %s\n%s\n%s\n" % (self.tx, self.ty, self.tz)

    def __repr__(self):
        return "TileDetail %s\n%s\n%s\n" % (self.tx, self.ty, self.tz)


class TileJobInfo(object):
    """Plain object to hold tile job configuration for a dataset."""

    src_file = ""
    nb_data_bands = 0
    nb_alpha_bands = 0
    output_file_path = ""
    tile_extension = ""
    tile_size = 0
    tile_driver = None
    kml = False
    tminmax: List[int] = []
    tminz = 0
    tmaxz = 0
    in_srs_wkt = 0
    out_geo_trans: Tuple = tuple()
    ominy = 0
    is_epsg_4326 = False
    options = None
    exclude_transparent = False

    def __init__(self, **kwargs):
        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __unicode__(self):
        return "TileJobInfo %s\n" % (self.src_file)

    def __str__(self):
        return "TileJobInfo %s\n" % (self.src_file)

    def __repr__(self):
        return "TileJobInfo %s\n" % (self.src_file)


class Gdal2TilesError(Exception):
    pass


class GDAL2Tiles(object):
    def __init__(self, input_file, output_folder, options):
        """Constructor function - initialization"""
        self.out_drv = None
        self.mem_drv = None
        self.warped_input_dataset = None
        self.out_srs = None
        self.nativezoom = None
        self.tminmax = None
        self.tsize = None
        self.mercator = None
        self.geodetic = None
        self.alphaband = None
        self.dataBandsCount = None
        self.out_gt = None
        self.tileswne = None
        self.swne = None
        self.ominx = None
        self.omaxx = None
        self.omaxy = None
        self.ominy = None

        self.input_file = None
        self.output_folder = None

        self.isepsg4326 = None
        self.in_srs = None
        self.in_srs_wkt = None

        # Tile format
        self.tile_size = 256
        if options.tilesize:
            self.tile_size = options.tilesize
        self.tiledriver = "PNG"
        self.tileext = "png"
        self.tmp_dir = tempfile.mkdtemp()
        self.tmp_vrt_filename = os.path.join(self.tmp_dir, str(uuid4()) + ".vrt")

        # Should we read bigger window of the input raster and scale it down?
        # Note: Modified later by open_input()
        # Not for 'near' resampling
        # Not for Wavelet based drivers (JPEG2000, ECW, MrSID)
        # Not for 'raster' profile
        self.scaledquery = True
        # How big should be query window be for scaling down
        # Later on reset according the chosen resampling algorithm
        self.querysize = 4 * self.tile_size

        # Should we use Read on the input file for generating overview tiles?
        # Note: Modified later by open_input()
        # Otherwise the overview tiles are generated from existing underlying tiles
        self.overviewquery = False

        self.input_file = input_file
        self.output_folder = output_folder
        self.options = options

        if self.options.resampling == "near":
            self.querysize = self.tile_size

        elif self.options.resampling == "bilinear":
            self.querysize = self.tile_size * 2

        # User specified zoom levels
        self.tminz = None
        self.tmaxz = None
        if self.options.zoom:
            minmax = self.options.zoom.split("-", 1)
            minmax.extend([""])
            zoom_min, zoom_max = minmax[:2]
            self.tminz = int(zoom_min)
            if zoom_max:
                if int(zoom_max) < self.tminz:
                    raise Exception(
                        "max zoom (%d) less than min zoom (%d)"
                        % (int(zoom_max), self.tminz)
                    )
                self.tmaxz = int(zoom_max)
            else:
                self.tmaxz = int(zoom_min)

        # KML generation
        self.kml = self.options.kml

    # -------------------------------------------------------------------------
    def open_input(self):
        """Initialization of the input raster, reprojection if necessary."""
        gdal.AllRegister()

        self.out_drv = gdal.GetDriverByName(self.tiledriver)
        self.mem_drv = gdal.GetDriverByName("MEM")

        if not self.out_drv:
            raise Exception(
                "The '%s' driver was not found, is it available in this GDAL build?"
                % self.tiledriver
            )
        if not self.mem_drv:
            raise Exception(
                "The 'MEM' driver was not found, is it available in this GDAL build?"
            )

        # Open the input file

        if self.input_file:
            input_dataset = gdal.Open(self.input_file, gdal.GA_ReadOnly)
        else:
            raise Exception("No input file was specified")

        if self.options.verbose:
            print(
                "Input file:",
                "( %sP x %sL - %s bands)"
                % (
                    input_dataset.RasterXSize,
                    input_dataset.RasterYSize,
                    input_dataset.RasterCount,
                ),
            )

        if not input_dataset:
            # Note: GDAL prints the ERROR message too
            exit_with_error(
                "It is not possible to open the input file '%s'." % self.input_file
            )

        # Read metadata from the input file
        if input_dataset.RasterCount == 0:
            exit_with_error("Input file '%s' has no raster band" % self.input_file)

        if input_dataset.GetRasterBand(1).GetRasterColorTable():
            exit_with_error(
                "Please convert this file to RGB/RGBA and run gdal2tiles on the result.",
                "From paletted file you can create RGBA file (temp.vrt) by:\n"
                "gdal_translate -of vrt -expand rgba %s temp.vrt\n"
                "then run:\n"
                "gdal2tiles temp.vrt" % self.input_file,
            )

        # if input_dataset.GetRasterBand(1).DataType != gdal.GDT_Byte:
        #     exit_with_error(
        #         "Please convert this file to 8-bit and run gdal2tiles on the result.",
        #         "To scale pixel values you can use:\n"
        #         "gdal_translate -of VRT -ot Byte -scale %s temp.vrt\n"
        #         "then run:\n"
        #         "gdal2tiles temp.vrt" % self.input_file
        #     )

        # in_nodata = setup_no_data_values(input_dataset, self.options)

        if self.options.verbose:
            print(
                "Preprocessed file:",
                "( %sP x %sL - %s bands)"
                % (
                    input_dataset.RasterXSize,
                    input_dataset.RasterYSize,
                    input_dataset.RasterCount,
                ),
            )

        self.in_srs, self.in_srs_wkt = setup_input_srs(input_dataset, self.options)

        self.out_srs = setup_output_srs(self.in_srs, self.options)

        # If input and output reference systems are different, we reproject the input dataset into
        # the output reference system for easier manipulation

        self.warped_input_dataset = None

        if self.options.profile != "raster":

            if not self.in_srs:
                exit_with_error(
                    "Input file has unknown SRS.",
                    "Use --s_srs EPSG:xyz (or similar) to provide source reference system.",
                )

            if not has_georeference(input_dataset):
                exit_with_error(
                    "There is no georeference - neither affine transformation (worldfile) "
                    "nor GCPs. You can generate only 'raster' profile tiles.",
                    "Either gdal2tiles with parameter -p 'raster' or use another GIS "
                    "software for georeference e.g. gdal_transform -gcp / -a_ullr / -a_srs",
                )

            if (self.in_srs.ExportToProj4() != self.out_srs.ExportToProj4()) or (
                input_dataset.GetGCPCount() != 0
            ):
                self.warped_input_dataset = reproject_dataset(
                    input_dataset, self.in_srs, self.out_srs
                )

                # if in_nodata:
                #     self.warped_input_dataset = update_no_data_values(
                #         self.warped_input_dataset, in_nodata, options=self.options)
                # else:
                #     self.warped_input_dataset = update_alpha_value_for_non_alpha_inputs(
                #         self.warped_input_dataset, options=self.options)

            if self.warped_input_dataset and self.options.verbose:
                print(
                    "Projected file:",
                    "tiles.vrt",
                    "( %sP x %sL - %s bands)"
                    % (
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                        self.warped_input_dataset.RasterCount,
                    ),
                )

        if not self.warped_input_dataset:
            self.warped_input_dataset = input_dataset

        gdal.GetDriverByName("VRT").CreateCopy(
            self.tmp_vrt_filename, self.warped_input_dataset
        )

        # Get alpha band (either directly or from NODATA value)
        # self.alphaband = self.warped_input_dataset.GetRasterBand(1).GetMaskBand()
        self.dataBandsCount = nb_data_bands(self.warped_input_dataset)
        self.alphaBandsCount = nb_alpha_bands(self.warped_input_dataset)

        # KML test
        self.isepsg4326 = False
        srs4326 = osr.SpatialReference()
        srs4326.ImportFromEPSG(4326)
        srs4326.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if self.out_srs and srs4326.ExportToProj4() == self.out_srs.ExportToProj4():
            self.kml = True
            self.isepsg4326 = True
            if self.options.verbose:
                print("KML autotest OK!")

        # Read the georeference
        self.out_gt = self.warped_input_dataset.GetGeoTransform()

        # Test the size of the pixel

        # Report error in case rotation/skew is in geotransform (possible only in 'raster' profile)
        if (self.out_gt[2], self.out_gt[4]) != (0, 0):
            exit_with_error(
                "Georeference of the raster contains rotation or skew. "
                "Such raster is not supported. Please use gdalwarp first."
            )

        # Here we expect: pixel is square, no rotation on the raster

        # Output Bounds - coordinates in the output SRS
        self.ominx = self.out_gt[0]
        self.omaxx = (
            self.out_gt[0] + self.warped_input_dataset.RasterXSize * self.out_gt[1]
        )
        self.omaxy = self.out_gt[3]
        self.ominy = (
            self.out_gt[3] - self.warped_input_dataset.RasterYSize * self.out_gt[1]
        )
        # Note: maybe round(x, 14) to avoid the gdal_translate behavior, when 0 becomes -1e-15

        if self.options.verbose:
            print(
                "Bounds (output srs):",
                round(self.ominx, 13),
                self.ominy,
                self.omaxx,
                self.omaxy,
            )

        # Calculating ranges for tiles in different zoom levels
        if self.options.profile == "mercator":

            self.mercator = GlobalMercator(tile_size=self.tile_size)

            # Function which generates SWNE in LatLong for given tile
            self.tileswne = self.mercator.TileLatLonBounds

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = list(range(0, 32))
            for tz in range(0, 32):
                tminx, tminy = self.mercator.MetersToTile(self.ominx, self.ominy, tz)
                tmaxx, tmaxy = self.mercator.MetersToTile(self.omaxx, self.omaxy, tz)
                # crop tiles extending world limits (+-180,+-90)
                tminx, tminy = max(0, tminx), max(0, tminy)
                tmaxx, tmaxy = min(2 ** tz - 1, tmaxx), min(2 ** tz - 1, tmaxy)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

            # TODO: Maps crossing 180E (Alaska?)

            # Get the minimal zoom level (map covers area equivalent to one tile)
            if self.tminz is None:
                self.tminz = self.mercator.ZoomForPixelSize(
                    self.out_gt[1]
                    * max(
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                    )
                    / float(self.tile_size)
                )

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tmaxz is None:
                self.tmaxz = self.mercator.ZoomForPixelSize(self.out_gt[1])

            self.tminz = min(self.tminz, self.tmaxz)

            if self.options.verbose:
                print(
                    "Bounds (latlong):",
                    self.mercator.MetersToLatLon(self.ominx, self.ominy),
                    self.mercator.MetersToLatLon(self.omaxx, self.omaxy),
                )
                print("MinZoomLevel:", self.tminz)
                print(
                    "MaxZoomLevel:",
                    self.tmaxz,
                    "(",
                    self.mercator.Resolution(self.tmaxz),
                    ")",
                )

        elif self.options.profile == "geodetic":

            self.geodetic = GlobalGeodetic(
                self.options.tmscompatible, tile_size=self.tile_size
            )

            # Function which generates SWNE in LatLong for given tile
            self.tileswne = self.geodetic.TileLatLonBounds

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = list(range(0, 32))
            for tz in range(0, 32):
                tminx, tminy = self.geodetic.LonLatToTile(self.ominx, self.ominy, tz)
                tmaxx, tmaxy = self.geodetic.LonLatToTile(self.omaxx, self.omaxy, tz)
                # crop tiles extending world limits (+-180,+-90)
                tminx, tminy = max(0, tminx), max(0, tminy)
                tmaxx, tmaxy = min(2 ** (tz + 1) - 1, tmaxx), min(2 ** tz - 1, tmaxy)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

            # TODO: Maps crossing 180E (Alaska?)

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tminz is None:
                self.tminz = self.geodetic.ZoomForPixelSize(
                    self.out_gt[1]
                    * max(
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                    )
                    / float(self.tile_size)
                )

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tmaxz is None:
                self.tmaxz = self.geodetic.ZoomForPixelSize(self.out_gt[1])

            self.tminz = min(self.tminz, self.tmaxz)

            if self.options.verbose:
                print(
                    "Bounds (latlong):", self.ominx, self.ominy, self.omaxx, self.omaxy
                )

        elif self.options.profile == "raster":

            def log2(x):
                return math.log10(x) / math.log10(2)

            self.nativezoom = max(
                0,
                int(
                    max(
                        math.ceil(
                            log2(
                                self.warped_input_dataset.RasterXSize
                                / float(self.tile_size)
                            )
                        ),
                        math.ceil(
                            log2(
                                self.warped_input_dataset.RasterYSize
                                / float(self.tile_size)
                            )
                        ),
                    )
                ),
            )

            if self.options.verbose:
                print("Native zoom of the raster:", self.nativezoom)

            # Get the minimal zoom level (whole raster in one tile)
            if self.tminz is None:
                self.tminz = 0

            # Get the maximal zoom level (native resolution of the raster)
            if self.tmaxz is None:
                self.tmaxz = self.nativezoom
            elif self.tmaxz > self.nativezoom:
                print("Clamping max zoom level to %d" % self.nativezoom)
                self.tmaxz = self.nativezoom

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = list(range(0, self.tmaxz + 1))
            self.tsize = list(range(0, self.tmaxz + 1))
            for tz in range(0, self.tmaxz + 1):
                tsize = 2.0 ** (self.nativezoom - tz) * self.tile_size
                tminx, tminy = 0, 0
                tmaxx = (
                    int(math.ceil(self.warped_input_dataset.RasterXSize / tsize)) - 1
                )
                tmaxy = (
                    int(math.ceil(self.warped_input_dataset.RasterYSize / tsize)) - 1
                )
                self.tsize[tz] = math.ceil(tsize)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

            # Function which generates SWNE in LatLong for given tile
            if self.kml and self.in_srs_wkt:
                ct = osr.CoordinateTransformation(self.in_srs, srs4326)

                def rastertileswne(x, y, z):
                    pixelsizex = (
                        2 ** (self.tmaxz - z) * self.out_gt[1]
                    )  # X-pixel size in level
                    west = self.out_gt[0] + x * self.tile_size * pixelsizex
                    east = west + self.tile_size * pixelsizex
                    if self.options.xyz:
                        north = self.omaxy - y * self.tile_size * pixelsizex
                        south = north - self.tile_size * pixelsizex
                    else:
                        south = self.ominy + y * self.tile_size * pixelsizex
                        north = south + self.tile_size * pixelsizex
                    if not self.isepsg4326:
                        # Transformation to EPSG:4326 (WGS84 datum)
                        west, south = ct.TransformPoint(west, south)[:2]
                        east, north = ct.TransformPoint(east, north)[:2]
                    return south, west, north, east

                self.tileswne = rastertileswne
            else:
                self.tileswne = lambda x, y, z: (0, 0, 0, 0)  # noqa

        else:

            tms = tmsMap[self.options.profile]

            # Function which generates SWNE in LatLong for given tile
            self.tileswne = None  # not implemented

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = list(range(0, tms.level_count + 1))
            for tz in range(0, tms.level_count + 1):
                tminx, tminy = tms.GeorefCoordToTileCoord(
                    self.ominx, self.ominy, tz, self.tile_size
                )
                tmaxx, tmaxy = tms.GeorefCoordToTileCoord(
                    self.omaxx, self.omaxy, tz, self.tile_size
                )
                tminx, tminy = max(0, tminx), max(0, tminy)
                tmaxx, tmaxy = min(tms.matrix_width * 2 ** tz - 1, tmaxx), min(
                    tms.matrix_height * 2 ** tz - 1, tmaxy
                )
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

            # Get the minimal zoom level (map covers area equivalent to one tile)
            if self.tminz is None:
                self.tminz = tms.ZoomForPixelSize(
                    self.out_gt[1]
                    * max(
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                    )
                    / float(self.tile_size),
                    self.tile_size,
                )

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tmaxz is None:
                self.tmaxz = tms.ZoomForPixelSize(self.out_gt[1], self.tile_size)

            self.tminz = min(self.tminz, self.tmaxz)

            if self.options.verbose:
                print(
                    "Bounds (georef):", self.ominx, self.ominy, self.omaxx, self.omaxy
                )
                print("MinZoomLevel:", self.tminz)
                print("MaxZoomLevel:", self.tmaxz)

    def generate_metadata(self):
        """Generation of main metadata files and HTML viewers (metadata related
        to particular tiles are generated during the tile processing)."""

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        if self.options.profile == "mercator":

            south, west = self.mercator.MetersToLatLon(self.ominx, self.ominy)
            north, east = self.mercator.MetersToLatLon(self.omaxx, self.omaxy)
            south, west = max(-85.05112878, south), max(-180.0, west)
            north, east = min(85.05112878, north), min(180.0, east)
            self.swne = (south, west, north, east)

        elif self.options.profile == "geodetic":

            west, south = self.ominx, self.ominy
            east, north = self.omaxx, self.omaxy
            south, west = max(-90.0, south), max(-180.0, west)
            north, east = min(90.0, north), min(180.0, east)
            self.swne = (south, west, north, east)

        elif self.options.profile == "raster":

            west, south = self.ominx, self.ominy
            east, north = self.omaxx, self.omaxy

            self.swne = (south, west, north, east)

        else:
            self.swne = None

    def generate_base_tiles(self):
        """Generation of the base tiles (the lowest in the pyramid) directly
        from the input raster."""

        if not self.options.quiet:
            print("Generating Base Tiles:")

        if self.options.verbose:
            print("")
            print("Tiles generated from the max zoom level:")
            print("----------------------------------------")
            print("")

        # Set the bounds
        tminx, tminy, tmaxx, tmaxy = self.tminmax[self.tmaxz]

        ds = self.warped_input_dataset
        tilebands = self.dataBandsCount + self.alphaBandsCount
        querysize = self.querysize

        if self.options.verbose:
            print("dataBandsCount: ", self.dataBandsCount)
            print("tilebands: ", tilebands)

        tcount = (1 + abs(tmaxx - tminx)) * (1 + abs(tmaxy - tminy))
        ti = 0

        tile_details = []

        tz = self.tmaxz
        for ty in range(tmaxy, tminy - 1, -1):
            for tx in range(tminx, tmaxx + 1):

                ti += 1
                ytile = GDAL2Tiles.getYTile(ty, tz, self.options)
                tilefilename = os.path.join(
                    self.output_folder,
                    str(tz),
                    str(tx),
                    "%s.%s" % (ytile, self.tileext),
                )
                if self.options.verbose:
                    print(ti, "/", tcount, tilefilename)

                if self.options.resume and os.path.exists(tilefilename):
                    if self.options.verbose:
                        print("Tile generation skipped because of --resume")
                    continue

                # Create directories for the tile
                if not os.path.exists(os.path.dirname(tilefilename)):
                    os.makedirs(os.path.dirname(tilefilename))

                if self.options.profile == "mercator":
                    # Tile bounds in EPSG:3857
                    b = self.mercator.TileBounds(tx, ty, tz)
                elif self.options.profile == "geodetic":
                    b = self.geodetic.TileBounds(tx, ty, tz)
                elif self.options.profile != "raster":
                    b = tmsMap[self.options.profile].TileBounds(
                        tx, ty, tz, self.tile_size
                    )

                # Don't scale up by nearest neighbour, better change the querysize
                # to the native resolution (and return smaller query tile) for scaling

                if self.options.profile != "raster":
                    rb, wb = self.geo_query(ds, b[0], b[3], b[2], b[1])

                    # Pixel size in the raster covering query geo extent
                    nativesize = wb[0] + wb[2]
                    if self.options.verbose:
                        print("\tNative Extent (querysize", nativesize, "): ", rb, wb)

                    # Tile bounds in raster coordinates for ReadRaster query
                    rb, wb = self.geo_query(
                        ds, b[0], b[3], b[2], b[1], querysize=querysize
                    )

                    rx, ry, rxsize, rysize = rb
                    wx, wy, wxsize, wysize = wb

                else:  # 'raster' profile:

                    tsize = int(
                        self.tsize[tz]
                    )  # tile_size in raster coordinates for actual zoom
                    xsize = (
                        self.warped_input_dataset.RasterXSize
                    )  # size of the raster in pixels
                    ysize = self.warped_input_dataset.RasterYSize
                    querysize = self.tile_size

                    rx = tx * tsize
                    rxsize = 0
                    if tx == tmaxx:
                        rxsize = xsize % tsize
                    if rxsize == 0:
                        rxsize = tsize

                    ry = ty * tsize
                    rysize = 0
                    if ty == tmaxy:
                        rysize = ysize % tsize
                    if rysize == 0:
                        rysize = tsize

                    wx, wy = 0, 0
                    wxsize = int(rxsize / float(tsize) * self.tile_size)
                    wysize = int(rysize / float(tsize) * self.tile_size)

                    if not self.options.xyz:
                        ry = ysize - (ty * tsize) - rysize
                        if wysize != self.tile_size:
                            wy = self.tile_size - wysize

                # Read the source raster if anything is going inside the tile as per the computed
                # geo_query
                tile_details.append(
                    TileDetail(
                        tx=tx,
                        ty=ytile,
                        tz=tz,
                        rx=rx,
                        ry=ry,
                        rxsize=rxsize,
                        rysize=rysize,
                        wx=wx,
                        wy=wy,
                        wxsize=wxsize,
                        wysize=wysize,
                        querysize=querysize,
                    )
                )

        conf = TileJobInfo(
            src_file=self.tmp_vrt_filename,
            nb_alpha_bands=self.alphaBandsCount,
            nb_data_bands=self.dataBandsCount,
            output_file_path=self.output_folder,
            tile_extension=self.tileext,
            tile_driver=self.tiledriver,
            tile_size=self.tile_size,
            kml=self.kml,
            tminmax=self.tminmax,
            tminz=self.tminz,
            tmaxz=self.tmaxz,
            in_srs_wkt=self.in_srs_wkt,
            out_geo_trans=self.out_gt,
            ominy=self.ominy,
            is_epsg_4326=self.isepsg4326,
            options=self.options,
            exclude_transparent=self.options.exclude_transparent,
        )

        return conf, tile_details

    def geo_query(self, ds, ulx, uly, lrx, lry, querysize=0):
        """For given dataset and query in cartographic coordinates returns
        parameters for ReadRaster() in raster coordinates and x/y shifts (for
        border tiles). If the querysize is not given, the extent is returned in
        the native resolution of dataset ds.

        raises Gdal2TilesError if the dataset does not contain anything
        inside this geo_query
        """
        geotran = ds.GetGeoTransform()
        rx = int((ulx - geotran[0]) / geotran[1] + 0.001)
        ry = int((uly - geotran[3]) / geotran[5] + 0.001)
        rxsize = max(1, int((lrx - ulx) / geotran[1] + 0.5))
        rysize = max(1, int((lry - uly) / geotran[5] + 0.5))

        if not querysize:
            wxsize, wysize = rxsize, rysize
        else:
            wxsize, wysize = querysize, querysize

        # Coordinates should not go out of the bounds of the raster
        wx = 0
        if rx < 0:
            rxshift = abs(rx)
            wx = int(wxsize * (float(rxshift) / rxsize))
            wxsize = wxsize - wx
            rxsize = rxsize - int(rxsize * (float(rxshift) / rxsize))
            rx = 0
        if rx + rxsize > ds.RasterXSize:
            wxsize = int(wxsize * (float(ds.RasterXSize - rx) / rxsize))
            rxsize = ds.RasterXSize - rx

        wy = 0
        if ry < 0:
            ryshift = abs(ry)
            wy = int(wysize * (float(ryshift) / rysize))
            wysize = wysize - wy
            rysize = rysize - int(rysize * (float(ryshift) / rysize))
            ry = 0
        if ry + rysize > ds.RasterYSize:
            wysize = int(wysize * (float(ds.RasterYSize - ry) / rysize))
            rysize = ds.RasterYSize - ry

        return (rx, ry, rxsize, rysize), (wx, wy, wxsize, wysize)

    @staticmethod
    def getYTile(ty, tz, options):
        """Calculates the y-tile number based on whether XYZ or TMS (default)
        system is used.

        :param ty: The y-tile number
        :param tz: The z-tile number
        :return: The transformed y-tile number
        """
        if options.xyz and options.profile != "raster":
            if options.profile in ("mercator", "geodetic"):
                return (2 ** tz - 1) - ty  # Convert from TMS to XYZ numbering system

            tms = tmsMap[options.profile]
            return (
                tms.matrix_height * 2 ** tz - 1
            ) - ty  # Convert from TMS to XYZ numbering system

        return ty


def worker_tile_details(input_file, output_folder, options):
    gdal2tiles = GDAL2Tiles(input_file, output_folder, options)
    gdal2tiles.open_input()
    gdal2tiles.generate_metadata()
    tile_job_info, tile_details = gdal2tiles.generate_base_tiles()
    return tile_job_info, tile_details


class ProgressBar(object):
    def __init__(self, total_items):
        self.total_items = total_items
        self.nb_items_done = 0
        self.current_progress = 0
        self.STEP = 2.5

    def start(self):
        sys.stdout.write("0")

    def log_progress(self, nb_items=1):
        self.nb_items_done += nb_items
        progress = float(self.nb_items_done) / self.total_items * 100
        if progress >= self.current_progress + self.STEP:
            done = False
            while not done:
                if self.current_progress + self.STEP <= progress:
                    self.current_progress += self.STEP
                    if self.current_progress % 10 == 0:
                        sys.stdout.write(str(int(self.current_progress)))
                        if self.current_progress == 100:
                            sys.stdout.write("\n")
                    else:
                        sys.stdout.write(".")
                else:
                    done = True
        sys.stdout.flush()


def get_tile_swne(tile_job_info, options):
    if options.profile == "mercator":
        mercator = GlobalMercator()
        tile_swne = mercator.TileLatLonBounds
    elif options.profile == "geodetic":
        geodetic = GlobalGeodetic(options.tmscompatible)
        tile_swne = geodetic.TileLatLonBounds
    elif options.profile == "raster":
        srs4326 = osr.SpatialReference()
        srs4326.ImportFromEPSG(4326)
        srs4326.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        if tile_job_info.kml and tile_job_info.in_srs_wkt:
            in_srs = osr.SpatialReference()
            in_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            in_srs.ImportFromWkt(tile_job_info.in_srs_wkt)
            ct = osr.CoordinateTransformation(in_srs, srs4326)

            def rastertileswne(x, y, z):
                pixelsizex = (
                    2 ** (tile_job_info.tmaxz - z) * tile_job_info.out_geo_trans[1]
                )
                west = (
                    tile_job_info.out_geo_trans[0]
                    + x * tile_job_info.tile_size * pixelsizex
                )
                east = west + tile_job_info.tile_size * pixelsizex
                if options.xyz:
                    north = (
                        tile_job_info.out_geo_trans[3]
                        - y * tile_job_info.tile_size * pixelsizex
                    )
                    south = north - tile_job_info.tile_size * pixelsizex
                else:
                    south = (
                        tile_job_info.ominy + y * tile_job_info.tile_size * pixelsizex
                    )
                    north = south + tile_job_info.tile_size * pixelsizex
                if not tile_job_info.is_epsg_4326:
                    # Transformation to EPSG:4326 (WGS84 datum)
                    west, south = ct.TransformPoint(west, south)[:2]
                    east, north = ct.TransformPoint(east, north)[:2]
                return south, west, north, east

            tile_swne = rastertileswne
        else:
            tile_swne = lambda x, y, z: (0, 0, 0, 0)  # noqa
    else:
        tile_swne = None

    return tile_swne


def single_threaded_tiling(input_file, output_folder, options):
    """Keep a single threaded version that stays clear of multiprocessing, for
    platforms that would not support it."""
    if options.verbose:
        print("Begin tiles details calc")
    conf, tile_details = worker_tile_details(input_file, output_folder, options)

    if options.verbose:
        print("Tiles details calc complete.")

    if not options.verbose and not options.quiet:
        progress_bar = ProgressBar(len(tile_details))
        progress_bar.start()

    for tile_detail in tile_details:
        create_base_tile(conf, tile_detail)

        if not options.verbose and not options.quiet:
            progress_bar.log_progress()

    if getattr(threadLocal, "cached_ds", None):
        del threadLocal.cached_ds

    create_overview_tiles(conf, output_folder, options)

    shutil.rmtree(os.path.dirname(conf.src_file))


def multi_threaded_tiling(input_file, output_folder, options):
    nb_processes = options.nb_processes or 1

    # Make sure that all processes do not consume more than `gdal.GetCacheMax()`
    gdal_cache_max = gdal.GetCacheMax()
    gdal_cache_max_per_process = max(
        1024 * 1024, math.floor(gdal_cache_max / nb_processes)
    )
    set_cache_max(gdal_cache_max_per_process)

    pool = Pool(processes=nb_processes)

    if options.verbose:
        print("Begin tiles details calc")

    conf, tile_details = worker_tile_details(input_file, output_folder, options)

    if options.verbose:
        print("Tiles details calc complete.")

    if not options.verbose and not options.quiet:
        progress_bar = ProgressBar(len(tile_details))
        progress_bar.start()

    # TODO: gbataille - check the confs for which each element is an array... one useless level?
    # TODO: gbataille - assign an ID to each job for print in verbose mode "ReadRaster Extent ..."
    for _ in pool.imap_unordered(
        partial(create_base_tile, conf), tile_details, chunksize=128
    ):
        if not options.verbose and not options.quiet:
            progress_bar.log_progress()

    pool.close()
    pool.join()  # Jobs finished

    # Set the maximum cache back to the original value
    set_cache_max(gdal_cache_max)

    create_overview_tiles(conf, output_folder, options)

    shutil.rmtree(os.path.dirname(conf.src_file))


def main(argv):
    # TODO: gbataille - use mkdtemp to work in a temp directory
    # TODO: gbataille - debug intermediate tiles.vrt not produced anymore?
    # TODO: gbataille - Refactor generate overview tiles to not depend on self variables

    # For multiprocessing, we need to propagate the configuration options to
    # the environment, so that forked processes can inherit them.
    for i in range(len(argv)):
        if argv[i] == "--config" and i + 2 < len(argv):
            os.environ[argv[i + 1]] = argv[i + 2]

    argv = gdal.GeneralCmdLineProcessor(argv)
    if argv is None:
        return
    input_file, output_folder, options = process_args(argv[1:])
    nb_processes = options.nb_processes or 1

    if nb_processes == 1:
        single_threaded_tiling(input_file, output_folder, options)
    else:
        multi_threaded_tiling(input_file, output_folder, options)


# vim: set tabstop=4 shiftwidth=4 expandtab:

if __name__ == "__main__":
    sys.exit(main(sys.argv))
