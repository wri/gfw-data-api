from typing import List


def seed_source_assets(source_type: str, source_uri: List[str]) -> None:
    pass


def _vector_source_asset():
    # check if input data are in a readable format (using ogrinfo)
    # import data using ogr2ogr

    # update geometry storage format
    # repair geometries
    # reproject geometries
    # calculate gestore items
    # create indicies
    # link with geostore
    pass


def _table_source_asset():
    # check if input data are in a readable format (must be csv or tsv file)
    # Create table
    #   either use provided schema or guess schema using csvkit
    # Create partitions if specified
    # upload data using pgsql COPY
    # create indicies if specified
    pass


def _raster_source_asset():
    pass