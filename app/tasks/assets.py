from typing import List
from app.models.pydantic.source import SourceType


def seed_source_assets(source_type: str, source_uri: List[str]) -> None:
    # create default asset for version (in database)
    # Version status = pending

    # Schedule batch job queues depending on source type
    if source_type == SourceType.vector:
        _vector_source_asset(source_type, source_uri)
    elif source_type == SourceType.table:
        _table_source_asset(source_type, source_uri)
    elif source_type == SourceType.raster:
        _raster_source_asset(source_type, source_uri)
    else:
        raise ValueError(f"Unsupported asset source type {source_type})")

    # Batch job would log to asset history

    # Monitor job queue to make sure all job terminate and once done, set version status to saved and register newly created asset with version
    # if job failed, set version status to failed with message "Default asset failed"


def _vector_source_asset(source_type: str, source_uri: List[str]):
    # check if input data are in a readable format (using ogrinfo)
    # import data using ogr2ogr

    # update geometry storage format
    # repair geometries
    # reproject geometries
    # calculate gestore items
    # create indicies
    # link with geostore
    pass


def _table_source_asset(source_type: str, source_uri: List[str]):
    # check if input data are in a readable format (must be csv or tsv file)
    # Create table
    #   either use provided schema or guess schema using csvkit
    # Create partitions if specified
    # upload data using pgsql COPY
    # create indicies if specified
    pass


def _raster_source_asset(source_type: str, source_uri: List[str]):
    pass
