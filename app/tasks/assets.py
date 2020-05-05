from typing import List
from app.models.pydantic.source import SourceType
from app.models.pydantic.job import Job
import os

JOB_QUEUE = os.environ["JOB_QUEUE"]
JOB_DEFINITION = os.environ["JOB_DEFINITION"]


def seed_source_assets(source_type: str, source_uri: List[str]) -> None:
    # create default asset for version (in database)
    # Version status = pending

    # Schedule batch job queues depending on source type
    if source_type == SourceType.vector:
        _vector_source_asset(source_type, source_uri)
    elif source_type == SourceType.tabular:
        _table_source_asset(source_type, source_uri)
    elif source_type == SourceType.raster:
        _raster_source_asset(source_type, source_uri)
    else:
        raise ValueError(f"Unsupported asset source type {source_type})")

    # Batch job would log to asset history

    # Monitor job queue to make sure all job terminate and once done, set version status to saved and register newly created asset with version
    # if job failed, set version status to failed with message "Default asset failed"


def _vector_source_asset():
    # check if input data are in a readable format (using ogrinfo)
    check_input = Job("check_input", ["python", "python/check_file_types.py", "get_vector_source_driver"])

    # import data using ogr2ogr
    import_env = {
        "VECTOR_SOURCE": "",
        "VECTOR_SOURCE_LAYER": "",
        "DATASET": "",
        "VERSION": "",
        "GEOMTETRY_NAME": "",
        "FID_NAME": "",
    }
    import_data = Job("check_input", "./scripts/load_vector_data.sh", parents=[check_input], environment=import_env)


    # update geometry storage format

    # repair geometries

    # reproject geometries

    # calculate gestore items

    # create indicies

    # link with geostore
    pass


def _table_source_asset(options):
    jobs = []

    # check if input data are in a readable format (must be csv or tsv file)
    check_input = Job("check_input", JOB_QUEUE, JOB_DEFINITION, ["python", "python/check_file_types.py", "get_csv_dialect"])

    # Create table
    #   either use provided schema or guess schema using csvkit
    create_table_env = {"S3URI": "", "TABLE": "", "DELIMITER": ""}
    create_table = Job("create_table", JOB_QUEUE, JOB_DEFINITION, ["python", "./scripts/load_tabular_data.sh"], parents=[check_input])

    # Create partitions if specified
    if "partitions" in options:
        pass

    # upload data using pgsql COPY
    # TODO: is this a separate script?

    # create indicies if specified
    if "indices" in options:
        pass


def _raster_source_asset():
    pass
