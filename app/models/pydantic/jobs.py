from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from ...settings.globals import (
    AURORA_JOB_QUEUE,
    DATA_LAKE_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    PIXETL_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    TILE_CACHE_JOB_DEFINITION,
    TILE_CACHE_JOB_QUEUE,
)
from .change_log import ChangeLog


class Job(BaseModel):
    job_name: str
    job_queue: str
    job_definition: str
    command: List[str]
    environment: List[Dict[str, str]] = []
    vcpus: int
    memory: int
    attempts: int
    attempt_duration_seconds: int
    parents: Optional[List[str]] = None
    # somehow mypy doesn't like the type when declared here?
    callback: Any  # Callable[[UUID, ChangeLog], Coroutine[Any, Any, Awaitable[None]]]


class PostgresqlClientJob(Job):
    """Use for simple write operations to PostgreSQL."""

    job_queue = AURORA_JOB_QUEUE
    job_definition = POSTGRESQL_CLIENT_JOB_DEFINITION
    vcpus = 1
    memory = 1500
    attempts = 1
    attempt_duration_seconds = 7500


class GdalPythonImportJob(Job):
    """Use for write operations to PostgreSQL which require GDAL/ Ogr2Ogr
    drivers."""

    job_queue = AURORA_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 2500
    attempts = 1
    attempt_duration_seconds = 7500


class GdalPythonExportJob(Job):
    """Use for export operations from PostgreSQL to S3 data lake which require
    GDAL/ Ogr2Ogr drivers."""

    job_queue = DATA_LAKE_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 2500
    attempts = 1
    attempt_duration_seconds = 7500


class TileCacheJob(Job):
    """Use for generating Vector Tile Cache using TippeCanoe."""

    job_queue = TILE_CACHE_JOB_QUEUE
    job_definition = TILE_CACHE_JOB_DEFINITION
    vcpus = 48
    memory = 96000
    attempts = 1
    attempt_duration_seconds = 3600


class PixETLJob(Job):
    """Use for raster transformations using PixETL."""

    job_queue = PIXETL_JOB_QUEUE
    job_definition = PIXETL_JOB_DEFINITION
    vcpus = 48
    memory = 350000
    attempts = 2
    attempt_duration_seconds = 9600
