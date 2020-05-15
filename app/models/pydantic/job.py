from typing import List, Optional, Dict

from pydantic import BaseModel

from ...settings.globals import (
    AURORA_JOB_QUEUE,
    DATA_LAKE_JOB_QUEUE,
    GDAL_PYTHON_JOB_DEFINITION,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    TILE_CACHE_JOB_QUEUE,
    TILE_CACHE_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    PIXETL_JOB_DEFINITION,
)


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


class PostgresqlClientJob(Job):
    job_queue = AURORA_JOB_QUEUE
    job_definition = POSTGRESQL_CLIENT_JOB_DEFINITION
    vcpus = 1
    memory = 500
    attempts = 1
    attempt_duration_seconds = 7500


class GdalPythonImportJob(Job):
    job_queue = AURORA_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 2500
    attempts = 1
    attempt_duration_seconds = 7500


class GdalPythonExportJob(Job):
    job_queue = DATA_LAKE_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 2500
    attempts = 1
    attempt_duration_seconds = 7500


class TileCacheJob(Job):
    job_queue = TILE_CACHE_JOB_QUEUE
    job_definition = TILE_CACHE_JOB_DEFINITION
    vcpus = 48
    memory = 96000
    attempts = 1
    attempt_duration_seconds = 3600


class PixETLJob(Job):
    job_queue = PIXETL_JOB_QUEUE
    job_definition = PIXETL_JOB_DEFINITION
    vcpus = 48
    memory = 350000
    attempts = 2
    attempt_duration_seconds = 9600
