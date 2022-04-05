from typing import Any, Dict, List, Optional

from pydantic import validator

from ...settings.globals import (
    AURORA_JOB_QUEUE,
    DATA_LAKE_JOB_QUEUE,
    DEFAULT_JOB_DURATION,
    GDAL_PYTHON_JOB_DEFINITION,
    MAX_CORES,
    MAX_MEM,
    PIXETL_CORES,
    PIXETL_JOB_DEFINITION,
    PIXETL_JOB_QUEUE,
    PIXETL_MAX_MEM,
    POSTGRESQL_CLIENT_JOB_DEFINITION,
    TILE_CACHE_JOB_DEFINITION,
    TILE_CACHE_JOB_QUEUE,
)
from .base import StrictBaseModel


class Job(StrictBaseModel):
    dataset: str  # used for tagging resources
    job_name: str
    job_queue: str
    job_definition: str
    command: List[str]
    environment: List[Dict[str, str]] = []
    vcpus: int
    memory: int
    attempts: int
    attempt_duration_seconds: int
    num_processes: Optional[int] = None
    parents: Optional[List[str]] = None
    # somehow mypy doesn't like the type when declared here?
    callback: Any  # Callable[[UUID, ChangeLog], Coroutine[Any, Any, Awaitable[None]]]

    @validator("environment", pre=True, always=True)
    def update_environment(cls, v, *, values, **kwargs):
        v = cls._update_environment(v, "CORES", values.get("vcpus"))
        v = cls._update_environment(v, "MAX_MEM", values.get("memory"))
        v = cls._update_environment(v, "NUM_PROCESSES", values.get("num_processes"))
        return v

    @validator("vcpus", pre=True, always=True, allow_reuse=True)
    def update_max_cores(cls, v, *, values, **kwargs):
        cls.environment = cls._update_environment(values["environment"], "CORES", v)
        return v

    @validator("num_processes", pre=True, always=True, allow_reuse=True)
    def update_num_processes(cls, v, *, values, **kwargs):
        cls.environment = cls._update_environment(
            values["environment"], "NUM_PROCESSES", v
        )
        return v

    @validator("memory", pre=True, always=True)
    def update_max_mem(cls, v, *, values, **kwargs):
        cls.environment = cls._update_environment(values["environment"], "MAX_MEM", v)
        return v

    @staticmethod
    def _update_environment(env: List[Dict[str, str]], name: str, value: Optional[str]):
        if value:
            found = False
            for i, item in enumerate(env):
                if item["name"] == name:
                    env[i]["value"] = str(value)
                    found = True
                    break
            if not found:
                env.append({"name": name, "value": str(value)})

        return env


class PostgresqlClientJob(Job):
    """Use for simple write operations to PostgreSQL."""

    job_queue = AURORA_JOB_QUEUE
    job_definition = POSTGRESQL_CLIENT_JOB_DEFINITION
    vcpus = 1
    memory = 1500
    attempts = 1
    attempt_duration_seconds = DEFAULT_JOB_DURATION


class GdalPythonImportJob(Job):
    """Use for write operations to PostgreSQL which require GDAL/ Ogr2Ogr
    drivers."""

    job_queue = AURORA_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 2500
    attempts = 1
    attempt_duration_seconds = DEFAULT_JOB_DURATION


class GdalPythonExportJob(Job):
    """Use for export operations from PostgreSQL to S3 data lake which require
    GDAL/ Ogr2Ogr drivers."""

    job_queue = DATA_LAKE_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = 1
    memory = 15000
    attempts = 1
    attempt_duration_seconds = DEFAULT_JOB_DURATION


class TileCacheJob(Job):
    """Use for generating Vector Tile Cache using TippeCanoe."""

    job_queue = TILE_CACHE_JOB_QUEUE
    job_definition = TILE_CACHE_JOB_DEFINITION
    vcpus = max(int(MAX_CORES / 2), 1)
    num_processes = max(int(MAX_CORES / 3), 1)
    memory = max(int(MAX_MEM / 2), 1)
    attempts = 4
    attempt_duration_seconds = int(DEFAULT_JOB_DURATION * 1.5)


class PixETLJob(Job):
    """Use for raster transformations using PixETL."""

    job_queue = PIXETL_JOB_QUEUE
    job_definition = PIXETL_JOB_DEFINITION
    vcpus = MAX_CORES
    memory = MAX_MEM
    num_processes = max(int(MAX_CORES * 2 / 3), 1)
    attempts = 10
    attempt_duration_seconds = int(DEFAULT_JOB_DURATION * 1.5)


class GDALDEMJob(Job):
    """Use for applying color maps to raster tiles with gdaldem."""

    job_queue = PIXETL_JOB_QUEUE
    job_definition = PIXETL_JOB_DEFINITION
    vcpus = PIXETL_CORES
    memory = PIXETL_MAX_MEM
    num_processes = max(int(PIXETL_CORES / 2), 1)
    attempts = 10
    attempt_duration_seconds = int(DEFAULT_JOB_DURATION * 1.5)


class GDAL2TilesJob(Job):
    """Use for generating a raster tile cache from web-mercator tiles."""

    job_queue = DATA_LAKE_JOB_QUEUE
    job_definition = GDAL_PYTHON_JOB_DEFINITION
    vcpus = MAX_CORES
    memory = MAX_MEM
    num_processes = max(int(MAX_CORES / 2), 1)
    attempts = 10
    attempt_duration_seconds = int(DEFAULT_JOB_DURATION * 1.5)
