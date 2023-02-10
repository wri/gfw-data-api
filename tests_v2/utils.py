import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import httpx
from _pytest.monkeypatch import MonkeyPatch

from app.application import ContextEngine
from app.models.pydantic.extent import Extent
from app.routes.datasets import versions
from app.tasks import batch, delete_assets
from app.tasks.raster_tile_set_assets import raster_tile_set_assets
from tests_v2.fixtures.creation_options.versions import RASTER_CREATION_OPTIONS


class BatchJobMock:
    def __init__(self, job_desc: Sequence[Dict[str, Any]] = tuple()):
        self.jobs: List[uuid.UUID] = list()
        self.job_descriptions = job_desc

    def describe_jobs(self, *, jobs: List[str]) -> Dict[str, Any]:
        return {
            "jobs": [desc for desc in self.job_descriptions if desc["jobId"] in jobs]
        }

    def submit_batch_job(self, *args, **kwargs) -> uuid.UUID:
        job_id = uuid.uuid4()
        self.jobs.append(job_id)
        return job_id


async def get_user_mocked() -> Tuple[str, str]:
    return "userid_123", "USER"


async def get_admin_mocked() -> Tuple[str, str]:
    return "adminid_123", "ADMIN"


async def get_api_key_mocked() -> Tuple[Optional[str], Optional[str]]:
    return str(uuid.uuid4()), "localhost"


async def get_extent_mocked(asset_id: str) -> Optional[Extent]:
    return None


async def invoke_lambda_mocked(
    function_name: str, params: Dict[str, Any]
) -> httpx.Response:
    return httpx.Response(200, json={"status": "success", "data": []})


def void_function(*args, **kwargs) -> None:
    return


async def void_coroutine(*args, **kwargs) -> None:
    return


def bool_function_closure(value: bool, with_args=True) -> Callable:
    def bool_function(*args, **kwargs) -> bool:
        return value

    def simple_bool_function() -> bool:
        return value

    if with_args:
        return bool_function
    else:
        return simple_bool_function


def int_function_closure(value: int) -> Callable:
    def int_function(*args, **kwargs) -> int:
        return value

    return int_function


def dict_function_closure(value: Dict) -> Callable:
    def dict_function(*args, **kwargs) -> Dict:
        return value

    return dict_function


async def _create_vector_source_assets(dataset_name, version_name):
    # TODO: we currently only do the bare minimum here.
    #  still need to add gfw columns
    #  and check back in all task so that asset and versions are correctly set to saved
    from app.application import db

    with open(f"{os.path.dirname(__file__)}/fixtures/geojson/test.geojson") as src:
        geojson = json.load(src)

    async with ContextEngine("WRITE"):
        await db.all(
            f"""CREATE TABLE "{dataset_name}"."{version_name}" (fid integer, geom geometry);"""
        )
        await db.all(
            f"""INSERT INTO "{dataset_name}"."{version_name}" (fid, geom) SELECT 1,  ST_GeomFromGeoJSON('{json.dumps(geojson["features"][0]["geometry"])}');"""
        )


@asynccontextmanager
async def custom_raster_version(
    async_client: httpx.AsyncClient,
    dataset_name: str,
    monkeypatch: MonkeyPatch,
    **kwarg_creation_options,
):
    version_name: str = "v1"

    # Patch all functions which reach out to external services
    # Basically we're leaving out everything but the DB entries being created
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "creation_options": {
                **RASTER_CREATION_OPTIONS,
                **kwarg_creation_options,
            }
        },
    )

    await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}", json={"is_latest": True}
    )

    try:
        yield version_name
    finally:
        pass
